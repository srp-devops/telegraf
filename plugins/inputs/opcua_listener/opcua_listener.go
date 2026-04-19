//go:generate ../../../tools/readme_config_includer/generator
package opcua_listener

import (
	"context"
	_ "embed"
	"fmt"
	"time"

	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/config"
	"github.com/influxdata/telegraf/plugins/common/opcua"
	"github.com/influxdata/telegraf/plugins/common/opcua/input"
	"github.com/influxdata/telegraf/plugins/inputs"
)

type OpcUaListener struct {
	subscribeClientConfig
	client *subscribeClient
	Log    telegraf.Logger `toml:"-"`

	goroutineCtx    context.Context
	goroutineCancel context.CancelFunc

	nextReconnectTime time.Time
	reconnectWait     time.Duration
}

//go:embed sample.conf
var sampleConfig string

func (*OpcUaListener) SampleConfig() string {
	return sampleConfig
}

func (o *OpcUaListener) Init() (err error) {
	switch o.ConnectFailBehavior {
	case "":
		o.ConnectFailBehavior = "error"
	case "error", "ignore", "retry":
		// Do nothing as these are valid
	default:
		return fmt.Errorf("unknown setting %q for 'connect_fail_behavior'", o.ConnectFailBehavior)
	}
	o.client, err = o.subscribeClientConfig.createSubscribeClient(o.Log)
	return err
}

func (o *OpcUaListener) Start(acc telegraf.Accumulator) error {
	return o.connectWithBackoff(acc)
}

func (o *OpcUaListener) Gather(acc telegraf.Accumulator) error {
	// If the subscription channel closed unexpectedly (e.g. OPC-UA server timeout),
	// reconnect regardless of what State() reports — State() can remain "Connected"
	// even after the server terminates the session, causing silent data loss.
	// print client state
	o.Log.Debugf("Gather: client state=%v", o.client.State())
	if o.client != nil && o.client.SubDropped() {
		o.Log.Warnf("Gather: subscription channel was closed by server (state=%v); reconnecting", o.client.State())
		o.client.ClearSubDropped()
		return o.connectWithBackoff(acc)
	}

	// Staleness watchdog: if connected but no data received for longer than
	// the configured threshold, force a full reconnect. This catches zombie
	// states where gopcua's internal auto-reconnect restored the session but
	// lost all subscriptions (see gopcua/opcua#854).
	if o.client != nil && o.client.State() == opcua.Connected {
		timeout := time.Duration(o.client.Config.StaleDataReconnectTimeout)
		stale := o.client.SecondsSinceLastData()
		o.Log.Debugf("Gather: staleness check: state=%v, secondsSinceLastData=%d, threshold=%v, subDropped=%v",
			o.client.State(), stale, timeout, o.client.SubDropped())
		if timeout > 0 && stale > 0 && stale > int64(timeout.Seconds()) {
			o.Log.Warnf("Gather: no data received for %d seconds despite Connected state; forcing full reconnect (threshold: %v)", stale, timeout)
			return o.connectWithBackoff(acc)
		}
	}

	if o.client == nil || o.client.State() == opcua.Connected || o.client.State() == opcua.Reconnecting || o.subscribeClientConfig.ConnectFailBehavior == "ignore" {
		if o.client != nil {
			o.client.RetryMissingItems(context.Background())
		}
		return nil
	}
	o.Log.Debugf("Gather: client in unexpected state=%v; attempting reconnect", o.client.State())
	return o.connectWithBackoff(acc)
}

func (o *OpcUaListener) connectWithBackoff(acc telegraf.Accumulator) error {
	if time.Now().Before(o.nextReconnectTime) {
		o.Log.Debugf("connectWithBackoff: waiting for backoff timer (%v remaining)", time.Until(o.nextReconnectTime).Round(time.Second))
		return nil
	}

	o.Log.Debugf("connectWithBackoff: attempting connection (state=%v)", o.client.State())
	err := o.connect(acc)
	if err != nil {
		if o.reconnectWait == 0 {
			o.reconnectWait = 30 * time.Second
		} else {
			o.reconnectWait *= 2
		}
		if o.reconnectWait > 5*time.Minute {
			o.reconnectWait = 5 * time.Minute
		}
		o.nextReconnectTime = time.Now().Add(o.reconnectWait)
		o.Log.Errorf("connectWithBackoff: reconnect failed: %v. Backing off for %v (next attempt at %v)", err, o.reconnectWait, o.nextReconnectTime.Format(time.RFC3339))

		if o.subscribeClientConfig.ConnectFailBehavior == "error" {
			return err
		}
		return nil
	}

	o.Log.Debugf("connectWithBackoff: connection successful, resetting backoff (state=%v)", o.client.State())
	o.reconnectWait = 0
	o.nextReconnectTime = time.Time{}
	return nil
}

func (o *OpcUaListener) Stop() {
	if o.goroutineCancel != nil {
		o.goroutineCancel()
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	select {
	case <-o.client.stop(ctx):
		o.Log.Infof("Unsubscribed OPC UA successfully")
	case <-ctx.Done(): // Timeout context
		o.Log.Warn("Timeout while stopping OPC UA subscription")
	}
	cancel()
}

func (o *OpcUaListener) connect(acc telegraf.Accumulator) error {
	o.Log.Debugf("connect: starting full connection cycle")

	// Cleanly stop any existing metric collection goroutine before starting a new one
	if o.goroutineCancel != nil {
		o.Log.Debugf("connect: cancelling previous metric collection goroutine")
		o.goroutineCancel()
	}

	ctx := context.Background()
	ch, err := o.client.startMonitoring(ctx)
	if err != nil {
		o.Log.Debugf("connect: startMonitoring failed: %v", err)
		return err
	}

	if ch == nil {
		o.Log.Debugf("connect: startMonitoring returned nil channel (retryable connect_fail_behavior)")
		return nil
	}

	o.Log.Debugf("connect: startMonitoring succeeded, launching metric collection goroutine (state=%v)", o.client.State())
	o.goroutineCtx, o.goroutineCancel = context.WithCancel(context.Background())

	go func(ctx context.Context) {
		for {
			select {
			case <-ctx.Done():
				o.Log.Debug("Metric collection stopped due to context cancellation on reconnect")
				return
			case m, ok := <-ch:
				if !ok {
					o.Log.Debug("Metric collection stopped due to closed channel")
					return
				}
				acc.AddMetric(m)
			}
		}
	}(o.goroutineCtx)

	return nil
}

func init() {
	inputs.Add("opcua_listener", func() telegraf.Input {
		return &OpcUaListener{
			subscribeClientConfig: subscribeClientConfig{
				InputClientConfig: input.InputClientConfig{
					OpcUAClientConfig: opcua.OpcUAClientConfig{
						Endpoint:       "opc.tcp://localhost:4840",
						SecurityPolicy: "auto",
						SecurityMode:   "auto",
						Certificate:    "/etc/telegraf/cert.pem",
						PrivateKey:     "/etc/telegraf/key.pem",
						AuthMethod:     "Anonymous",
						ConnectTimeout: config.Duration(5 * time.Second),
						RequestTimeout: config.Duration(10 * time.Second),
					},
					MetricName: "opcua",
					Timestamp:  input.TimestampSourceTelegraf,
				},
				SubscriptionInterval:      config.Duration(100 * time.Millisecond),
				MaxRetryChunkSize:         500,
				StaleDataReconnectTimeout: config.Duration(2 * time.Minute),
			},
		}
	})
}
