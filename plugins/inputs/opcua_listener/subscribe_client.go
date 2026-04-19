package opcua_listener

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync/atomic"
	"time"

	"github.com/gopcua/opcua"
	"github.com/gopcua/opcua/ua"
	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/config"
	opcuaclient "github.com/influxdata/telegraf/plugins/common/opcua"
	"github.com/influxdata/telegraf/plugins/common/opcua/input"
)

type subscribeClientConfig struct {
	input.InputClientConfig
	SubscriptionInterval config.Duration `toml:"subscription_interval"`
	ConnectFailBehavior  string          `toml:"connect_fail_behavior"`
	MaxRetryChunkSize    int             `toml:"max_retry_chunk_size"`
}

type subscribeClient struct {
	*input.OpcUAInputClient
	Config subscribeClientConfig

	sub                *opcua.Subscription
	monitoredItemsReqs []*ua.MonitoredItemCreateRequest
	eventItemsReqs     []*ua.MonitoredItemCreateRequest
	dataNotifications  chan *opcua.PublishNotificationData
	metrics            chan telegraf.Metric

	ctx    context.Context
	cancel context.CancelFunc

	// subDropped is set when the dataNotifications channel is closed unexpectedly
	// by the gopcua library. Gather() polls this to trigger a Telegraf-level reconnect.
	subDropped int32

	failedItemsReqs      []*ua.MonitoredItemCreateRequest
	failedEventItemsReqs []*ua.MonitoredItemCreateRequest
}

func (o *subscribeClient) SubDropped() bool {
	return atomic.LoadInt32(&o.subDropped) == 1
}

func (o *subscribeClient) ClearSubDropped() {
	atomic.StoreInt32(&o.subDropped, 0)
}

// RetryMissingItems attempts to re-register monitored items that previously failed.
// Called from Gather() on each interval so failed items are recovered without a full reconnect.
func (o *subscribeClient) RetryMissingItems(ctx context.Context) {
	if o.sub == nil {
		return
	}

	chunkSize := o.Config.MaxRetryChunkSize
	if chunkSize <= 0 {
		chunkSize = 500
	}

	if len(o.failedItemsReqs) > 0 {
		end := len(o.failedItemsReqs)
		if end > chunkSize {
			end = chunkSize
		}
		retrying := o.failedItemsReqs[:end]
		o.Log.Debugf("Retrying %d missing monitored items", len(retrying))
		resp, err := o.sub.Monitor(ctx, ua.TimestampsToReturnBoth, retrying...)
		if err != nil {
			o.Log.Debugf("Retry Monitor call failed: %v", err)
		} else {
			stillFailed := o.failedItemsReqs[end:]
			var backToFailed []*ua.MonitoredItemCreateRequest
			for idx, res := range resp.Results {
				if o.StatusCodeOK(res.StatusCode) {
					o.Log.Infof("Successfully recovered monitored item: %v", retrying[idx].ItemToMonitor.NodeID.String())
				} else {
					backToFailed = append(backToFailed, retrying[idx])
				}
			}
			o.failedItemsReqs = append(stillFailed, backToFailed...)
		}
	}

	if len(o.failedEventItemsReqs) > 0 {
		end := len(o.failedEventItemsReqs)
		if end > chunkSize {
			end = chunkSize
		}
		retrying := o.failedEventItemsReqs[:end]
		o.Log.Debugf("Retrying %d missing event streaming items", len(retrying))
		resp, err := o.sub.Monitor(ctx, ua.TimestampsToReturnBoth, retrying...)
		if err != nil {
			o.Log.Debugf("Retry Monitor call failed for event items: %v", err)
		} else {
			stillFailed := o.failedEventItemsReqs[end:]
			var backToFailed []*ua.MonitoredItemCreateRequest
			for idx, res := range resp.Results {
				if o.StatusCodeOK(res.StatusCode) {
					o.Log.Infof("Successfully recovered monitored event streaming item")
				} else {
					backToFailed = append(backToFailed, retrying[idx])
				}
			}
			o.failedEventItemsReqs = append(stillFailed, backToFailed...)
		}
	}
}

func checkDataChangeFilterParameters(params *input.DataChangeFilter) error {
	switch {
	case params.Trigger != input.Status &&
		params.Trigger != input.StatusValue &&
		params.Trigger != input.StatusValueTimestamp:
		return fmt.Errorf("trigger '%s' not supported", params.Trigger)
	case params.DeadbandType != input.None &&
		params.DeadbandType != input.Absolute &&
		params.DeadbandType != input.Percent:
		return fmt.Errorf("deadband_type '%s' not supported", params.DeadbandType)
	case params.DeadbandType != input.None && params.DeadbandValue == nil:
		return errors.New("deadband_value was not set")
	case params.DeadbandValue != nil && *params.DeadbandValue < 0:
		return errors.New("negative deadband_value not supported")
	default:
		return nil
	}
}

func assignConfigValuesToRequest(req *ua.MonitoredItemCreateRequest, monParams *input.MonitoringParameters) error {
	req.RequestedParameters.SamplingInterval = float64(time.Duration(monParams.SamplingInterval) / time.Millisecond)

	if monParams.QueueSize != nil {
		req.RequestedParameters.QueueSize = *monParams.QueueSize
	}

	if monParams.DiscardOldest != nil {
		req.RequestedParameters.DiscardOldest = *monParams.DiscardOldest
	}

	if monParams.DataChangeFilter != nil {
		if err := checkDataChangeFilterParameters(monParams.DataChangeFilter); err != nil {
			return fmt.Errorf(err.Error()+", node '%s'", req.ItemToMonitor.NodeID)
		}

		var deadbandValue float64
		if monParams.DataChangeFilter.DeadbandValue != nil {
			deadbandValue = *monParams.DataChangeFilter.DeadbandValue
		}

		req.RequestedParameters.Filter = ua.NewExtensionObject(
			&ua.DataChangeFilter{
				Trigger:       ua.DataChangeTriggerFromString(string(monParams.DataChangeFilter.Trigger)),
				DeadbandType:  uint32(ua.DeadbandTypeFromString(string(monParams.DataChangeFilter.DeadbandType))),
				DeadbandValue: deadbandValue,
			},
		)
	}

	return nil
}

func (sc *subscribeClientConfig) createSubscribeClient(log telegraf.Logger) (*subscribeClient, error) {
	client, err := sc.InputClientConfig.CreateInputClient(log)
	if err != nil {
		return nil, err
	}

	// Validate monitoring parameters at config time (no server connection needed)
	for _, node := range client.NodeMetricMapping {
		if node.Tag.MonitoringParams.DataChangeFilter != nil {
			if err := checkDataChangeFilterParameters(node.Tag.MonitoringParams.DataChangeFilter); err != nil {
				return nil, fmt.Errorf("node '%s': %w", node.Tag.NodeID(), err)
			}
		}
	}

	subClient := &subscribeClient{
		OpcUAInputClient:   client,
		Config:             *sc,
		monitoredItemsReqs: make([]*ua.MonitoredItemCreateRequest, 0, len(client.NodeMetricMapping)),
		eventItemsReqs:     make([]*ua.MonitoredItemCreateRequest, 0, len(client.EventNodeMetricMapping)),
		// Channels are allocated here; they will be recreated in startMonitoring on each
		// (re)connect so that a closed channel from a prior session cannot be reused.
		dataNotifications: make(chan *opcua.PublishNotificationData, 100),
		metrics:           make(chan telegraf.Metric, 100),
	}
	return subClient, nil
}

func (o *subscribeClient) connect() error {
	err := o.OpcUAClient.Connect(o.ctx)
	if err != nil {
		return err
	}

	// Fetch namespace array for namespace URI support
	if err := o.OpcUAClient.UpdateNamespaceArray(o.ctx); err != nil {
		o.Log.Warnf("Failed to fetch namespace array: %v", err)
	}

	// Initialize node IDs after connection so namespace URIs can be resolved
	if err := o.OpcUAInputClient.InitNodeIDs(); err != nil {
		return fmt.Errorf("initializing node IDs failed: %w", err)
	}
	if err := o.OpcUAInputClient.InitEventNodeIDs(); err != nil {
		return fmt.Errorf("initializing event node IDs failed: %w", err)
	}

	o.Log.Debugf("Creating monitored items")
	o.monitoredItemsReqs = make([]*ua.MonitoredItemCreateRequest, 0, len(o.NodeIDs))
	for i, nodeID := range o.NodeIDs {
		req := opcua.NewMonitoredItemCreateRequestWithDefaults(nodeID, ua.AttributeIDValue, uint32(i))
		if err := assignConfigValuesToRequest(req, &o.NodeMetricMapping[i].Tag.MonitoringParams); err != nil {
			return fmt.Errorf("assigning monitoring params failed: %w", err)
		}
		o.monitoredItemsReqs = append(o.monitoredItemsReqs, req)
	}

	o.Log.Debugf("Creating event streaming items")
	o.eventItemsReqs = make([]*ua.MonitoredItemCreateRequest, 0, len(o.EventNodeMetricMapping))
	for i, node := range o.EventNodeMetricMapping {
		req := opcua.NewMonitoredItemCreateRequestWithDefaults(node.NodeID, ua.AttributeIDEventNotifier, uint32(i))
		if node.SamplingInterval != nil {
			req.RequestedParameters.SamplingInterval = float64(time.Duration(*node.SamplingInterval) / time.Millisecond)
		}
		if node.QueueSize != nil {
			req.RequestedParameters.QueueSize = *node.QueueSize
		}

		filterExtObj, err := node.CreateEventFilter()
		if err != nil {
			return fmt.Errorf("creating event filter failed: %w", err)
		}
		req.RequestedParameters.Filter = filterExtObj
		o.eventItemsReqs = append(o.eventItemsReqs, req)
	}

	o.Log.Debugf("Creating OPC UA subscription")
	o.sub, err = o.Client.Subscribe(o.ctx, &opcua.SubscriptionParameters{
		Interval: time.Duration(o.Config.SubscriptionInterval),
	}, o.dataNotifications)
	if err != nil {
		o.Log.Error("Failed to create subscription")
		return err
	}

	o.Log.Debugf("Subscribed with subscription ID %d", o.sub.SubscriptionID)
	return nil
}

func (o *subscribeClient) stop(ctx context.Context) <-chan struct{} {
	o.Log.Debugf("Stopping OPC subscription...")
	if o.State() != opcuaclient.Connected {
		return nil
	}
	if o.sub != nil {
		if err := o.sub.Cancel(ctx); err != nil {
			o.Log.Warn("Cancelling OPC UA subscription failed with error ", err)
		}
	}
	closing := o.OpcUAInputClient.Stop(ctx)
	if o.cancel != nil {
		o.cancel()
		o.cancel = nil
	}
	return closing
}

func (o *subscribeClient) startMonitoring(ctx context.Context) (<-chan telegraf.Metric, error) {
	// Cancel the previous processReceivedNotifications goroutine (if any) before
	// creating fresh channels and a new connection. This prevents two goroutines
	// from writing to the same metrics channel simultaneously.
	if o.cancel != nil {
		o.cancel()
	}
	o.ctx, o.cancel = context.WithCancel(context.Background())

	// Recreate channels on every (re)connect. gopcua closes the dataNotifications
	// channel when the subscription is torn down, so reusing a closed channel would
	// panic. The metrics channel is recreated to drain any stale buffered values.
	o.dataNotifications = make(chan *opcua.PublishNotificationData, 100)
	o.metrics = make(chan telegraf.Metric, 100)

	err := o.connect()
	if err != nil {
		switch o.Config.ConnectFailBehavior {
		case "retry":
			o.Log.Warnf("Failed to connect to OPC UA server %s. Will attempt to connect again at the next interval: %s", o.Config.Endpoint, err)
			return nil, nil
		case "ignore":
			o.Log.Errorf("Failed to connect to OPC UA server %s. Will not retry: %s", o.Config.Endpoint, err)
			return nil, nil
		}
		return nil, err
	}

	o.failedItemsReqs = nil
	o.failedEventItemsReqs = nil

	if len(o.monitoredItemsReqs) != 0 {
		resp, err := o.sub.Monitor(ctx, ua.TimestampsToReturnBoth, o.monitoredItemsReqs...)
		if err != nil {
			return nil, fmt.Errorf("failed to start monitoring items: %w", err)
		}
		o.Log.Debug("Monitoring items")
		for idx, res := range resp.Results {
			if !o.StatusCodeOK(res.StatusCode) {
				if len(o.OpcUAInputClient.NodeIDs) > idx {
					o.Log.Errorf("Failed to create monitored item for node %v (%v) with status code: %v",
						o.OpcUAInputClient.NodeMetricMapping[idx].Tag.FieldName, o.OpcUAInputClient.NodeIDs[idx].String(), res.StatusCode)
				} else {
					o.Log.Errorf("Failed to create monitored item for node %v (%v) with status code: %v",
						o.OpcUAInputClient.NodeMetricMapping[idx].Tag.FieldName, '?', res.StatusCode)
				}
				o.failedItemsReqs = append(o.failedItemsReqs, o.monitoredItemsReqs[idx])
			}
		}
	}

	if len(o.eventItemsReqs) != 0 {
		resp, err := o.sub.Monitor(ctx, ua.TimestampsToReturnBoth, o.eventItemsReqs...)
		if err != nil {
			return nil, fmt.Errorf("failed to start monitoring event stream: %w", err)
		}
		o.Log.Debug("Monitoring events")
		for idx, res := range resp.Results {
			if !o.StatusCodeOK(res.StatusCode) {
				o.Log.Errorf("creating monitored event streaming item failed with status code: %v", res.StatusCode)
				o.failedEventItemsReqs = append(o.failedEventItemsReqs, o.eventItemsReqs[idx])
			}
		}
	}

	go o.processReceivedNotifications()

	return o.metrics, nil
}

func (o *subscribeClient) processReceivedNotifications() {
	for {
		select {
		case <-o.ctx.Done():
			o.Log.Debug("Processing received notifications stopped")
			return

		case res, ok := <-o.dataNotifications:
			if !ok {
				// gopcua closed the channel — the session is unrecoverable at this level.
				// Signal Gather() to perform a full Telegraf-level reconnect.
				o.Log.Debugf("Data notification channel closed; marking subscription as dropped")
				atomic.StoreInt32(&o.subDropped, 1)
				return
			}
			if res.Error != nil {
				o.Log.Error(res.Error)
				continue
			}
			if res.Value == nil {
				// gopcua sends nil-value notifications as internal bookkeeping during
				// session reconnects. Safe to skip — the session is still live.
				o.Log.Warn("Received nil notification value, skipping")
				continue
			}

			switch notif := res.Value.(type) {
			case *ua.DataChangeNotification:
				o.Log.Debugf("Received data change notification with %d items", len(notif.MonitoredItems))
				for _, monitoredItemNotif := range notif.MonitoredItems {
					i := int(monitoredItemNotif.ClientHandle)
					oldValue := o.LastReceivedData[i].Value
					o.UpdateNodeValue(i, monitoredItemNotif.Value)
					o.Log.Debugf("Data change notification: node %q value changed from %v to %v",
						o.NodeIDs[i].String(), oldValue, o.LastReceivedData[i].Value)
					o.metrics <- o.MetricForNode(i)

					// Track individual nodes that return permanent fatal errors so
					// RetryMissingItems() can attempt to re-register them without
					// triggering a full reconnect.
					if !o.StatusCodeOK(monitoredItemNotif.Value.Status) {
						errCode := monitoredItemNotif.Value.Status.Error()
						if strings.Contains(errCode, "BadNodeIdUnknown") ||
							strings.Contains(errCode, "BadNodeIdInvalid") ||
							strings.Contains(errCode, "BadNotReadable") ||
							strings.Contains(errCode, "BadUserAccessDenied") ||
							strings.Contains(errCode, "BadTypeMismatch") {
							o.Log.Warnf("Node %q returned fatal status %q; adding to retry queue", o.NodeIDs[i].String(), errCode)
							o.failedItemsReqs = append(o.failedItemsReqs, o.monitoredItemsReqs[i])
						}
					}
				}

				// If ALL configured nodes went bad, check whether they carry fatal error codes
				// (e.g. the server restarted and the namespace changed). A full reconnect is far
				// faster than background chunk-retries in that situation. Transient errors like
				// BadNoCommunication are deliberately excluded so the OPC session can ride them out.
				if len(o.NodeIDs) > 0 && o.BadNodeCount >= len(o.NodeIDs) {
					fatalCount := 0
					for _, d := range o.LastReceivedData {
						errCode := d.Quality.Error()
						if strings.Contains(errCode, "BadNodeIdUnknown") ||
							strings.Contains(errCode, "BadNodeIdInvalid") ||
							strings.Contains(errCode, "BadNotReadable") ||
							strings.Contains(errCode, "BadUserAccessDenied") ||
							strings.Contains(errCode, "BadTypeMismatch") {
							fatalCount++
						}
					}
					if fatalCount >= len(o.NodeIDs) {
						o.Log.Warnf("All %d nodes returned fatal errors. Triggering a fast full reconnect to restore subscriptions.", fatalCount)
						atomic.StoreInt32(&o.subDropped, 1)
						return // stop processing so Gather() handles the reconnect
					}
				}

			case *ua.EventNotificationList:
				o.Log.Debugf("Processing event notification with %d events", len(notif.Events))
				for _, event := range notif.Events {
					i := int(event.ClientHandle)
					if m := o.MetricForEvent(i, event); m != nil {
						o.metrics <- m
					}
				}

			default:
				o.Log.Warnf("Received notification has unexpected type %s", reflect.TypeOf(res.Value))
			}
		}
	}
}
