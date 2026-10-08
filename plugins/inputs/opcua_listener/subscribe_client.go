package opcua_listener

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gopcua/opcua"
	"github.com/gopcua/opcua/ua"
	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/config"
	"github.com/influxdata/telegraf/plugins/common/opcua/input"
)

type subscribeClientConfig struct {
	input.InputClientConfig
	SubscriptionInterval      config.Duration `toml:"subscription_interval"`
	ConnectFailBehavior       string          `toml:"connect_fail_behavior"`
	MaxRetryChunkSize         int             `toml:"max_retry_chunk_size"`
	StaleDataReconnectTimeout config.Duration `toml:"stale_data_reconnect_timeout"`
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

	// lastDataReceived stores the Unix timestamp of the most recent data or event
	// notification. Used by the staleness watchdog in Gather() to detect zombie
	// connections where gopcua reports Connected but no data is flowing.
	lastDataReceived int64

	failedItemsReqs      []*ua.MonitoredItemCreateRequest
	failedEventItemsReqs []*ua.MonitoredItemCreateRequest
	failedMu             sync.Mutex       // protects failedItemsReqs, failedEventItemsReqs, failedNodeSet
	failedNodeSet        map[int]struct{} // node indices already queued for retry (dedup)
	monitoredItemIDs     map[int]uint32   // node index → server-assigned MonitoredItemID
}

func (o *subscribeClient) SubDropped() bool {
	return atomic.LoadInt32(&o.subDropped) == 1
}

func (o *subscribeClient) ClearSubDropped() {
	atomic.StoreInt32(&o.subDropped, 0)
}

// UpdateLastDataReceived records that a data/event notification was just received.
func (o *subscribeClient) UpdateLastDataReceived() {
	atomic.StoreInt64(&o.lastDataReceived, time.Now().Unix())
}

// SecondsSinceLastData returns the number of seconds since the last data notification.
// Returns 0 if no data has ever been received (watchdog should not fire).
func (o *subscribeClient) SecondsSinceLastData() int64 {
	last := atomic.LoadInt64(&o.lastDataReceived)
	if last == 0 {
		return 0
	}
	return time.Now().Unix() - last
}

// isFatalStatusCode returns true if the error string contains a permanent OPC UA
// status code that indicates the node cannot recover without re-registration
// (e.g. after a server restart or namespace change).
func isFatalStatusCode(errCode string) bool {
	return strings.Contains(errCode, "BadNodeIDUnknown") ||
		strings.Contains(errCode, "BadNodeIDInvalid") ||
		strings.Contains(errCode, "BadNotReadable") ||
		strings.Contains(errCode, "BadUserAccessDenied") ||
		strings.Contains(errCode, "BadTypeMismatch")
}

// enqueueFailedNode adds a node to the retry queue if it is not already queued.
// Safe to call from any goroutine.
func (o *subscribeClient) enqueueFailedNode(nodeIdx int) {
	o.failedMu.Lock()
	defer o.failedMu.Unlock()
	if _, already := o.failedNodeSet[nodeIdx]; already {
		return
	}
	o.Log.Warnf("Node %q returned fatal status; adding to retry queue", o.NodeIDs[nodeIdx].String())
	o.failedItemsReqs = append(o.failedItemsReqs, o.monitoredItemsReqs[nodeIdx])
	o.failedNodeSet[nodeIdx] = struct{}{}
}

// retryChunkSize returns the configured chunk size or a default of 500.
func (o *subscribeClient) retryChunkSize() int {
	if o.Config.MaxRetryChunkSize > 0 {
		return o.Config.MaxRetryChunkSize
	}
	return 500
}

// registerMonitoredItemsChunked registers items with the OPC UA server in chunks.
// For each result, onResult is called with the global index and the result.
// Returns an error only if the Monitor RPC itself fails.
func (o *subscribeClient) registerMonitoredItemsChunked(
	ctx context.Context,
	items []*ua.MonitoredItemCreateRequest,
	label string,
	onResult func(globalIdx int, res *ua.MonitoredItemCreateResult),
) error {
	chunkSize := o.retryChunkSize()
	for i := 0; i < len(items); i += chunkSize {
		end := i + chunkSize
		if end > len(items) {
			end = len(items)
		}
		chunk := items[i:end]
		resp, err := o.sub.Monitor(ctx, ua.TimestampsToReturnBoth, chunk...)
		if err != nil {
			return fmt.Errorf("failed to start monitoring %s: %w", label, err)
		}
		o.Log.Debugf("Monitoring %d %s (chunk starting at %d)", len(chunk), label, i)
		for idx, res := range resp.Results {
			onResult(i+idx, res)
		}
	}
	return nil
}

// RetryMissingItems attempts to re-register monitored items that previously failed.
// Called from Gather() on each interval so failed items are recovered without a full reconnect.
func (o *subscribeClient) RetryMissingItems(ctx context.Context) {
	if o.sub == nil {
		return
	}
	o.retryFailedDataItems(ctx)
	o.retryFailedEventItems(ctx)
}

func (o *subscribeClient) retryFailedDataItems(ctx context.Context) {
	o.failedMu.Lock()
	if len(o.failedItemsReqs) == 0 {
		o.failedMu.Unlock()
		return
	}

	chunkSize := o.retryChunkSize()
	end := len(o.failedItemsReqs)
	if end > chunkSize {
		end = chunkSize
	}
	retrying := make([]*ua.MonitoredItemCreateRequest, end)
	copy(retrying, o.failedItemsReqs[:end])

	// Collect old server-side MonitoredItemIDs so we can cancel them
	// before re-registering to avoid duplicate items on the server.
	var oldIDs []uint32
	for _, req := range retrying {
		nodeIdx := int(req.RequestedParameters.ClientHandle)
		if id, ok := o.monitoredItemIDs[nodeIdx]; ok {
			oldIDs = append(oldIDs, id)
			delete(o.monitoredItemIDs, nodeIdx)
		}
	}
	o.failedMu.Unlock()

	if len(oldIDs) > 0 {
		if _, err := o.sub.Unmonitor(ctx, oldIDs...); err != nil {
			o.Log.Warnf("Failed to unmonitor %d old items before retry: %v", len(oldIDs), err)
		}
	}

	o.Log.Infof("Retrying %d missing monitored items", len(retrying))
	resp, err := o.sub.Monitor(ctx, ua.TimestampsToReturnBoth, retrying...)
	if err != nil {
		o.Log.Infof("Retry Monitor call failed: %v", err)
		return
	}

	o.failedMu.Lock()
	defer o.failedMu.Unlock()
	stillFailed := o.failedItemsReqs[end:]
	var backToFailed []*ua.MonitoredItemCreateRequest
	for idx, res := range resp.Results {
		nodeIdx := int(retrying[idx].RequestedParameters.ClientHandle)
		if o.StatusCodeOK(res.StatusCode) {
			o.Log.Infof("Successfully recovered monitored item: %v", retrying[idx].ItemToMonitor.NodeID.String())
			o.monitoredItemIDs[nodeIdx] = res.MonitoredItemID
			delete(o.failedNodeSet, nodeIdx)
		} else {
			backToFailed = append(backToFailed, retrying[idx])
		}
	}
	o.failedItemsReqs = append(stillFailed, backToFailed...)
}

func (o *subscribeClient) retryFailedEventItems(ctx context.Context) {
	o.failedMu.Lock()
	if len(o.failedEventItemsReqs) == 0 {
		o.failedMu.Unlock()
		return
	}

	chunkSize := o.retryChunkSize()
	end := len(o.failedEventItemsReqs)
	if end > chunkSize {
		end = chunkSize
	}
	retrying := make([]*ua.MonitoredItemCreateRequest, end)
	copy(retrying, o.failedEventItemsReqs[:end])
	o.failedMu.Unlock()

	o.Log.Debugf("Retrying %d missing event streaming items", len(retrying))
	resp, err := o.sub.Monitor(ctx, ua.TimestampsToReturnBoth, retrying...)
	if err != nil {
		o.Log.Debugf("Retry Monitor call failed for event items: %v", err)
		return
	}

	o.failedMu.Lock()
	defer o.failedMu.Unlock()
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

	// Let the caller own reconnection. Without this, gopcua's monitor and the
	// caller's reconnect cycle both create sessions, flooding the server.
	client.DisableAutoReconnect = true

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
	ctx := o.ctx
	if ctx == nil {
		ctx = context.Background()
	}
	err := o.OpcUAClient.Connect(ctx)
	if err != nil {
		return err
	}

	// Fetch namespace array for namespace URI support
	if err := o.OpcUAClient.UpdateNamespaceArray(ctx); err != nil {
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
	o.sub, err = o.Client.Subscribe(ctx, &opcua.SubscriptionParameters{
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
	// if o.State() != opcuaclient.Connected && o.State() != opcuaclient.Reconnecting {
	// 	return nil
	// }
	if o.sub != nil {
		if err := o.sub.Cancel(ctx); err != nil {
			o.Log.Warn("Cancelling OPC UA subscription failed with error ", err)
		}
		o.sub = nil
	}
	if o.cancel != nil {
		o.cancel()
		o.cancel = nil
	}
	if o.Client != nil {
		return o.OpcUAInputClient.Stop(ctx)
	}

	ch := make(chan struct{})
	close(ch)
	return ch
}

func (o *subscribeClient) startMonitoring(ctx context.Context) (<-chan telegraf.Metric, error) {
	o.Log.Debugf("startMonitoring: beginning (re)connection sequence, current state=%v, subDropped=%v", o.State(), o.SubDropped())

	// Cancel the previous processReceivedNotifications goroutine (if any) before
	// creating fresh channels and a new connection. This prevents two goroutines
	// from writing to the same metrics channel simultaneously.
	if o.cancel != nil {
		o.Log.Debugf("startMonitoring: cancelling previous notification goroutine")
		o.cancel()
	}
	o.ctx, o.cancel = context.WithCancel(context.Background())

	// Reset staleness timer to zero while we set up the connection and register
	// monitored items. It will be seeded to time.Now() after setup completes.
	atomic.StoreInt64(&o.lastDataReceived, 0)

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
	o.failedNodeSet = make(map[int]struct{})
	o.monitoredItemIDs = make(map[int]uint32, len(o.NodeIDs))

	if len(o.monitoredItemsReqs) != 0 {
		err = o.registerMonitoredItemsChunked(ctx, o.monitoredItemsReqs, "items", func(globalIdx int, res *ua.MonitoredItemCreateResult) {
			if !o.StatusCodeOK(res.StatusCode) {
				nodeName := "?"
				nodeID := "?"
				if globalIdx < len(o.OpcUAInputClient.NodeIDs) {
					nodeName = o.OpcUAInputClient.NodeMetricMapping[globalIdx].Tag.FieldName
					nodeID = o.OpcUAInputClient.NodeIDs[globalIdx].String()
				}
				o.Log.Errorf("Failed to create monitored item for node %v (%v) with status code: %v", nodeName, nodeID, res.StatusCode)
				o.failedItemsReqs = append(o.failedItemsReqs, o.monitoredItemsReqs[globalIdx])
				o.failedNodeSet[globalIdx] = struct{}{}
			} else {
				o.monitoredItemIDs[globalIdx] = res.MonitoredItemID
			}
		})
		if err != nil {
			return nil, err
		}
	}

	if len(o.eventItemsReqs) != 0 {
		err = o.registerMonitoredItemsChunked(ctx, o.eventItemsReqs, "events", func(globalIdx int, res *ua.MonitoredItemCreateResult) {
			if !o.StatusCodeOK(res.StatusCode) {
				o.Log.Errorf("creating monitored event streaming item failed with status code: %v", res.StatusCode)
				o.failedEventItemsReqs = append(o.failedEventItemsReqs, o.eventItemsReqs[globalIdx])
			}
		})
		if err != nil {
			return nil, err
		}
	}

	o.Log.Debugf("startMonitoring: launching processReceivedNotifications goroutine (monitoredItems=%d, failedItems=%d, eventItems=%d, failedEventItems=%d)",
		len(o.monitoredItemsReqs), len(o.failedItemsReqs), len(o.eventItemsReqs), len(o.failedEventItemsReqs))

	// Seed staleness timer now that all monitored items are registered.
	// This gives the watchdog a baseline: if no DataChangeNotification arrives
	// within the threshold after this point, the subscription is considered
	// zombie and Gather() will force a reconnect.
	atomic.StoreInt64(&o.lastDataReceived, time.Now().Unix())
	o.Log.Debugf("startMonitoring: staleness timer seeded to now")
	go o.processReceivedNotifications()

	return o.metrics, nil
}

func (o *subscribeClient) processReceivedNotifications() {
	o.Log.Debugf("processReceivedNotifications: goroutine started")
	for {
		select {
		case <-o.ctx.Done():
			o.Log.Debugf("processReceivedNotifications: context cancelled (state=%v, subDropped=%v)", o.State(), o.SubDropped())
			return

		case res, ok := <-o.dataNotifications:
			if !ok {
				o.Log.Debugf("processReceivedNotifications: channel closed (state=%v); marking subscription as dropped", o.State())
				atomic.StoreInt32(&o.subDropped, 1)
				return
			}
			if res.Error != nil {
				o.Log.Errorf("processReceivedNotifications: error: %v (state=%v)", res.Error, o.State())
				continue
			}
			if res.Value == nil {
				o.Log.Warnf("processReceivedNotifications: nil notification value (state=%v), skipping", o.State())
				continue
			}

			switch notif := res.Value.(type) {
			case *ua.DataChangeNotification:
				o.handleDataChangeNotification(notif)
			case *ua.EventNotificationList:
				o.handleEventNotification(notif)
			default:
				o.Log.Warnf("Received notification has unexpected type %s", reflect.TypeOf(res.Value))
			}
		}
	}
}

func (o *subscribeClient) handleDataChangeNotification(notif *ua.DataChangeNotification) {
	o.UpdateLastDataReceived()
	subID := uint32(0)
	if o.sub != nil {
		subID = o.sub.SubscriptionID
	}
	o.Log.Infof("Received data change notification on subscription %d with %d items", subID, len(notif.MonitoredItems))

	for _, item := range notif.MonitoredItems {
		i := int(item.ClientHandle)
		o.UpdateNodeValue(i, item.Value)
		o.metrics <- o.MetricForNode(i)

		// Queue nodes with permanent fatal errors for retry via RetryMissingItems().
		if !o.StatusCodeOK(item.Value.Status) && isFatalStatusCode(item.Value.Status.Error()) {
			o.enqueueFailedNode(i)
		}
	}

	// If ALL configured nodes carry fatal errors (e.g. server restarted with a
	// different namespace), trigger a full reconnect instead of slow chunk-retries.
	if len(o.NodeIDs) > 0 && o.BadNodeCount >= len(o.NodeIDs) {
		fatalCount := 0
		for _, d := range o.LastReceivedData {
			if isFatalStatusCode(d.Quality.Error()) {
				fatalCount++
			}
		}
		if fatalCount >= len(o.NodeIDs) {
			o.Log.Warnf("All %d nodes returned fatal errors. Triggering a fast full reconnect.", fatalCount)
			atomic.StoreInt32(&o.subDropped, 1)
		}
	}
}

func (o *subscribeClient) handleEventNotification(notif *ua.EventNotificationList) {
	o.UpdateLastDataReceived()
	o.Log.Debugf("Processing event notification with %d events", len(notif.Events))
	for _, event := range notif.Events {
		i := int(event.ClientHandle)
		if m := o.MetricForEvent(i, event); m != nil {
			o.metrics <- m
		}
	}
}
