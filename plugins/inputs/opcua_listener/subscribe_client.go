package opcua_listener

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"
	"sync/atomic"
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

func (o *subscribeClient) RetryMissingItems(ctx context.Context) {
	if o.sub == nil {
		return
	}

	chunkSize := o.Config.MaxRetryChunkSize
	if chunkSize <= 0 {
		chunkSize = 500 // Ensure default
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
	case params.DeadbandType != input.Absolute &&
		params.DeadbandType != input.Percent:
		return fmt.Errorf("deadband_type '%s' not supported", params.DeadbandType)
	case params.DeadbandValue == nil:
		return errors.New("deadband_value was not set")
	case *params.DeadbandValue < 0:
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

		req.RequestedParameters.Filter = ua.NewExtensionObject(
			&ua.DataChangeFilter{
				Trigger:       ua.DataChangeTriggerFromString(string(monParams.DataChangeFilter.Trigger)),
				DeadbandType:  uint32(ua.DeadbandTypeFromString(string(monParams.DataChangeFilter.DeadbandType))),
				DeadbandValue: *monParams.DataChangeFilter.DeadbandValue,
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

	// Initialize node IDs (namespace URI resolution will happen during connect if needed)
	if err := client.InitNodeIDs(); err != nil {
		return nil, err
	}

	if err := client.InitEventNodeIDs(); err != nil {
		return nil, err
	}

	subClient := &subscribeClient{
		OpcUAInputClient:   client,
		Config:             *sc,
	}


	return subClient, nil
}

func (o *subscribeClient) connect() error {
	err := o.OpcUAClient.Connect(o.ctx)
	if err != nil {
		return err
	}

	// Fetch namespace array for namespace URI support
	// This is needed if any nodes use nsu= format instead of ns= format
	if err := o.OpcUAClient.UpdateNamespaceArray(o.ctx); err != nil {
		o.Log.Warnf("Failed to fetch namespace array: %v", err)
		// Continue anyway - this is only needed if using namespace URIs
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
	if o.metrics != nil {
		close(o.metrics)
		o.metrics = nil
	}
	return closing
}

func (o *subscribeClient) startMonitoring(ctx context.Context) (<-chan telegraf.Metric, error) {
	if o.cancel != nil {
		o.cancel()
	}

	o.ctx, o.cancel = context.WithCancel(context.Background())
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
	o.Log.Debugf("Creating monitored items")
	o.monitoredItemsReqs = make([]*ua.MonitoredItemCreateRequest, len(o.NodeIDs))
	for i, nodeID := range o.NodeIDs {
		req := opcua.NewMonitoredItemCreateRequestWithDefaults(nodeID, ua.AttributeIDValue, uint32(i))
		if err := assignConfigValuesToRequest(req, &o.NodeMetricMapping[i].Tag.MonitoringParams); err != nil {
			return nil, err
		}
		o.monitoredItemsReqs[i] = req
	}

	if len(o.monitoredItemsReqs) != 0 {
		resp, err := o.sub.Monitor(ctx, ua.TimestampsToReturnBoth, o.monitoredItemsReqs...)
		if err != nil {
			return nil, fmt.Errorf("failed to start monitoring items: %w", err)
		}
		o.Log.Debug("Monitoring items")

		for idx, res := range resp.Results {
			if !o.StatusCodeOK(res.StatusCode) {
				// Verify NodeIDs array has been built before trying to get item; otherwise show '?' for node id
				if len(o.OpcUAInputClient.NodeIDs) > idx {
					o.Log.Errorf("Failed to create monitored item for node %v (%v) with status code: %v",
						o.OpcUAInputClient.NodeMetricMapping[idx].Tag.FieldName, o.OpcUAInputClient.NodeIDs[idx].String(), res.StatusCode)
				} else {
					o.Log.Errorf("Failed to create monitored item for node %v (%v) with status code: %v", o.OpcUAInputClient.NodeMetricMapping[idx].Tag.FieldName, '?', res.StatusCode)
				}
				// Do not return error here to allow other valid items to be monitored
				// Save failed request to retry later
				o.failedItemsReqs = append(o.failedItemsReqs, o.monitoredItemsReqs[idx])
			}
		}
	}

	o.Log.Debugf("Creating event streaming items")
	o.eventItemsReqs = make([]*ua.MonitoredItemCreateRequest, len(o.EventNodeMetricMapping))
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
			return nil, fmt.Errorf("failed to create event filter: %w", err)
		}
		req.RequestedParameters.Filter = filterExtObj
		o.eventItemsReqs[i] = req
	}

	if len(o.eventItemsReqs) != 0 {
		resp, err := o.sub.Monitor(ctx, ua.TimestampsToReturnBoth, o.eventItemsReqs...)
		if err != nil {
			return nil, fmt.Errorf("failed to start monitoring event stream: %w", err)
		}
		o.Log.Debug("Monitoring events")

		o.Log.Debug("Monitoring events")

		for idx, res := range resp.Results {
			if !o.StatusCodeOK(res.StatusCode) {
				o.Log.Errorf("creating monitored event streaming item failed with status code: %v", res.StatusCode)
				// Do not return error here to allow other valid items to be monitored
				// Save failed event string to retry later
				o.failedEventItemsReqs = append(o.failedEventItemsReqs, o.eventItemsReqs[idx])
			}
		}
	}

	go o.processReceivedNotifications(o.ctx, o.dataNotifications, o.metrics)

	return o.metrics, nil
}

func (o *subscribeClient) processReceivedNotifications(ctx context.Context, dataNotifs <-chan *opcua.PublishNotificationData, metrics chan<- telegraf.Metric) {
	defer close(metrics)
	for {
		select {
		case <-ctx.Done():
			o.Log.Debug("Processing received notifications stopped")
			return

		case res, ok := <-dataNotifs:
			if !ok {
				o.Log.Debugf("Data notification channel closed. Processing of received notifications stopped")
				atomic.StoreInt32(&o.subDropped, 1)
				return
			}
			if res.Error != nil {
				o.Log.Error(res.Error)
				continue
			}
			if res.Value == nil {
				o.Log.Error("Received nil notification")
				return
			}

			switch notif := res.Value.(type) {
			case *ua.DataChangeNotification:
				o.Log.Debugf("Received data change notification with %d items", len(notif.MonitoredItems))
				// It is assumed the notifications are ordered chronologically
				for _, monitoredItemNotif := range notif.MonitoredItems {
					i := int(monitoredItemNotif.ClientHandle)
					oldValue := o.LastReceivedData[i].Value
					o.UpdateNodeValue(i, monitoredItemNotif.Value)
					o.Log.Debugf("Data change notification: node %q value changed from %v to %v",
						o.NodeIDs[i].String(), oldValue, o.LastReceivedData[i].Value)
					metrics <- o.MetricForNode(i)

					// If a node explicitly returns a fatal error, the server has likely discarded the 
					// monitored item or revoked access. Instead of reconnecting, we add it to the failed 
					// items list so the standard Gather() cycle can background-retry it non-destructively.
					errCode := monitoredItemNotif.Value.Status.Error()
					isFatalNodeError := strings.Contains(errCode, "BadNodeIdUnknown") || 
										strings.Contains(errCode, "BadNodeIdInvalid") || 
										strings.Contains(errCode, "BadNotReadable") || 
										strings.Contains(errCode, "BadUserAccessDenied") || 
										strings.Contains(errCode, "BadTypeMismatch")

					if !o.StatusCodeOK(monitoredItemNotif.Value.Status) && isFatalNodeError {
						o.Log.Warnf("Node %q returned fatal status %q. Adding to retry queue for background recovery.", o.NodeIDs[i].String(), errCode)
						o.failedItemsReqs = append(o.failedItemsReqs, o.monitoredItemsReqs[i])
					}
				}

				// If ALL configured nodes went bad, the server might have invalidated the MonitoredItems.
				// We need to trigger a full reconnect to restore them.
				if o.SubDropped() || (len(o.NodeIDs) > 0 && o.BadNodeCount >= len(o.NodeIDs)) {
					o.Log.Warnf("Triggering a reconnect to restore subscriptions.")
					atomic.StoreInt32(&o.subDropped, 1)
					return // stop processing so Gather() handles the reconnect
				}
			case *ua.EventNotificationList:
				o.Log.Debugf("Processing event notification with %d events", len(notif.Events))
				// It is assumed the events are ordered chronologically
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
