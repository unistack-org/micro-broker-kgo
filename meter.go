package kgo

import (
	"net"
	"strconv"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"go.unistack.org/micro/v3/meter"
	"go.unistack.org/micro/v3/semconv"
)

type hookMeter struct {
	meter meter.Meter
}

var (
	_ kgo.HookBrokerConnect    = (*hookMeter)(nil)
	_ kgo.HookBrokerDisconnect = (*hookMeter)(nil)
	// HookBrokerE2E
	_ kgo.HookBrokerRead     = (*hookMeter)(nil)
	_ kgo.HookBrokerThrottle = (*hookMeter)(nil)
	_ kgo.HookBrokerWrite    = (*hookMeter)(nil)
	_ kgo.HookFetchBatchRead = (*hookMeter)(nil)
	// HookFetchRecordBuffered
	// HookFetchRecordUnbuffered
	_ kgo.HookGroupManageError = (*hookMeter)(nil)
	// HookNewClient
	_ kgo.HookProduceBatchWritten = (*hookMeter)(nil)
	// HookProduceRecordBuffered
	// HookProduceRecordPartitioned
	// HookProduceRecordUnbuffered
)

const (
	metricBrokerConnects    = "micro_broker_connects_total"
	metricBrokerDisconnects = "micro_broker_disconnects_total"

	metricBrokerWriteErrors        = "micro_broker_write_errors_total"
	metricBrokerWriteBytes         = "micro_broker_write_bytes_total"
	metricBrokerWriteWaitLatencies = "micro_broker_write_wait_latencies"
	metricBrokerWriteLatencies     = "micro_broker_write_latencies"

	metricBrokerReadErrors        = "micro_broker_read_errors_total"
	metricBrokerReadBytes         = "micro_broker_read_bytes_total"
	metricBrokerReadWaitLatencies = "micro_broker_read_wait_latencies"
	metricBrokerReadLatencies     = "micro_broker_read_latencies"

	metricBrokerThrottleLatencies = "micro_broker_throttle_latencies"

	metricBrokerProduceBytesCompressed   = "micro_broker_produce_bytes_compressed_total"
	metricBrokerProduceBytesUncompressed = "micro_broker_produce_bytes_uncompressed_total"
	metricBrokerFetchBytesCompressed     = "micro_broker_consume_bytes_compressed_total"
	metricBrokerFetchBytesUncompressed   = "micro_broker_consume_bytes_uncompressed_total"

	metricBrokerGroupErrors  = "micro_broker_group_errors_total"
	metricBrokerLostMessages = "micro_broker_lost_messages_total"

	metricBrokerRebalanceTotal   = "micro_broker_rebalance_total"
	metricBrokerCommitErrorTotal = "micro_broker_commit_errors_total"

	labelNode    = "node_id"
	labelSuccess = "success"
	labelFailure = "failure"
	labelStatus  = "status"
	labelTopic   = "topic"
)

func (m *hookMeter) OnGroupManageError(_ error) {
	m.meter.Counter(metricBrokerGroupErrors).Inc()
}

func (m *hookMeter) OnBrokerConnect(meta kgo.BrokerMetadata, _ time.Duration, _ net.Conn, err error) {
	node := strconv.Itoa(int(meta.NodeID))
	if err != nil {
		m.meter.Counter(metricBrokerConnects, labelNode, node, labelStatus, labelFailure).Inc()
		return
	}
	m.meter.Counter(metricBrokerConnects, labelNode, node, labelStatus, labelSuccess).Inc()
}

func (m *hookMeter) OnBrokerDisconnect(meta kgo.BrokerMetadata, _ net.Conn) {
	node := strconv.Itoa(int(meta.NodeID))
	m.meter.Counter(metricBrokerDisconnects, labelNode, node).Inc()
}

func (m *hookMeter) OnBrokerWrite(meta kgo.BrokerMetadata, _ int16, bytesWritten int, writeWait, timeToWrite time.Duration, err error) {
	node := strconv.Itoa(int(meta.NodeID))
	if err != nil {
		m.meter.Counter(metricBrokerWriteErrors, labelNode, node).Inc()
		return
	}
	m.meter.Counter(metricBrokerWriteBytes, labelNode, node).Add(bytesWritten)
	m.meter.Histogram(metricBrokerWriteWaitLatencies, labelNode, node).Update(writeWait.Seconds())
	m.meter.Histogram(metricBrokerWriteLatencies, labelNode, node).Update(timeToWrite.Seconds())
}

func (m *hookMeter) OnBrokerRead(meta kgo.BrokerMetadata, _ int16, bytesRead int, readWait, timeToRead time.Duration, err error) {
	node := strconv.Itoa(int(meta.NodeID))
	if err != nil {
		m.meter.Counter(metricBrokerReadErrors, labelNode, node).Inc()
		return
	}
	m.meter.Counter(metricBrokerReadBytes, labelNode, node).Add(bytesRead)

	m.meter.Histogram(metricBrokerReadWaitLatencies, labelNode, node).Update(readWait.Seconds())
	m.meter.Histogram(metricBrokerReadLatencies, labelNode, node).Update(timeToRead.Seconds())
}

func (m *hookMeter) OnBrokerThrottle(meta kgo.BrokerMetadata, throttleInterval time.Duration, _ bool) {
	node := strconv.Itoa(int(meta.NodeID))
	m.meter.Histogram(metricBrokerThrottleLatencies, labelNode, node).Update(throttleInterval.Seconds())
}

func (m *hookMeter) OnProduceBatchWritten(meta kgo.BrokerMetadata, topic string, _ int32, kmetrics kgo.ProduceBatchMetrics) {
	node := strconv.Itoa(int(meta.NodeID))
	m.meter.Counter(metricBrokerProduceBytesUncompressed, labelNode, node, labelTopic, topic).Add(kmetrics.UncompressedBytes)
	m.meter.Counter(metricBrokerProduceBytesCompressed, labelNode, node, labelTopic, topic).Add(kmetrics.CompressedBytes)
}

func (m *hookMeter) OnFetchBatchRead(meta kgo.BrokerMetadata, topic string, _ int32, kmetrics kgo.FetchBatchMetrics) {
	node := strconv.Itoa(int(meta.NodeID))
	m.meter.Counter(metricBrokerFetchBytesUncompressed, labelNode, node, labelTopic, topic).Add(kmetrics.UncompressedBytes)
	m.meter.Counter(metricBrokerFetchBytesCompressed, labelNode, node, labelTopic, topic).Add(kmetrics.CompressedBytes)
}

type subscribeMetrics struct {
	m     meter.Meter
	topic string
}

func (sm subscribeMetrics) incTotal(status string) {
	sm.m.Counter(semconv.SubscribeMessageTotal, "endpoint", sm.topic, "topic", sm.topic, "status", status).Inc()
}

func (sm subscribeMetrics) recordLatency(d time.Duration) {
	sm.m.Summary(semconv.SubscribeMessageLatencyMicroseconds, "endpoint", sm.topic, "topic", sm.topic).Update(d.Seconds())
	sm.m.Histogram(semconv.SubscribeMessageDurationSeconds, "endpoint", sm.topic, "topic", sm.topic).Update(d.Seconds())
}

func (sm subscribeMetrics) incLost() {
	sm.m.Counter(metricBrokerLostMessages, "endpoint", sm.topic, "topic", sm.topic).Inc()
}

func (sm subscribeMetrics) incRebalance(event string) {
	sm.m.Counter(metricBrokerRebalanceTotal, "endpoint", sm.topic, "topic", sm.topic, "event", event).Inc()
}

func (sm subscribeMetrics) incCommitError() {
	sm.m.Counter(metricBrokerCommitErrorTotal, "endpoint", sm.topic, "topic", sm.topic).Inc()
}

func (sm subscribeMetrics) incGroupError() {
	sm.m.Counter(metricBrokerGroupErrors, "endpoint", sm.topic, "topic", sm.topic).Inc()
}

type publishMetrics struct {
	m     meter.Meter
	topic string
}

func (pm publishMetrics) incTotal(status string) {
	pm.m.Counter(semconv.PublishMessageTotal, "endpoint", pm.topic, "topic", pm.topic, "status", status).Inc()
}

func (pm publishMetrics) recordLatency(d time.Duration) {
	pm.m.Summary(semconv.PublishMessageLatencyMicroseconds, "endpoint", pm.topic, "topic", pm.topic).Update(d.Seconds())
	pm.m.Histogram(semconv.PublishMessageDurationSeconds, "endpoint", pm.topic, "topic", pm.topic).Update(d.Seconds())
}
