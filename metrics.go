package kgo

import (
	"net"
	"strconv"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"go.unistack.org/micro/v3/meter"
)

type metrics struct {
	meter meter.Meter
}

var (
	_ kgo.HookBrokerConnect       = &metrics{}
	_ kgo.HookBrokerDisconnect    = &metrics{}
	_ kgo.HookBrokerRead          = &metrics{}
	_ kgo.HookBrokerThrottle      = &metrics{}
	_ kgo.HookBrokerWrite         = &metrics{}
	_ kgo.HookFetchBatchRead      = &metrics{}
	_ kgo.HookProduceBatchWritten = &metrics{}
	_ kgo.HookGroupManageError    = &metrics{}
)

const (
	metricBrokerConnects    = "broker_connects_total"
	metricBrokerDisconnects = "broker_disconnects_total"

	metricBrokerWriteErrors        = "broker_write_errors_total"
	metricBrokerWriteBytes         = "broker_write_bytes_total"
	metricBrokerWriteWaitLatencies = "broker_write_wait_latencies"
	metricBrokerWriteLatencies     = "broker_write_latencies"

	metricBrokerReadErrors        = "broker_read_errors_total"
	metricBrokerReadBytes         = "broker_read_bytes_total"
	metricBrokerReadWaitLatencies = "broker_read_wait_latencies"
	metricBrokerReadLatencies     = "broker_read_latencies"

	metricBrokerThrottleLatencies = "broker_throttle_latencies"

	metricBrokerProduceBytesCompressed   = "broker_produce_bytes_compressed_total"
	metricBrokerProduceBytesUncompressed = "broker_produce_bytes_uncompressed_total"
	metricBrokerFetchBytesCompressed     = "broker_consume_bytes_compressed_total"
	metricBrokerFetchBytesUncompressed   = "broker_consume_bytes_uncompressed_total"

	metricBrokerGroupErrors = "broker_group_errors_total"

	labelNode    = "node_id"
	labelSuccess = "success"
	labelFaulure = "failure"
	labelStatus  = "status"
	labelTopic   = "topic"
)

func (m *metrics) OnGroupManageError(err error) {
	m.meter.Counter(metricBrokerGroupErrors).Inc()
}

func (m *metrics) OnBrokerConnect(meta kgo.BrokerMetadata, _ time.Duration, _ net.Conn, err error) {
	node := strconv.Itoa(int(meta.NodeID))
	if err != nil {
		m.meter.Counter(metricBrokerConnects, labelNode, node, labelStatus, labelFaulure).Inc()
		return
	}
	m.meter.Counter(metricBrokerConnects, labelNode, node, labelStatus, labelSuccess).Inc()
}

func (m *metrics) OnBrokerDisconnect(meta kgo.BrokerMetadata, _ net.Conn) {
	node := strconv.Itoa(int(meta.NodeID))
	m.meter.Counter(metricBrokerDisconnects, labelNode, node).Inc()
}

func (m *metrics) OnBrokerWrite(meta kgo.BrokerMetadata, _ int16, bytesWritten int, writeWait, timeToWrite time.Duration, err error) {
	node := strconv.Itoa(int(meta.NodeID))
	if err != nil {
		m.meter.Counter(metricBrokerWriteErrors, labelNode, node).Inc()
		return
	}
	m.meter.Counter(metricBrokerWriteBytes, labelNode, node).Add(bytesWritten)
	m.meter.Histogram(metricBrokerWriteWaitLatencies, labelNode, node).Update(writeWait.Seconds())
	m.meter.Histogram(metricBrokerWriteLatencies, labelNode, node).Update(timeToWrite.Seconds())
}

func (m *metrics) OnBrokerRead(meta kgo.BrokerMetadata, _ int16, bytesRead int, readWait, timeToRead time.Duration, err error) {
	node := strconv.Itoa(int(meta.NodeID))
	if err != nil {
		m.meter.Counter(metricBrokerReadErrors, labelNode, node).Inc()
		return
	}
	m.meter.Counter(metricBrokerReadBytes, labelNode, node).Add(bytesRead)

	m.meter.Histogram(metricBrokerReadWaitLatencies, labelNode, node).Update(readWait.Seconds())
	m.meter.Histogram(metricBrokerReadLatencies, labelNode, node).Update(timeToRead.Seconds())
}

func (m *metrics) OnBrokerThrottle(meta kgo.BrokerMetadata, throttleInterval time.Duration, _ bool) {
	node := strconv.Itoa(int(meta.NodeID))
	m.meter.Histogram(metricBrokerThrottleLatencies, labelNode, node).Update(throttleInterval.Seconds())
}

func (m *metrics) OnProduceBatchWritten(meta kgo.BrokerMetadata, topic string, _ int32, kmetrics kgo.ProduceBatchMetrics) {
	node := strconv.Itoa(int(meta.NodeID))
	m.meter.Counter(metricBrokerProduceBytesUncompressed, labelNode, node, labelTopic, topic).Add(kmetrics.UncompressedBytes)
	m.meter.Counter(metricBrokerProduceBytesCompressed, labelNode, node, labelTopic, topic).Add(kmetrics.CompressedBytes)
}

func (m *metrics) OnFetchBatchRead(meta kgo.BrokerMetadata, topic string, _ int32, kmetrics kgo.FetchBatchMetrics) {
	node := strconv.Itoa(int(meta.NodeID))
	m.meter.Counter(metricBrokerFetchBytesUncompressed, labelNode, node, labelTopic, topic).Add(kmetrics.UncompressedBytes)
	m.meter.Counter(metricBrokerFetchBytesCompressed, labelNode, node, labelTopic, topic).Add(kmetrics.CompressedBytes)
}
