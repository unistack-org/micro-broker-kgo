package kgo

import (
	"context"
	"net"
	"time"
	"unicode/utf8"

	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel/attribute"
	semconv "go.opentelemetry.io/otel/semconv/v1.18.0"
	"go.unistack.org/micro/v3/tracer"
)

type hookTracer struct {
	clientID string
	group    string
	tracer   tracer.Tracer
}

var (
	_ kgo.HookBrokerConnect       = &hookTracer{}
	_ kgo.HookBrokerDisconnect    = &hookTracer{}
	_ kgo.HookBrokerRead          = &hookTracer{}
	_ kgo.HookBrokerThrottle      = &hookTracer{}
	_ kgo.HookBrokerWrite         = &hookTracer{}
	_ kgo.HookFetchBatchRead      = &hookTracer{}
	_ kgo.HookProduceBatchWritten = &hookTracer{}
	_ kgo.HookGroupManageError    = &hookTracer{}
)

func (m *hookTracer) OnGroupManageError(err error) {
}

func (m *hookTracer) OnBrokerConnect(meta kgo.BrokerMetadata, _ time.Duration, _ net.Conn, err error) {
}

func (m *hookTracer) OnBrokerDisconnect(meta kgo.BrokerMetadata, _ net.Conn) {
}

func (m *hookTracer) OnBrokerWrite(meta kgo.BrokerMetadata, _ int16, bytesWritten int, writeWait, timeToWrite time.Duration, err error) {
}

func (m *hookTracer) OnBrokerRead(meta kgo.BrokerMetadata, _ int16, bytesRead int, readWait, timeToRead time.Duration, err error) {
}

func (m *hookTracer) OnBrokerThrottle(meta kgo.BrokerMetadata, throttleInterval time.Duration, _ bool) {
}

func (m *hookTracer) OnProduceBatchWritten(meta kgo.BrokerMetadata, topic string, _ int32, kmetrics kgo.ProduceBatchMetrics) {
}

func (m *hookTracer) OnFetchBatchRead(meta kgo.BrokerMetadata, topic string, _ int32, kmetrics kgo.FetchBatchMetrics) {
}

// OnProduceRecordBuffered starts a new span for the "publish" operation on a
// buffered record.
//
// It sets span options and injects the span context into record and updates
// the record's context, so it can be ended in the OnProduceRecordUnbuffered
// hook.
func (m *hookTracer) OnProduceRecordBuffered(r *kgo.Record) {
	// Set up span options.
	attrs := []attribute.KeyValue{
		semconv.MessagingSystemKey.String("kafka"),
		semconv.MessagingDestinationKindTopic,
		semconv.MessagingDestinationName(r.Topic),
		semconv.MessagingOperationPublish,
	}
	attrs = maybeKeyAttr(attrs, r)
	if m.clientID != "" {
		attrs = append(attrs, semconv.MessagingKafkaClientIDKey.String(m.clientID))
	}
	opts := []tracer.SpanOption{
		tracer.WithSpanLabels(otel2Micro(attrs...)),
		tracer.WithSpanKind(tracer.SpanKindProducer),
	}
	// Start the "publish" span.
	ctx, _ := m.tracer.Start(r.Context, r.Topic+" publish", opts...)
	// Inject the span context into the record.
	// t.propagators.Inject(ctx, NewRecordCarrier(r))
	// Update the record context.
	r.Context = ctx
}

// OnProduceRecordUnbuffered continues and ends the "publish" span for an
// unbuffered record.
//
// It sets attributes with values unset when producing and records any error
// that occurred during the publish operation.
func (m *hookTracer) OnProduceRecordUnbuffered(r *kgo.Record, err error) {
	span, ok := tracer.SpanFromContext(r.Context)
	if !ok {
		return
	}
	defer span.Finish()
	span.AddLabels(
		semconv.MessagingKafkaDestinationPartition(int(r.Partition)),
	)
	if err != nil {
		span.SetStatus(tracer.SpanStatusError, err.Error())
	}
}

// OnFetchRecordBuffered starts a new span for the "receive" operation on a
// buffered record.
//
// It sets the span options and extracts the span context from the record,
// updates the record's context to ensure it can be ended in the
// OnFetchRecordUnbuffered hook and can be used in downstream consumer
// processing.
func (m *hookTracer) OnFetchRecordBuffered(r *kgo.Record) {
	// Set up the span options.
	attrs := []attribute.KeyValue{
		semconv.MessagingSystemKey.String("kafka"),
		semconv.MessagingSourceKindTopic,
		semconv.MessagingSourceName(r.Topic),
		semconv.MessagingOperationReceive,
		semconv.MessagingKafkaSourcePartition(int(r.Partition)),
	}
	attrs = maybeKeyAttr(attrs, r)
	if m.clientID != "" {
		attrs = append(attrs, semconv.MessagingKafkaClientIDKey.String(m.clientID))
	}
	if m.group != "" {
		attrs = append(attrs, semconv.MessagingKafkaConsumerGroupKey.String(m.group))
	}
	opts := []tracer.SpanOption{
		tracer.WithSpanLabels(otel2Micro(attrs...)),
		tracer.WithSpanKind(tracer.SpanKindConsumer),
	}

	if r.Context == nil {
		r.Context = context.Background()
	}
	// Extract the span context from the record.
	// ctx := t.propagators.Extract(r.Context, NewRecordCarrier(r))
	// Start the "receive" span.
	newCtx, _ := m.tracer.Start(r.Context, r.Topic+" receive", opts...)
	// Update the record context.
	r.Context = newCtx
}

// OnFetchRecordUnbuffered continues and ends the "receive" span for an
// unbuffered record.
func (m *hookTracer) OnFetchRecordUnbuffered(r *kgo.Record, _ bool) {
	if span, ok := tracer.SpanFromContext(r.Context); ok {
		defer span.Finish()
	}
}

// WithProcessSpan starts a new span for the "process" operation on a consumer
// record.
//
// It sets up the span options. The user's application code is responsible for
// ending the span.
//
// This should only ever be called within a polling loop of a consumed record and
// not a record which has been created for producing, so call this at the start of each
// iteration of your processing for the record.
func (m *hookTracer) WithProcessSpan(r *kgo.Record) (context.Context, tracer.Span) {
	// Set up the span options.
	attrs := []attribute.KeyValue{
		semconv.MessagingSystemKey.String("kafka"),
		semconv.MessagingSourceKindTopic,
		semconv.MessagingSourceName(r.Topic),
		semconv.MessagingOperationProcess,
		semconv.MessagingKafkaSourcePartition(int(r.Partition)),
		semconv.MessagingKafkaMessageOffset(int(r.Offset)),
	}
	attrs = maybeKeyAttr(attrs, r)
	if m.clientID != "" {
		attrs = append(attrs, semconv.MessagingKafkaClientIDKey.String(m.clientID))
	}
	if m.group != "" {
		attrs = append(attrs, semconv.MessagingKafkaConsumerGroupKey.String(m.group))
	}
	opts := []tracer.SpanOption{
		tracer.WithSpanLabels(otel2Micro(attrs...)),
		tracer.WithSpanKind(tracer.SpanKindConsumer),
	}

	if r.Context == nil {
		r.Context = context.Background()
	}
	// Start a new span using the provided context and options.
	return m.tracer.Start(r.Context, r.Topic+" process", opts...)
}

func maybeKeyAttr(attrs []attribute.KeyValue, r *kgo.Record) []attribute.KeyValue {
	if r.Key == nil {
		return attrs
	}
	var keykey string
	if !utf8.Valid(r.Key) {
		return attrs
	}
	keykey = string(r.Key)
	return append(attrs, semconv.MessagingKafkaMessageKeyKey.String(keykey))
}

func otel2Micro(attrs ...attribute.KeyValue) []interface{} {
	ret := make([]interface{}, 0, len(attrs))
	for _, a := range attrs {
		ret = append(ret, a.Key, a.Value)
	}
	return ret
}
