package kgo

import (
	"context"
	"unicode/utf8"

	"github.com/twmb/franz-go/pkg/kgo"
	semconv "go.opentelemetry.io/otel/semconv/v1.18.0"
	"go.unistack.org/micro/v3/metadata"
	"go.unistack.org/micro/v3/tracer"
)

type hookTracer struct {
	clientID string
	group    string
	tracer   tracer.Tracer
}

var (
	_ kgo.HookProduceRecordBuffered   = (*hookTracer)(nil)
	_ kgo.HookProduceRecordUnbuffered = (*hookTracer)(nil)
	_ kgo.HookFetchRecordBuffered     = (*hookTracer)(nil)
	_ kgo.HookFetchRecordUnbuffered   = (*hookTracer)(nil)
)

// OnProduceRecordBuffered starts a new span for the "publish" operation on a
// buffered record.
//
// It sets span options and injects the span context into record and updates
// the record's context, so it can be ended in the OnProduceRecordUnbuffered
// hook.
func (m *hookTracer) OnProduceRecordBuffered(r *kgo.Record) {
	// Set up span options.
	attrs := []interface{}{
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
		tracer.WithSpanLabels(attrs...),
		tracer.WithSpanKind(tracer.SpanKindProducer),
	}

	if r.Context == nil {
		r.Context = context.Background()
	}

	md, ok := metadata.FromOutgoingContext(r.Context)
	if !ok {
		md = metadata.New(len(r.Headers))
	}
	for _, h := range r.Headers {
		md.Set(h.Key, string(h.Value))
	}

	if !ok {
		r.Context, _ = m.tracer.Start(metadata.NewOutgoingContext(r.Context, md), r.Topic+" publish", opts...)
	} else {
		r.Context, _ = m.tracer.Start(r.Context, r.Topic+" publish", opts...)
	}

	md, _ = metadata.FromOutgoingContext(r.Context)

	setHeaders(r, md)
}

// OnProduceRecordUnbuffered continues and ends the "publish" span for an
// unbuffered record.
//
// It sets attributes with values unset when producing and records any error
// that occurred during the publish operation.
func (m *hookTracer) OnProduceRecordUnbuffered(r *kgo.Record, err error) {
	if span, ok := tracer.SpanFromContext(r.Context); ok {
		span.AddLabels(
			semconv.MessagingKafkaDestinationPartition(int(r.Partition)),
		)
		if err != nil {
			span.SetStatus(tracer.SpanStatusError, err.Error())
		}
		span.Finish()
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
	attrs := []interface{}{
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
		tracer.WithSpanLabels(attrs...),
		tracer.WithSpanKind(tracer.SpanKindConsumer),
	}

	if r.Context == nil {
		r.Context = context.Background()
	}
	md, ok := metadata.FromIncomingContext(r.Context)
	if !ok {
		md = metadata.New(len(r.Headers))
	}
	for _, h := range r.Headers {
		md.Set(h.Key, string(h.Value))
	}

	if !ok {
		r.Context, _ = m.tracer.Start(metadata.NewIncomingContext(r.Context, md), r.Topic+" receive", opts...)
	} else {
		r.Context, _ = m.tracer.Start(r.Context, r.Topic+" receive", opts...)
	}

	md, _ = metadata.FromIncomingContext(r.Context)

	setHeaders(r, md)
}

// OnFetchRecordUnbuffered continues and ends the "receive" span for an
// unbuffered record.
func (m *hookTracer) OnFetchRecordUnbuffered(r *kgo.Record, _ bool) {
	span, _ := tracer.SpanFromContext(r.Context)
	span.Finish()
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
	attrs := []interface{}{
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
		tracer.WithSpanLabels(attrs...),
		tracer.WithSpanKind(tracer.SpanKindConsumer),
	}

	if r.Context == nil {
		r.Context = context.Background()
	}
	md, ok := metadata.FromIncomingContext(r.Context)
	if !ok {
		md = metadata.New(len(r.Headers))
	}
	for _, h := range r.Headers {
		md.Set(h.Key, string(h.Value))
	}

	// Start a new span using the provided context and options.
	return m.tracer.Start(r.Context, r.Topic+" process", opts...)
}

func maybeKeyAttr(attrs []interface{}, r *kgo.Record) []interface{} {
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
