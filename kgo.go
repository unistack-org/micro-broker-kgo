// Package kgo provides a kafka broker using kgo
package kgo

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"go.unistack.org/micro/v4/broker"
	"go.unistack.org/micro/v4/codec"
	"go.unistack.org/micro/v4/logger"
	"go.unistack.org/micro/v4/metadata"
	"go.unistack.org/micro/v4/options"
	"go.unistack.org/micro/v4/semconv"
	"go.unistack.org/micro/v4/tracer"
	"go.unistack.org/micro/v4/util/id"
	mrand "go.unistack.org/micro/v4/util/rand"
)

var _ broker.Broker = (*Broker)(nil)

var messagePool = sync.Pool{
	New: func() interface{} {
		return &kgoMessage{}
	},
}

var ErrLostMessage = errors.New("message not marked for offsets commit and will be lost in next iteration")

var DefaultRetryBackoffFn = func() func(int) time.Duration {
	var rngMu sync.Mutex
	return func(fails int) time.Duration {
		const (
			min = 100 * time.Millisecond
			max = time.Second
		)
		if fails <= 0 {
			return min
		}
		if fails > 10 {
			return max
		}

		backoff := min * time.Duration(1<<(fails-1))

		rngMu.Lock()
		jitter := 0.8 + 0.4*rand.Float64()
		rngMu.Unlock()

		backoff = time.Duration(float64(backoff) * jitter)

		if backoff > max {
			return max
		}
		return backoff
	}
}()

type Broker struct {
	funcPublish   broker.FuncPublish
	funcSubscribe broker.FuncSubscribe
	c             *kgo.Client
	connected     *atomic.Uint32

	kopts []kgo.Opt
	subs  []*Subscriber

	opts broker.Options

	sync.RWMutex
	init bool
}

func (r *Broker) Live() bool {
	return r.connected.Load() == 1
}

func (r *Broker) Ready() bool {
	return r.connected.Load() == 1
}

func (r *Broker) Health() bool {
	return r.connected.Load() == 1
}

func (k *Broker) Address() string {
	return strings.Join(k.opts.Addrs, ",")
}

func (k *Broker) Name() string {
	return k.opts.Name
}

func (k *Broker) Client() *kgo.Client {
	return k.c
}

type kgoMessage struct {
	c     codec.Codec
	topic string
	ctx   context.Context
	body  []byte
	hdr   metadata.Metadata
	opts  broker.PublishOptions
	ack   bool
}

func (m *kgoMessage) Ack() error {
	m.ack = true
	return nil
}

func (m *kgoMessage) Body() []byte {
	return m.body
}

func (m *kgoMessage) Header() metadata.Metadata {
	return m.hdr
}

func (m *kgoMessage) Context() context.Context {
	return m.ctx
}

func (m *kgoMessage) Topic() string {
	return ""
}

func (m *kgoMessage) Unmarshal(dst interface{}, opts ...codec.Option) error {
	return m.c.Unmarshal(m.body, dst)
}

func (b *Broker) newCodec(ct string) (codec.Codec, error) {
	if idx := strings.IndexRune(ct, ';'); idx >= 0 {
		ct = ct[:idx]
	}
	b.RLock()
	c, ok := b.opts.Codecs[ct]
	b.RUnlock()
	if ok {
		return c, nil
	}
	return nil, codec.ErrUnknownContentType
}

func (b *Broker) NewMessage(ctx context.Context, hdr metadata.Metadata, body interface{}, opts ...broker.PublishOption) (broker.Message, error) {
	options := broker.NewPublishOptions(opts...)
	if options.ContentType == "" {
		options.ContentType = b.opts.ContentType
	}

	m := &kgoMessage{ctx: ctx, hdr: hdr, opts: options}
	c, err := b.newCodec(m.opts.ContentType)
	if err == nil {
		m.body, err = c.Marshal(body)
	}
	if err != nil {
		return nil, err
	}

	return m, nil
}

func (k *Broker) connect(ctx context.Context, opts ...kgo.Opt) (*kgo.Client, *hookTracer, error) {
	var c *kgo.Client
	var err error

	sp, _ := tracer.SpanFromContext(ctx)

	clientID := "kgo"
	group := ""
	if k.opts.Context != nil {
		if id, ok := k.opts.Context.Value(clientIDKey{}).(string); ok {
			clientID = id
		}
		if id, ok := k.opts.Context.Value(groupKey{}).(string); ok {
			group = id
		}
	}

	var fatalOnError bool
	if k.opts.Context != nil {
		if v, ok := k.opts.Context.Value(fatalOnErrorKey{}).(bool); ok && v {
			fatalOnError = v
		}
	}

	htracer := &hookTracer{group: group, clientID: clientID, tracer: k.opts.Tracer}
	opts = append(opts,
		kgo.WithHooks(&hookMeter{meter: k.opts.Meter}),
		kgo.WithHooks(htracer),
		kgo.WithHooks(&hookEvent{log: k.opts.Logger, fatalOnError: fatalOnError, connected: k.connected}),
	)

	select {
	case <-ctx.Done():
		if ctx.Err() != nil {
			if sp != nil {
				sp.SetStatus(tracer.SpanStatusError, ctx.Err().Error())
			}
		}
		return nil, nil, ctx.Err()
	default:
		c, err = kgo.NewClient(opts...)
		if err == nil {
			err = c.Ping(ctx) // check connectivity to cluster
		}
		if err != nil {
			if sp != nil {
				sp.SetStatus(tracer.SpanStatusError, err.Error())
			}
			return nil, nil, err
		}
		k.connected.Store(1)
		return c, htracer, nil
	}
}

func (k *Broker) Connect(ctx context.Context) error {
	if k.connected.Load() == 1 {
		return nil
	}

	nctx := k.opts.Context
	if ctx != nil {
		nctx = ctx
	}

	c, _, err := k.connect(nctx, k.kopts...)
	if err != nil {
		return err
	}

	k.Lock()
	k.c = c
	k.connected.Store(1)
	k.Unlock()

	return nil
}

func (k *Broker) Disconnect(ctx context.Context) error {
	if k.connected.Load() == 0 {
		return nil
	}

	nctx := k.opts.Context
	if ctx != nil {
		nctx = ctx
	}
	var span tracer.Span
	ctx, span = k.opts.Tracer.Start(ctx, "Disconnect")
	defer span.Finish()

	k.Lock()
	defer k.Unlock()
	select {
	case <-nctx.Done():
		return nctx.Err()
	default:
		for _, sub := range k.subs {
			if sub.closed {
				continue
			}
			if err := sub.Unsubscribe(ctx); err != nil {
				return err
			}
		}
		if k.c != nil {
			k.c.CloseAllowingRebalance()
			// k.c.Close()
		}
	}

	k.connected.Store(0)
	return nil
}

func (k *Broker) Init(opts ...broker.Option) error {
	k.Lock()
	defer k.Unlock()

	if len(opts) == 0 && k.init {
		return nil
	}

	for _, o := range opts {
		o(&k.opts)
	}

	if err := k.opts.Register.Init(); err != nil {
		return err
	}
	if err := k.opts.Tracer.Init(); err != nil {
		return err
	}
	if err := k.opts.Logger.Init(); err != nil {
		return err
	}
	if err := k.opts.Meter.Init(); err != nil {
		return err
	}

	if k.opts.Context != nil {
		if v, ok := k.opts.Context.Value(optionsKey{}).([]kgo.Opt); ok && len(v) > 0 {
			k.kopts = append(k.kopts, v...)
		}
	}

	k.funcPublish = k.fnPublish
	k.funcSubscribe = k.fnSubscribe

	k.opts.Hooks.EachPrev(func(hook options.Hook) {
		switch h := hook.(type) {
		case broker.HookPublish:
			k.funcPublish = h(k.funcPublish)
		case broker.HookSubscribe:
			k.funcSubscribe = h(k.funcSubscribe)
		}
	})

	k.init = true

	return nil
}

func (k *Broker) Options() broker.Options {
	return k.opts
}

func (b *Broker) Publish(ctx context.Context, topic string, messages ...broker.Message) error {
	return b.funcPublish(ctx, topic, messages...)
}

func (b *Broker) fnPublish(ctx context.Context, topic string, messages ...broker.Message) error {
	return b.publish(ctx, topic, messages...)
}

func (b *Broker) publish(ctx context.Context, topic string, messages ...broker.Message) error {
	if b.connected.Load() == 0 {
		c, _, err := b.connect(ctx, b.kopts...)
		if err != nil {
			return err
		}
		b.Lock()
		b.c = c
		b.Unlock()
	}

	records := make([]*kgo.Record, 0, len(messages))
	var errs []string
	var key []byte
	var promise func(*kgo.Record, error)

	for _, msg := range messages {

		if mctx := msg.Context(); mctx != nil {
			if k, ok := mctx.Value(publishKey{}).([]byte); ok && k != nil {
				key = k
			}
			if p, ok := mctx.Value(publishPromiseKey{}).(func(*kgo.Record, error)); ok && p != nil {
				promise = p
			}
		}

		rec := &kgo.Record{
			Context: ctx,
			Key:     key,
			Topic:   topic,
			Value:   msg.Body(),
		}

		b.opts.Meter.Counter(semconv.PublishMessageInflight, "endpoint", rec.Topic, "topic", rec.Topic).Inc()

		setHeaders(rec, msg.Header())

		records = append(records, rec)
	}

	ts := time.Now()

	if promise != nil {

		for _, rec := range records {
			b.c.Produce(ctx, rec, func(r *kgo.Record, err error) {
				te := time.Since(ts)
				b.opts.Meter.Counter(semconv.PublishMessageInflight, "endpoint", rec.Topic, "topic", rec.Topic).Dec()
				b.opts.Meter.Summary(semconv.PublishMessageLatencyMicroseconds, "endpoint", rec.Topic, "topic", rec.Topic).Update(te.Seconds())
				b.opts.Meter.Histogram(semconv.PublishMessageDurationSeconds, "endpoint", rec.Topic, "topic", rec.Topic).Update(te.Seconds())
				if err != nil {
					b.opts.Meter.Counter(semconv.PublishMessageTotal, "endpoint", rec.Topic, "topic", rec.Topic, "status", "failure").Inc()
				} else {
					b.opts.Meter.Counter(semconv.PublishMessageTotal, "endpoint", rec.Topic, "topic", rec.Topic, "status", "success").Inc()
				}
				promise(r, err)
			})
		}
		return nil
	}

	results := b.c.ProduceSync(ctx, records...)

	te := time.Since(ts)
	for _, result := range results {
		b.opts.Meter.Summary(semconv.PublishMessageLatencyMicroseconds, "endpoint", result.Record.Topic, "topic", result.Record.Topic).Update(te.Seconds())
		b.opts.Meter.Histogram(semconv.PublishMessageDurationSeconds, "endpoint", result.Record.Topic, "topic", result.Record.Topic).Update(te.Seconds())
		b.opts.Meter.Counter(semconv.PublishMessageInflight, "endpoint", result.Record.Topic, "topic", result.Record.Topic).Dec()
		if result.Err != nil {
			b.opts.Meter.Counter(semconv.PublishMessageTotal, "endpoint", result.Record.Topic, "topic", result.Record.Topic, "status", "failure").Inc()
			errs = append(errs, result.Err.Error())
		} else {
			b.opts.Meter.Counter(semconv.PublishMessageTotal, "endpoint", result.Record.Topic, "topic", result.Record.Topic, "status", "success").Inc()
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("publish error: %s", strings.Join(errs, "\n"))
	}

	return nil
}

func (k *Broker) TopicExists(ctx context.Context, topic string) error {
	mdreq := kmsg.NewMetadataRequest()
	mdreq.Topics = []kmsg.MetadataRequestTopic{
		{Topic: &topic},
	}

	mdrsp, err := mdreq.RequestWith(ctx, k.c)
	if err != nil {
		return err
	} else if mdrsp.Topics[0].ErrorCode != 0 {
		return fmt.Errorf("topic %s not exists or permission error", topic)
	}

	return nil
}

func (b *Broker) Subscribe(ctx context.Context, topic string, handler interface{}, opts ...broker.SubscribeOption) (broker.Subscriber, error) {
	return b.funcSubscribe(ctx, topic, handler, opts...)
}

func (b *Broker) fnSubscribe(ctx context.Context, topic string, handler interface{}, opts ...broker.SubscribeOption) (broker.Subscriber, error) {
	if err := broker.IsValidHandler(handler); err != nil {
		return nil, err
	}

	options := broker.NewSubscribeOptions(opts...)

	switch handler.(type) {
	default:
		return nil, broker.ErrInvalidHandler
	case func(broker.Message) error:
		break
	case func([]broker.Message) error:
		break
	}

	if options.Group == "" {
		uid, err := id.New()
		if err != nil {
			return nil, err
		}
		options.Group = uid
	}

	commitInterval := DefaultCommitInterval
	if b.opts.Context != nil {
		if v, ok := b.opts.Context.Value(commitIntervalKey{}).(time.Duration); ok && v > 0 {
			commitInterval = v
		}
	}

	var fatalOnError bool
	if b.opts.Context != nil {
		if v, ok := b.opts.Context.Value(fatalOnErrorKey{}).(bool); ok && v {
			fatalOnError = v
		}
	}

	if options.Context != nil {
		if v, ok := options.Context.Value(fatalOnErrorKey{}).(bool); ok && v {
			fatalOnError = v
		}
	}

	sub := &Subscriber{
		topic:        topic,
		opts:         options,
		handler:      handler,
		kopts:        b.opts,
		consumers:    make(map[tp]*consumer),
		done:         make(chan struct{}),
		fatalOnError: fatalOnError,
		connected:    b.connected,
	}

	kopts := append(b.kopts,
		kgo.ConsumerGroup(options.Group),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.FetchMaxWait(1*time.Second),
		kgo.AutoCommitInterval(commitInterval),
		kgo.OnPartitionsAssigned(sub.assigned),
		kgo.OnPartitionsRevoked(sub.revoked),
		kgo.StopProducerOnDataLossDetected(),
		kgo.OnPartitionsLost(sub.lost),
		kgo.AutoCommitCallback(sub.autocommit),
		kgo.AutoCommitMarks(),
	)

	if options.Context != nil {
		if v, ok := options.Context.Value(optionsKey{}).([]kgo.Opt); ok && len(v) > 0 {
			kopts = append(kopts, v...)
		}
	}

	c, htracer, err := b.connect(ctx, kopts...)
	if err != nil {
		return nil, err
	}

	mdreq := kmsg.NewMetadataRequest()
	mdreq.Topics = []kmsg.MetadataRequestTopic{
		{Topic: &topic},
	}

	mdrsp, err := mdreq.RequestWith(ctx, c)
	if err != nil {
		return nil, err
	} else if mdrsp.Topics[0].ErrorCode != 0 {
		return nil, fmt.Errorf("topic %s not exists or permission error", topic)
	}

	sub.c = c
	sub.htracer = htracer

	go sub.poll(ctx)

	b.Lock()
	b.subs = append(b.subs, sub)
	b.Unlock()

	return sub, nil
}

func (k *Broker) String() string {
	return "kgo"
}

func NewBroker(opts ...broker.Option) *Broker {
	options := broker.NewOptions(opts...)

	kaddrs := options.Addrs
	// shuffle addrs
	var rng mrand.Rand
	rng.Shuffle(len(kaddrs), func(i, j int) {
		kaddrs[i], kaddrs[j] = kaddrs[j], kaddrs[i]
	})
	kopts := []kgo.Opt{
		kgo.DialTimeout(3 * time.Second),
		kgo.DisableIdempotentWrite(),
		kgo.ProducerBatchCompression(kgo.NoCompression()),
		kgo.WithLogger(&mlogger{l: options.Logger.Clone(logger.WithAddCallerSkipCount(2)), ctx: options.Context}),
		kgo.SeedBrokers(kaddrs...),
		kgo.RetryBackoffFn(DefaultRetryBackoffFn),
		kgo.BlockRebalanceOnPoll(),
		kgo.Balancers(kgo.CooperativeStickyBalancer()),
		kgo.FetchIsolationLevel(kgo.ReadUncommitted()),
		kgo.UnknownTopicRetries(1),
	}

	if options.Context != nil {
		if v, ok := options.Context.Value(optionsKey{}).([]kgo.Opt); ok && len(v) > 0 {
			kopts = append(kopts, v...)
		}
	}

	return &Broker{
		connected: &atomic.Uint32{},
		opts:      options,
		kopts:     kopts,
	}
}
