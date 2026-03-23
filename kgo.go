// Package kgo provides a kafka broker using kgo
package kgo

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
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

	mu   sync.RWMutex
	init bool
}

func (b *Broker) Live() bool {
	return b.connected.Load() == 1
}

func (b *Broker) Ready() bool {
	return b.connected.Load() == 1
}

func (b *Broker) Health() bool {
	return b.connected.Load() == 1
}

func (b *Broker) Address() string {
	return strings.Join(b.opts.Addrs, ",")
}

func (b *Broker) Name() string {
	return b.opts.Name
}

func (b *Broker) Client() *kgo.Client {
	return b.c
}

type kgoMessage struct {
	c     codec.Codec
	topic string
	ctx   context.Context
	body  []byte
	hdr   metadata.Metadata
	opts  broker.MessageOptions
	ack   bool
	err   error
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
	return m.topic
}

func (m *kgoMessage) Error() error {
	return m.err
}

func (m *kgoMessage) Unmarshal(dst interface{}, opts ...codec.Option) error {
	return m.c.Unmarshal(m.body, dst)
}

func (b *Broker) newCodec(ct string) (codec.Codec, error) {
	if idx := strings.IndexRune(ct, ';'); idx >= 0 {
		ct = ct[:idx]
	}
	b.mu.RLock()
	c, ok := b.opts.Codecs[ct]
	b.mu.RUnlock()
	if ok {
		return c, nil
	}
	return nil, codec.ErrUnknownContentType
}

func (b *Broker) NewMessage(ctx context.Context, hdr metadata.Metadata, body interface{}, opts ...broker.MessageOption) (broker.Message, error) {
	options := broker.NewMessageOptions(opts...)
	if options.ContentType == "" {
		options.ContentType = b.opts.ContentType
	}

	m := &kgoMessage{ctx: ctx, hdr: hdr.Copy(), opts: options}
	c, err := b.newCodec(m.opts.ContentType)
	if err == nil {
		m.body, err = c.Marshal(body)
	}
	if err != nil {
		return nil, err
	}

	m.hdr.Set(metadata.HeaderContentType, m.opts.ContentType)

	return m, nil
}

func (b *Broker) connect(ctx context.Context, opts ...kgo.Opt) (*kgo.Client, *hookTracer, error) {
	var ckgo *kgo.Client
	var err error

	sp, _ := tracer.SpanFromContext(ctx)

	clientID := "kgo"
	group := ""
	if b.opts.Context != nil {
		if id, ok := b.opts.Context.Value(clientIDKey{}).(string); ok {
			clientID = id
		}
		if id, ok := b.opts.Context.Value(groupKey{}).(string); ok {
			group = id
		}
	}

	var fatalOnError bool
	if b.opts.Context != nil {
		if v, ok := b.opts.Context.Value(fatalOnErrorKey{}).(bool); ok && v {
			fatalOnError = v
		}
	}

	htracer := &hookTracer{group: group, clientID: clientID, tracer: b.opts.Tracer}
	opts = append(opts,
		kgo.WithHooks(&hookMeter{meter: b.opts.Meter}),
		kgo.WithHooks(htracer),
		kgo.WithHooks(&hookEvent{log: b.opts.Logger, fatalOnError: fatalOnError, connected: b.connected}),
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
		ckgo, err = kgo.NewClient(opts...)
		if err == nil {
			err = ckgo.Ping(ctx) // check connectivity to cluster
		}
		if err != nil {
			if sp != nil {
				sp.SetStatus(tracer.SpanStatusError, err.Error())
			}
			return nil, nil, err
		}
		return ckgo, htracer, nil
	}
}

func (b *Broker) Connect(ctx context.Context) error {
	if b.connected.Load() == 1 {
		return nil
	}

	nctx := b.opts.Context
	if ctx != nil {
		nctx = ctx
	}

	c, _, err := b.connect(nctx, b.kopts...)
	if err != nil {
		return err
	}

	b.mu.Lock()
	if b.c != nil {
		// another goroutine connected concurrently
		b.mu.Unlock()
		c.CloseAllowingRebalance()
		return nil
	}
	b.c = c
	b.connected.Store(1)
	b.mu.Unlock()

	exposeLag := false
	if b.opts.Context != nil {
		if v, ok := b.opts.Context.Value(exposeLagKey{}).(bool); ok && v {
			exposeLag = v
		}
	}

	if exposeLag {
		var (
			mu          sync.Mutex
			lagValues   = make(map[string]float64)
			lagReg      = make(map[string]bool)
			lastUpdated time.Time
			refreshing  bool
		)
		ac := kadm.NewClient(b.c)

		var refresh func()
		refresh = func() {
			mu.Lock()
			if refreshing || time.Since(lastUpdated) < DefaultStatsInterval {
				mu.Unlock()
				return
			}
			refreshing = true
			mu.Unlock()

			defer func() {
				mu.Lock()
				refreshing = false
				mu.Unlock()
			}()

			b.mu.RLock()
			groups := make([]string, 0, len(b.subs))
			for _, s := range b.subs {
				groups = append(groups, s.opts.Group)
			}
			b.mu.RUnlock()

			if len(groups) == 0 {
				return
			}

			dgls, err := ac.Lag(b.opts.Context, groups...)
			if err != nil || !dgls.Ok() {
				b.opts.Logger.Error(b.opts.Context, "kgo describe group lag error", err)
				return
			}

			type entry struct{ key, tn, gn, ps string }
			var newEntries []entry

			mu.Lock()
			lastUpdated = time.Now()
			for gn, dgl := range dgls {
				for tn, lmap := range dgl.Lag {
					for p, l := range lmap {
						ps := strconv.Itoa(int(p))
						key := tn + "/" + gn + "/" + ps
						lagValues[key] = float64(l.Lag)
						if !lagReg[key] {
							lagReg[key] = true
							newEntries = append(newEntries, entry{key, tn, gn, ps})
						}
					}
				}
			}
			mu.Unlock()

			for _, e := range newEntries {
				key := e.key
				b.opts.Meter.Gauge(semconv.BrokerGroupLag,
					func() float64 {
						refresh()
						mu.Lock()
						v := lagValues[key]
						mu.Unlock()
						return v
					},
					"topic", e.tn,
					"group", e.gn,
					"partition", e.ps,
				)
			}
		}

		go refresh()
	}

	return nil
}

func (b *Broker) Disconnect(ctx context.Context) error {
	if b.connected.Load() == 0 {
		return nil
	}

	if ctx == nil {
		ctx = b.opts.Context
	}
	var span tracer.Span
	ctx, span = b.opts.Tracer.Start(ctx, "Disconnect")
	defer span.Finish()

	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	b.mu.RLock()
	subs := make([]*Subscriber, len(b.subs))
	copy(subs, b.subs)
	b.mu.RUnlock()

	for _, sub := range subs {
		sub.draining.Store(true) // in the process of stopping
	}

	var wg sync.WaitGroup
	for _, sub := range subs {
		if sub.closed.Load() {
			continue
		}
		wg.Add(1)
		go func(s *Subscriber) {
			defer wg.Done()
			_ = s.Unsubscribe(ctx)
		}(sub)
	}
	wg.Wait()

	b.mu.Lock()
	if b.c != nil {
		b.c.CloseAllowingRebalance()
	}
	b.mu.Unlock()

	b.connected.Store(0)
	return nil
}

func (b *Broker) Init(opts ...broker.Option) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if len(opts) == 0 && b.init {
		return nil
	}

	for _, o := range opts {
		o(&b.opts)
	}

	if err := b.opts.Register.Init(); err != nil {
		return err
	}
	if err := b.opts.Tracer.Init(); err != nil {
		return err
	}
	if err := b.opts.Logger.Init(); err != nil {
		return err
	}
	if err := b.opts.Meter.Init(); err != nil {
		return err
	}

	if b.opts.Context != nil {
		if v, ok := b.opts.Context.Value(optionsKey{}).([]kgo.Opt); ok && len(v) > 0 {
			b.kopts = append(b.kopts, v...)
		}
	}

	b.funcPublish = b.fnPublish
	b.funcSubscribe = b.fnSubscribe

	b.opts.Hooks.EachPrev(func(hook options.Hook) {
		switch h := hook.(type) {
		case broker.HookPublish:
			b.funcPublish = h(b.funcPublish)
		case broker.HookSubscribe:
			b.funcSubscribe = h(b.funcSubscribe)
		}
	})

	b.init = true

	return nil
}

func (b *Broker) Options() broker.Options {
	return b.opts
}

func (b *Broker) Publish(ctx context.Context, topic string, messages ...broker.Message) error {
	return b.funcPublish(ctx, topic, messages...)
}

func (b *Broker) fnPublish(ctx context.Context, topic string, messages ...broker.Message) error {
	return b.publish(ctx, topic, messages...)
}

func (b *Broker) publish(ctx context.Context, topic string, messages ...broker.Message) error {
	var records []*kgo.Record

	for _, msg := range messages {

		rec := &kgo.Record{
			Context: msg.Context(),
			Topic:   topic,
			Value:   msg.Body(),
		}

		var promise func(*kgo.Record, error)
		if rec.Context != nil {
			if k, ok := rec.Context.Value(messageKey{}).([]byte); ok && k != nil {
				rec.Key = k
			}
			if p, ok := rec.Context.Value(messagePromiseKey{}).(func(*kgo.Record, error)); ok && p != nil {
				promise = p
			}
		}

		kmsg, ok := msg.(*kgoMessage)
		if !ok {
			continue
		}
		if kmsg.opts.Context != nil {
			if k, ok := kmsg.opts.Context.Value(messageKey{}).([]byte); ok && k != nil {
				rec.Key = k
			}
			if p, ok := kmsg.opts.Context.Value(messagePromiseKey{}).(func(*kgo.Record, error)); ok && p != nil {
				promise = p
			}
		}

		setHeaders(rec, msg.Header())

		if promise != nil {
			ts := time.Now()
			b.opts.Meter.Counter(semconv.PublishMessageInflight, "endpoint", rec.Topic, "topic", rec.Topic).Inc()
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
			continue
		} else {
			records = append(records, rec)
		}
	}

	if len(records) > 0 {
		var errs []string
		ts := time.Now()
		inflightCtr := b.opts.Meter.Counter(semconv.PublishMessageInflight, "endpoint", topic, "topic", topic)
		inflightCtr.Add(len(records))
		results := b.c.ProduceSync(ctx, records...)
		te := time.Since(ts)
		for _, result := range results {
			inflightCtr.Dec()
			b.opts.Meter.Summary(semconv.PublishMessageLatencyMicroseconds, "endpoint", result.Record.Topic, "topic", result.Record.Topic).Update(te.Seconds())
			b.opts.Meter.Histogram(semconv.PublishMessageDurationSeconds, "endpoint", result.Record.Topic, "topic", result.Record.Topic).Update(te.Seconds())
			if result.Err != nil {
				b.opts.Meter.Counter(semconv.PublishMessageTotal, "endpoint", result.Record.Topic, "topic", result.Record.Topic, "status", "failure").Inc()
				errs = append(errs, result.Err.Error())
			} else {
				b.opts.Meter.Counter(semconv.PublishMessageTotal, "endpoint", result.Record.Topic, "topic", result.Record.Topic, "status", "success").Inc()
			}
		}
		// Ensure inflight counter is balanced even if results count diverges from records count.
		if remaining := len(records) - len(results); remaining > 0 {
			inflightCtr.Add(-remaining)
		}

		if len(errs) > 0 {
			return fmt.Errorf("publish error: %s", strings.Join(errs, "\n"))
		}
	}

	return nil
}

func (b *Broker) TopicExists(ctx context.Context, topic string) error {
	mdreq := kmsg.NewMetadataRequest()
	mdreq.Topics = []kmsg.MetadataRequestTopic{
		{Topic: &topic},
	}

	mdrsp, err := mdreq.RequestWith(ctx, b.c)
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

	var useMessagePool bool
	var fatalOnError bool
	if b.opts.Context != nil {
		if v, ok := b.opts.Context.Value(fatalOnErrorKey{}).(bool); ok && v {
			fatalOnError = v
		}
		if v, ok := b.opts.Context.Value(subscribeMessagePoolKey{}).(bool); ok && v {
			useMessagePool = v
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
		done:         make(chan struct{}),
		fatalOnError: fatalOnError,
		connected:    b.connected,
		messagePool:  useMessagePool,
	}
	sub.initConsumers()

	kopts := append(
		[]kgo.Opt{
			kgo.ConsumerGroup(options.Group),
			kgo.ConsumeTopics(topic),
			kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
			kgo.FetchMaxWait(1 * time.Second),
			kgo.AutoCommitInterval(commitInterval),
			kgo.OnPartitionsAssigned(sub.assigned),
			kgo.OnPartitionsRevoked(sub.revoked),
			kgo.StopProducerOnDataLossDetected(),
			kgo.OnPartitionsLost(sub.lost),
			kgo.AutoCommitCallback(sub.autocommit),
			kgo.AutoCommitMarks(),
			kgo.WithHooks(sub),
		},
		b.kopts...,
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

	go sub.poll(sub.kopts.Context)

	b.mu.Lock()
	active := b.subs[:0]
	for _, s := range b.subs {
		if !s.closed.Load() {
			active = append(active, s)
		}
	}
	b.subs = append(active, sub)
	b.mu.Unlock()

	return sub, nil
}

func (b *Broker) String() string {
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
