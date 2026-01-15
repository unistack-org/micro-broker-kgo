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

	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"go.unistack.org/micro/v3/broker"
	"go.unistack.org/micro/v3/logger"
	"go.unistack.org/micro/v3/metadata"
	"go.unistack.org/micro/v3/semconv"
	"go.unistack.org/micro/v3/tracer"
	mjitter "go.unistack.org/micro/v3/util/jitter"
	mrand "go.unistack.org/micro/v3/util/rand"
)

var _ broker.Broker = (*Broker)(nil)

var ErrLostMessage = errors.New("message not marked for offsets commit and will be lost in next iteration")

var DefaultRetryBackoffFn = func() func(int) time.Duration {
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

		jitter := 0.8 + 0.4*rand.Float64()

		backoff = time.Duration(float64(backoff) * jitter)

		if backoff > max {
			return max
		}
		return backoff
	}
}()

type Broker struct {
	c         *kgo.Client
	connected *atomic.Uint32

	done chan struct{}

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

func (b *Broker) connect(ctx context.Context, opts ...kgo.Opt) (*kgo.Client, *hookTracer, error) {
	var c *kgo.Client
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
		b.connected.Store(1)

		if fatalOnError {
			go func() {
				c := 3
				n := 0
				tc := mjitter.NewTicker(500*time.Millisecond, 1*time.Second)
				defer tc.Stop()
				for range tc.C {
					if b.connected.Load() == 0 {
						if n > c {
							b.opts.Logger.Fatal(context.Background(), "broker fatal error")
						}
						n++
					} else {
						n = 0
					}
				}
			}()
		}
		return c, htracer, nil
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
		var mu sync.Mutex
		var lastUpdate time.Time
		type pl struct {
			p string
			l float64
		}

		lag := make(map[string]map[string]pl) // topic => group => partition => lag
		ac := kadm.NewClient(b.c)

		updateStats := func() {
			mu.Lock()
			if time.Since(lastUpdate) < DefaultStatsInterval {
				mu.Unlock()
				return
			}
			mu.Unlock()

			b.mu.Lock()
			groups := make([]string, 0, len(b.subs))
			for _, g := range b.subs {
				groups = append(groups, g.opts.Group)
			}
			b.mu.Unlock()

			dgls, err := ac.Lag(ctx, groups...)
			if err != nil || !dgls.Ok() {
				b.opts.Logger.Error(b.opts.Context, "kgo describe group lag error", err)
				return
			}

			for gn, dgl := range dgls {
				for tn, lmap := range dgl.Lag {
					if _, ok := lag[tn]; !ok {
						lag[tn] = make(map[string]pl)
					}
					for p, l := range lmap {
						lag[tn][gn] = pl{p: strconv.Itoa(int(p)), l: float64(l.Lag)}
					}
				}
			}
		}

		for tn, dg := range lag {
			for gn, gl := range dg {
				b.opts.Meter.Gauge(semconv.BrokerGroupLag,
					func() float64 { updateStats(); return gl.l },
					"topic", tn,
					"group", gn,
					"partition", gl.p)
			}
		}

	}

	return nil
}

func (b *Broker) Disconnect(ctx context.Context) error {
	if b.connected.Load() == 0 {
		return nil
	}

	nctx := b.opts.Context
	if ctx != nil {
		nctx = ctx
	}
	var span tracer.Span
	ctx, span = b.opts.Tracer.Start(ctx, "Disconnect")
	defer span.Finish()

	b.mu.Lock()
	defer b.mu.Unlock()
	select {
	case <-nctx.Done():
		return nctx.Err()
	default:
		for _, sub := range b.subs {
			if sub.closed.Load() {
				continue
			}
			if err := sub.Unsubscribe(ctx); err != nil {
				return err
			}
		}
		if b.c != nil {
			b.c.CloseAllowingRebalance()
			// b.c.Close()
		}
	}

	b.connected.Store(0)
	close(b.done)
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

	b.init = true

	return nil
}

func (b *Broker) Options() broker.Options {
	return b.opts
}

func (b *Broker) BatchPublish(ctx context.Context, msgs []*broker.Message, opts ...broker.PublishOption) error {
	return b.publish(ctx, msgs, opts...)
}

func (b *Broker) Publish(ctx context.Context, topic string, msg *broker.Message, opts ...broker.PublishOption) error {
	msg.Header.Set(metadata.HeaderTopic, topic)
	return b.publish(ctx, []*broker.Message{msg}, opts...)
}

func (b *Broker) publish(ctx context.Context, msgs []*broker.Message, opts ...broker.PublishOption) error {
	options := broker.NewPublishOptions(opts...)
	records := make([]*kgo.Record, 0, len(msgs))
	var errs []string
	var err error
	var key []byte
	var promise func(*kgo.Record, error)

	if options.Context != nil {
		if k, ok := options.Context.Value(publishKey{}).([]byte); ok && k != nil {
			key = k
		}
		if p, ok := options.Context.Value(publishPromiseKey{}).(func(*kgo.Record, error)); ok && p != nil {
			promise = p
		}
	}

	for _, msg := range msgs {
		rec := &kgo.Record{Context: ctx, Key: key}

		rec.Topic, _ = msg.Header.Get(metadata.HeaderTopic)
		msg.Header.Del(metadata.HeaderTopic)

		b.opts.Meter.Counter(semconv.PublishMessageInflight, "endpoint", rec.Topic, "topic", rec.Topic).Inc()
		if options.BodyOnly || b.opts.Codec.String() == "noop" {
			rec.Value = msg.Body
			setHeaders(rec, msg.Header)
		} else {
			rec.Value, err = b.opts.Codec.Marshal(msg)
			if err != nil {
				return err
			}
		}
		records = append(records, rec)
	}

	if promise != nil {
		ts := time.Now()
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
	ts := time.Now()

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

func (b *Broker) TopicExists(ctx context.Context, topic string) error {
	mdreq := kmsg.NewMetadataRequest()
	mdreq.Topics = []kmsg.MetadataRequestTopic{
		{Topic: &topic},
	}

	mdrsp, err := mdreq.RequestWith(ctx, b.c)
	if err != nil {
		return err
	}
	if len(mdrsp.Topics) == 0 {
		return fmt.Errorf("topic %s: empty response", topic)
	}
	if mdrsp.Topics[0].ErrorCode != 0 {
		return fmt.Errorf("topic %s not exists or permission error", topic)
	}

	return nil
}

func (b *Broker) BatchSubscribe(_ context.Context, _ string, _ broker.BatchHandler, _ ...broker.SubscribeOption) (broker.Subscriber, error) {
	return nil, nil
}

func (b *Broker) Subscribe(ctx context.Context, topic string, handler broker.Handler, opts ...broker.SubscribeOption) (broker.Subscriber, error) {
	options := broker.NewSubscribeOptions(opts...)

	if options.Group == "" {
		uid, err := uuid.NewRandom()
		if err != nil {
			return nil, err
		}
		options.Group = uid.String()
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

	sub := &Subscriber{
		topic:        topic,
		opts:         options,
		handler:      handler,
		kopts:        b.opts,
		done:         make(chan struct{}),
		fatalOnError: fatalOnError,
		connected:    b.connected,
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
		k.kopts...,
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
	} else if len(mdrsp.Topics) == 0 || mdrsp.Topics[0].ErrorCode != 0 {
		return nil, fmt.Errorf("topic %s not exists or permission error", topic)
	}

	sub.c = c
	sub.htracer = htracer

	go sub.poll(ctx)

	b.mu.Lock()
	// Cleanup closed subscribers to prevent memory leak
	n := 0
	for _, s := range b.subs {
		if !s.closed.Load() {
			b.subs[n] = s
			n++
		}
	}
	b.subs = b.subs[:n]
	b.subs = append(b.subs, sub)
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
		done:      make(chan struct{}),
	}
}
