// Package kgo provides a kafka broker using kgo
package kgo // import "go.unistack.org/micro-broker-kgo/v3"

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	kgo "github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"github.com/twmb/franz-go/pkg/kversion"
	"go.unistack.org/micro/v3/broker"
	"go.unistack.org/micro/v3/logger"
	"go.unistack.org/micro/v3/metadata"
	"go.unistack.org/micro/v3/util/id"
	mrand "go.unistack.org/micro/v3/util/rand"
)

var _ broker.Broker = &kBroker{}

type kBroker struct {
	writer    *kgo.Client // used only to push messages
	kopts     []kgo.Opt
	connected bool
	init      bool
	sync.RWMutex
	opts broker.Options
	subs []*subscriber
}

type subscriber struct {
	reader       *kgo.Client // used only to pull messages
	topic        string
	opts         broker.SubscribeOptions
	kopts        broker.Options
	handler      broker.Handler
	batchhandler broker.BatchHandler
	closed       bool
	done         chan struct{}
	consumers    map[string]map[int32]worker
	sync.RWMutex
}

type publication struct {
	topic string
	err   error
	sync.RWMutex
	msg *broker.Message
	ack bool
}

func (p *publication) Topic() string {
	return p.topic
}

func (p *publication) Message() *broker.Message {
	return p.msg
}

func (p *publication) Ack() error {
	p.ack = true
	return nil
}

func (p *publication) Error() error {
	return p.err
}

func (p *publication) SetError(err error) {
	p.err = err
}

func (s *subscriber) Options() broker.SubscribeOptions {
	return s.opts
}

func (s *subscriber) Topic() string {
	return s.topic
}

func (s *subscriber) Unsubscribe(ctx context.Context) error {
	if s.closed {
		return nil
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		close(s.done)
		s.closed = true
	}
	return nil
}

func (k *kBroker) Address() string {
	return strings.Join(k.opts.Addrs, ",")
}

func (k *kBroker) Name() string {
	return k.opts.Name
}

func (k *kBroker) Connect(ctx context.Context) error {
	k.RLock()
	if k.connected {
		k.RUnlock()
		return nil
	}
	k.RUnlock()

	nctx := k.opts.Context
	if ctx != nil {
		nctx = ctx
	}

	kaddrs := k.opts.Addrs

	// shuffle addrs
	var rng mrand.Rand
	rng.Shuffle(len(kaddrs), func(i, j int) {
		kaddrs[i], kaddrs[j] = kaddrs[j], kaddrs[i]
	})

	kopts := append(k.kopts, kgo.SeedBrokers(kaddrs...))

	select {
	case <-nctx.Done():
		return nctx.Err()
	default:
		c, err := kgo.NewClient(kopts...)
		if err != nil {
			return err
		}

		// Request versions in order to guess Kafka Cluster version
		versionsReq := kmsg.NewApiVersionsRequest()
		versionsRes, err := versionsReq.RequestWith(ctx, c)
		if err != nil {
			return fmt.Errorf("failed to request api versions: %w", err)
		}
		err = kerr.ErrorForCode(versionsRes.ErrorCode)
		if err != nil {
			return fmt.Errorf("failed to request api versions. Inner kafka error: %w", err)
		}
		versions := kversion.FromApiVersionsResponse(versionsRes)

		if k.opts.Logger.V(logger.InfoLevel) {
			logger.Infof(ctx, "[kgo] connected to to kafka cluster version %v", versions.VersionGuess())
		}

		k.Lock()
		k.connected = true
		k.writer = c
		k.Unlock()
	}

	return nil
}

func (k *kBroker) Disconnect(ctx context.Context) error {
	k.RLock()
	if !k.connected {
		k.RUnlock()
		return nil
	}
	k.RUnlock()

	k.Lock()
	defer k.Unlock()

	nctx := k.opts.Context
	if ctx != nil {
		nctx = ctx
	}

	select {
	case <-nctx.Done():
		return nctx.Err()
	default:
		for _, sub := range k.subs {
			if err := sub.Unsubscribe(ctx); err != nil {
				return err
			}
		}
		k.writer.Close()
	}

	k.connected = false
	return nil
}

func (k *kBroker) Init(opts ...broker.Option) error {
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

	//	kgo.RecordPartitioner(),

	k.init = true

	return nil
}

func (k *kBroker) Options() broker.Options {
	return k.opts
}

func (k *kBroker) BatchPublish(ctx context.Context, msgs []*broker.Message, opts ...broker.PublishOption) error {
	return k.publish(ctx, msgs, opts...)
}

func (k *kBroker) Publish(ctx context.Context, topic string, msg *broker.Message, opts ...broker.PublishOption) error {
	msg.Header.Set(metadata.HeaderTopic, topic)
	return k.publish(ctx, []*broker.Message{msg}, opts...)
}

func (k *kBroker) publish(ctx context.Context, msgs []*broker.Message, opts ...broker.PublishOption) error {
	k.RLock()
	if !k.connected {
		k.RUnlock()
		return broker.ErrNotConnected
	}
	k.RUnlock()
	options := broker.NewPublishOptions(opts...)
	records := make([]*kgo.Record, 0, len(msgs))
	var errs []string
	var err error
	var key []byte

	if options.Context != nil {
		if k, ok := options.Context.Value(publishKey{}).([]byte); ok && k != nil {
			key = k
		}
	}

	for _, msg := range msgs {
		rec := &kgo.Record{Key: key}
		rec.Topic, _ = msg.Header.Get(metadata.HeaderTopic)
		if k.opts.Codec.String() == "noop" {
			rec.Value = msg.Body
			for k, v := range msg.Header {
				rec.Headers = append(rec.Headers, kgo.RecordHeader{Key: k, Value: []byte(v)})
			}
		} else if options.BodyOnly {
			rec.Value = msg.Body
		} else {
			rec.Value, err = k.opts.Codec.Marshal(msg)
			if err != nil {
				return err
			}
		}
		records = append(records, rec)
	}

	results := k.writer.ProduceSync(ctx, records...)
	for _, result := range results {
		if result.Err != nil {
			errs = append(errs, result.Err.Error())
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("publish error: %s", strings.Join(errs, "\n"))
	}

	return nil
}

type mlogger struct {
	l   logger.Logger
	ctx context.Context
}

func (l *mlogger) Log(lvl kgo.LogLevel, msg string, args ...interface{}) {
	var mlvl logger.Level
	switch lvl {
	case kgo.LogLevelNone:
		return
	case kgo.LogLevelError:
		mlvl = logger.ErrorLevel
	case kgo.LogLevelWarn:
		mlvl = logger.WarnLevel
	case kgo.LogLevelInfo:
		mlvl = logger.InfoLevel
	case kgo.LogLevelDebug:
		mlvl = logger.DebugLevel
	default:
		return
	}
	fields := make(map[string]interface{}, int(len(args)/2))
	for i := 0; i < len(args)/2; i += 2 {
		fields[fmt.Sprintf("%v", args[i])] = args[i+1]
	}
	l.l.Fields(fields).Log(l.ctx, mlvl, msg)
}

func (l *mlogger) Level() kgo.LogLevel {
	switch l.l.Options().Level {
	case logger.ErrorLevel:
		return kgo.LogLevelError
	case logger.WarnLevel:
		return kgo.LogLevelWarn
	case logger.InfoLevel:
		return kgo.LogLevelInfo
	case logger.DebugLevel, logger.TraceLevel:
		return kgo.LogLevelDebug
	}
	return kgo.LogLevelNone
}

func (k *kBroker) BatchSubscribe(ctx context.Context, topic string, handler broker.BatchHandler, opts ...broker.SubscribeOption) (broker.Subscriber, error) {
	return nil, nil
}

func (k *kBroker) Subscribe(ctx context.Context, topic string, handler broker.Handler, opts ...broker.SubscribeOption) (broker.Subscriber, error) {
	options := broker.NewSubscribeOptions(opts...)

	if options.Group == "" {
		uid, err := id.New()
		if err != nil {
			return nil, err
		}
		options.Group = uid
	}

	kaddrs := k.opts.Addrs

	// shuffle addrs
	var rng mrand.Rand
	rng.Shuffle(len(kaddrs), func(i, j int) {
		kaddrs[i], kaddrs[j] = kaddrs[j], kaddrs[i]
	})

	td := DefaultCommitInterval
	if k.opts.Context != nil {
		if v, ok := k.opts.Context.Value(commitIntervalKey{}).(time.Duration); ok && v > 0 {
			td = v
		}
	}

	sub := &subscriber{
		topic:     topic,
		done:      make(chan struct{}),
		opts:      options,
		handler:   handler,
		kopts:     k.opts,
		consumers: make(map[string]map[int32]worker),
	}

	kopts := append(k.kopts,
		kgo.SeedBrokers(kaddrs...),
		kgo.ConsumerGroup(options.Group),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.FetchMaxWait(1*time.Second),
		// kgo.KeepControlRecords(),
		kgo.Balancers(kgo.CooperativeStickyBalancer(), kgo.StickyBalancer()),
		kgo.FetchIsolationLevel(kgo.ReadUncommitted()),
		kgo.WithHooks(&metrics{meter: k.opts.Meter}),
		kgo.AutoCommitMarks(),
		kgo.AutoCommitInterval(td),
		kgo.OnPartitionsAssigned(sub.assigned),
		kgo.OnPartitionsRevoked(sub.revoked),
		kgo.OnPartitionsLost(sub.revoked),
	)

	reader, err := kgo.NewClient(kopts...)
	if err != nil {
		return nil, err
	}

	sub.reader = reader
	go sub.run(ctx)

	k.Lock()
	k.subs = append(k.subs, sub)
	k.Unlock()
	return sub, nil
}

func (k *kBroker) String() string {
	return "kgo"
}

func NewBroker(opts ...broker.Option) *kBroker {
	options := broker.NewOptions(opts...)
	if options.Codec.String() != "noop" {
		options.Logger.Infof(options.Context, "broker codec not noop, disable plain kafka headers usage")
	}
	kopts := []kgo.Opt{
		kgo.DisableIdempotentWrite(),
		kgo.ProducerBatchCompression(kgo.NoCompression()),
		kgo.WithLogger(&mlogger{l: options.Logger, ctx: options.Context}),
		kgo.RetryBackoffFn(
			func() func(int) time.Duration {
				var rng mrand.Rand
				return func(fails int) time.Duration {
					const (
						min = 250 * time.Millisecond
						max = 2 * time.Second
					)
					if fails <= 0 {
						return min
					}
					if fails > 10 {
						return max
					}

					backoff := min * time.Duration(1<<(fails-1))
					jitter := 0.8 + 0.4*rng.Float64()
					backoff = time.Duration(float64(backoff) * jitter)

					if backoff > max {
						return max
					}
					return backoff
				}
			}(),
		),
	}

	if options.Context != nil {
		if v, ok := options.Context.Value(optionsKey{}).([]kgo.Opt); ok && len(v) > 0 {
			kopts = append(kopts, v...)
		}
	}

	return &kBroker{
		opts:  options,
		kopts: kopts,
	}
}
