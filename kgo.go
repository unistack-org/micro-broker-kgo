// Package kgo provides a kafka broker using kgo
package kgo

import (
	"context"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	kerr "github.com/twmb/franz-go/pkg/kerr"
	kgo "github.com/twmb/franz-go/pkg/kgo"
	kmsg "github.com/twmb/franz-go/pkg/kmsg"
	"github.com/unistack-org/micro/v3/broker"
	"github.com/unistack-org/micro/v3/logger"
	"github.com/unistack-org/micro/v3/metadata"
	"github.com/unistack-org/micro/v3/util/id"
	mrand "github.com/unistack-org/micro/v3/util/rand"
	"golang.org/x/sync/errgroup"
)

var pPool = sync.Pool{
	New: func() interface{} {
		return &publication{msg: &broker.Message{}}
	},
}

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
	sync.RWMutex
}

type publication struct {
	topic string
	err   error
	sync.RWMutex
	msg *broker.Message
	ack bool
}

func init() {
	rand.Seed(time.Now().UnixNano())
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
	rand.Shuffle(len(kaddrs), func(i, j int) {
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
	options := broker.NewPublishOptions(opts...)
	records := make([]*kgo.Record, 0, len(msgs))
	var errs []string
	var err error
	var buf []byte

	for _, msg := range msgs {
		if options.BodyOnly {
			buf = msg.Body
		} else {
			buf, err = k.opts.Codec.Marshal(msg)
			if err != nil {
				return err
			}
		}
		topic, _ := msg.Header.Get(metadata.HeaderTopic)
		rec := &kgo.Record{Value: buf, Topic: topic}
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
	rand.Shuffle(len(kaddrs), func(i, j int) {
		kaddrs[i], kaddrs[j] = kaddrs[j], kaddrs[i]
	})

	kopts := append(k.kopts,
		kgo.SeedBrokers(kaddrs...),
		kgo.ConsumerGroup(options.Group),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.DisableAutoCommit(),
		kgo.FetchMaxWait(1*time.Second),
		// kgo.KeepControlRecords(),
		kgo.Balancers(kgo.CooperativeStickyBalancer(), kgo.StickyBalancer()),
		kgo.FetchIsolationLevel(kgo.ReadUncommitted()),
	//	kgo.WithHooks(&metrics{meter: k.opts.Meter}),
	// TODO: must set https://pkg.go.dev/github.com/twmb/franz-go/pkg/kgo#OnRevoked
	)

	reader, err := kgo.NewClient(kopts...)
	if err != nil {
		return nil, err
	}

	sub := &subscriber{topic: topic, done: make(chan struct{}), opts: options, reader: reader, handler: handler, kopts: k.opts}
	go sub.run(ctx)

	k.Lock()
	k.subs = append(k.subs, sub)
	k.Unlock()
	return sub, nil
}

func (s *subscriber) run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-s.kopts.Context.Done():
			return
		default:
			fetches := s.reader.PollFetches(ctx)
			if fetches.IsClientClosed() {
				// TODO: fatal ?
				return
			}
			if len(fetches.Errors()) > 0 {
				for _, err := range fetches.Errors() {
					s.kopts.Logger.Errorf(ctx, "fetch err topic %s partition %d: %v", err.Topic, err.Partition, err.Err)
				}
				// TODO: fatal ?
				return
			}

			if err := s.handleFetches(ctx, fetches); err != nil {
				s.kopts.Logger.Errorf(ctx, "fetch handler err: %v", err)
				// TODO: fatal ?
				// return
			}
		}
	}
}

func (s *subscriber) handleFetches(ctx context.Context, fetches kgo.Fetches) error {
	var err error

	eh := s.kopts.ErrorHandler
	if s.opts.ErrorHandler != nil {
		eh = s.opts.ErrorHandler
	}

	var mu sync.Mutex

	done := int32(0)
	doneCh := make(chan struct{})
	g, gctx := errgroup.WithContext(ctx)

	td := DefaultCommitInterval
	if s.kopts.Context != nil {
		if v, ok := s.kopts.Context.Value(commitIntervalKey{}).(time.Duration); ok && v > 0 {
			td = v
		}
	}

	// ticker for commit offsets
	ticker := time.NewTicker(td)
	defer ticker.Stop()

	offsets := make(map[string]map[int32]kgo.EpochOffset)
	offsets[s.topic] = make(map[int32]kgo.EpochOffset)

	fillOffsets := func(off map[string]map[int32]kgo.EpochOffset, rec *kgo.Record) {
		mu.Lock()
		if at, ok := off[s.topic][rec.Partition]; ok {
			if at.Epoch > rec.LeaderEpoch || at.Epoch == rec.LeaderEpoch && at.Offset > rec.Offset {
				mu.Unlock()
				return
			}
		}
		off[s.topic][rec.Partition] = kgo.EpochOffset{Epoch: rec.LeaderEpoch, Offset: rec.Offset + 1}
		mu.Unlock()
	}

	commitOffsets := func(cl *kgo.Client, ctx context.Context, off map[string]map[int32]kgo.EpochOffset) error {
		var rerr error

		mu.Lock()
		offsets := off
		mu.Unlock()

		cl.CommitOffsetsSync(ctx, offsets, func(_ *kgo.Client, _ *kmsg.OffsetCommitRequest, resp *kmsg.OffsetCommitResponse, err error) {
			if err != nil {
				rerr = err
				return
			}

			for _, topic := range resp.Topics {
				for _, partition := range topic.Partitions {
					if err := kerr.ErrorForCode(partition.ErrorCode); err != nil {
						rerr = err
						return
					}
				}
			}
		})

		return rerr
	}

	go func() {
		for {
			select {
			case <-gctx.Done():
				return
			case <-s.done:
				atomic.StoreInt32(&done, 1)
				if err := commitOffsets(s.reader, ctx, offsets); err != nil && s.kopts.Logger.V(logger.ErrorLevel) {
					s.kopts.Logger.Errorf(s.kopts.Context, "[kgo]: failed to commit offsets: %v", err)
				}
				return
			case <-doneCh:
				return
			case <-ticker.C:
				if err := commitOffsets(s.reader, ctx, offsets); err != nil {
					if s.kopts.Logger.V(logger.ErrorLevel) {
						s.kopts.Logger.Errorf(s.kopts.Context, "[kgo]: failed to commit offsets: %v", err)
					}
					return
				}
			}
		}
	}()

	for _, fetch := range fetches {
		for _, ftopic := range fetch.Topics {
			for _, partition := range ftopic.Partitions {
				precords := partition.Records
				g.Go(func() error {
					for _, record := range precords {
						if atomic.LoadInt32(&done) == 1 {
							return nil
						}
						p := pPool.Get().(*publication)
						p.msg.Header = nil
						p.msg.Body = nil
						p.topic = s.topic
						p.err = nil
						p.ack = false
						if s.opts.BodyOnly {
							p.msg.Body = record.Value
						} else {
							if err := s.kopts.Codec.Unmarshal(record.Value, p.msg); err != nil {
								p.err = err
								p.msg.Body = record.Value
								if eh != nil {
									_ = eh(p)
									if p.ack {
										fillOffsets(offsets, record)
									}
									pPool.Put(p)
									continue
								} else {
									if s.kopts.Logger.V(logger.ErrorLevel) {
										s.kopts.Logger.Errorf(s.kopts.Context, "[kgo]: failed to unmarshal: %v", err)
									}
								}
								pPool.Put(p)
								return err
							}
						}
						err = s.handler(p)
						if err == nil && s.opts.AutoAck {
							p.ack = true
						} else if err != nil {
							p.err = err
							if eh != nil {
								_ = eh(p)
							} else {
								if s.kopts.Logger.V(logger.ErrorLevel) {
									s.kopts.Logger.Errorf(s.kopts.Context, "[kgo]: subscriber error: %v", err)
								}
							}
						}
						if p.ack {
							fillOffsets(offsets, record)
						}
						pPool.Put(p)
					}
					return nil
				})
			}
			if err := g.Wait(); err != nil {
				return err
			}
		}
	}

	close(doneCh)

	return commitOffsets(s.reader, ctx, offsets)
}

func (k *kBroker) String() string {
	return "kgo"
}

func NewBroker(opts ...broker.Option) broker.Broker {
	options := broker.NewOptions(opts...)
	kopts := []kgo.Opt{
		kgo.BatchCompression(kgo.NoCompression()),
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
