// Package kgo provides a kafka broker using kgo
package kgo

import (
	"context"
	"fmt"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	kgo "github.com/twmb/franz-go/pkg/kgo"
	sasl "github.com/twmb/franz-go/pkg/sasl"
	"github.com/unistack-org/micro/v3/broker"
	"github.com/unistack-org/micro/v3/logger"
	"github.com/unistack-org/micro/v3/metadata"
)

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
}

func (p *publication) Topic() string {
	return p.topic
}

func (p *publication) Message() *broker.Message {
	return p.msg
}

func (p *publication) Ack() error {
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
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		s.reader.Close()
	}
	return nil
}

func (k *kBroker) Address() string {
	if len(k.opts.Addrs) > 0 {
		return k.opts.Addrs[0]
	}
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

	select {
	case <-nctx.Done():
		return nctx.Err()
	default:
		c, err := kgo.NewClient(k.kopts...)
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

	kopts := append(k.kopts, kgo.SeedBrokers(k.opts.Addrs...))
	if k.opts.Context != nil {
		if v, ok := k.opts.Context.Value(optionsKey{}).([]kgo.Opt); ok && len(v) > 0 {
			kopts = append(kopts, v...)
		}
		if v, ok := k.opts.Context.Value(clientIDKey{}).(string); ok && v != "" {
			kopts = append(kopts, kgo.ClientID(v))
		}
		if v, ok := k.opts.Context.Value(maxReadBytesKey{}).(int32); ok {
			kopts = append(kopts, kgo.BrokerMaxReadBytes(v))
		}
		if v, ok := k.opts.Context.Value(maxWriteBytesKey{}).(int32); ok {
			kopts = append(kopts, kgo.BrokerMaxWriteBytes(v))
		}
		if v, ok := k.opts.Context.Value(connIdleTimeoutKey{}).(time.Duration); ok {
			kopts = append(kopts, kgo.ConnIdleTimeout(v))
		}
		if v, ok := k.opts.Context.Value(connTimeoutOverheadKey{}).(time.Duration); ok {
			kopts = append(kopts, kgo.ConnTimeoutOverhead(v))
		}
		if v, ok := k.opts.Context.Value(dialerKey{}).(func(ctx context.Context, network, host string) (net.Conn, error)); ok {
			kopts = append(kopts, kgo.Dialer(v))
		}
		if v, ok := k.opts.Context.Value(metadataMaxAgeKey{}).(time.Duration); ok {
			kopts = append(kopts, kgo.MetadataMaxAge(v))
		}
		if v, ok := k.opts.Context.Value(metadataMinAgeKey{}).(time.Duration); ok {
			kopts = append(kopts, kgo.MetadataMinAge(v))
		}
		if v, ok := k.opts.Context.Value(produceRetriesKey{}).(int); ok {
			kopts = append(kopts, kgo.ProduceRetries(v))
		}
		if v, ok := k.opts.Context.Value(requestRetriesKey{}).(int); ok {
			kopts = append(kopts, kgo.RequestRetries(v))
		}
		if v, ok := k.opts.Context.Value(retryBackoffKey{}).(func(int) time.Duration); ok {
			kopts = append(kopts, kgo.RetryBackoff(v))
		}
		if v, ok := k.opts.Context.Value(retryTimeoutKey{}).(func(int16) time.Duration); ok {
			kopts = append(kopts, kgo.RetryTimeout(v))
		}
		if v, ok := k.opts.Context.Value(saslKey{}).([]sasl.Mechanism); ok {
			kopts = append(kopts, kgo.SASL(v...))
		}
		if v, ok := k.opts.Context.Value(hooksKey{}).([]kgo.Hook); ok {
			kopts = append(kopts, kgo.WithHooks(v...))
		}
	}

	k.kopts = kopts
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
	records := make([]*kgo.Record, 0, len(msgs))
	var errs []string
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

func (k *kBroker) BatchSubscribe(ctx context.Context, topic string, handler broker.BatchHandler, opts ...broker.SubscribeOption) (broker.Subscriber, error) {
	return nil, nil
}

type mlogger struct {
	l   logger.Logger
	ctx context.Context
}

func (l *mlogger) Log(lvl kgo.LogLevel, msg string, args ...interface{}) {
	mlvl := logger.ErrorLevel
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
	l.l.Fields(l.ctx, mlvl, msg, args...)
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

func (k *kBroker) Subscribe(ctx context.Context, topic string, handler broker.Handler, opts ...broker.SubscribeOption) (broker.Subscriber, error) {
	options := broker.NewSubscribeOptions(opts...)

	if options.Group == "" {
		uid, err := uuid.NewRandom()
		if err != nil {
			return nil, err
		}
		options.Group = uid.String()
	}

	kopts := append(k.kopts,
		kgo.ConsumerGroup(options.Group),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.DisableAutoCommit(),
		kgo.WithLogger(&mlogger{l: k.opts.Logger, ctx: k.opts.Context}),
		// TODO: must set https://pkg.go.dev/github.com/twmb/franz-go/pkg/kgo#OnRevoked
	)

	reader, err := kgo.NewClient(kopts...)
	if err != nil {
		return nil, err
	}

	sub := &subscriber{opts: options, reader: reader, handler: handler, kopts: k.opts}
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
				return
			}
			fetches.EachError(func(t string, p int32, err error) {
				s.kopts.Logger.Errorf(ctx, "fetch err topic %s partition %d: %v", t, p, err)
			})

			s.handleFetches(ctx, fetches)
		}
	}
}

func (s *subscriber) handleFetches(ctx context.Context, fetches kgo.Fetches) error {
	var records []*kgo.Record
	var batch bool
	var err error

	if s.batchhandler != nil {
		batch = true
	}
	_ = batch

	for _, fetch := range fetches {
		for _, ftopic := range fetch.Topics {
			ridx := 0
			for {
				for _, partition := range ftopic.Partitions {
					if ridx >= len(partition.Records) {
						continue
					}
					records = append(records, partition.Records[ridx])
				}

				for _, record := range records {
					eh := s.kopts.ErrorHandler
					if s.opts.ErrorHandler != nil {
						eh = s.opts.ErrorHandler
					}
					p := &publication{topic: record.Topic, msg: &broker.Message{}}

					if s.opts.BodyOnly {
						p.msg.Body = record.Value
					} else {
						if err := s.kopts.Codec.Unmarshal(record.Value, p.msg); err != nil {
							p.err = err
							p.msg.Body = record.Value
							if eh != nil {
								_ = eh(p)
							} else {
								if s.kopts.Logger.V(logger.ErrorLevel) {
									s.kopts.Logger.Errorf(s.kopts.Context, "[kgo]: failed to unmarshal: %v", err)
								}
							}
							continue
						}
					}
					err = s.handler(p)
					if err == nil && s.opts.AutoAck {
						records = append(records, record)
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
				}
			}
			ridx++
		}
	}
	return s.reader.CommitRecords(ctx, records...)
}

func (k *kBroker) String() string {
	return "kgo"
}

func NewBroker(opts ...broker.Option) broker.Broker {
	return &kBroker{
		opts: broker.NewOptions(opts...),
	}
}
