// Package kgo provides a kafka broker using kgo
package kgo // import "go.unistack.org/micro-broker-kgo/v3"

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"go.unistack.org/micro/v3/broker"
	"go.unistack.org/micro/v3/metadata"
	id "go.unistack.org/micro/v3/util/id"
	mrand "go.unistack.org/micro/v3/util/rand"
)

var _ broker.Broker = (*Broker)(nil)

var ErrLostMessage = errors.New("message not marked for offsets commit and will be lost in next iteration")

var DefaultRetryBackoffFn = func() func(int) time.Duration {
	var rngMu sync.Mutex
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
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
		jitter := 0.8 + 0.4*rng.Float64()
		rngMu.Unlock()

		backoff = time.Duration(float64(backoff) * jitter)

		if backoff > max {
			return max
		}
		return backoff
	}
}()

type Broker struct {
	c         *kgo.Client
	kopts     []kgo.Opt
	connected bool
	init      bool
	sync.RWMutex
	opts broker.Options
	subs []*subscriber
}

func (k *Broker) Address() string {
	return strings.Join(k.opts.Addrs, ",")
}

func (k *Broker) Name() string {
	return k.opts.Name
}

func (k *Broker) connect(ctx context.Context, opts ...kgo.Opt) (*kgo.Client, error) {
	var c *kgo.Client
	var err error

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
		c, err = kgo.NewClient(opts...)
		if err == nil {
			err = c.Ping(ctx) // check connectivity to cluster
		}
		if err != nil {
			return nil, err
		}
	}
	return c, nil
}

func (k *Broker) Connect(ctx context.Context) error {
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

	c, err := k.connect(nctx, k.kopts...)
	if err != nil {
		return err
	}

	k.Lock()
	k.c = c
	k.connected = true
	k.Unlock()

	return nil
}

func (k *Broker) Disconnect(ctx context.Context) error {
	k.RLock()
	if !k.connected {
		k.RUnlock()
		return nil
	}
	k.RUnlock()

	nctx := k.opts.Context
	if ctx != nil {
		nctx = ctx
	}

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

	k.connected = false
	return nil
}

func (k *Broker) Init(opts ...broker.Option) error {
	k.Lock()
	defer k.Unlock()

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

	return nil
}

func (k *Broker) Options() broker.Options {
	return k.opts
}

func (k *Broker) BatchPublish(ctx context.Context, msgs []*broker.Message, opts ...broker.PublishOption) error {
	return k.publish(ctx, msgs, opts...)
}

func (k *Broker) Publish(ctx context.Context, topic string, msg *broker.Message, opts ...broker.PublishOption) error {
	msg.Header.Set(metadata.HeaderTopic, topic)
	return k.publish(ctx, []*broker.Message{msg}, opts...)
}

func (k *Broker) publish(ctx context.Context, msgs []*broker.Message, opts ...broker.PublishOption) error {
	k.RLock()
	if !k.connected {
		k.RUnlock()
		k.Lock()
		c, err := k.connect(ctx, k.kopts...)
		if err != nil {
			k.Unlock()
			return err
		}
		k.c = c
		k.connected = true
		k.Unlock()
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
		rec := &kgo.Record{Context: ctx, Key: key}
		rec.Topic, _ = msg.Header.Get(metadata.HeaderTopic)
		if options.BodyOnly {
			rec.Value = msg.Body
		} else if k.opts.Codec.String() == "noop" {
			rec.Value = msg.Body
			for k, v := range msg.Header {
				rec.Headers = append(rec.Headers, kgo.RecordHeader{Key: k, Value: []byte(v)})
			}
		} else {
			rec.Value, err = k.opts.Codec.Marshal(msg)
			if err != nil {
				return err
			}
		}
		records = append(records, rec)
	}

	results := k.c.ProduceSync(ctx, records...)
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

func (k *Broker) BatchSubscribe(ctx context.Context, topic string, handler broker.BatchHandler, opts ...broker.SubscribeOption) (broker.Subscriber, error) {
	return nil, nil
}

func (k *Broker) Subscribe(ctx context.Context, topic string, handler broker.Handler, opts ...broker.SubscribeOption) (broker.Subscriber, error) {
	options := broker.NewSubscribeOptions(opts...)

	if options.Group == "" {
		uid, err := id.New()
		if err != nil {
			return nil, err
		}
		options.Group = uid
	}

	commitInterval := DefaultCommitInterval
	if k.opts.Context != nil {
		if v, ok := k.opts.Context.Value(commitIntervalKey{}).(time.Duration); ok && v > 0 {
			commitInterval = v
		}
	}

	sub := &subscriber{
		topic:     topic,
		opts:      options,
		handler:   handler,
		kopts:     k.opts,
		consumers: make(map[tp]*consumer),
		done:      make(chan struct{}),
	}

	kopts := append(k.kopts,
		kgo.ConsumerGroup(options.Group),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.FetchMaxWait(1*time.Second),
		kgo.AutoCommitInterval(commitInterval),
		kgo.OnPartitionsAssigned(sub.assigned),
		kgo.OnPartitionsRevoked(sub.revoked),
		kgo.OnPartitionsLost(sub.lost),
		kgo.AutoCommitMarks(),
	)

	if options.Context != nil {
		if v, ok := options.Context.Value(optionsKey{}).([]kgo.Opt); ok && len(v) > 0 {
			kopts = append(kopts, v...)
		}
	}

	c, err := k.connect(ctx, kopts...)
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
	go sub.poll(ctx)

	k.Lock()
	k.subs = append(k.subs, sub)
	k.Unlock()
	return sub, nil
}

func (k *Broker) String() string {
	return "kgo"
}

func NewBroker(opts ...broker.Option) *Broker {
	rand.Seed(time.Now().Unix())
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
		kgo.WithLogger(&mlogger{l: options.Logger, ctx: options.Context}),
		// kgo.WithLogger(kgo.BasicLogger(os.Stderr, kgo.LogLevelDebug, func() string { return time.Now().Format(time.StampMilli) })),
		kgo.WithHooks(&metrics{meter: options.Meter}),
		kgo.SeedBrokers(kaddrs...),
		kgo.RetryBackoffFn(DefaultRetryBackoffFn),
		kgo.BlockRebalanceOnPoll(),
		kgo.Balancers(kgo.CooperativeStickyBalancer()),
		kgo.FetchIsolationLevel(kgo.ReadUncommitted()),
	}

	if options.Context != nil {
		if v, ok := options.Context.Value(optionsKey{}).([]kgo.Opt); ok && len(v) > 0 {
			kopts = append(kopts, v...)
		}
	}

	return &Broker{
		opts:  options,
		kopts: kopts,
	}
}
