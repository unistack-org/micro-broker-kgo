// Package kgo provides a kafka broker using kgo
package kgo

import (
	"context"
	"net"
	"sync"
	"time"

	kgo "github.com/twmb/franz-go/pkg/kgo"
	sasl "github.com/twmb/franz-go/pkg/sasl"
	"github.com/unistack-org/micro/v3/broker"
)

type kBroker struct {
	client    *kgo.Client
	connected bool
	init      bool
	sync.RWMutex
	opts broker.Options
}

type subscriber struct {
	topic   string
	opts    broker.SubscribeOptions
	handler broker.Handler
	closed  bool
	done    chan struct{}
	sync.RWMutex
}

type publication struct {
	topic     string
	partition int
	offset    int64
	err       error
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

func (s *subscriber) Options() broker.SubscribeOptions {
	return s.opts
}

func (s *subscriber) Topic() string {
	return s.topic
}

func (s *subscriber) Unsubscribe(ctx context.Context) error {
	return nil
}

func (k *kBroker) Address() string {
	if len(k.opts.Addrs) > 0 {
		return k.opts.Addrs[0]
	}
	return "127.0.0.1:9092"
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

	opts := []kgo.Opt{kgo.SeedBrokers(k.opts.Addrs...)}
	if k.opts.Context != nil {
		if v, ok := k.opts.Context.Value(clientIDKey{}).(string); ok && v != "" {
			opts = append(opts, kgo.ClientID(v))
		}
		if v, ok := k.opts.Context.Value(maxReadBytesKey{}).(int32); ok {
			opts = append(opts, kgo.BrokerMaxReadBytes(v))
		}
		if v, ok := k.opts.Context.Value(maxWriteBytesKey{}).(int32); ok {
			opts = append(opts, kgo.BrokerMaxWriteBytes(v))
		}

		if v, ok := k.opts.Context.Value(connIdleTimeoutKey{}).(time.Duration); ok {
			opts = append(opts, kgo.ConnIdleTimeout(v))
		}
		if v, ok := k.opts.Context.Value(connTimeoutOverheadKey{}).(time.Duration); ok {
			opts = append(opts, kgo.ConnTimeoutOverhead(v))
		}
		if v, ok := k.opts.Context.Value(dialerKey{}).(func(ctx context.Context, network, host string) (net.Conn, error)); ok {
			opts = append(opts, kgo.Dialer(v))
		}
		if v, ok := k.opts.Context.Value(metadataMaxAgeKey{}).(time.Duration); ok {
			opts = append(opts, kgo.MetadataMaxAge(v))
		}
		if v, ok := k.opts.Context.Value(metadataMinAgeKey{}).(time.Duration); ok {
			opts = append(opts, kgo.MetadataMinAge(v))
		}
		if v, ok := k.opts.Context.Value(produceRetriesKey{}).(int); ok {
			opts = append(opts, kgo.ProduceRetries(v))
		}
		if v, ok := k.opts.Context.Value(requestRetriesKey{}).(int); ok {
			opts = append(opts, kgo.RequestRetries(v))
		}
		if v, ok := k.opts.Context.Value(retryBackoffKey{}).(func(int) time.Duration); ok {
			opts = append(opts, kgo.RetryBackoff(v))
		}
		if v, ok := k.opts.Context.Value(retryTimeoutKey{}).(func(int16) time.Duration); ok {
			opts = append(opts, kgo.RetryTimeout(v))
		}
		if v, ok := k.opts.Context.Value(saslKey{}).([]sasl.Mechanism); ok {
			opts = append(opts, kgo.SASL(v...))
		}
		if v, ok := k.opts.Context.Value(hooksKey{}).([]kgo.Hook); ok {
			opts = append(opts, kgo.WithHooks(v...))
		}
	}

	var c *kgo.Client
	var err error

	select {
	case <-nctx.Done():
		return nctx.Err()
	default:
		c, err = kgo.NewClient(opts...)
		if err != nil {
			return err
		}
	}

	k.client = c

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
		k.client.Close()
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

	k.init = true

	return nil
}

func (k *kBroker) Options() broker.Options {
	return k.opts
}

func (k *kBroker) Publish(ctx context.Context, topic string, msg *broker.Message, opts ...broker.PublishOption) error {
	return nil
}

func (k *kBroker) Subscribe(ctx context.Context, topic string, handler broker.Handler, opts ...broker.SubscribeOption) (broker.Subscriber, error) {
	options := broker.NewSubscribeOptions(opts...)
	sub := &subscriber{opts: options}
	return sub, nil
}

func (k *kBroker) String() string {
	return "kgo"
}

func NewBroker(opts ...broker.Option) broker.Broker {
	return &kBroker{
		opts: broker.NewOptions(opts...),
	}
}
