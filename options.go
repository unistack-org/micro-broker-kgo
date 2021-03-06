package kgo

import (
	"context"
	"time"

	kgo "github.com/twmb/franz-go/pkg/kgo"
	"go.unistack.org/micro/v3/broker"
	"go.unistack.org/micro/v3/client"
	"go.unistack.org/micro/v3/server"
)

// DefaultCommitInterval specifies how fast send commit offsets to kafka
var DefaultCommitInterval = 5 * time.Second

type subscribeContextKey struct{}

// SubscribeContext set the context for broker.SubscribeOption
func SubscribeContext(ctx context.Context) broker.SubscribeOption {
	return broker.SetSubscribeOption(subscribeContextKey{}, ctx)
}

type publishKey struct{}

// PublishKey set the kafka message key (broker option)
func PublishKey(key []byte) broker.PublishOption {
	return broker.SetPublishOption(publishKey{}, key)
}

// ClientPublishKey set the kafka message key (client option)
func ClientPublishKey(key []byte) client.PublishOption {
	return client.SetPublishOption(publishKey{}, key)
}

type optionsKey struct{}

// Options pass additional options to broker
func Options(opts ...kgo.Opt) broker.Option {
	return func(o *broker.Options) {
		if o.Context == nil {
			o.Context = context.Background()
		}
		options, ok := o.Context.Value(optionsKey{}).([]kgo.Opt)
		if !ok {
			options = make([]kgo.Opt, 0, len(opts))
		}
		options = append(options, opts...)
		o.Context = context.WithValue(o.Context, optionsKey{}, options)
	}
}

// SubscribeOptions pass additional options to broker
func SubscribeOptions(opts ...kgo.Opt) broker.SubscribeOption {
	return func(o *broker.SubscribeOptions) {
		if o.Context == nil {
			o.Context = context.Background()
		}
		options, ok := o.Context.Value(optionsKey{}).([]kgo.Opt)
		if !ok {
			options = make([]kgo.Opt, 0, len(opts))
		}
		options = append(options, opts...)
		o.Context = context.WithValue(o.Context, optionsKey{}, options)
	}
}

// SubscriberOptions pass additional options to broker
func SubscriberOptions(opts ...kgo.Opt) server.SubscriberOption {
	return func(o *server.SubscriberOptions) {
		if o.Context == nil {
			o.Context = context.Background()
		}
		options, ok := o.Context.Value(optionsKey{}).([]kgo.Opt)
		if !ok {
			options = make([]kgo.Opt, 0, len(opts))
		}
		options = append(options, opts...)
		o.Context = context.WithValue(o.Context, optionsKey{}, options)
	}
}

type commitIntervalKey struct{}

// CommitInterval specifies interval to send commits
func CommitInterval(td time.Duration) broker.Option {
	return broker.SetOption(commitIntervalKey{}, td)
}

var DefaultSubscribeMaxInflight = 1000

type subscribeMaxInflightKey struct{}

// SubscribeMaxInFlight max queued messages
func SubscribeMaxInFlight(n int) broker.SubscribeOption {
	return broker.SetSubscribeOption(subscribeMaxInflightKey{}, n)
}
