package kgo

import (
	"context"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"go.unistack.org/micro/v4/broker"
)

var (

	// DefaultCommitInterval specifies how fast send commit offsets to kafka
	DefaultCommitInterval = 5 * time.Second

	// DefaultStatsInterval specifies how fast check consumer lag
	DefaultStatsInterval = 5 * time.Second

	// DefaultSubscribeMaxInflight specifies how much messages keep inflight
	DefaultSubscribeMaxInflight = 100
)

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

// SubscribeOptions pass additional options to broker in Subscribe
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

type fatalOnErrorKey struct{}

func FatalOnError(b bool) broker.Option {
	return broker.SetOption(fatalOnErrorKey{}, b)
}

type clientIDKey struct{}

func ClientID(id string) broker.Option {
	return broker.SetOption(clientIDKey{}, id)
}

type groupKey struct{}

func Group(id string) broker.Option {
	return broker.SetOption(groupKey{}, id)
}

type commitIntervalKey struct{}

// CommitInterval specifies interval to send commits
func CommitInterval(td time.Duration) broker.Option {
	return broker.SetOption(commitIntervalKey{}, td)
}

type subscribeMaxInflightKey struct{}

// SubscribeMaxInFlight max queued messages
func SubscribeMaxInFlight(n int) broker.SubscribeOption {
	return broker.SetSubscribeOption(subscribeMaxInflightKey{}, n)
}

// SubscribeMaxInFlight max queued messages
func SubscribeFatalOnError(b bool) broker.SubscribeOption {
	return broker.SetSubscribeOption(fatalOnErrorKey{}, b)
}

type publishPromiseKey struct{}

// PublishPromise set the kafka promise func for Produce
func PublishPromise(fn func(*kgo.Record, error)) broker.PublishOption {
	return broker.SetPublishOption(publishPromiseKey{}, fn)
}
