package kgo

import (
	"context"
	"time"

	kgo "github.com/twmb/franz-go/pkg/kgo"
	"github.com/unistack-org/micro/v3/broker"
	"github.com/unistack-org/micro/v3/client"
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

type commitIntervalKey struct{}

// CommitInterval specifies interval to send commits
func CommitInterval(td time.Duration) broker.Option {
	return broker.SetOption(commitIntervalKey{}, td)
}

var DefaultSubscribeMaxInflight = 1000

type subscribeMaxInflightKey struct{}

// SubscribeMaxInFlight specifies interval to send commits
func SubscribeMaxInFlight(n int) broker.SubscribeOption {
	return broker.SetSubscribeOption(subscribeMaxInflightKey{}, n)
}
