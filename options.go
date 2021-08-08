package kgo

import (
	"context"
	"net"
	"time"

	kgo "github.com/twmb/franz-go/pkg/kgo"
	sasl "github.com/twmb/franz-go/pkg/sasl"
	"github.com/unistack-org/micro/v3/broker"
	"github.com/unistack-org/micro/v3/client"
)

type subscribeContextKey struct{}

// SubscribeContext set the context for broker.SubscribeOption
func SubscribeContext(ctx context.Context) broker.SubscribeOption {
	return broker.SetSubscribeOption(subscribeContextKey{}, ctx)
}

type publishKey struct{}

func PublishKey(key []byte) broker.PublishOption {
	return broker.SetPublishOption(publishKey{}, key)
}

func ClientPublishKey(key []byte) client.PublishOption {
	return client.SetPublishOption(publishKey{}, key)
}

type clientIDKey struct{}

func ClientID(id string) broker.Option {
	return broker.SetOption(clientIDKey{}, id)
}

type maxReadBytesKey struct{}

func MaxReadBytes(n int32) broker.Option {
	return broker.SetOption(maxReadBytesKey{}, n)
}

type maxWriteBytesKey struct{}

func MaxWriteBytes(n int32) broker.Option {
	return broker.SetOption(maxWriteBytesKey{}, n)
}

type connIdleTimeoutKey struct{}

func ConnIdleTimeout(td time.Duration) broker.Option {
	return broker.SetOption(connIdleTimeoutKey{}, td)
}

type connTimeoutOverheadKey struct{}

func ConnTimeoutOverhead(td time.Duration) broker.Option {
	return broker.SetOption(connTimeoutOverheadKey{}, td)
}

type dialerKey struct{}

func Dialer(fn func(ctx context.Context, network, host string) (net.Conn, error)) broker.Option {
	return broker.SetOption(dialerKey{}, fn)
}

type metadataMaxAgeKey struct{}

func MetadataMaxAge(td time.Duration) broker.Option {
	return broker.SetOption(metadataMaxAgeKey{}, td)
}

type metadataMinAgeKey struct{}

func MetadataMinAge(td time.Duration) broker.Option {
	return broker.SetOption(metadataMinAgeKey{}, td)
}

type produceRetriesKey struct{}

func ProduceRetries(n int) broker.Option {
	return broker.SetOption(produceRetriesKey{}, n)
}

type requestRetriesKey struct{}

func RequestRetries(n int) broker.Option {
	return broker.SetOption(requestRetriesKey{}, n)
}

type retryBackoffKey struct{}

func RetryBackoff(fn func(int) time.Duration) broker.Option {
	return broker.SetOption(retryBackoffKey{}, fn)
}

type retryTimeoutKey struct{}

func RetryTimeout(fn func(int16) time.Duration) broker.Option {
	return broker.SetOption(retryTimeoutKey{}, fn)
}

type saslKey struct{}

func SASL(sasls ...sasl.Mechanism) broker.Option {
	return broker.SetOption(saslKey{}, sasls)
}

type hooksKey struct{}

func Hooks(hooks ...kgo.Hook) broker.Option {
	return broker.SetOption(hooksKey{}, hooks)
}

type optionsKey struct{}

func Options(opts ...kgo.Opt) broker.Option {
	return broker.SetOption(optionsKey{}, opts)
}
