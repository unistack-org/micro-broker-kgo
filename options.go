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

// PublishKey set the kafka message key (broker option)
func PublishKey(key []byte) broker.PublishOption {
	return broker.SetPublishOption(publishKey{}, key)
}

// ClientPublishKey set the kafka message key (client option)
func ClientPublishKey(key []byte) client.PublishOption {
	return client.SetPublishOption(publishKey{}, key)
}

type clientIDKey struct{}

// ClientID sets the kafka client id
func ClientID(id string) broker.Option {
	return broker.SetOption(clientIDKey{}, id)
}

type maxReadBytesKey struct{}

// MaxReadBytes limit max bytes to read
func MaxReadBytes(n int32) broker.Option {
	return broker.SetOption(maxReadBytesKey{}, n)
}

type maxWriteBytesKey struct{}

// MaxWriteBytes limit max bytes to write
func MaxWriteBytes(n int32) broker.Option {
	return broker.SetOption(maxWriteBytesKey{}, n)
}

type connIdleTimeoutKey struct{}

// ConnIdleTimeout limit timeout for connection
func ConnIdleTimeout(td time.Duration) broker.Option {
	return broker.SetOption(connIdleTimeoutKey{}, td)
}

type connTimeoutOverheadKey struct{}

// ConnTimeoutOverhead ...
func ConnTimeoutOverhead(td time.Duration) broker.Option {
	return broker.SetOption(connTimeoutOverheadKey{}, td)
}

type dialerKey struct{}

// Dialer pass dialer
func Dialer(fn func(ctx context.Context, network, host string) (net.Conn, error)) broker.Option {
	return broker.SetOption(dialerKey{}, fn)
}

type metadataMaxAgeKey struct{}

// MetadataMaxAge limit metadata max age
func MetadataMaxAge(td time.Duration) broker.Option {
	return broker.SetOption(metadataMaxAgeKey{}, td)
}

type metadataMinAgeKey struct{}

// MetadataMinAge limit metadata min age
func MetadataMinAge(td time.Duration) broker.Option {
	return broker.SetOption(metadataMinAgeKey{}, td)
}

type produceRetriesKey struct{}

// ProduceRetries limit number of retries
func ProduceRetries(n int) broker.Option {
	return broker.SetOption(produceRetriesKey{}, n)
}

type requestRetriesKey struct{}

// RequestRetries limit number of retries
func RequestRetries(n int) broker.Option {
	return broker.SetOption(requestRetriesKey{}, n)
}

type retryBackoffKey struct{}

// RetryBackoff set backoff func for retry
func RetryBackoff(fn func(int) time.Duration) broker.Option {
	return broker.SetOption(retryBackoffKey{}, fn)
}

type retryTimeoutKey struct{}

// RetryTimeout limit retry timeout
func RetryTimeout(fn func(int16) time.Duration) broker.Option {
	return broker.SetOption(retryTimeoutKey{}, fn)
}

type saslKey struct{}

// SASL pass sasl mechanism to auth
func SASL(sasls ...sasl.Mechanism) broker.Option {
	return broker.SetOption(saslKey{}, sasls)
}

type hooksKey struct{}

// Hooks pass hooks (useful for tracing and logging)
func Hooks(hooks ...kgo.Hook) broker.Option {
	return broker.SetOption(hooksKey{}, hooks)
}

type optionsKey struct{}

// Options pass additional options to broker
func Options(opts ...kgo.Opt) broker.Option {
	return broker.SetOption(optionsKey{}, opts)
}
