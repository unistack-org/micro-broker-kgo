package kgo_test

import (
	"context"
	"crypto/tls"
	"fmt"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kfake"
	kg "github.com/twmb/franz-go/pkg/kgo"
	kgo "go.unistack.org/micro-broker-kgo/v3"
	jsonpb "go.unistack.org/micro-codec-jsonpb/v3"
	proto "go.unistack.org/micro-codec-proto/v3"
	requestid "go.unistack.org/micro-wrapper-requestid/v3"
	"go.unistack.org/micro/v3"
	"go.unistack.org/micro/v3/broker"
	"go.unistack.org/micro/v3/client"
	"go.unistack.org/micro/v3/logger"
	"go.unistack.org/micro/v3/metadata"
	"go.unistack.org/micro/v3/server"
)

var (
	msgcnt   = int64(10)
	group    = "38"
	prefill  = true
	loglevel = logger.ErrorLevel
	cluster  *kfake.Cluster
)

func TestMain(m *testing.M) {
	cluster = kfake.MustCluster(
		kfake.AllowAutoTopicCreation(),
	)
	defer cluster.Close()
	os.Exit(m.Run())
}

func helperCreateBroker(t *testing.T) *kgo.Broker {
	t.Helper()
	b := kgo.NewBroker(
		broker.Addrs(cluster.ListenAddrs()...),
		kgo.CommitInterval(5*time.Second),
		kgo.Options(kg.ClientID("test"), kg.FetchMaxBytes(10*1024*1024),
			kg.AllowAutoTopicCreation(),
		),
		kgo.CommitInterval(1*time.Second),
		broker.ErrorHandler(func(event broker.Event) error {
			msg := event.Message()
			log := logger.DefaultLogger.
				Fields("topic", event.Topic(),
					"header", msg.Header,
					"body", msg.Body)
			err := event.Ack()

			if err != nil {
				log.Fields("ack_error", err).Error(context.Background(), fmt.Sprintf("brokerHandlerErr: Ack error | %v", event.Error()))
				return err
			}

			log.Error(context.Background(), fmt.Sprintf("brokerHandlerErr: %v", event.Error()))

			return nil
		}),
	)

	return b
}

func helperCreateService(t *testing.T, ctx context.Context, b *kgo.Broker) micro.Service {
	t.Helper()

	rh := requestid.NewHook()

	sopts := []server.Option{
		server.Name("test"),
		server.Version("latest"),
		server.Context(ctx),
		server.Codec("application/json", jsonpb.NewCodec()),
		server.Codec("application/protobuf", proto.NewCodec()),
		server.Codec("application/grpc", proto.NewCodec()),
		server.Codec("application/grpc+proto", proto.NewCodec()),
		server.Codec("application/grpc+json", jsonpb.NewCodec()),
		server.Broker(b),
		server.Hooks(
			server.HookHandler(rh.ServerHandler),
			server.HookSubHandler(rh.ServerSubscriber),
		),
	}

	copts := []client.Option{
		client.Codec("application/json", jsonpb.NewCodec()),
		client.Codec("application/protobuf", proto.NewCodec()),
		client.Codec("application/grpc", proto.NewCodec()),
		client.Codec("application/grpc+proto", proto.NewCodec()),
		client.Codec("application/grpc+json", jsonpb.NewCodec()),
		client.ContentType("application/grpc"),
		client.Retries(0),
		client.TLSConfig(&tls.Config{InsecureSkipVerify: true}),
		client.Broker(b),
		client.Hooks(
			client.HookPublish(rh.ClientPublish),
			client.HookCall(rh.ClientCall),
			client.HookBatchPublish(rh.ClientBatchPublish),
			client.HookStream(rh.ClientStream),
		),
	}

	return micro.NewService(
		micro.Server(server.NewServer(sopts...)),
		micro.Client(client.NewClient(copts...)),
		micro.Broker(b),
		micro.Context(ctx),
		micro.Name("test"),
		micro.Version("latest"),
	)
}

func TestFail(t *testing.T) {
	ctx := context.Background()
	err := logger.DefaultLogger.Init(logger.WithLevel(loglevel))
	require.Nil(t, err)

	b := helperCreateBroker(t)

	t.Logf("broker init")
	require.Nil(t, b.Init())

	t.Logf("broker connect")
	require.Nil(t, b.Connect(ctx))

	defer func() {
		t.Logf("broker disconnect")
		require.Nil(t, b.Disconnect(ctx))
	}()

	t.Logf("broker health %v", b.Health())
	msgs := make([]*broker.Message, 0, msgcnt)
	for i := int64(0); i < msgcnt; i++ {
		msgs = append(msgs, &broker.Message{
			Header: map[string]string{"hkey": "hval", metadata.HeaderTopic: "test"},
			Body:   []byte(`"body"`),
		})
	}

	for _, msg := range msgs {
		t.Logf("broker publish")
		if err := b.Publish(ctx, "test", msg); err != nil {
			break
		}
	}

	idx := int64(0)
	fn := func(msg broker.Event) error {
		atomic.AddInt64(&idx, 1)
		time.Sleep(500 * time.Millisecond)
		t.Logf("ack")
		return msg.Ack()
	}

	sub, err := b.Subscribe(ctx, "test", fn,
		broker.SubscribeAutoAck(true),
		broker.SubscribeGroup(group),
		broker.SubscribeBodyOnly(true))

	require.Nil(t, err)

	defer func() {
		require.Nil(t, sub.Unsubscribe(ctx))
	}()

	for {
		t.Logf("health check")
		if b.Health() {
			t.Logf("health works")
			break
		}
		time.Sleep(1 * time.Second)
	}
}

func TestConnect(t *testing.T) {
	ctx := context.TODO()
	b := helperCreateBroker(t)

	require.Nil(t, b.Init())

	require.Nil(t, b.Connect(ctx))
}

func TestPubSub(t *testing.T) {
	ctx := context.Background()
	err := logger.DefaultLogger.Init(logger.WithLevel(loglevel))
	require.Nil(t, err)

	b := helperCreateBroker(t)

	require.Nil(t, b.Init())
	require.Nil(t, b.Connect(ctx))
	defer func() {
		require.Nil(t, b.Disconnect(ctx))
	}()

	if prefill {
		var msgs []*broker.Message
		for i := int64(0); i < msgcnt; i++ {
			msgs = append(msgs, &broker.Message{
				Header: map[string]string{"hkey": "hval", metadata.HeaderTopic: "test.pubsub"},
				Body:   []byte(`"body"`),
			})
		}
		require.Nil(t, b.BatchPublish(ctx, msgs))
	}
	done := make(chan bool, 1)
	idx := int64(0)
	fn := func(msg broker.Event) error {
		atomic.AddInt64(&idx, 1)
		return msg.Ack()
	}

	sub, err := b.Subscribe(ctx, "test.pubsub", fn,
		broker.SubscribeAutoAck(true),
		broker.SubscribeGroup(group),
		broker.SubscribeBodyOnly(true),
	)

	require.Nil(t, err)

	defer func() {
		require.Nil(t, sub.Unsubscribe(ctx))
	}()

	ticker := time.NewTicker(2 * time.Minute)
	defer ticker.Stop()

	pticker := time.NewTicker(1 * time.Second)
	defer pticker.Stop()

	go func() {
		for {
			select {
			case <-pticker.C:
				if prc := atomic.LoadInt64(&idx); prc == msgcnt {
					t.Log("everything is read")
					close(done)
				} else {
					t.Logf("processed %v of %v\n", prc, msgcnt)
				}
			case <-ticker.C:
				close(done)
			}
		}
	}()
	<-done
}
