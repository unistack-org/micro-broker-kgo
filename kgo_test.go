package kgo_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kfake"
	kg "github.com/twmb/franz-go/pkg/kgo"
	kgo "go.unistack.org/micro-broker-kgo/v4"
	"go.unistack.org/micro/v4/broker"
	"go.unistack.org/micro/v4/codec"
	"go.unistack.org/micro/v4/logger"
	"go.unistack.org/micro/v4/logger/slog"
	"go.unistack.org/micro/v4/metadata"
)

var (
	msgcnt   = int64(1200)
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
	m.Run()
}

func TestFail(t *testing.T) {
	logger.DefaultLogger = slog.NewLogger()
	if err := logger.DefaultLogger.Init(logger.WithLevel(loglevel)); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	b := kgo.NewBroker(
		broker.ContentType("application/octet-stream"),
		broker.Codec("application/octet-stream", codec.NewCodec()),
		broker.Addrs(cluster.ListenAddrs()...),
		kgo.CommitInterval(5*time.Second),
		kgo.Options(
			kg.ClientID("test"),
			kg.FetchMaxBytes(10*1024*1024),
			kg.AllowAutoTopicCreation(),
		),
	)

	t.Logf("broker init")
	if err := b.Init(); err != nil {
		t.Fatal(err)
	}

	t.Logf("broker connect")
	if err := b.Connect(ctx); err != nil {
		t.Fatal(err)
	}

	defer func() {
		t.Logf("broker disconnect")
		if err := b.Disconnect(ctx); err != nil {
			t.Fatal(err)
		}
	}()

	t.Logf("broker health %v", b.Health())
	msgs := make([]broker.Message, 0, msgcnt)
	for i := int64(0); i < msgcnt; i++ {
		m, err := b.NewMessage(ctx, metadata.Pairs("hkey", "hval"), []byte(`test`))
		if err != nil {
			t.Fatal(err)
		}
		msgs = append(msgs, m)
	}

	go func() {
		for _, msg := range msgs {
			//		t.Logf("broker publish")
			if err := b.Publish(ctx, "test", msg); err != nil {
				t.Fatal(err)
			}
		}
	}()
	//	t.Skip()

	idx := int64(0)
	fn := func(msg broker.Message) error {
		atomic.AddInt64(&idx, 1)
		time.Sleep(100 * time.Millisecond)
		// t.Logf("ack")
		return msg.Ack()
	}

	sub, err := b.Subscribe(ctx, "test", fn,
		broker.SubscribeAutoAck(true),
		broker.SubscribeGroup(group),
		broker.SubscribeBodyOnly(true))
	if err != nil {
		t.Fatal(err)
	}

	defer func() {
		if err := sub.Unsubscribe(ctx); err != nil {
			t.Fatal(err)
		}
	}()

	for {
		t.Logf("health check")
		if !b.Health() {
			t.Logf("health works")
			break
		}
		t.Logf("health sleep")
		time.Sleep(100 * time.Millisecond)
		if err := b.Disconnect(ctx); err != nil {
			t.Fatal(err)
		}
	}
}

func TestConnect(t *testing.T) {
	ctx := context.TODO()
	b := kgo.NewBroker(
		broker.ContentType("application/octet-stream"),
		broker.Codec("application/octet-stream", codec.NewCodec()),
		broker.Addrs(cluster.ListenAddrs()...),
		kgo.CommitInterval(5*time.Second),
		kgo.Options(
			kg.ClientID("test"),
			kg.FetchMaxBytes(10*1024*1024),
			kg.AllowAutoTopicCreation(),
		),
	)
	if err := b.Init(); err != nil {
		t.Fatal(err)
	}

	if err := b.Connect(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestPubSub(t *testing.T) {
	if err := logger.DefaultLogger.Init(logger.WithLevel(loglevel)); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	b := kgo.NewBroker(
		broker.ContentType("application/octet-stream"),
		broker.Codec("application/octet-stream", codec.NewCodec()),
		broker.Addrs(cluster.ListenAddrs()...),
		kgo.CommitInterval(5*time.Second),
		kgo.Options(
			kg.ClientID("test"),
			kg.FetchMaxBytes(10*1024*1024),
			kg.AllowAutoTopicCreation(),
		),
	)

	if err := b.Init(); err != nil {
		t.Fatal(err)
	}

	if err := b.Connect(ctx); err != nil {
		t.Fatal(err)
	}

	defer func() {
		if err := b.Disconnect(ctx); err != nil {
			t.Fatal(err)
		}
	}()
	if prefill {
		msgs := make([]broker.Message, 0, msgcnt)
		for i := int64(0); i < msgcnt; i++ {
			m, _ := b.NewMessage(ctx, metadata.Pairs("hkey", "hval"), []byte(`test`))
			msgs = append(msgs, m)
		}

		if err := b.Publish(ctx, "test", msgs...); err != nil {
			t.Fatal(err)
		}
		//	t.Skip()
	}
	done := make(chan bool, 1)
	idx := int64(0)
	fn := func(msg broker.Message) error {
		atomic.AddInt64(&idx, 1)
		// time.Sleep(200 * time.Millisecond)
		return msg.Ack()
	}

	sub, err := b.Subscribe(ctx, "test", fn,
		broker.SubscribeAutoAck(true),
		broker.SubscribeGroup(group),
		broker.SubscribeBodyOnly(true))
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := sub.Unsubscribe(ctx); err != nil {
			t.Fatal(err)
		}
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
					close(done)
				} else {
					t.Logf("processed %v\n", prc)
				}
			case <-ticker.C:
				close(done)
			}
		}
	}()
	<-done
}
