package kgo_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
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
			kg.MaxBufferedRecords(10),
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
			if err := b.Publish(ctx, "test.fail", msg); err != nil {
				// t.Fatal(err)
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

	sub, err := b.Subscribe(ctx, "test.fail", fn,
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

		if err := b.Publish(ctx, "test.pubsub", msgs...); err != nil {
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

	sub, err := b.Subscribe(ctx, "test.pubsub", fn,
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
					t.Logf("processed %v of %v\n", prc, msgcnt)
				}
			case <-ticker.C:
				close(done)
			}
		}
	}()
	<-done
}

func TestKillConsumers_E2E_Rebalance(t *testing.T) {
	logger.DefaultLogger = slog.NewLogger()
	if err := logger.DefaultLogger.Init(logger.WithLevel(logger.InfoLevel)); err != nil {
		t.Fatal(err)
	}

	bLogger := broker.Logger(logger.DefaultLogger.Clone(logger.WithLevel(logger.DebugLevel)))

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	b1 := kgo.NewBroker(
		broker.ContentType("application/octet-stream"),
		broker.Codec("application/octet-stream", codec.NewCodec()),
		broker.Addrs(cluster.ListenAddrs()...),
		bLogger,
		kgo.CommitInterval(500*time.Millisecond),
		kgo.Options(
			kg.ClientID("test-1"),
			kg.FetchMaxBytes(10*1024*1024),
			kg.AllowAutoTopicCreation(),
			kg.MaxBufferedRecords(10),
		),
	)
	if err := b1.Init(); err != nil {
		t.Fatal(err)
	}
	if err := b1.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = b1.Disconnect(context.Background()) }()

	b2 := kgo.NewBroker(
		broker.ContentType("application/octet-stream"),
		broker.Codec("application/octet-stream", codec.NewCodec()),
		broker.Addrs(cluster.ListenAddrs()...),
		bLogger,
		kgo.CommitInterval(500*time.Millisecond),
		kgo.Options(
			kg.ClientID("test-2"),
			kg.FetchMaxBytes(10*1024*1024),
			kg.AllowAutoTopicCreation(),
			kg.MaxBufferedRecords(10),
		),
	)
	if err := b2.Init(); err != nil {
		t.Fatal(err)
	}
	if err := b2.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = b2.Disconnect(context.Background()) }()

	const topic = "test.kill"
	const total = int64(1000)

	var (
		processed int64
		c1Count   int64
		c2Count   int64
	)

	done := make(chan struct{})

	tryClose := func() {
		if atomic.LoadInt64(&processed) >= total && atomic.LoadInt64(&c2Count) > 0 {
			select {
			case <-done:
			default:
				close(done)
			}
		}
	}

	h1 := func(msg broker.Message) error {
		time.Sleep(2 * time.Millisecond)
		atomic.AddInt64(&processed, 1)
		atomic.AddInt64(&c1Count, 1)
		tryClose()
		return msg.Ack()
	}

	h2 := func(msg broker.Message) error {
		time.Sleep(2 * time.Millisecond)
		atomic.AddInt64(&processed, 1)
		atomic.AddInt64(&c2Count, 1)
		tryClose()
		return msg.Ack()
	}

	sub1, err := b1.Subscribe(ctx, topic, h1,
		broker.SubscribeAutoAck(true),
		broker.SubscribeGroup(group),
		broker.SubscribeBodyOnly(true),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sub1.Unsubscribe(context.Background()) }()

	go func() {
		for atomic.LoadInt64(&processed) < total || atomic.LoadInt64(&c2Count) == 0 {
			batchSize := int64(10)
			msgs := make([]broker.Message, 0, batchSize)
			for i := int64(0); i < batchSize; i++ {
				m, _ := b1.NewMessage(ctx, metadata.New(0), []byte("msg"))
				msgs = append(msgs, m)
			}
			_ = b1.Publish(ctx, topic, msgs...)

			time.Sleep(5 * time.Millisecond)
		}
	}()

	time.Sleep(200 * time.Millisecond)

	// второй consumer подключается -> KAFKA REBALANCE
	sub2, err := b2.Subscribe(ctx, topic, h2,
		broker.SubscribeAutoAck(true),
		broker.SubscribeGroup(group),
		broker.SubscribeBodyOnly(true),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sub2.Unsubscribe(context.Background()) }()

	// ждём окончания
	select {
	case <-done:
		t.Log("DONE")
	case <-ctx.Done():
		t.Fatalf("timeout: processed=%d of %d (c1=%d, c2=%d)",
			atomic.LoadInt64(&processed),
			total,
			atomic.LoadInt64(&c1Count),
			atomic.LoadInt64(&c2Count),
		)
	}

	if got := atomic.LoadInt64(&processed); got < total {
		t.Fatalf("processed %d, want >= %d", got, total)
	}

	if atomic.LoadInt64(&c1Count) == 0 {
		t.Fatalf("consumer1 did not process any messages")
	}
	if atomic.LoadInt64(&c2Count) == 0 {
		t.Fatalf("consumer2 did not process any messages (rebalance/killConsumers likely broken)")
	}
}

func TestGracefulShutdown_HandlersComplete(t *testing.T) {
	ctx := context.Background()

	const handlerDuration = 3 * time.Second
	const gracefulTimeout = 5 * time.Second

	b := kgo.NewBroker(
		broker.ContentType("application/octet-stream"),
		broker.Codec("application/octet-stream", codec.NewCodec()),
		broker.Addrs(cluster.ListenAddrs()...),
		broker.GracefulTimeout(gracefulTimeout),
		kgo.CommitInterval(5*time.Second),
		kgo.Options(
			kg.ClientID("test-graceful"),
			kg.AllowAutoTopicCreation(),
			kg.MaxBufferedRecords(10),
		),
	)
	require.NoError(t, b.Init())
	require.NoError(t, b.Connect(ctx))

	m, err := b.NewMessage(ctx, metadata.New(0), []byte("graceful-test"))
	require.NoError(t, err)
	require.NoError(t, b.Publish(ctx, "test.graceful", m))

	var handlerCompleted atomic.Bool
	handlerStarted := make(chan struct{})

	fn := func(msg broker.Message) error {
		close(handlerStarted)
		time.Sleep(handlerDuration)
		handlerCompleted.Store(true)
		return msg.Ack()
	}

	_, err = b.Subscribe(ctx, "test.graceful", fn,
		broker.SubscribeAutoAck(true),
		broker.SubscribeGroup("test-graceful-group"),
		broker.SubscribeBodyOnly(true),
	)
	require.NoError(t, err)

	// ждём, пока хендлер стартует
	select {
	case <-handlerStarted:
	case <-time.After(10 * time.Second):
		t.Fatal("handler did not start in time")
	}

	start := time.Now()
	require.NoError(t, b.Disconnect(ctx))
	elapsed := time.Since(start)

	if !handlerCompleted.Load() {
		t.Fatal("handler was killed before completion — graceful shutdown broken")
	}
	if elapsed < handlerDuration/2 {
		t.Fatalf("Unsubscribe returned too fast (%v) — handler likely not waited", elapsed)
	}
	t.Logf("Unsubscribe waited %v for handler (handler took %v)", elapsed, handlerDuration)
}

func TestBrokerErrors_ReachHandler(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	errCluster := kfake.MustCluster(kfake.AllowAutoTopicCreation())

	b := kgo.NewBroker(
		broker.ContentType("application/octet-stream"),
		broker.Codec("application/octet-stream", codec.NewCodec()),
		broker.Addrs(errCluster.ListenAddrs()...),
		kgo.CommitInterval(5*time.Second),
		kgo.Options(
			kg.ClientID("test-broker-errors"),
			kg.AllowAutoTopicCreation(),
		),
	)
	require.NoError(t, b.Init())
	require.NoError(t, b.Connect(ctx))
	defer func() { _ = b.Disconnect(context.Background()) }()

	type errWrapper struct{ err error }
	var errReceived atomic.Value

	fn := func(msg broker.Message) error {
		if km, ok := msg.(interface{ Error() error }); ok && km.Error() != nil {
			errReceived.Store(errWrapper{km.Error()})
		}
		return msg.Ack()
	}

	sub, err := b.Subscribe(ctx, "test.broker.errors", fn,
		broker.SubscribeAutoAck(true),
		broker.SubscribeGroup("test-broker-errors"),
		broker.SubscribeBodyOnly(true),
	)
	require.NoError(t, err)
	defer func() { _ = sub.Unsubscribe(context.Background()) }()

	// ждём, чтобы consumer запустился
	time.Sleep(500 * time.Millisecond)

	// рвём все соединения — kgo получит io.EOF/net.ErrClosed
	errCluster.Close()

	require.Eventually(t, func() bool {
		return errReceived.Load() != nil
	}, 10*time.Second, 50*time.Millisecond, "handler не получил ошибку брокера после разрыва соединения")

	if w, ok := errReceived.Load().(errWrapper); ok {
		t.Logf("handler получил ошибку: %v", w.err)
	}
}
