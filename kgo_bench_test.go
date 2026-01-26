package kgo_test

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	kg "github.com/twmb/franz-go/pkg/kgo"
	kgo "go.unistack.org/micro-broker-kgo/v3"
	"go.unistack.org/micro/v3/broker"
	"go.unistack.org/micro/v3/logger"
	"go.unistack.org/micro/v3/metadata"
)

func Benchmark_PubSub(b *testing.B) {
	ctx := context.Background()
	_ = logger.DefaultLogger.Init(logger.WithLevel(logger.ErrorLevel))

	msgCounts := []int{100, 1000, 10000}

	for _, msgCount := range msgCounts {
		b.Run(fmt.Sprintf("msgs=%d", msgCount), func(b *testing.B) {
			brk := kgo.NewBroker(
				broker.Addrs(defCluster.ListenAddrs()...),
				kgo.CommitInterval(1*time.Second),
				kgo.Options(
					kg.ClientID("bench"),
					kg.AllowAutoTopicCreation(),
				),
			)

			if err := brk.Init(); err != nil {
				b.Fatal(err)
			}
			if err := brk.Connect(ctx); err != nil {
				b.Fatal(err)
			}
			defer brk.Disconnect(ctx)

			topic := fmt.Sprintf("bench.topic.%d", msgCount)

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				b.StopTimer()

				// Prepare messages
				msgs := make([]*broker.Message, msgCount)
				for j := 0; j < msgCount; j++ {
					msgs[j] = &broker.Message{
						Header: map[string]string{"key": "value", metadata.HeaderTopic: topic},
						Body:   []byte(`"benchmark message"`),
					}
				}

				received := atomic.Int64{}
				done := make(chan struct{})

				fn := func(msg broker.Event) error {
					if received.Add(1) == int64(msgCount) {
						close(done)
					}
					return msg.Ack()
				}

				sub, err := brk.Subscribe(ctx, topic, fn,
					broker.SubscribeAutoAck(true),
					broker.SubscribeGroup(fmt.Sprintf("bench-group-%d-%d", msgCount, i)),
					broker.SubscribeBodyOnly(true),
				)
				if err != nil {
					b.Fatal(err)
				}

				// Wait for consumer to be ready
				time.Sleep(100 * time.Millisecond)

				b.StartTimer()

				// Publish
				if err := brk.BatchPublish(ctx, msgs); err != nil {
					b.Fatal(err)
				}

				// Wait for all messages
				select {
				case <-done:
				case <-time.After(30 * time.Second):
					b.Fatalf("timeout: received %d of %d", received.Load(), msgCount)
				}

				b.StopTimer()
				sub.Unsubscribe(ctx)
			}
		})
	}
}

func Benchmark_PublishOnly(b *testing.B) {
	b.Skip()
	ctx := context.Background()
	_ = logger.DefaultLogger.Init(logger.WithLevel(logger.ErrorLevel))

	brk := kgo.NewBroker(
		broker.Addrs(defCluster.ListenAddrs()...),
		kgo.CommitInterval(1*time.Second),
		kgo.Options(
			kg.ClientID("bench-pub"),
			kg.AllowAutoTopicCreation(),
		),
	)

	if err := brk.Init(); err != nil {
		b.Fatal(err)
	}
	if err := brk.Connect(ctx); err != nil {
		b.Fatal(err)
	}
	defer brk.Disconnect(ctx)

	msg := &broker.Message{
		Header: map[string]string{"key": "value"},
		Body:   []byte(`"benchmark message"`),
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if err := brk.Publish(ctx, "bench.publish", msg); err != nil {
				b.Error(err)
			}
		}
	})
}

func Benchmark_SubscribeHandler(b *testing.B) {
	//b.Skip()
	ctx := context.Background()
	_ = logger.DefaultLogger.Init(logger.WithLevel(logger.ErrorLevel))

	msgCount := 10000

	brk := kgo.NewBroker(
		broker.Addrs(defCluster.ListenAddrs()...),
		kgo.CommitInterval(1*time.Second),
		kgo.Options(
			kg.ClientID("bench-sub"),
			kg.AllowAutoTopicCreation(),
		),
	)

	if err := brk.Init(); err != nil {
		b.Fatal(err)
	}
	if err := brk.Connect(ctx); err != nil {
		b.Fatal(err)
	}
	defer brk.Disconnect(ctx)

	topic := "bench.subscribe.handler"

	// Pre-publish messages
	msgs := make([]*broker.Message, msgCount)
	for j := 0; j < msgCount; j++ {
		msgs[j] = &broker.Message{
			Header: map[string]string{"key": "value", metadata.HeaderTopic: topic},
			Body:   []byte(`"benchmark message"`),
		}
	}
	if err := brk.BatchPublish(ctx, msgs); err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		received := atomic.Int64{}
		done := make(chan struct{})

		fn := func(msg broker.Event) error {
			if received.Add(1) == int64(msgCount) {
				close(done)
			}
			return msg.Ack()
		}

		sub, err := brk.Subscribe(ctx, topic, fn,
			broker.SubscribeAutoAck(true),
			broker.SubscribeGroup(fmt.Sprintf("bench-handler-%d", i)),
			broker.SubscribeBodyOnly(true),
		)
		if err != nil {
			b.Fatal(err)
		}

		select {
		case <-done:
		case <-time.After(30 * time.Second):
			b.Fatalf("timeout: received %d of %d", received.Load(), msgCount)
		}

		sub.Unsubscribe(ctx)
	}
}
