package kgo_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	kg "github.com/twmb/franz-go/pkg/kgo"
	"go.unistack.org/micro/v3"
	"go.unistack.org/micro/v3/codec"
	"go.unistack.org/micro/v3/logger"
	"go.unistack.org/micro/v3/server"
)

func TestSubscriberHandlesTombstoneMessages(t *testing.T) {
	ctx := context.Background()
	err := logger.DefaultLogger.Init(logger.WithLevel(loglevel))
	require.Nil(t, err)

	cl := defCluster
	_ = cl

	b := helperCreateBroker(t, defCluster)
	require.Nil(t, b.Init())
	require.Nil(t, b.Connect(ctx))
	defer func() {
		require.Nil(t, b.Disconnect(ctx))
	}()

	svc := helperCreateService(t, ctx, b)
	require.Nil(t, svc.Init())

	var (
		msgsPublished = 1000
		done          = make(chan bool, 1)
		counterMsgs   = atomic.Int64{}
		topicName     = "test.tombstone.topic"

		// INCORRECT: func(ctx context.Context, req any) error
		fnHandler = func(ctx context.Context, req *codec.Frame) error {
			counterMsgs.Add(1)
			return nil
		}
	)

	err = micro.RegisterSubscriber(
		topicName,
		svc.Server(),
		fnHandler,
		server.SubscriberQueue("queue"),
		server.SubscriberAck(true),
		server.SubscriberBodyOnly(true),
	)
	require.Nil(t, err)

	{ // PUBLISH tombstones via franz-go client
		client, err := kg.NewClient(
			kg.SeedBrokers(defCluster.ListenAddrs()...),
			kg.AllowAutoTopicCreation(),
		)
		require.Nil(t, err)
		defer client.Close()

		var records []*kg.Record
		for i := 0; i < msgsPublished; i++ {
			records = append(records, &kg.Record{
				Topic: topicName,
				Key:   []byte("tombstone-key"),
			})
		}

		results := client.ProduceSync(ctx, records...)
		for _, r := range results {
			require.Nil(t, r.Err)
		}
	}

	require.Nil(t, svc.Start())

	go func() {
		require.Nil(t, svc.Run())
	}()

	ticker := time.NewTicker(2 * time.Minute)
	defer ticker.Stop()

	pticker := time.NewTicker(1 * time.Second)
	defer pticker.Stop()

	go func() {
		for {
			select {
			case <-pticker.C:
				if prc := counterMsgs.Load(); prc == int64(msgsPublished) {
					t.Log("everything is read:", prc)
					close(done)
				} else {
					t.Logf("processed %v of %v\n", prc, msgsPublished)
				}
			case <-ticker.C:
				close(done)
			}
		}
	}()
	<-done
	require.Nil(t, svc.Stop())
}
