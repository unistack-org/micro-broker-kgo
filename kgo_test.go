package kgo_test

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	kg "github.com/twmb/franz-go/pkg/kgo"
	kgo "github.com/unistack-org/micro-broker-kgo/v3"
	jsoncodec "github.com/unistack-org/micro-codec-json/v3"
	"github.com/unistack-org/micro/v3/broker"
	"github.com/unistack-org/micro/v3/logger"
	"github.com/unistack-org/micro/v3/metadata"
)

var bm = &broker.Message{
	Header: map[string]string{"hkey": "hval", metadata.HeaderTopic: "test"},
	Body:   []byte(`"body"`),
}

func TestPubSub(t *testing.T) {
	if tr := os.Getenv("INTEGRATION_TESTS"); len(tr) > 0 {
		t.Skip()
	}

	logger.DefaultLogger.Init(logger.WithLevel(logger.TraceLevel), logger.WithCallerSkipCount(3))
	ctx := context.Background()

	var addrs []string
	if addr := os.Getenv("BROKER_ADDRS"); len(addr) == 0 {
		addrs = []string{"127.0.0.1:29091", "127.0.0.2:29092", "127.0.0.3:29093"}
	} else {
		addrs = strings.Split(addr, ",")
	}

	b := kgo.NewBroker(
		broker.Codec(jsoncodec.NewCodec()),
		broker.Addrs(addrs...),
		kgo.CommitInterval(5*time.Second),
		kgo.Options(kg.ClientID("test"), kg.FetchMaxBytes(10*1024*1024)),
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

	/*
			msgs := make([]*broker.Message, 0, 600000)
			for i := 0; i < 600000; i++ {
				msgs = append(msgs, bm)
			}

			if err := b.BatchPublish(ctx, msgs); err != nil {
				t.Fatal(err)
			}
		t.Skip()
	*/
	done := make(chan bool, 1)
	idx := int64(0)
	fn := func(msg broker.Event) error {
		atomic.AddInt64(&idx, 1)
		// time.Sleep(200 * time.Millisecond)
		return msg.Ack()
	}

	sub, err := b.Subscribe(ctx, "test", fn,
		broker.SubscribeAutoAck(true),
		broker.SubscribeGroup("test29"),
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
				fmt.Printf("processed %v\n", atomic.LoadInt64(&idx))
			case <-ticker.C:
				close(done)
			}
		}
	}()
	<-done
}
