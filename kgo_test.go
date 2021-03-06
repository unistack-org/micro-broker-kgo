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
	kgo "go.unistack.org/micro-broker-kgo/v3"
	"go.unistack.org/micro/v3/broker"
	"go.unistack.org/micro/v3/logger"
	"go.unistack.org/micro/v3/metadata"
)

var (
	msgcnt   = int64(12000000)
	group    = "38"
	prefill  = false
	loglevel = logger.InfoLevel
)

var bm = &broker.Message{
	Header: map[string]string{"hkey": "hval", metadata.HeaderTopic: "test"},
	Body:   []byte(`"body"`),
}

func TestPubSub(t *testing.T) {
	if tr := os.Getenv("INTEGRATION_TESTS"); len(tr) > 0 {
		t.Skip()
	}

	if err := logger.DefaultLogger.Init(logger.WithLevel(loglevel), logger.WithCallerSkipCount(3)); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	var addrs []string
	if addr := os.Getenv("BROKER_ADDRS"); len(addr) == 0 {
		addrs = []string{"127.0.0.1:29091", "127.0.0.2:29092", "127.0.0.3:29093"}
	} else {
		addrs = strings.Split(addr, ",")
	}

	b := kgo.NewBroker(
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
	if prefill {
		msgs := make([]*broker.Message, 0, msgcnt)
		for i := int64(0); i < msgcnt; i++ {
			msgs = append(msgs, bm)
		}

		if err := b.BatchPublish(ctx, msgs); err != nil {
			t.Fatal(err)
		}
		//	t.Skip()
	}
	done := make(chan bool, 1)
	idx := int64(0)
	fn := func(msg broker.Event) error {
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
					fmt.Printf("processed %v\n", prc)
				}
			case <-ticker.C:
				close(done)
			}
		}
	}()
	<-done
}
