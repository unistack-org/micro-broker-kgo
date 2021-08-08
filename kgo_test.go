package kgo_test

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"

	kgo "github.com/unistack-org/micro-broker-kgo/v3"
	"github.com/unistack-org/micro/v3/broker"
	"github.com/unistack-org/micro/v3/logger"
)

var (
	bm = &broker.Message{
		Header: map[string]string{"hkey": "hval"},
		Body:   []byte(`"body"`),
	}
)

func TestPubSub(t *testing.T) {
	if tr := os.Getenv("INTEGRATION_TESTS"); len(tr) > 0 {
		t.Skip()
	}

	logger.DefaultLogger.Init(logger.WithLevel(logger.TraceLevel))
	ctx := context.Background()

	var addrs []string
	if addr := os.Getenv("BROKER_ADDRS"); len(addr) == 0 {
		addrs = []string{"127.0.0.1:9092"}
	} else {
		addrs = strings.Split(addr, ",")
	}

	b := kgo.NewBroker(broker.Addrs(addrs...), kgo.ClientID("test"))
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

	done := make(chan bool, 1)
	fn := func(msg broker.Event) error {
		fmt.Printf("EEEE %s\n", msg.Message().Body)
		done <- true
		return msg.Ack()
	}

	sub, err := b.Subscribe(ctx, "test_topic", fn, broker.SubscribeGroup("test"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := sub.Unsubscribe(ctx); err != nil {
			t.Fatal(err)
		}
	}()
	if err := b.Publish(ctx, "test_topic", bm); err != nil {
		t.Fatal(err)
	}
	<-done
}
