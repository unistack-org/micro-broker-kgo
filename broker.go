package kgo

import (
	"context"
	"net"
	"sync/atomic"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"go.unistack.org/micro/v4/logger"
)

type hookEvent struct {
	log          logger.Logger
	fatalOnError bool
	connected    *atomic.Uint32
}

var (
	_ kgo.HookBrokerConnect           = &hookEvent{}
	_ kgo.HookBrokerDisconnect        = &hookEvent{}
	_ kgo.HookBrokerRead              = &hookEvent{}
	_ kgo.HookBrokerWrite             = &hookEvent{}
	_ kgo.HookGroupManageError        = &hookEvent{}
	_ kgo.HookProduceRecordUnbuffered = &hookEvent{}
)

func (m *hookEvent) OnGroupManageError(err error) {
	if err != nil {
		//		m.connected.Store(0)
		if m.fatalOnError {
			m.log.Fatal(context.TODO(), "kgo.OnGroupManageError", err)
		}
	}
}

func (m *hookEvent) OnBrokerConnect(_ kgo.BrokerMetadata, _ time.Duration, _ net.Conn, err error) {
	if err != nil {
		//		m.connected.Store(0)
		if m.fatalOnError {
			m.log.Fatal(context.TODO(), "kgo.OnBrokerConnect", err)
		}
	}
}

func (m *hookEvent) OnBrokerDisconnect(_ kgo.BrokerMetadata, _ net.Conn) {
	// m.connected.Store(0)
}

func (m *hookEvent) OnBrokerWrite(_ kgo.BrokerMetadata, _ int16, _ int, _ time.Duration, _ time.Duration, err error) {
	if err != nil {
		//		m.connected.Store(0)
		if m.fatalOnError {
			m.log.Fatal(context.TODO(), "kgo.OnBrokerWrite", err)
		}
	}
}

func (m *hookEvent) OnBrokerRead(_ kgo.BrokerMetadata, _ int16, _ int, _ time.Duration, _ time.Duration, err error) {
	if err != nil {
		//	m.connected.Store(0)
	}
}

func (m *hookEvent) OnProduceRecordUnbuffered(_ *kgo.Record, err error) {
	if err != nil {
		//	m.connected.Store(0)
	}
}
