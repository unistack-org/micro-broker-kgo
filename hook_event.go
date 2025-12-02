package kgo

import (
	"context"
	"net"
	"sync/atomic"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"go.unistack.org/micro/v3/logger"
)

type hookEvent struct {
	log          logger.Logger
	connected    *atomic.Uint32
	fatalOnError bool
}

var (
	_ kgo.HookBrokerConnect           = (*hookEvent)(nil)
	_ kgo.HookBrokerDisconnect        = (*hookEvent)(nil)
	_ kgo.HookBrokerRead              = (*hookEvent)(nil)
	_ kgo.HookBrokerWrite             = (*hookEvent)(nil)
	_ kgo.HookGroupManageError        = (*hookEvent)(nil)
	_ kgo.HookProduceRecordUnbuffered = (*hookEvent)(nil)
)

func (m *hookEvent) OnGroupManageError(err error) {
	switch {
	case err == nil || isContextError(err) || kgo.IsRetryableBrokerErr(err):
		return
	default:
		m.log.Error(context.TODO(), "kgo.OnGroupManageError", err)
	}
}

func (m *hookEvent) OnBrokerConnect(_ kgo.BrokerMetadata, _ time.Duration, _ net.Conn, err error) {
	switch {
	case err == nil || isContextError(err) || kgo.IsRetryableBrokerErr(err):
		return
	default:
		m.log.Error(context.TODO(), "kgo.OnBrokerConnect", err)
	}
}

func (m *hookEvent) OnBrokerDisconnect(_ kgo.BrokerMetadata, _ net.Conn) {}

func (m *hookEvent) OnBrokerWrite(_ kgo.BrokerMetadata, _ int16, _ int, _ time.Duration, _ time.Duration, err error) {
	switch {
	case err == nil || isContextError(err) || kgo.IsRetryableBrokerErr(err):
		return
	default:
		m.log.Error(context.TODO(), "kgo.OnBrokerWrite", err)
	}
}

func (m *hookEvent) OnBrokerRead(_ kgo.BrokerMetadata, _ int16, _ int, _ time.Duration, _ time.Duration, err error) {
	switch {
	case err == nil || isContextError(err) || kgo.IsRetryableBrokerErr(err):
		return
	default:
		m.log.Error(context.TODO(), "kgo.OnBrokerRead", err)
	}
}

func (m *hookEvent) OnProduceRecordUnbuffered(_ *kgo.Record, err error) {
	switch {
	case err == nil || isContextError(err) || kgo.IsRetryableBrokerErr(err):
		return
	default:
		m.log.Error(context.TODO(), "kgo.OnProduceRecordUnbuffered", err)
	}
}
