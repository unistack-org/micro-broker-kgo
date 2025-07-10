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
	switch {
	case err == nil || isContextError(err) || kgo.IsRetryableBrokerErr(err):
		return
	default:
		ctx := context.TODO()
		logMsg := "kgo.OnGroupManageError"

		if m.fatalOnError {
			m.log.Fatal(ctx, logMsg, err)
		} else {
			m.log.Error(ctx, logMsg, err)
		}
	}
}

func (m *hookEvent) OnBrokerConnect(_ kgo.BrokerMetadata, _ time.Duration, _ net.Conn, err error) {
	switch {
	case err == nil || isContextError(err) || kgo.IsRetryableBrokerErr(err):
		return
	default:
		ctx := context.TODO()
		logMsg := "kgo.OnBrokerConnect"

		if m.fatalOnError {
			m.log.Fatal(ctx, logMsg, err)
		} else {
			m.log.Error(ctx, logMsg, err)
		}
	}
}

func (m *hookEvent) OnBrokerDisconnect(_ kgo.BrokerMetadata, _ net.Conn) {}

func (m *hookEvent) OnBrokerWrite(_ kgo.BrokerMetadata, _ int16, _ int, _ time.Duration, _ time.Duration, err error) {
	switch {
	case err == nil || isContextError(err) || kgo.IsRetryableBrokerErr(err):
		return
	default:
		ctx := context.TODO()
		logMsg := "kgo.OnBrokerWrite"

		if m.fatalOnError {
			m.log.Fatal(ctx, logMsg, err)
		} else {
			m.log.Error(ctx, logMsg, err)
		}
	}
}

func (m *hookEvent) OnBrokerRead(_ kgo.BrokerMetadata, _ int16, _ int, _ time.Duration, _ time.Duration, err error) {
	switch {
	case err == nil || isContextError(err) || kgo.IsRetryableBrokerErr(err):
		return
	default:
		ctx := context.TODO()
		logMsg := "kgo.OnBrokerRead"

		if m.fatalOnError {
			m.log.Fatal(ctx, logMsg, err)
		} else {
			m.log.Error(ctx, logMsg, err)
		}
	}
}

func (m *hookEvent) OnProduceRecordUnbuffered(_ *kgo.Record, err error) {
	switch {
	case err == nil || isContextError(err) || kgo.IsRetryableBrokerErr(err):
		return
	default:
		ctx := context.TODO()
		logMsg := "kgo.OnProduceRecordUnbuffered"

		if m.fatalOnError {
			m.log.Fatal(ctx, logMsg, err)
		} else {
			m.log.Error(ctx, logMsg, err)
		}
	}
}
