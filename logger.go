package kgo

import (
	"context"

	"github.com/twmb/franz-go/pkg/kgo"
	"go.unistack.org/micro/v3/logger"
)

type mlogger struct {
	l   logger.Logger
	ctx context.Context
}

func (l *mlogger) Log(lvl kgo.LogLevel, msg string, args ...interface{}) {
	var mlvl logger.Level
	switch lvl {
	case kgo.LogLevelNone:
		return
	case kgo.LogLevelError:
		mlvl = logger.ErrorLevel
	case kgo.LogLevelWarn:
		mlvl = logger.WarnLevel
	case kgo.LogLevelInfo:
		mlvl = logger.InfoLevel
	case kgo.LogLevelDebug:
		mlvl = logger.DebugLevel
	default:
		return
	}

	l.l.Log(l.ctx, mlvl, msg, args...)
}

func (l *mlogger) Level() kgo.LogLevel {
	switch l.l.Options().Level {
	case logger.ErrorLevel:
		return kgo.LogLevelError
	case logger.WarnLevel:
		return kgo.LogLevelWarn
	case logger.InfoLevel:
		return kgo.LogLevelInfo
	case logger.DebugLevel, logger.TraceLevel:
		return kgo.LogLevelDebug
	}
	return kgo.LogLevelNone
}
