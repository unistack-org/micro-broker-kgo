package kgo

import (
	"context"
	"fmt"

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
	if len(args) > 0 {
		fields := make([]interface{}, 0, len(args))
		for i := 0; i <= len(args)/2; i += 2 {
			fields = append(fields, fmt.Sprintf("%v", args[i]), args[i+1])
		}
		l.l.Fields(fields...).Log(l.ctx, mlvl, msg)
	} else {
		l.l.Log(l.ctx, mlvl, msg)
	}
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
