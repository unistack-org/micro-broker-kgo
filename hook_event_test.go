package kgo

import (
	"context"
	"errors"
	"io"
	"net"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.unistack.org/micro/v4/logger"
)

func TestHookEvent_OnGroupManageError(t *testing.T) {
	tests := []struct {
		name                  string
		inputErr              error
		fatalOnError          bool
		expectedErrorIsCalled bool
		expectedErrorMsg      string
		expectedFatalIsCalled bool
		expectedFatalMsg      string
	}{
		{
			name:                  "error is nil",
			inputErr:              nil,
			expectedErrorIsCalled: false,
			expectedFatalIsCalled: false,
		},
		{
			name:                  "context canceled",
			inputErr:              context.Canceled,
			expectedErrorIsCalled: false,
			expectedFatalIsCalled: false,
		},
		{
			name:                  "context deadline exceeded",
			inputErr:              context.DeadlineExceeded,
			expectedErrorIsCalled: false,
			expectedFatalIsCalled: false,
		},
		{
			name:                  "retryable error: deadline exceeded (os package)",
			inputErr:              os.ErrDeadlineExceeded,
			expectedErrorIsCalled: false,
			expectedFatalIsCalled: false,
		},
		{
			name:                  "retryable error: EOF (io package)",
			inputErr:              io.EOF,
			expectedErrorIsCalled: false,
			expectedFatalIsCalled: false,
		},
		{
			name:                  "retryable error: closed network connection (net package)",
			inputErr:              net.ErrClosed,
			expectedErrorIsCalled: false,
			expectedFatalIsCalled: false,
		},
		{
			name:                  "some error (non-fatal)",
			inputErr:              errors.New("some error"),
			fatalOnError:          false,
			expectedErrorIsCalled: true,
			expectedErrorMsg:      "kgo.OnGroupManageError",
			expectedFatalIsCalled: false,
		},
		{
			name:                  "some error (fatal)",
			inputErr:              errors.New("some error"),
			fatalOnError:          true,
			expectedErrorIsCalled: false,
			expectedFatalIsCalled: true,
			expectedFatalMsg:      "kgo.OnGroupManageError",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			log := &mockLogger{}
			he := &hookEvent{log: log, fatalOnError: tt.fatalOnError}
			he.OnGroupManageError(tt.inputErr)
			require.Equal(t, tt.expectedErrorIsCalled, log.errorIsCalled)
			require.Equal(t, tt.expectedErrorMsg, log.errorMsg)
			require.Equal(t, tt.expectedFatalIsCalled, log.fatalIsCalled)
			require.Equal(t, tt.expectedFatalMsg, log.fatalMsg)
		})
	}
}

func TestHookEvent_OnBrokerConnect(t *testing.T) {
	tests := []struct {
		name                  string
		inputErr              error
		fatalOnError          bool
		expectedErrorIsCalled bool
		expectedErrorMsg      string
		expectedFatalIsCalled bool
		expectedFatalMsg      string
	}{
		{
			name:                  "error is nil",
			inputErr:              nil,
			expectedErrorIsCalled: false,
			expectedFatalIsCalled: false,
		},
		{
			name:                  "context canceled",
			inputErr:              context.Canceled,
			expectedErrorIsCalled: false,
			expectedFatalIsCalled: false,
		},
		{
			name:                  "context deadline exceeded",
			inputErr:              context.DeadlineExceeded,
			expectedErrorIsCalled: false,
			expectedFatalIsCalled: false,
		},
		{
			name:                  "retryable error: deadline exceeded (os package)",
			inputErr:              os.ErrDeadlineExceeded,
			expectedErrorIsCalled: false,
			expectedFatalIsCalled: false,
		},
		{
			name:                  "retryable error: EOF (io package)",
			inputErr:              io.EOF,
			expectedErrorIsCalled: false,
			expectedFatalIsCalled: false,
		},
		{
			name:                  "retryable error: closed network connection (net package)",
			inputErr:              net.ErrClosed,
			expectedErrorIsCalled: false,
			expectedFatalIsCalled: false,
		},
		{
			name:                  "some error (non-fatal)",
			inputErr:              errors.New("some error"),
			fatalOnError:          false,
			expectedErrorIsCalled: true,
			expectedErrorMsg:      "kgo.OnBrokerConnect",
			expectedFatalIsCalled: false,
		},
		{
			name:                  "some error (fatal)",
			inputErr:              errors.New("some error"),
			fatalOnError:          true,
			expectedErrorIsCalled: false,
			expectedFatalIsCalled: true,
			expectedFatalMsg:      "kgo.OnBrokerConnect",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			log := &mockLogger{}
			he := &hookEvent{log: log, fatalOnError: tt.fatalOnError}
			he.OnBrokerConnect(kgo.BrokerMetadata{}, 0, nil, tt.inputErr)
			require.Equal(t, tt.expectedErrorIsCalled, log.errorIsCalled)
			require.Equal(t, tt.expectedErrorMsg, log.errorMsg)
			require.Equal(t, tt.expectedFatalIsCalled, log.fatalIsCalled)
			require.Equal(t, tt.expectedFatalMsg, log.fatalMsg)
		})
	}
}

func TestHookEvent_OnBrokerWrite(t *testing.T) {
	tests := []struct {
		name                  string
		inputErr              error
		fatalOnError          bool
		expectedErrorIsCalled bool
		expectedErrorMsg      string
		expectedFatalIsCalled bool
		expectedFatalMsg      string
	}{
		{
			name:                  "error is nil",
			inputErr:              nil,
			expectedErrorIsCalled: false,
			expectedFatalIsCalled: false,
		},
		{
			name:                  "context canceled",
			inputErr:              context.Canceled,
			expectedErrorIsCalled: false,
			expectedFatalIsCalled: false,
		},
		{
			name:                  "context deadline exceeded",
			inputErr:              context.DeadlineExceeded,
			expectedErrorIsCalled: false,
			expectedFatalIsCalled: false,
		},
		{
			name:                  "retryable error: deadline exceeded (os package)",
			inputErr:              os.ErrDeadlineExceeded,
			expectedErrorIsCalled: false,
			expectedFatalIsCalled: false,
		},
		{
			name:                  "retryable error: EOF (io package)",
			inputErr:              io.EOF,
			expectedErrorIsCalled: false,
			expectedFatalIsCalled: false,
		},
		{
			name:                  "retryable error: closed network connection (net package)",
			inputErr:              net.ErrClosed,
			expectedErrorIsCalled: false,
			expectedFatalIsCalled: false,
		},
		{
			name:                  "some error (non-fatal)",
			inputErr:              errors.New("some error"),
			fatalOnError:          false,
			expectedErrorIsCalled: true,
			expectedErrorMsg:      "kgo.OnBrokerWrite",
			expectedFatalIsCalled: false,
		},
		{
			name:                  "some error (fatal)",
			inputErr:              errors.New("some error"),
			fatalOnError:          true,
			expectedErrorIsCalled: false,
			expectedFatalIsCalled: true,
			expectedFatalMsg:      "kgo.OnBrokerWrite",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			log := &mockLogger{}
			he := &hookEvent{log: log, fatalOnError: tt.fatalOnError}
			he.OnBrokerWrite(kgo.BrokerMetadata{}, 0, 0, 0, 0, tt.inputErr)
			require.Equal(t, tt.expectedErrorIsCalled, log.errorIsCalled)
			require.Equal(t, tt.expectedErrorMsg, log.errorMsg)
			require.Equal(t, tt.expectedFatalIsCalled, log.fatalIsCalled)
			require.Equal(t, tt.expectedFatalMsg, log.fatalMsg)
		})
	}
}

func TestHookEvent_OnBrokerRead(t *testing.T) {
	tests := []struct {
		name                  string
		inputErr              error
		fatalOnError          bool
		expectedErrorIsCalled bool
		expectedErrorMsg      string
		expectedFatalIsCalled bool
		expectedFatalMsg      string
	}{
		{
			name:                  "error is nil",
			inputErr:              nil,
			expectedErrorIsCalled: false,
			expectedFatalIsCalled: false,
		},
		{
			name:                  "context canceled",
			inputErr:              context.Canceled,
			expectedErrorIsCalled: false,
			expectedFatalIsCalled: false,
		},
		{
			name:                  "context deadline exceeded",
			inputErr:              context.DeadlineExceeded,
			expectedErrorIsCalled: false,
			expectedFatalIsCalled: false,
		},
		{
			name:                  "retryable error: deadline exceeded (os package)",
			inputErr:              os.ErrDeadlineExceeded,
			expectedErrorIsCalled: false,
			expectedFatalIsCalled: false,
		},
		{
			name:                  "retryable error: EOF (io package)",
			inputErr:              io.EOF,
			expectedErrorIsCalled: false,
			expectedFatalIsCalled: false,
		},
		{
			name:                  "retryable error: closed network connection (net package)",
			inputErr:              net.ErrClosed,
			expectedErrorIsCalled: false,
			expectedFatalIsCalled: false,
		},
		{
			name:                  "some error (non-fatal)",
			inputErr:              errors.New("some error"),
			fatalOnError:          false,
			expectedErrorIsCalled: true,
			expectedErrorMsg:      "kgo.OnBrokerRead",
			expectedFatalIsCalled: false,
		},
		{
			name:                  "some error (fatal)",
			inputErr:              errors.New("some error"),
			fatalOnError:          true,
			expectedErrorIsCalled: false,
			expectedFatalIsCalled: true,
			expectedFatalMsg:      "kgo.OnBrokerRead",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			log := &mockLogger{}
			he := &hookEvent{log: log, fatalOnError: tt.fatalOnError}
			he.OnBrokerRead(kgo.BrokerMetadata{}, 0, 0, 0, 0, tt.inputErr)
			require.Equal(t, tt.expectedErrorIsCalled, log.errorIsCalled)
			require.Equal(t, tt.expectedErrorMsg, log.errorMsg)
			require.Equal(t, tt.expectedFatalIsCalled, log.fatalIsCalled)
			require.Equal(t, tt.expectedFatalMsg, log.fatalMsg)
		})
	}
}

func TestHookEvent_OnProduceRecordUnbuffered(t *testing.T) {
	tests := []struct {
		name                  string
		inputErr              error
		fatalOnError          bool
		expectedErrorIsCalled bool
		expectedErrorMsg      string
		expectedFatalIsCalled bool
		expectedFatalMsg      string
	}{
		{
			name:                  "error is nil",
			inputErr:              nil,
			expectedErrorIsCalled: false,
			expectedFatalIsCalled: false,
		},
		{
			name:                  "context canceled",
			inputErr:              context.Canceled,
			expectedErrorIsCalled: false,
			expectedFatalIsCalled: false,
		},
		{
			name:                  "context deadline exceeded",
			inputErr:              context.DeadlineExceeded,
			expectedErrorIsCalled: false,
			expectedFatalIsCalled: false,
		},
		{
			name:                  "retryable error: deadline exceeded (os package)",
			inputErr:              os.ErrDeadlineExceeded,
			expectedErrorIsCalled: false,
			expectedFatalIsCalled: false,
		},
		{
			name:                  "retryable error: EOF (io package)",
			inputErr:              io.EOF,
			expectedErrorIsCalled: false,
			expectedFatalIsCalled: false,
		},
		{
			name:                  "retryable error: closed network connection (net package)",
			inputErr:              net.ErrClosed,
			expectedErrorIsCalled: false,
			expectedFatalIsCalled: false,
		},
		{
			name:                  "some error (non-fatal)",
			inputErr:              errors.New("some error"),
			fatalOnError:          false,
			expectedErrorIsCalled: true,
			expectedErrorMsg:      "kgo.OnProduceRecordUnbuffered",
			expectedFatalIsCalled: false,
		},
		{
			name:                  "some error (fatal)",
			inputErr:              errors.New("some error"),
			fatalOnError:          true,
			expectedErrorIsCalled: false,
			expectedFatalIsCalled: true,
			expectedFatalMsg:      "kgo.OnProduceRecordUnbuffered",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			log := &mockLogger{}
			he := &hookEvent{log: log, fatalOnError: tt.fatalOnError}
			he.OnProduceRecordUnbuffered(&kgo.Record{}, tt.inputErr)
			require.Equal(t, tt.expectedErrorIsCalled, log.errorIsCalled)
			require.Equal(t, tt.expectedErrorMsg, log.errorMsg)
			require.Equal(t, tt.expectedFatalIsCalled, log.fatalIsCalled)
			require.Equal(t, tt.expectedFatalMsg, log.fatalMsg)
		})
	}
}

// Mocks

type mockLogger struct {
	errorIsCalled bool
	errorMsg      string

	fatalIsCalled bool
	fatalMsg      string
}

func (m *mockLogger) Init(...logger.Option) error {
	panic("implement me")
}

func (m *mockLogger) Clone(...logger.Option) logger.Logger {
	panic("implement me")
}

func (m *mockLogger) V(logger.Level) bool {
	panic("implement me")
}

func (m *mockLogger) Level(logger.Level) {
	panic("implement me")
}

func (m *mockLogger) Options() logger.Options {
	panic("implement me")
}

func (m *mockLogger) Fields(...interface{}) logger.Logger {
	panic("implement me")
}

func (m *mockLogger) Info(context.Context, string, ...interface{}) {
	panic("implement me")
}

func (m *mockLogger) Trace(context.Context, string, ...interface{}) {
	panic("implement me")
}

func (m *mockLogger) Debug(context.Context, string, ...interface{}) {
	panic("implement me")
}

func (m *mockLogger) Warn(context.Context, string, ...interface{}) {
	panic("implement me")
}

func (m *mockLogger) Error(ctx context.Context, msg string, args ...interface{}) {
	m.errorIsCalled = true
	m.errorMsg = msg
}

func (m *mockLogger) Fatal(ctx context.Context, msg string, args ...interface{}) {
	m.fatalIsCalled = true
	m.fatalMsg = msg
}

func (m *mockLogger) Log(context.Context, logger.Level, string, ...interface{}) {
	panic("implement me")
}

func (m *mockLogger) Name() string {
	panic("implement me")
}

func (m *mockLogger) String() string {
	panic("implement me")
}
