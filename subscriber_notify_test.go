package kgo

import (
	"context"
	"errors"
	"io"
	"net"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.unistack.org/micro/v3/broker"
	"go.unistack.org/micro/v3/logger"
)

func TestShouldSendErr_Filtering(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		wantSend bool
	}{
		{"context canceled", context.Canceled, false},
		{"context deadline exceeded", context.DeadlineExceeded, false},
		{"retryable: EOF", io.EOF, true},
		{"retryable: net.ErrClosed", net.ErrClosed, true},
		{"retryable: os.ErrDeadlineExceeded", os.ErrDeadlineExceeded, true},
		{"non-retryable error", errors.New("fatal broker error"), true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Subscriber{}
			assert.Equal(t, tt.wantSend, s.shouldSendErr(tt.err))
		})
	}
}

func TestShouldSendErr_Debounce_SameErrorFiltered(t *testing.T) {
	s := &Subscriber{}
	err := errors.New("broker error")

	assert.True(t, s.shouldSendErr(err))

	assert.False(t, s.shouldSendErr(err))
}

func TestShouldSendErr_Debounce_PassesAfterWindow(t *testing.T) {
	s := &Subscriber{}
	err := errors.New("broker error")

	assert.True(t, s.shouldSendErr(err))

	s.lastErrTime = time.Now().Add(-errDebounceInterval - time.Second)

	assert.True(t, s.shouldSendErr(err))
}

func TestShouldSendErr_DifferentErrors_NotDebounced(t *testing.T) {
	s := &Subscriber{}

	assert.True(t, s.shouldSendErr(errors.New("error one")))
	assert.True(t, s.shouldSendErr(errors.New("error two")))
}

func TestTryErrSend_NonBlocking_WhenChannelFull(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pc := &consumer{
		ctx:  ctx,
		errs: make(chan error, 2),
	}

	pc.tryErrSend(errors.New("err1"))
	pc.tryErrSend(errors.New("err2"))

	done := make(chan struct{})
	go func() {
		pc.tryErrSend(errors.New("err3"))
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("tryErrSend заблокировался на заполненном канале")
	}

	assert.Equal(t, 2, len(pc.errs))
}

func TestConsume_ErrChannel_NonFatal(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var handlerCallCount atomic.Int64
	var lastErrReceived atomic.Value

	pc := &consumer{
		topic:     "test-topic",
		partition: 0,
		ctx:       ctx,
		cancel:    cancel,
		done:      make(chan struct{}),
		recs:      make(chan kgo.FetchTopicPartition, 100),
		errs:      make(chan error, 8),
		handler: func(e broker.Event) error {
			handlerCallCount.Add(1)
			if e.Error() != nil {
				lastErrReceived.Store(e.Error())
			}
			return e.Ack()
		},
		kopts: broker.NewOptions(broker.Logger(logger.DefaultLogger)),
	}

	go pc.consume()

	injectedErr := errors.New("broker connection dropped")
	pc.errs <- injectedErr

	require.Eventually(t, func() bool {
		return handlerCallCount.Load() == 1
	}, time.Second, 5*time.Millisecond, "handler не был вызван после получения ошибки из errs")

	storedErr, _ := lastErrReceived.Load().(error)
	assert.ErrorIs(t, storedErr, injectedErr)

	select {
	case <-pc.done:
		t.Fatal("консюмер умер после нефатальной инфра-ошибки")
	default:
	}

	pc.errs <- errors.New("second infra error")
	require.Eventually(t, func() bool {
		return handlerCallCount.Load() == 2
	}, time.Second, 5*time.Millisecond, "handler не был вызван для второй ошибки")
}

func TestConsume_RecsError_Fatal(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var handlerCallCount atomic.Int64

	pc := &consumer{
		topic:     "test-topic",
		partition: 0,
		ctx:       ctx,
		cancel:    cancel,
		done:      make(chan struct{}),
		recs:      make(chan kgo.FetchTopicPartition, 100),
		errs:      make(chan error, 8),
		handler: func(e broker.Event) error {
			handlerCallCount.Add(1)
			return e.Ack()
		},
		kopts: broker.NewOptions(broker.Logger(logger.DefaultLogger)),
	}

	go pc.consume()

	pc.recs <- newErrorFetchTopicPartition(errors.New("partition error"), "test-topic", 0)

	require.Eventually(t, func() bool {
		return handlerCallCount.Load() == 1
	}, time.Second, 5*time.Millisecond, "handler не был вызван")

	select {
	case <-pc.done:
		// OK — ожидаемое поведение
	case <-time.After(time.Second):
		t.Fatal("консюмер не умер после ошибки из recs (ожидалась смерть)")
	}
}
