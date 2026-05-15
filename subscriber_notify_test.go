package kgo

import (
	"context"
	"errors"
	"io"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.unistack.org/micro/v5/broker"
	"go.unistack.org/micro/v5/logger"
)

// --- shouldSendErr unit tests ---

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

// --- tryErrSend / trySend unit tests ---

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

// TestTrySend_NonBlocking: trySend не блокируется при заполненном recs канале.
func TestTrySend_NonBlocking(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pc := &consumer{
		ctx:  ctx,
		recs: make(chan kgo.FetchTopicPartition, 1),
		errs: make(chan error, 8),
	}

	// заполняем
	pc.recs <- kgo.FetchTopicPartition{}

	done := make(chan struct{})
	go func() {
		pc.trySend(newErrorFetchTopicPartition(errors.New("err"), "t", 0))
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("trySend заблокировался на заполненном recs канале")
	}
}

// --- consume() unit tests ---

func newTestConsumer(handler func(broker.Message) error) *consumer {
	ctx, cancel := context.WithCancel(context.Background())
	return &consumer{
		topic:     "test-topic",
		partition: 0,
		ctx:       ctx,
		cancel:    cancel,
		done:      make(chan struct{}),
		recs:      make(chan kgo.FetchTopicPartition, 100),
		errs:      make(chan error, 8),
		handler:   handler,
		kopts:     broker.NewOptions(broker.Logger(logger.DefaultLogger)),
	}
}

// TestConsume_ErrChannel_NonFatal: ошибка из errs не убивает consumer.
func TestConsume_ErrChannel_NonFatal(t *testing.T) {
	var handlerCallCount atomic.Int64
	var lastErrReceived atomic.Value

	pc := newTestConsumer(func(msg broker.Message) error {
		handlerCallCount.Add(1)
		if km, ok := msg.(*kgoMessage); ok && km.err != nil {
			lastErrReceived.Store(km.err)
		}
		return nil
	})

	go pc.consume()
	defer func() {
		pc.cancel()
		<-pc.done
	}()

	injectedErr := errors.New("broker connection dropped")
	pc.errs <- injectedErr

	require.Eventually(t, func() bool {
		return handlerCallCount.Load() == 1
	}, time.Second, 5*time.Millisecond, "handler не был вызван после ошибки из errs")

	storedErr, _ := lastErrReceived.Load().(error)
	assert.ErrorIs(t, storedErr, injectedErr)

	select {
	case <-pc.done:
		t.Fatal("consumer умер после нефатальной инфра-ошибки")
	default:
	}

	pc.errs <- errors.New("second infra error")
	require.Eventually(t, func() bool {
		return handlerCallCount.Load() == 2
	}, time.Second, 5*time.Millisecond, "handler не был вызван для второй ошибки")
}

// TestConsume_RecsError_Fatal: ошибка из recs убивает consumer (ожидаемое поведение).
func TestConsume_RecsError_Fatal(t *testing.T) {
	var handlerCallCount atomic.Int64

	pc := newTestConsumer(func(msg broker.Message) error {
		handlerCallCount.Add(1)
		return nil
	})

	go pc.consume()

	pc.recs <- newErrorFetchTopicPartition(errors.New("partition error"), "test-topic", 0)

	require.Eventually(t, func() bool {
		return handlerCallCount.Load() == 1
	}, time.Second, 5*time.Millisecond, "handler не был вызван")

	select {
	case <-pc.done:
		// OK — consumer должен умереть после recs-ошибки
	case <-time.After(time.Second):
		t.Fatal("consumer не умер после ошибки из recs (ожидалась смерть)")
	}
}

// --- autocommit unit tests ---

// TestAutocommit_NoError: при err==nil ничего не происходит.
func TestAutocommit_NoError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var notified atomic.Bool

	pc := &consumer{
		ctx:  ctx,
		errs: make(chan error, 8),
	}

	s := &Subscriber{}
	s.initConsumers()
	s.setConsumer(tp{t: "topic", p: 0}, pc)

	s.autocommit(nil, nil, nil, nil)

	select {
	case <-s.getConsumer(tp{t: "topic", p: 0}).errs:
		notified.Store(true)
	default:
	}

	assert.False(t, notified.Load(), "при err==nil consumers не должны получать уведомление")
}

// TestAutocommit_ClosedSubscriber: при closed==true ошибка не рассылается.
func TestAutocommit_ClosedSubscriber(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pc := &consumer{
		ctx:  ctx,
		errs: make(chan error, 8),
	}

	s := &Subscriber{}
	s.initConsumers()
	s.setConsumer(tp{t: "topic", p: 0}, pc)
	s.closed.Store(true)

	s.autocommit(nil, nil, nil, errors.New("commit error"))

	select {
	case <-pc.errs:
		t.Fatal("closed subscriber не должен рассылать ошибки")
	default:
	}
}

// TestAutocommit_ErrorRoutedViaErrs: ошибка уходит в errs (не recs), consumer не умирает.
func TestAutocommit_ErrorRoutedViaErrs(t *testing.T) {
	var handlerCallCount atomic.Int64
	var lastErrReceived atomic.Value

	pc := newTestConsumer(func(msg broker.Message) error {
		handlerCallCount.Add(1)
		if km, ok := msg.(*kgoMessage); ok && km.err != nil {
			lastErrReceived.Store(km.err)
		}
		return nil
	})

	s := &Subscriber{
		kopts: broker.NewOptions(broker.Logger(logger.DefaultLogger)),
		topic: "test-topic",
	}
	s.initConsumers()
	s.setConsumer(tp{t: "test-topic", p: 0}, pc)

	go pc.consume()
	defer func() {
		pc.cancel()
		<-pc.done
	}()

	commitErr := errors.New("offset commit failed")
	s.autocommit(nil, nil, nil, commitErr)

	require.Eventually(t, func() bool {
		return handlerCallCount.Load() == 1
	}, time.Second, 5*time.Millisecond, "handler не был вызван после autocommit ошибки")

	storedErr, _ := lastErrReceived.Load().(error)
	assert.ErrorIs(t, storedErr, commitErr)

	select {
	case <-pc.done:
		t.Fatal("consumer умер после autocommit ошибки (должен быть жив)")
	default:
	}
}

// TestAutocommit_NoRaceOnConsumersMap: autocommit не обращается к s.consumers без лока.
// Запускается с -race.
func TestAutocommit_NoRaceOnConsumersMap(t *testing.T) {
	s := &Subscriber{
		kopts: broker.NewOptions(broker.Logger(logger.DefaultLogger)),
		topic: "topic",
	}
	s.initConsumers()

	commitErr := errors.New("commit error")

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(2)
		go func(i int) {
			defer wg.Done()
			s.setConsumer(tp{t: "topic", p: int32(i)}, nil)
		}(i)
		go func() {
			defer wg.Done()
			s.autocommit(nil, nil, nil, commitErr)
		}()
	}
	wg.Wait()
}

// --- assigned() guard tests ---

// TestAssigned_SkipsWhenClosed: при closed==true assigned() не создаёт consumers.
func TestAssigned_SkipsWhenClosed(t *testing.T) {
	s := &Subscriber{}
	s.initConsumers()
	s.closed.Store(true)

	s.assigned(context.Background(), nil, map[string][]int32{
		"topic": {0, 1, 2},
	})

	assert.Equal(t, 0, s.consumersLen(), "closed subscriber не должен создавать consumers")
}

// TestAssigned_SpawnsConsumers: при closed==false assigned() создаёт consumers и запускает goroutines.
func TestAssigned_SpawnsConsumers(t *testing.T) {
	s := &Subscriber{
		kopts: broker.NewOptions(broker.Logger(logger.DefaultLogger)),
		handler: func(msg broker.Message) error {
			return msg.Ack()
		},
	}
	s.initConsumers()

	s.assigned(context.Background(), nil, map[string][]int32{
		"topic": {0, 1},
	})

	assert.Equal(t, 2, s.consumersLen(), "должны быть созданы 2 consumers")

	// Корректно останавливаем
	s.rangeConsumers(func(_ tp, pc *consumer) bool {
		pc.cancel()
		<-pc.done
		return true
	})
}

// TestKillConsumers_DoubleCancelSafe: двойной вызов cancel() не паникует.
func TestKillConsumers_DoubleCancelSafe(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	pc := &consumer{
		ctx:    ctx,
		cancel: cancel,
		done:   make(chan struct{}),
		recs:   make(chan kgo.FetchTopicPartition, 100),
		errs:   make(chan error, 8),
		kopts:  broker.NewOptions(broker.Logger(logger.DefaultLogger)),
		handler: func(msg broker.Message) error {
			return nil
		},
	}

	go pc.consume()

	// Двойная отмена — не должна паниковать (в отличие от close на закрытом канале)
	assert.NotPanics(t, func() {
		pc.cancel()
		pc.cancel()
	})

	<-pc.done
}
