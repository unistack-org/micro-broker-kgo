//go:build syncmap

package kgo

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"go.unistack.org/micro/v3/broker"
)

type Subscriber struct {
	topic string

	consumers sync.Map // map[tp]*consumer

	c         *kgo.Client
	htracer   *hookTracer
	connected *atomic.Uint32

	handler broker.Handler

	done chan struct{}

	kopts broker.Options
	opts  broker.SubscribeOptions

	closed       atomic.Bool
	fatalOnError bool

	lastErrMu   sync.Mutex
	lastErr     error
	lastErrTime time.Time
}

func (s *Subscriber) initConsumers() {
	// sync.Map is zero-value usable, no initialization needed
}

func (s *Subscriber) getConsumer(key tp) *consumer {
	v, ok := s.consumers.Load(key)
	if !ok {
		return nil
	}
	return v.(*consumer)
}

func (s *Subscriber) setConsumer(key tp, c *consumer) {
	s.consumers.Store(key, c)
}

func (s *Subscriber) deleteConsumer(key tp) (*consumer, bool) {
	v, ok := s.consumers.LoadAndDelete(key)
	if !ok {
		return nil, false
	}
	return v.(*consumer), true
}

func (s *Subscriber) rangeConsumers(fn func(tp, *consumer) bool) {
	s.consumers.Range(func(key, value any) bool {
		return fn(key.(tp), value.(*consumer))
	})
}

func (s *Subscriber) copyConsumers() map[tp]*consumer {
	tpc := make(map[tp]*consumer)
	s.consumers.Range(func(key, value any) bool {
		tpc[key.(tp)] = value.(*consumer)
		return true
	})
	return tpc
}

func (s *Subscriber) consumersLen() int {
	n := 0
	s.consumers.Range(func(_, _ any) bool {
		n++
		return true
	})
	return n
}

func (s *Subscriber) sendToConsumer(key tp, ftp kgo.FetchTopicPartition) {
	v, ok := s.consumers.Load(key)
	if !ok {
		return
	}
	c := v.(*consumer)
	select {
	case c.recs <- ftp:
	case <-c.ctx.Done():
	}
}
