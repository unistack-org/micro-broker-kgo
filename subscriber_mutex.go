//go:build !syncmap

package kgo

import (
	"sync"
	"sync/atomic"

	"github.com/twmb/franz-go/pkg/kgo"
	"go.unistack.org/micro/v3/broker"
)

type Subscriber struct {
	topic string

	consumers map[tp]*consumer

	c         *kgo.Client
	htracer   *hookTracer
	connected *atomic.Uint32

	handler broker.Handler

	done chan struct{}

	kopts broker.Options
	opts  broker.SubscribeOptions

	closed       atomic.Bool
	fatalOnError bool

	mu sync.RWMutex
}

func (s *Subscriber) initConsumers() {
	s.consumers = make(map[tp]*consumer)
}

func (s *Subscriber) getConsumer(key tp) *consumer {
	s.mu.RLock()
	c := s.consumers[key]
	s.mu.RUnlock()
	return c
}

func (s *Subscriber) setConsumer(key tp, c *consumer) {
	s.mu.Lock()
	s.consumers[key] = c
	s.mu.Unlock()
}

func (s *Subscriber) deleteConsumer(key tp) (*consumer, bool) {
	s.mu.Lock()
	c, ok := s.consumers[key]
	if ok {
		delete(s.consumers, key)
	}
	s.mu.Unlock()
	return c, ok
}

func (s *Subscriber) rangeConsumers(fn func(tp, *consumer) bool) {
	s.mu.RLock()
	for k, v := range s.consumers {
		if !fn(k, v) {
			break
		}
	}
	s.mu.RUnlock()
}

func (s *Subscriber) copyConsumers() map[tp]*consumer {
	s.mu.RLock()
	tpc := make(map[tp]*consumer, len(s.consumers))
	for k, v := range s.consumers {
		tpc[k] = v
	}
	s.mu.RUnlock()
	return tpc
}

// nolint
func (s *Subscriber) consumersLen() int {
	s.mu.RLock()
	n := len(s.consumers)
	s.mu.RUnlock()
	return n
}

func (s *Subscriber) sendToConsumer(key tp, ftp kgo.FetchTopicPartition) {
	s.mu.RLock()
	c := s.consumers[key]
	s.mu.RUnlock()
	if c == nil {
		return
	}
	select {
	case c.recs <- ftp:
	case <-c.quit:
	}
}
