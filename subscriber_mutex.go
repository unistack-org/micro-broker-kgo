//go:build !microbroker_syncmap

package kgo

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"go.unistack.org/micro/v4/broker"
)

type Subscriber struct {
	consumers map[tp]*consumer

	c           *kgo.Client
	htracer     *hookTracer
	topic       string
	messagePool bool
	handler     interface{}
	done        chan struct{}
	kopts       broker.Options
	opts        broker.SubscribeOptions
	connected   *atomic.Uint32

	lastErrMu   sync.Mutex
	lastErr     error
	lastErrTime time.Time

	mu           sync.RWMutex
	closed       atomic.Bool
	fatalOnError bool
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

//nolint:unused
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
	case <-c.ctx.Done():
	}
}