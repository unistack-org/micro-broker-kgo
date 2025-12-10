package kgo

import (
	"context"
	"fmt"
	"maps"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"go.unistack.org/micro/v4/broker"
	"go.unistack.org/micro/v4/logger"
	"go.unistack.org/micro/v4/metadata"
	"go.unistack.org/micro/v4/semconv"
	"go.unistack.org/micro/v4/tracer"
)

type tp struct {
	t string
	p int32
}

type consumer struct {
	topic       string
	c           *kgo.Client
	htracer     *hookTracer
	quit        chan struct{}
	done        chan struct{}
	recs        chan kgo.FetchTopicPartition
	kopts       broker.Options
	partition   int32
	opts        broker.SubscribeOptions
	handler     interface{}
	connected   *atomic.Uint32
	messagePool bool
}

type Subscriber struct {
	consumers    map[tp]*consumer
	c            *kgo.Client
	htracer      *hookTracer
	topic        string
	messagePool  bool
	handler      interface{}
	done         chan struct{}
	kopts        broker.Options
	opts         broker.SubscribeOptions
	connected    *atomic.Uint32
	mu           sync.RWMutex
	closed       bool
	fatalOnError bool
}

func (s *Subscriber) Client() *kgo.Client {
	return s.c
}

func (s *Subscriber) Options() broker.SubscribeOptions {
	return s.opts
}

func (s *Subscriber) Topic() string {
	return s.topic
}

func (s *Subscriber) Unsubscribe(ctx context.Context) error {
	if s.closed {
		return nil
	}

	s.c.PauseFetchTopics(s.topic)
	s.c.CloseAllowingRebalance()
	kc := make(map[string][]int32)
	for ctp := range s.consumers {
		kc[ctp.t] = append(kc[ctp.t], ctp.p)
	}
	s.killConsumers(ctx, kc)
	close(s.done)
	s.closed = true
	s.c.ResumeFetchTopics(s.topic)

	return nil
}

func (s *Subscriber) poll(ctx context.Context) {
	maxInflight := DefaultSubscribeMaxInflight

	if s.opts.Context != nil {
		if n, ok := s.opts.Context.Value(subscribeMaxInflightKey{}).(int); n > 0 && ok {
			maxInflight = n
		}
	}

	for {
		select {
		case <-ctx.Done():
			s.c.CloseAllowingRebalance()
			return
		case <-s.done:
			return
		default:
			fetches := s.c.PollRecords(ctx, maxInflight)
			if !s.closed && fetches.IsClientClosed() {
				s.closed = true
				s.mu.Lock()
				tpc := make(map[tp]*consumer, len(s.consumers))
				maps.Copy(tpc, s.consumers)
				s.mu.Unlock()
				for tp, c := range tpc {
					if c != nil {
						c.recs <- newErrorFetchTopicPartition(kgo.ErrClientClosed, tp.t, tp.p)
					}
				}
				return
			}
			fetches.EachError(func(t string, p int32, err error) {
				tps := tp{t, p}
				s.mu.RLock()
				c := s.consumers[tps]
				s.mu.RUnlock()
				if c != nil {
					c.recs <- newErrorFetchTopicPartition(err, t, p)
				}
			})

			fetches.EachPartition(func(p kgo.FetchTopicPartition) {
				tps := tp{p.Topic, p.Partition}
				s.mu.RLock()
				c := s.consumers[tps]
				s.mu.RUnlock()
				if c != nil {
					c.recs <- p
				}
			})
			s.c.AllowRebalance()
		}
	}
}

func (s *Subscriber) killConsumers(ctx context.Context, lost map[string][]int32) {
	var wg sync.WaitGroup
	defer wg.Wait()

	for topic, partitions := range lost {
		for _, partition := range partitions {
			tps := tp{topic, partition}
			s.mu.Lock()
			pc, ok := s.consumers[tps]
			if ok {
				delete(s.consumers, tps)
			}
			s.mu.Unlock()
			if !ok || pc == nil {
				continue
			}

			if s.kopts.Logger.V(logger.DebugLevel) {
				s.kopts.Logger.Debug(ctx, fmt.Sprintf("[kgo] killing consumer topic %s partition %d", topic, partition))
			}

			close(pc.quit)

			wg.Add(1)
			go func(c *consumer, t string, p int32) {
				defer wg.Done()

				timeout := time.NewTimer(s.kopts.GracefulTimeout)
				defer timeout.Stop()

				select {
				case <-c.done:
					if s.kopts.Logger.V(logger.DebugLevel) {
						s.kopts.Logger.Debug(ctx, fmt.Sprintf("[kgo] consumer stopped topic %s partition %d", t, p))
					}
				case <-timeout.C:
					if s.kopts.Logger.V(logger.DebugLevel) {
						s.kopts.Logger.Debug(ctx, fmt.Sprintf("[kgo] timeout waiting for consumer topic %s partition %d", t, p))
					}
				}
			}(pc, topic, partition)
		}
	}
}

func (s *Subscriber) autocommit(_ *kgo.Client, r *kmsg.OffsetCommitRequest, _ *kmsg.OffsetCommitResponse, err error) {
	if err != nil {
		s.mu.Lock()
		tpc := make(map[tp]*consumer, len(s.consumers))
		maps.Copy(tpc, s.consumers)
		s.mu.Unlock()

		for _, tc := range r.Topics {
			for _, c := range s.consumers {
				if c != nil {
					for _, p := range tc.Partitions {
						c.recs <- newErrorFetchTopicPartition(err, tc.Topic, p.Partition)
					}
				}
			}
		}
	}
}

func (s *Subscriber) lost(ctx context.Context, _ *kgo.Client, lost map[string][]int32) {
	if s.kopts.Logger.V(logger.ErrorLevel) {
		s.kopts.Logger.Error(ctx, fmt.Sprintf("[kgo] lost %#+v", lost))
	}
	s.killConsumers(ctx, lost)
}

func (s *Subscriber) revoked(ctx context.Context, c *kgo.Client, revoked map[string][]int32) {
	if s.kopts.Logger.V(logger.DebugLevel) {
		s.kopts.Logger.Debug(ctx, fmt.Sprintf("[kgo] revoked %#+v", revoked))
	}
	if err := c.CommitMarkedOffsets(ctx); err != nil {
		s.mu.Lock()
		tpc := make(map[tp]*consumer, len(s.consumers))
		maps.Copy(tpc, s.consumers)
		s.mu.Unlock()
		for tp, c := range tpc {
			if c != nil {
				c.recs <- newErrorFetchTopicPartition(err, tp.t, tp.p)
			}
		}
	}
	s.killConsumers(ctx, revoked)
}

func (s *Subscriber) assigned(_ context.Context, c *kgo.Client, assigned map[string][]int32) {
	for topic, partitions := range assigned {
		for _, partition := range partitions {
			pc := &consumer{
				c:           c,
				topic:       topic,
				partition:   partition,
				htracer:     s.htracer,
				quit:        make(chan struct{}),
				done:        make(chan struct{}),
				recs:        make(chan kgo.FetchTopicPartition, 100),
				handler:     s.handler,
				messagePool: s.messagePool,
				kopts:       s.kopts,
				opts:        s.opts,
				connected:   s.connected,
			}
			s.mu.Lock()
			s.consumers[tp{topic, partition}] = pc
			s.mu.Unlock()
			go pc.consume()
		}
	}
}

func (pc *consumer) consume() {
	var err error

	defer close(pc.done)
	if pc.kopts.Logger.V(logger.DebugLevel) {
		pc.kopts.Logger.Debug(pc.kopts.Context, fmt.Sprintf("starting, topic %s partition %d", pc.topic, pc.partition))
		defer pc.kopts.Logger.Debug(pc.kopts.Context, fmt.Sprintf("killing, topic %s partition %d", pc.topic, pc.partition))
	}

	var pm *kgoMessage

	for {
		select {
		case <-pc.quit:
			return
		case p, ok := <-pc.recs:
			if !ok {
				return
			}
			if p.Err != nil || p.FetchPartition.Err != nil {

				if p.Err != nil {
					pm = pc.newErrorMessage(p.Err, p.Topic, p.Partition)
				} else if p.FetchPartition.Err != nil {
					pm = pc.newErrorMessage(p.FetchPartition.Err, p.Topic, p.Partition)
				}

				switch h := pc.handler.(type) {
				case func(broker.Message) error:
					_ = h(pm)
				case func([]broker.Message) error:
					_ = h([]broker.Message{pm})
				}
				if pc.messagePool {
					messagePool.Put(pm)
				}

				return
			}

			for _, record := range p.Records {
				select {
				case <-pc.quit:
					return
				default:
				}
				ctx, sp := pc.htracer.WithProcessSpan(record)
				ts := time.Now()
				pc.kopts.Meter.Counter(semconv.SubscribeMessageInflight, "endpoint", record.Topic, "topic", record.Topic).Inc()

				if pc.messagePool {
					pm = messagePool.Get().(*kgoMessage)
				} else {
					pm = &kgoMessage{}
				}

				pm.body = record.Value
				pm.topic = record.Topic
				pm.ack = false
				pm.hdr = metadata.New(len(record.Headers))
				pm.ctx = ctx

				for _, hdr := range record.Headers {
					pm.hdr.Set(hdr.Key, string(hdr.Value))
				}

				pm.hdr.Set("Micro-Offset", strconv.FormatInt(record.Offset, 10))
				pm.hdr.Set("Micro-Partition", strconv.FormatInt(int64(record.Partition), 10))
				pm.hdr.Set("Micro-Topic", record.Topic)
				pm.hdr.Set("Micro-Key", string(record.Key))
				pm.hdr.Set("Micro-Timestamp", strconv.FormatInt(record.Timestamp.Unix(), 10))

				processCtx, cancel := context.WithTimeout(ctx, pc.kopts.GracefulTimeout)
				errChan := make(chan error, 1)

				go func() {
					switch h := pc.handler.(type) {
					case func(broker.Message) error:
						errChan <- h(pm)
					case func([]broker.Message) error:
						errChan <- h([]broker.Message{pm})
					}
				}()

				select {
				case err = <-errChan:
				case <-processCtx.Done():
					//err = fmt.Errorf("[kgo] message processing timeout topic %s partition %d offset %d", record.Topic, record.Partition, record.Offset)
					err = processCtx.Err()
				}
				cancel()

				pc.kopts.Meter.Counter(semconv.SubscribeMessageInflight, "endpoint", record.Topic, "topic", record.Topic).Dec()
				if err != nil {
					if sp != nil {
						sp.SetStatus(tracer.SpanStatusError, err.Error())
					}
					pc.kopts.Meter.Counter(semconv.SubscribeMessageTotal, "endpoint", record.Topic, "topic", record.Topic, "status", "failure").Inc()
				} else if pc.opts.AutoAck {
					pm.ack = true
				}

				te := time.Since(ts)
				pc.kopts.Meter.Summary(semconv.SubscribeMessageLatencyMicroseconds, "endpoint", record.Topic, "topic", record.Topic).Update(te.Seconds())
				pc.kopts.Meter.Histogram(semconv.SubscribeMessageDurationSeconds, "endpoint", record.Topic, "topic", record.Topic).Update(te.Seconds())

				ack := pm.ack
				if pc.messagePool {
					messagePool.Put(pm)
				}

				if sp != nil {
					sp.Finish()
				}

				if ack {
					pc.c.MarkCommitRecords(record)
					continue
				}

				pc.kopts.Logger.Debug(pc.kopts.Context, fmt.Sprintf("[kgo] message not acknowledged topic %s partition %d offset %d", record.Topic, record.Partition, record.Offset))

				pm := pc.newErrorMessage(ErrLostMessage, p.Topic, p.Partition)
				switch h := pc.handler.(type) {
				case func(broker.Message) error:
					_ = h(pm)
				case func([]broker.Message) error:
					_ = h([]broker.Message{pm})
				}
				if pc.messagePool {
					messagePool.Put(pm)
				}
				return
			}
		}
	}
}

func (pc *consumer) newErrorMessage(err error, t string, p int32) *kgoMessage {
	var pm *kgoMessage

	if pc.messagePool {
		pm = messagePool.Get().(*kgoMessage)
	} else {
		pm = &kgoMessage{}
	}

	pm.ack = false
	pm.body = nil
	pm.err = err
	pm.topic = t
	pm.hdr = metadata.New(2)
	pm.ctx = context.Background()
	pm.hdr.Set("Micro-Partition", strconv.FormatInt(int64(p), 10))
	pm.hdr.Set("Micro-Topic", t)

	return pm
}

func newErrorFetchTopicPartition(err error, t string, p int32) kgo.FetchTopicPartition {
	return kgo.FetchTopicPartition{
		Topic: t,
		FetchPartition: kgo.FetchPartition{
			Partition: p,
			Err:       err,
		},
	}
}

var (
	_ kgo.HookBrokerConnect           = (*Subscriber)(nil)
	_ kgo.HookBrokerDisconnect        = (*Subscriber)(nil)
	_ kgo.HookBrokerRead              = (*Subscriber)(nil)
	_ kgo.HookBrokerWrite             = (*Subscriber)(nil)
	_ kgo.HookGroupManageError        = (*Subscriber)(nil)
	_ kgo.HookProduceRecordUnbuffered = (*Subscriber)(nil)
)

func (s *Subscriber) OnGroupManageError(err error) {
	if err == nil {
		return
	}
	s.mu.Lock()
	tpc := make(map[tp]*consumer, len(s.consumers))
	maps.Copy(tpc, s.consumers)
	s.mu.Unlock()
	for tp, c := range tpc {
		if c != nil {
			c.recs <- newErrorFetchTopicPartition(err, tp.t, tp.p)
		}
	}
}

func (s *Subscriber) OnBrokerConnect(_ kgo.BrokerMetadata, _ time.Duration, _ net.Conn, err error) {
	if err == nil {
		return
	}
	s.mu.Lock()
	tpc := make(map[tp]*consumer, len(s.consumers))
	maps.Copy(tpc, s.consumers)
	s.mu.Unlock()
	for tp, c := range tpc {
		if c != nil {
			c.recs <- newErrorFetchTopicPartition(err, tp.t, tp.p)
		}
	}
}

func (s *Subscriber) OnBrokerDisconnect(_ kgo.BrokerMetadata, _ net.Conn) {
}

func (s *Subscriber) OnBrokerWrite(_ kgo.BrokerMetadata, _ int16, _ int, _ time.Duration, _ time.Duration, err error) {
	if err == nil {
		return
	}
	s.mu.Lock()
	tpc := make(map[tp]*consumer, len(s.consumers))
	maps.Copy(tpc, s.consumers)
	s.mu.Unlock()
	for tp, c := range tpc {
		if c != nil {
			c.recs <- newErrorFetchTopicPartition(err, tp.t, tp.p)
		}
	}
}

func (s *Subscriber) OnBrokerRead(_ kgo.BrokerMetadata, _ int16, _ int, _ time.Duration, _ time.Duration, err error) {
	if err == nil {
		return
	}
	s.mu.Lock()
	tpc := make(map[tp]*consumer, len(s.consumers))
	maps.Copy(tpc, s.consumers)
	s.mu.Unlock()
	for tp, c := range tpc {
		if c != nil {
			c.recs <- newErrorFetchTopicPartition(err, tp.t, tp.p)
		}
	}
}

func (s *Subscriber) OnProduceRecordUnbuffered(_ *kgo.Record, err error) {
	if err == nil {
		return
	}
	s.mu.Lock()
	tpc := make(map[tp]*consumer, len(s.consumers))
	maps.Copy(tpc, s.consumers)
	s.mu.Unlock()
	for tp, c := range tpc {
		if c != nil {
			c.recs <- newErrorFetchTopicPartition(err, tp.t, tp.p)
		}
	}
}
