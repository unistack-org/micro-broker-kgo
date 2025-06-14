package kgo

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
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
<<<<<<< HEAD
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

	go func() {
		ac := kadm.NewClient(s.c)
		ticker := time.NewTicker(DefaultStatsInterval)

		for {
			select {
			case <-ctx.Done():
				ticker.Stop()
				return
			case <-ticker.C:
				dgls, err := ac.Lag(ctx, s.opts.Group)
				if err != nil || !dgls.Ok() {
					continue
				}

				dgl, ok := dgls[s.opts.Group]
				if !ok {
					continue
				}
				lmap, ok := dgl.Lag[s.topic]
				if !ok {
					continue
				}

				s.mu.Lock()
				for p, l := range lmap {
					s.kopts.Meter.Counter(semconv.BrokerGroupLag, "topic", s.topic, "group", s.opts.Group, "partition", strconv.Itoa(int(p))).Set(uint64(l.Lag))
				}
				s.mu.Unlock()

			}
		}
	}()

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
				return
			}
			fetches.EachError(func(t string, p int32, err error) {
				s.kopts.Logger.Fatal(ctx, fmt.Sprintf("[kgo] fetch topic %s partition %d error", t, p), err)
			})

			fetches.EachPartition(func(p kgo.FetchTopicPartition) {
				tps := tp{p.Topic, p.Partition}
				s.mu.Lock()
				c := s.consumers[tps]
				s.mu.Unlock()
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
			s.mu.Unlock()
			if !ok {
				continue
			}
			s.mu.Lock()
			delete(s.consumers, tps)
			s.mu.Unlock()
			close(pc.quit)
			if s.kopts.Logger.V(logger.DebugLevel) {
				s.kopts.Logger.Debug(ctx, fmt.Sprintf("[kgo] waiting for work to finish topic %s partition %d", topic, partition))
			}
			wg.Add(1)
			go func() { <-pc.done; wg.Done() }()
		}
	}
}

func (s *Subscriber) autocommit(_ *kgo.Client, _ *kmsg.OffsetCommitRequest, _ *kmsg.OffsetCommitResponse, err error) {
	if err != nil {
		//		s.connected.Store(0)
		if s.fatalOnError {
			s.kopts.Logger.Fatal(context.TODO(), "kgo.AutoCommitCallback error", err)
		}
	}
}

func (s *Subscriber) lost(ctx context.Context, _ *kgo.Client, lost map[string][]int32) {
	if s.kopts.Logger.V(logger.ErrorLevel) {
		s.kopts.Logger.Error(ctx, fmt.Sprintf("[kgo] lost %#+v", lost))
	}
	s.killConsumers(ctx, lost)
	// s.connected.Store(0)
}

func (s *Subscriber) revoked(ctx context.Context, c *kgo.Client, revoked map[string][]int32) {
	if s.kopts.Logger.V(logger.DebugLevel) {
		s.kopts.Logger.Debug(ctx, fmt.Sprintf("[kgo] revoked %#+v", revoked))
	}
	s.killConsumers(ctx, revoked)
	if err := c.CommitMarkedOffsets(ctx); err != nil {
		s.kopts.Logger.Error(ctx, "[kgo] revoked CommitMarkedOffsets error", err)
		// s.connected.Store(0)
	}
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
		case p := <-pc.recs:
			for _, record := range p.Records {
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

				switch h := pc.handler.(type) {
				case func(broker.Message) error:
					err = h(pm)
				case func([]broker.Message) error:
					err = h([]broker.Message{pm})
				}

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
					messagePool.Put(p)
				}
				if ack {
					pc.c.MarkCommitRecords(record)
				} else {
					if sp != nil {
						sp.Finish()
					}
					//					pc.connected.Store(0)
					pc.kopts.Logger.Fatal(pc.kopts.Context, "[kgo] message not commited")
					return
				}
				if sp != nil {
					sp.Finish()
				}
			}
		}
	}
}
