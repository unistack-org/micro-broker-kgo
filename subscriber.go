package kgo

import (
	"context"
	"fmt"
	"sync"

	"github.com/twmb/franz-go/pkg/kgo"
	"go.unistack.org/micro/v3/broker"
	"go.unistack.org/micro/v3/logger"
	"go.unistack.org/micro/v3/metadata"
)

type tp struct {
	t string
	p int32
}

type consumer struct {
	topic string

	c *kgo.Client

	handler broker.Handler
	quit    chan struct{}
	done    chan struct{}
	recs    chan kgo.FetchTopicPartition

	kopts broker.Options
	opts  broker.SubscribeOptions

	partition int32
}

type subscriber struct {
	consumers map[tp]*consumer
	c         *kgo.Client
	topic     string

	handler broker.Handler
	done    chan struct{}
	kopts   broker.Options
	opts    broker.SubscribeOptions

	sync.RWMutex
	closed bool
}

func (s *subscriber) Options() broker.SubscribeOptions {
	return s.opts
}

func (s *subscriber) Topic() string {
	return s.topic
}

func (s *subscriber) Unsubscribe(ctx context.Context) error {
	if s.closed {
		return nil
	}
	// select {
	// case <-ctx.Done():
	//	return ctx.Err()
	// default:
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
	// }
	return nil
}

func (s *subscriber) poll(ctx context.Context) {
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
				return
			}
			fetches.EachError(func(t string, p int32, err error) {
				s.kopts.Logger.Fatal(ctx, fmt.Sprintf("[kgo] fetch topic %s partition %d", t, p), err)
			})

			fetches.EachPartition(func(p kgo.FetchTopicPartition) {
				nTp := tp{p.Topic, p.Partition}
				s.consumers[nTp].recs <- p
			})
			s.c.AllowRebalance()
		}
	}
}

func (s *subscriber) killConsumers(ctx context.Context, lost map[string][]int32) {
	var wg sync.WaitGroup
	defer wg.Wait()

	for topic, partitions := range lost {
		for _, partition := range partitions {
			nTp := tp{topic, partition}
			pc := s.consumers[nTp]
			delete(s.consumers, nTp)
			close(pc.quit)
			s.kopts.Logger.Debug(ctx, fmt.Sprintf("[kgo] waiting for work to finish topic %s partition %d", topic, partition))
			wg.Add(1)
			go func() { <-pc.done; wg.Done() }()
		}
	}
}

func (s *subscriber) lost(ctx context.Context, _ *kgo.Client, lost map[string][]int32) {
	s.kopts.Logger.Debug(ctx, fmt.Sprintf("[kgo] lost %#+v", lost))
	s.killConsumers(ctx, lost)
}

func (s *subscriber) revoked(ctx context.Context, c *kgo.Client, revoked map[string][]int32) {
	s.kopts.Logger.Debug(ctx, fmt.Sprintf("[kgo] revoked %#+v", revoked))
	s.killConsumers(ctx, revoked)
	if err := c.CommitMarkedOffsets(ctx); err != nil {
		s.kopts.Logger.Error(ctx, "[kgo] revoked CommitMarkedOffsets", err)
	}
}

func (s *subscriber) assigned(_ context.Context, c *kgo.Client, assigned map[string][]int32) {
	for topic, partitions := range assigned {
		for _, partition := range partitions {
			pc := &consumer{
				c:         c,
				topic:     topic,
				partition: partition,

				quit:    make(chan struct{}),
				done:    make(chan struct{}),
				recs:    make(chan kgo.FetchTopicPartition, 100),
				handler: s.handler,
				kopts:   s.kopts,
				opts:    s.opts,
			}
			s.consumers[tp{topic, partition}] = pc
			go pc.consume()
		}
	}
}

func (pc *consumer) consume() {
	defer close(pc.done)
	pc.kopts.Logger.Debug(pc.kopts.Context, fmt.Sprintf("starting, topic %s partition %d", pc.topic, pc.partition))
	defer pc.kopts.Logger.Debug(pc.kopts.Context, fmt.Sprintf("killing, topic %s partition %d", pc.topic, pc.partition))

	eh := pc.kopts.ErrorHandler
	if pc.opts.ErrorHandler != nil {
		eh = pc.opts.ErrorHandler
	}

	for {
		select {
		case <-pc.quit:
			return
		case p := <-pc.recs:
			for _, record := range p.Records {
				// ts := time.Now()
				//	pc.kopts.Meter.Counter(broker.SubscribeMessageInflight, "endpoint", record.Topic).Inc()
				p := eventPool.Get().(*event)
				p.msg.Header = nil
				p.msg.Body = nil
				p.topic = record.Topic
				p.err = nil
				p.ack = false
				p.msg.Header = metadata.New(len(record.Headers))
				for _, hdr := range record.Headers {
					p.msg.Header.Set(hdr.Key, string(hdr.Value))
				}
				if pc.kopts.Codec.String() == "noop" {
					p.msg.Body = record.Value
				} else if pc.opts.BodyOnly {
					p.msg.Body = record.Value
				} else {
					if err := pc.kopts.Codec.Unmarshal(record.Value, p.msg); err != nil {
						//	pc.kopts.Meter.Counter(broker.SubscribeMessageTotal, "endpoint", record.Topic, "status", "failure").Inc()
						p.err = err
						p.msg.Body = record.Value
						if eh != nil {
							_ = eh(p)
							//	pc.kopts.Meter.Counter(broker.SubscribeMessageInflight, "endpoint", record.Topic).Dec()
							if p.ack {
								pc.c.MarkCommitRecords(record)
							} else {
								eventPool.Put(p)
								pc.kopts.Logger.Fatal(pc.kopts.Context, "[kgo] ErrLostMessage wtf?")
								return
							}
							eventPool.Put(p)
							// te := time.Since(ts)
							//	pc.kopts.Meter.Summary(broker.SubscribeMessageLatencyMicroseconds, "endpoint", record.Topic).Update(te.Seconds())
							//	pc.kopts.Meter.Histogram(broker.SubscribeMessageDurationSeconds, "endpoint", record.Topic).Update(te.Seconds())
							continue
						} else {
							if pc.kopts.Logger.V(logger.ErrorLevel) {
								pc.kopts.Logger.Error(pc.kopts.Context, "[kgo]: failed to unmarshal", err)
							}
						}
						//		te := time.Since(ts)
						//		pc.kopts.Meter.Counter(broker.SubscribeMessageInflight, "endpoint", record.Topic).Dec()
						//		pc.kopts.Meter.Summary(broker.SubscribeMessageLatencyMicroseconds, "endpoint", record.Topic).Update(te.Seconds())
						//			pc.kopts.Meter.Histogram(broker.SubscribeMessageDurationSeconds, "endpoint", record.Topic).Update(te.Seconds())
						eventPool.Put(p)
						pc.kopts.Logger.Fatal(pc.kopts.Context, "[kgo] Unmarshal err not handled wtf?")
						return
					}
				}
				err := pc.handler(p)
				// if err == nil {
				//		pc.kopts.Meter.Counter(broker.SubscribeMessageTotal, "endpoint", record.Topic, "status", "success").Inc()
				// } else {
				//		pc.kopts.Meter.Counter(broker.SubscribeMessageTotal, "endpoint", record.Topic, "status", "failure").Inc()
				// }
				// pc.kopts.Meter.Counter(broker.SubscribeMessageInflight, "endpoint", record.Topic).Dec()
				if err == nil && pc.opts.AutoAck {
					p.ack = true
				} else if err != nil {
					p.err = err
					if eh != nil {
						_ = eh(p)
					} else {
						if pc.kopts.Logger.V(logger.ErrorLevel) {
							pc.kopts.Logger.Error(pc.kopts.Context, "[kgo]: subscriber error", err)
						}
					}
				}
				//	te := time.Since(ts)
				//	pc.kopts.Meter.Summary(broker.SubscribeMessageLatencyMicroseconds, "endpoint", record.Topic).Update(te.Seconds())
				//	pc.kopts.Meter.Histogram(broker.SubscribeMessageDurationSeconds, "endpoint", record.Topic).Update(te.Seconds())
				if p.ack {
					eventPool.Put(p)
					pc.c.MarkCommitRecords(record)
				} else {
					eventPool.Put(p)
					pc.kopts.Logger.Fatal(pc.kopts.Context, "[kgo] ErrLostMessage wtf?")
					return
				}
			}
		}
	}
}
