package kgo

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"go.unistack.org/micro/v3/broker"
	"go.unistack.org/micro/v3/logger"
	"go.unistack.org/micro/v3/metadata"
	"go.unistack.org/micro/v3/semconv"
	"go.unistack.org/micro/v3/tracer"
)

type tp struct {
	t string
	p int32
}

const errDebounceInterval = 5 * time.Second

type consumer struct {
	topic string

	c         *kgo.Client
	htracer   *hookTracer
	connected *atomic.Uint32

	ctx    context.Context
	cancel context.CancelFunc
	done   chan struct{}
	recs   chan kgo.FetchTopicPartition
	errs   chan error

	handler broker.Handler

	kopts broker.Options
	opts  broker.SubscribeOptions

	partition int32
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
	if !s.closed.CompareAndSwap(false, true) {
		return nil
	}

	s.c.PauseFetchTopics(s.topic)
	s.c.CloseAllowingRebalance()
	kc := make(map[string][]int32)
	s.rangeConsumers(func(ctp tp, _ *consumer) bool {
		kc[ctp.t] = append(kc[ctp.t], ctp.p)
		return true
	})
	s.killConsumers(ctx, kc)
	close(s.done)
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
			if !s.closed.Load() && fetches.IsClientClosed() {
				s.closed.Store(true)
				tpc := s.copyConsumers()
				for key, c := range tpc {
					if c != nil {
						c.trySend(newErrorFetchTopicPartition(kgo.ErrClientClosed, key.t, key.p))
					}
				}
				return
			}
			fetches.EachError(func(t string, p int32, err error) {
				tps := tp{t, p}
				if c := s.getConsumer(tps); c != nil {
					c.trySend(newErrorFetchTopicPartition(err, t, p))
				}
			})

			fetches.EachPartition(func(p kgo.FetchTopicPartition) {
				tps := tp{p.Topic, p.Partition}
				s.sendToConsumer(tps, p)
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
			pc, ok := s.deleteConsumer(tps)
			if !ok || pc == nil {
				continue
			}
			pc.cancel()

			if s.kopts.Logger.V(logger.DebugLevel) {
				s.kopts.Logger.Debug(ctx, fmt.Sprintf("[kgo] killing consumer topic %s partition %d", topic, partition))
			}

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
	if err == nil || !s.shouldSendErr(err) {
		return
	}
	for _, tc := range r.Topics {
		for _, p := range tc.Partitions {
			if c := s.getConsumer(tp{tc.Topic, p.Partition}); c != nil {
				c.tryErrSend(err)
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
	s.killConsumers(ctx, revoked)
	if err := c.CommitMarkedOffsets(ctx); err != nil {
		tpc := s.copyConsumers()
		for key, c := range tpc {
			if c != nil {
				c.trySend(newErrorFetchTopicPartition(err, key.t, key.p))
			}
		}
	}
}

func (s *Subscriber) assigned(_ context.Context, c *kgo.Client, assigned map[string][]int32) {
	for topic, partitions := range assigned {
		for _, partition := range partitions {
			ctx, cancel := context.WithCancel(s.kopts.Context)
			pc := &consumer{
				c:         c,
				topic:     topic,
				partition: partition,
				htracer:   s.htracer,
				ctx:       ctx,
				cancel:    cancel,
				done:      make(chan struct{}),
				recs:      make(chan kgo.FetchTopicPartition, 100),
				errs:      make(chan error, 8),
				handler:   s.handler,
				kopts:     s.kopts,
				opts:      s.opts,
				connected: s.connected,
			}
			s.setConsumer(tp{topic, partition}, pc)
			go pc.consume()
		}
	}
}

func (c *consumer) consume() {
	defer close(c.done)
	defer c.cancel()
	if c.kopts.Logger.V(logger.DebugLevel) {
		c.kopts.Logger.Debug(c.kopts.Context, fmt.Sprintf("starting, topic %s partition %d", c.topic, c.partition))
		defer c.kopts.Logger.Debug(c.kopts.Context, fmt.Sprintf("killing, topic %s partition %d", c.topic, c.partition))
	}

	eh := c.kopts.ErrorHandler
	if c.opts.ErrorHandler != nil {
		eh = c.opts.ErrorHandler
	}

	var pm *event

	for {
		select {
		case <-c.ctx.Done():
			return
		case err := <-c.errs:
			pm = c.newErrorMessage(err, c.topic, c.partition)
			_ = c.handler(pm)
			eventPool.Put(pm)
			// non-fatal: continue processing
		case p := <-c.recs:
			if p.Err != nil || p.FetchPartition.Err != nil { //nolint:staticcheck
				if p.Err != nil {
					pm = c.newErrorMessage(p.Err, p.Topic, p.Partition)
				} else if p.FetchPartition.Err != nil { //nolint:staticcheck
					pm = c.newErrorMessage(p.FetchPartition.Err, p.Topic, p.Partition) //nolint:staticcheck
				}
				_ = c.handler(pm)
				eventPool.Put(pm)
				return
			}

			for _, record := range p.Records {
				if c.ctx.Err() != nil {
					return
				}
				ctx, sp := c.htracer.WithProcessSpan(record)
				ts := time.Now()
				c.kopts.Meter.Counter(semconv.SubscribeMessageInflight, "endpoint", record.Topic, "topic", record.Topic).Inc()
				p := eventPool.Get().(*event)
				p.msg.Header = nil
				p.msg.Body = nil
				p.topic = record.Topic
				p.err = nil
				p.ack.Store(false)
				p.msg.Header = metadata.New(len(record.Headers))
				p.ctx = ctx
				for _, hdr := range record.Headers {
					p.msg.Header.Set(hdr.Key, string(hdr.Value))
				}
				p.msg.Header.Set("Micro-Offset", strconv.FormatInt(record.Offset, 10))
				p.msg.Header.Set("Micro-Partition", strconv.FormatInt(int64(record.Partition), 10))
				p.msg.Header.Set("Micro-Topic", record.Topic)
				p.msg.Header.Set("Micro-Key", string(record.Key))
				p.msg.Header.Set("Micro-Timestamp", strconv.FormatInt(record.Timestamp.Unix(), 10))
				if c.kopts.Codec.String() == "noop" {
					p.msg.Body = record.Value
				} else if c.opts.BodyOnly {
					p.msg.Body = record.Value
				} else {
					if sp != nil {
						sp.AddEvent("codec unmarshal start")
					}
					err := c.kopts.Codec.Unmarshal(record.Value, p.msg)
					if sp != nil {
						sp.AddEvent("codec unmarshal stop")
					}
					if err != nil {
						if sp != nil {
							sp.SetStatus(tracer.SpanStatusError, err.Error())
						}
						c.kopts.Meter.Counter(semconv.SubscribeMessageTotal, "endpoint", record.Topic, "topic", record.Topic, "status", "failure").Inc()
						p.err = err
						p.msg.Body = record.Value
						if eh != nil {
							_ = eh(p)
							c.kopts.Meter.Counter(semconv.SubscribeMessageInflight, "endpoint", record.Topic, "topic", record.Topic).Dec()
							if p.ack.Load() {
								c.c.MarkCommitRecords(record)
							} else {
								if sp != nil {
									sp.Finish()
								}
								eventPool.Put(p)
								pm := c.newErrorMessage(ErrLostMessage, record.Topic, record.Partition)
								_ = c.handler(pm) //TODO need check
								return
							}
							eventPool.Put(p)
							te := time.Since(ts)
							c.kopts.Meter.Summary(semconv.SubscribeMessageLatencyMicroseconds, "endpoint", record.Topic, "topic", record.Topic).Update(te.Seconds())
							c.kopts.Meter.Histogram(semconv.SubscribeMessageDurationSeconds, "endpoint", record.Topic, "topic", record.Topic).Update(te.Seconds())
							continue
						} else {
							pm := c.newErrorMessage(err, record.Topic, record.Partition)
							_ = c.handler(pm) // TODO need check
						}
						te := time.Since(ts)
						c.kopts.Meter.Counter(semconv.SubscribeMessageInflight, "endpoint", record.Topic, "topic", record.Topic).Dec()
						c.kopts.Meter.Summary(semconv.SubscribeMessageLatencyMicroseconds, "endpoint", record.Topic, "topic", record.Topic).Update(te.Seconds())
						c.kopts.Meter.Histogram(semconv.SubscribeMessageDurationSeconds, "endpoint", record.Topic, "topic", record.Topic).Update(te.Seconds())
						eventPool.Put(p)
						pm := c.newErrorMessage(ErrLostMessage, record.Topic, record.Partition)
						_ = c.handler(pm) // TODO need check
						if sp != nil {
							sp.Finish()
						}
						return
					}
				}
				if sp != nil {
					sp.AddEvent("handler start")
				}
				err := c.handler(p)
				if sp != nil {
					sp.AddEvent("handler stop")
				}
				if err == nil {
					c.kopts.Meter.Counter(semconv.SubscribeMessageTotal, "endpoint", record.Topic, "topic", record.Topic, "status", "success").Inc()
				} else {
					if sp != nil {
						sp.SetStatus(tracer.SpanStatusError, err.Error())
					}
					c.kopts.Meter.Counter(semconv.SubscribeMessageTotal, "endpoint", record.Topic, "topic", record.Topic, "status", "failure").Inc()
				}
				c.kopts.Meter.Counter(semconv.SubscribeMessageInflight, "endpoint", record.Topic, "topic", record.Topic).Dec()
				if err == nil && c.opts.AutoAck {
					p.ack.Store(true)
				} else if err != nil {
					p.err = err
					if eh != nil {
						if sp != nil {
							sp.AddEvent("error handler start")
						}
						_ = eh(p)
						if sp != nil {
							sp.AddEvent("error handler stop")
						}
					} else {
						if c.kopts.Logger.V(logger.ErrorLevel) {
							c.kopts.Logger.Error(c.kopts.Context, "[kgo]: subscriber error", err)
						}
					}
				}
				te := time.Since(ts)
				c.kopts.Meter.Summary(semconv.SubscribeMessageLatencyMicroseconds, "endpoint", record.Topic, "topic", record.Topic).Update(te.Seconds())
				c.kopts.Meter.Histogram(semconv.SubscribeMessageDurationSeconds, "endpoint", record.Topic, "topic", record.Topic).Update(te.Seconds())
				if p.ack.Load() {
					eventPool.Put(p)
					c.c.MarkCommitRecords(record)
				} else {
					eventPool.Put(p)
					pm := c.newErrorMessage(ErrLostMessage, record.Topic, record.Partition)
					_ = c.handler(pm) // TODO need check
					if sp != nil {
						sp.SetStatus(tracer.SpanStatusError, "ErrLostMessage")
						sp.Finish()
					}
					return
				}
				if sp != nil {
					sp.Finish()
				}
			}
		}
	}
}

func (c *consumer) newErrorMessage(err error, t string, p int32) *event {
	pm := eventPool.Get().(*event)

	pm.ack.Store(false)
	pm.msg = &broker.Message{Header: metadata.New(2)}
	pm.err = err
	pm.topic = t
	pm.ctx = context.Background()
	pm.msg.Header.Set("Micro-Partition", strconv.FormatInt(int64(p), 10))
	pm.msg.Header.Set("Micro-Topic", t)
	return pm
}

func (c *consumer) trySend(ftp kgo.FetchTopicPartition) {
	select {
	case c.recs <- ftp:
	case <-c.ctx.Done():
	default:
	}
}

func (c *consumer) tryErrSend(err error) {
	select {
	case c.errs <- err:
	case <-c.ctx.Done():
	default:
	}
}

func (s *Subscriber) shouldSendErr(err error) bool {
	if kgo.IsRetryableBrokerErr(err) || isContextError(err) {
		return false
	}
	s.lastErrMu.Lock()
	defer s.lastErrMu.Unlock()
	if errors.Is(err, s.lastErr) && time.Since(s.lastErrTime) < errDebounceInterval {
		return false
	}
	s.lastErr = err
	s.lastErrTime = time.Now()
	return true
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

func (s *Subscriber) notifyConsumers(err error) {
	tpc := s.copyConsumers()
	for _, c := range tpc {
		if c != nil {
			c.tryErrSend(err)
		}
	}
}

func (s *Subscriber) OnGroupManageError(err error) {
	if err == nil || !s.shouldSendErr(err) {
		return
	}
	s.notifyConsumers(err)
}

func (s *Subscriber) OnBrokerConnect(_ kgo.BrokerMetadata, _ time.Duration, _ net.Conn, err error) {
	if err == nil || !s.shouldSendErr(err) {
		return
	}
	s.notifyConsumers(err)
}

func (s *Subscriber) OnBrokerDisconnect(_ kgo.BrokerMetadata, _ net.Conn) {
}

func (s *Subscriber) OnBrokerWrite(_ kgo.BrokerMetadata, _ int16, _ int, _ time.Duration, _ time.Duration, err error) {
	if err == nil || !s.shouldSendErr(err) {
		return
	}
	s.notifyConsumers(err)
}

func (s *Subscriber) OnBrokerRead(_ kgo.BrokerMetadata, _ int16, _ int, _ time.Duration, _ time.Duration, err error) {
	if err == nil || !s.shouldSendErr(err) {
		return
	}
	s.notifyConsumers(err)
}

func (s *Subscriber) OnProduceRecordUnbuffered(_ *kgo.Record, err error) {
	if err == nil || !s.shouldSendErr(err) {
		return
	}
	s.notifyConsumers(err)
}
