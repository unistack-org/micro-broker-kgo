package kgo

import (
	"context"
	"sync"

	kgo "github.com/twmb/franz-go/pkg/kgo"
	"github.com/unistack-org/micro/v3/broker"
	"github.com/unistack-org/micro/v3/logger"
)

var pPool = sync.Pool{
	New: func() interface{} {
		return &publication{msg: &broker.Message{}}
	},
}

type worker struct {
	done         chan struct{}
	recs         chan []*kgo.Record
	cherr        chan error
	handler      broker.Handler
	batchHandler broker.BatchHandler
	opts         broker.SubscribeOptions
	kopts        broker.Options
	tpmap        map[string][]int32
	maxInflight  int
	reader       *kgo.Client
	ctx          context.Context
}

func (s *subscriber) run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-s.kopts.Context.Done():
			return
		default:
			fetches := s.reader.PollFetches(ctx)
			if fetches.IsClientClosed() {
				// TODO: fatal ?
				return
			}
			if len(fetches.Errors()) > 0 {
				for _, err := range fetches.Errors() {
					s.kopts.Logger.Errorf(ctx, "fetch err topic %s partition %d: %v", err.Topic, err.Partition, err.Err)
				}
				// TODO: fatal ?
				return
			}

			s.kopts.Logger.Infof(ctx, "handle fetches")
			fetches.EachPartition(func(p kgo.FetchTopicPartition) {
				s.Lock()
				consumers := s.consumers[p.Topic]
				s.Unlock()
				if consumers == nil {
					return
				}
				w, ok := consumers[p.Partition]
				if !ok {
					return
				}
				select {
				case w.recs <- p.Records:
				case <-w.done:
				}
			})
		}
	}
}

func (s *subscriber) assigned(ctx context.Context, _ *kgo.Client, assigned map[string][]int32) {
	maxInflight := DefaultSubscribeMaxInflight

	if s.opts.Context != nil {
		if n, ok := s.opts.Context.Value(subscribeMaxInflightKey{}).(int); n > 0 && ok {
			maxInflight = n
		}
	}

	s.Lock()
	for topic, partitions := range assigned {
		if s.consumers[topic] == nil {
			s.consumers[topic] = make(map[int32]worker)
		}
		for _, partition := range partitions {
			w := worker{
				done:         make(chan struct{}),
				recs:         make(chan []*kgo.Record, maxInflight),
				cherr:        make(chan error),
				kopts:        s.kopts,
				opts:         s.opts,
				ctx:          ctx,
				tpmap:        map[string][]int32{topic: []int32{partition}},
				reader:       s.reader,
				handler:      s.handler,
				batchHandler: s.batchhandler,
				maxInflight:  maxInflight,
			}
			s.consumers[topic][partition] = w
			go w.handle()
		}
	}
	s.Unlock()
}

func (s *subscriber) revoked(_ context.Context, _ *kgo.Client, revoked map[string][]int32) {
	s.Lock()
	for topic, partitions := range revoked {
		ptopics := s.consumers[topic]
		for _, partition := range partitions {
			w := ptopics[partition]
			delete(ptopics, partition)
			if len(ptopics) == 0 {
				delete(s.consumers, topic)
			}
			close(w.done)
		}
	}
	s.Unlock()
}

func (w *worker) handle() {
	var err error

	eh := w.kopts.ErrorHandler
	if w.opts.ErrorHandler != nil {
		eh = w.opts.ErrorHandler
	}

	for {
		select {
		case <-w.ctx.Done():
			w.cherr <- w.ctx.Err()
			return
		case <-w.done:
			return
		case recs := <-w.recs:
			if len(recs) == w.maxInflight {
				w.reader.PauseFetchPartitions(w.tpmap)
			}
			for _, record := range recs {
				p := pPool.Get().(*publication)
				p.msg.Header = nil
				p.msg.Body = nil
				p.topic = record.Topic
				p.err = nil
				p.ack = false
				if w.opts.BodyOnly {
					p.msg.Body = record.Value
				} else {
					if err := w.kopts.Codec.Unmarshal(record.Value, p.msg); err != nil {
						p.err = err
						p.msg.Body = record.Value
						if eh != nil {
							_ = eh(p)
							if p.ack {
								w.reader.MarkCommitRecords(record)
							}
							pPool.Put(p)
							continue
						} else {
							if w.kopts.Logger.V(logger.ErrorLevel) {
								w.kopts.Logger.Errorf(w.kopts.Context, "[kgo]: failed to unmarshal: %v", err)
							}
						}
						pPool.Put(p)
						w.cherr <- err
						return
					}
				}
				err = w.handler(p)
				if err == nil && w.opts.AutoAck {
					p.ack = true
				} else if err != nil {
					p.err = err
					if eh != nil {
						_ = eh(p)
					} else {
						if w.kopts.Logger.V(logger.ErrorLevel) {
							w.kopts.Logger.Errorf(w.kopts.Context, "[kgo]: subscriber error: %v", err)
						}
					}
				}
				if p.ack {
					w.reader.MarkCommitRecords(record)
				}
				pPool.Put(p)
			}

			w.reader.ResumeFetchPartitions(w.tpmap)
		}
	}
}
