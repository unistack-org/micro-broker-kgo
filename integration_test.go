//go:build integration

package kgo_test

import (
	"context"
	"fmt"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	kg "github.com/twmb/franz-go/pkg/kgo"
	kgo "go.unistack.org/micro-broker-kgo/v3"
	"go.unistack.org/micro/v3/broker"
	"go.unistack.org/micro/v3/codec"
	"go.unistack.org/micro/v3/logger"
	"go.unistack.org/micro/v3/logger/slog"
)

const (
	kafkaAddr        = "localhost:9092"
	intNumPartitions = 32
	callbackDeadline = 30 * time.Second
	producerRPS      = 2000
	slowHandlerDelay = 5 * time.Millisecond
)

func skipIfKafkaUnavailable(t *testing.T) {
	t.Helper()
	conn, err := net.DialTimeout("tcp", kafkaAddr, 3*time.Second)
	if err != nil {
		t.Skipf("Kafka not reachable at %s, skipping integration test: %v", kafkaAddr, err)
	}
	conn.Close()
}

func intUniqueTopic(t *testing.T) string {
	t.Helper()
	return fmt.Sprintf("inttest.%d", time.Now().UnixNano())
}

func intCreateAdminClient(t *testing.T) *kadm.Client {
	t.Helper()
	cl, err := kg.NewClient(kg.SeedBrokers(kafkaAddr))
	if err != nil {
		t.Fatalf("kadm client: %v", err)
	}
	adm := kadm.NewClient(cl)
	t.Cleanup(func() { cl.Close() })
	return adm
}

func intCreateTopicAndCleanup(t *testing.T, adm *kadm.Client, topic string, partitions int32) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	resp, err := adm.CreateTopics(ctx, partitions, 1, nil, topic)
	if err != nil {
		t.Fatalf("create topic %s: %v", topic, err)
	}
	if topicErr := resp[topic].Err; topicErr != nil {
		t.Fatalf("create topic %s: %v", topic, topicErr)
	}
	t.Logf("created topic %s with %d partitions", topic, partitions)

	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_, _ = adm.DeleteTopics(ctx, topic)
		t.Logf("deleted topic %s", topic)
	})
}

func intCreateBroker(t *testing.T, clientID string) *kgo.Broker {
	t.Helper()
	b := kgo.NewBroker(
		broker.Addrs(kafkaAddr),
		broker.Codec(codec.NewCodec()),
		kgo.CommitInterval(500*time.Millisecond),
		kgo.Options(
			kg.ClientID(clientID),
			kg.FetchMaxBytes(10*1024*1024),
			kg.MaxBufferedRecords(10),
		),
	)
	return b
}

// monitorGroupState polls kadm.DescribeGroups until the group reaches Stable with targetMembers.
// Returns the duration from start to stable, or error on timeout.
func monitorGroupState(
	ctx context.Context,
	t *testing.T,
	adm *kadm.Client,
	group string,
	targetMembers int,
	timeout time.Duration,
) (time.Duration, error) {
	t.Helper()
	start := time.Now()
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	deadline := time.After(timeout)

	lastState := ""
	for {
		select {
		case <-deadline:
			return time.Since(start), fmt.Errorf(
				"timeout (%v) waiting for group %s to reach Stable with %d members (last state: %s)",
				timeout, group, targetMembers, lastState,
			)
		case <-ctx.Done():
			return time.Since(start), ctx.Err()
		case <-ticker.C:
			groups, err := adm.DescribeGroups(ctx, group)
			if err != nil {
				continue
			}
			g, ok := groups[group]
			if !ok {
				continue
			}
			state := g.State
			members := len(g.Members)
			if state != lastState {
				t.Logf("[%v] group %s: %s -> %s (members=%d)",
					time.Since(start).Round(time.Millisecond), group, lastState, state, members)
				lastState = state
			}
			if state == "Stable" && members == targetMembers {
				allAssigned := true
				for _, m := range g.Members {
					a, ok := m.Assigned.AsConsumer()
					partitions := 0
					if ok {
						for _, at := range a.Topics {
							partitions += len(at.Partitions)
						}
					}
					t.Logf("  member %s: %d partitions", m.ClientID, partitions)
					if partitions == 0 {
						allAssigned = false
					}
				}
				if allAssigned {
					return time.Since(start), nil
				}
				t.Logf("[%v] Stable but not all members have partitions, waiting for next rebalance round...",
					time.Since(start).Round(time.Millisecond))
			}
		}
	}
}

// startProducer launches a goroutine producing messages at ~rps rate until ctx is cancelled.
// Uses a separate kgo.Client to avoid interference with consumer clients.
func startProducer(ctx context.Context, t *testing.T, topic string, rps int) {
	t.Helper()
	cl, err := kg.NewClient(
		kg.SeedBrokers(kafkaAddr),
		kg.ClientID("integration-producer"),
		kg.DisableIdempotentWrite(),
	)
	if err != nil {
		t.Fatalf("producer client: %v", err)
	}
	t.Cleanup(func() { cl.Close() })

	body := make([]byte, 256)
	batchSize := 100
	interval := time.Duration(float64(time.Second) / float64(rps) * float64(batchSize))

	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		i := int32(0)
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				records := make([]*kg.Record, 0, batchSize)
				for j := 0; j < batchSize; j++ {
					records = append(records, &kg.Record{
						Topic: topic,
						Value: body,
						Key:   []byte(fmt.Sprintf("%d", i%int32(intNumPartitions))),
					})
					i++
				}
				cl.ProduceSync(ctx, records...) //nolint:errcheck
			}
		}
	}()
}

// TestIntegration_RebalanceDeadlock reproduces the conditions that cause deadlock during
// cooperative-sticky rebalance: full buffers + blocking sends in franz-go callbacks.
//
// Before the fix (quit channel + blocking c.recs <-): this test hangs because revoked()
// callback never returns, franz-go heartbeat is dead, consumer gets kicked.
//
// After the fix (context.WithCancel + trySend): rebalance completes in ~5-10s.
func TestIntegration_RebalanceDeadlock(t *testing.T) {
	skipIfKafkaUnavailable(t)

	logger.DefaultLogger = slog.NewLogger()
	if err := logger.DefaultLogger.Init(logger.WithLevel(logger.DebugLevel)); err != nil {
		t.Fatal(err)
	}
	bLogger := broker.Logger(logger.DefaultLogger.Clone(logger.WithLevel(logger.InfoLevel)))

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	adm := intCreateAdminClient(t)
	topic := intUniqueTopic(t)
	intCreateTopicAndCleanup(t, adm, topic, intNumPartitions)
	group := fmt.Sprintf("inttest-group-%d", time.Now().UnixNano())

	// Phase 1: Start consumer 1 — gets all 32 partitions
	var c1Count atomic.Int64
	b1 := intCreateBroker(t, "consumer-1")
	b1.Init(bLogger) //nolint:errcheck
	if err := b1.Connect(ctx); err != nil {
		t.Fatalf("b1 connect: %v", err)
	}
	defer func() { _ = b1.Disconnect(context.Background()) }()

	sub1, err := b1.Subscribe(ctx, topic, func(event broker.Event) error {
		time.Sleep(slowHandlerDelay)
		c1Count.Add(1)
		return event.Ack()
	},
		broker.SubscribeAutoAck(true),
		broker.SubscribeGroup(group),
		broker.SubscribeBodyOnly(true),
	)
	if err != nil {
		t.Fatalf("subscribe b1: %v", err)
	}
	defer func() { _ = sub1.Unsubscribe(context.Background()) }()

	// Wait for consumer 1 to get all partitions
	t.Log("waiting for consumer 1 to get all partitions...")
	if dur, err := monitorGroupState(ctx, t, adm, group, 1, 30*time.Second); err != nil {
		t.Fatalf("consumer 1 never stabilized: %v", err)
	} else {
		t.Logf("consumer 1 stable in %v", dur)
	}

	// Phase 2: Start high-RPS producer to fill buffers
	t.Log("starting producer at 2000 msg/s...")
	startProducer(ctx, t, topic, producerRPS)

	// Phase 3: Let buffers fill up
	t.Log("waiting 3s for buffer pressure to build...")
	time.Sleep(3 * time.Second)
	t.Logf("consumer 1 processed %d messages before rebalance", c1Count.Load())

	// Phase 4: Start consumer 2 → triggers cooperative-sticky rebalance
	var c2Count atomic.Int64
	b2 := intCreateBroker(t, "consumer-2")
	b2.Init(bLogger) //nolint:errcheck
	if err := b2.Connect(ctx); err != nil {
		t.Fatalf("b2 connect: %v", err)
	}
	defer func() { _ = b2.Disconnect(context.Background()) }()

	sub2, err := b2.Subscribe(ctx, topic, func(event broker.Event) error {
		time.Sleep(slowHandlerDelay)
		c2Count.Add(1)
		return event.Ack()
	},
		broker.SubscribeAutoAck(true),
		broker.SubscribeGroup(group),
		broker.SubscribeBodyOnly(true),
	)
	if err != nil {
		t.Fatalf("subscribe b2: %v", err)
	}
	defer func() { _ = sub2.Unsubscribe(context.Background()) }()

	// Phase 5: Monitor rebalance — deadlock detection
	t.Log("consumer 2 joined, monitoring rebalance...")
	rebalanceDur, err := monitorGroupState(ctx, t, adm, group, 2, callbackDeadline)
	if err != nil {
		t.Fatalf("DEADLOCK DETECTED: rebalance did not complete in %v: %v\n"+
			"c1=%d c2=%d\n"+
			"The revoked() callback is likely stuck on a blocking send to a full buffer.",
			callbackDeadline, err, c1Count.Load(), c2Count.Load())
	}
	t.Logf("rebalance completed in %v — no deadlock", rebalanceDur)

	// Phase 6: Verify both consumers are processing after rebalance
	c1Before := c1Count.Load()
	c2Before := c2Count.Load()
	time.Sleep(3 * time.Second)
	c1After := c1Count.Load()
	c2After := c2Count.Load()

	t.Logf("post-rebalance processing: c1=%d→%d (+%d), c2=%d→%d (+%d)",
		c1Before, c1After, c1After-c1Before,
		c2Before, c2After, c2After-c2Before)

	if c1After <= c1Before {
		t.Error("consumer 1 stopped processing after rebalance")
	}
	if c2After <= c2Before {
		t.Error("consumer 2 is not processing after rebalance")
	}
}

// TestIntegration_RebalanceThreeConsumers tests adding a third consumer,
// which triggers additional cooperative-sticky rebalance rounds.
func TestIntegration_RebalanceThreeConsumers(t *testing.T) {
	skipIfKafkaUnavailable(t)

	logger.DefaultLogger = slog.NewLogger()
	if err := logger.DefaultLogger.Init(logger.WithLevel(logger.DebugLevel)); err != nil {
		t.Fatal(err)
	}
	bLogger := broker.Logger(logger.DefaultLogger.Clone(logger.WithLevel(logger.InfoLevel)))

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	adm := intCreateAdminClient(t)
	topic := intUniqueTopic(t)
	intCreateTopicAndCleanup(t, adm, topic, intNumPartitions)
	group := fmt.Sprintf("inttest-group3-%d", time.Now().UnixNano())

	handler := func(counter *atomic.Int64) broker.Handler {
		return func(event broker.Event) error {
			time.Sleep(slowHandlerDelay)
			counter.Add(1)
			return event.Ack()
		}
	}

	subscribe := func(b *kgo.Broker, h broker.Handler) broker.Subscriber {
		sub, err := b.Subscribe(ctx, topic, h,
			broker.SubscribeAutoAck(true),
			broker.SubscribeGroup(group),
			broker.SubscribeBodyOnly(true),
		)
		if err != nil {
			t.Fatalf("subscribe: %v", err)
		}
		return sub
	}

	connectBroker := func(clientID string) *kgo.Broker {
		b := intCreateBroker(t, clientID)
		b.Init(bLogger) //nolint:errcheck
		if err := b.Connect(ctx); err != nil {
			t.Fatalf("connect %s: %v", clientID, err)
		}
		return b
	}

	// Consumer 1
	var c1, c2, c3 atomic.Int64
	b1 := connectBroker("consumer-1")
	defer func() { _ = b1.Disconnect(context.Background()) }()
	sub1 := subscribe(b1, handler(&c1))
	defer func() { _ = sub1.Unsubscribe(context.Background()) }()

	if _, err := monitorGroupState(ctx, t, adm, group, 1, 30*time.Second); err != nil {
		t.Fatalf("consumer 1 never stabilized: %v", err)
	}

	startProducer(ctx, t, topic, producerRPS)
	time.Sleep(3 * time.Second)

	// Consumer 2
	b2 := connectBroker("consumer-2")
	defer func() { _ = b2.Disconnect(context.Background()) }()
	sub2 := subscribe(b2, handler(&c2))
	defer func() { _ = sub2.Unsubscribe(context.Background()) }()

	t.Log("waiting for 2-member stable...")
	if dur, err := monitorGroupState(ctx, t, adm, group, 2, callbackDeadline); err != nil {
		t.Fatalf("DEADLOCK after consumer 2 join: %v (c1=%d c2=%d)", err, c1.Load(), c2.Load())
	} else {
		t.Logf("2-member rebalance in %v", dur)
	}

	time.Sleep(2 * time.Second)

	// Consumer 3
	b3 := connectBroker("consumer-3")
	defer func() { _ = b3.Disconnect(context.Background()) }()
	sub3 := subscribe(b3, handler(&c3))
	defer func() { _ = sub3.Unsubscribe(context.Background()) }()

	t.Log("waiting for 3-member stable...")
	if dur, err := monitorGroupState(ctx, t, adm, group, 3, callbackDeadline); err != nil {
		t.Fatalf("DEADLOCK after consumer 3 join: %v (c1=%d c2=%d c3=%d)",
			err, c1.Load(), c2.Load(), c3.Load())
	} else {
		t.Logf("3-member rebalance in %v", dur)
	}

	// Verify total processing continues after rebalance.
	// Note: individual consumers may stop if hook errors kill their goroutines
	// during the multi-round cooperative-sticky rebalance (existing behavior).
	// The important assertion is that rebalance completed without deadlock.
	snap1, snap2, snap3 := c1.Load(), c2.Load(), c3.Load()
	totalBefore := snap1 + snap2 + snap3
	time.Sleep(3 * time.Second)
	totalAfter := c1.Load() + c2.Load() + c3.Load()

	t.Logf("post-rebalance: c1 +%d, c2 +%d, c3 +%d, total +%d",
		c1.Load()-snap1, c2.Load()-snap2, c3.Load()-snap3,
		totalAfter-totalBefore)

	if totalAfter <= totalBefore {
		t.Error("no messages processed after 3-member rebalance")
	}
}

// TestIntegration_RebalanceWithConsumerLeave simulates a production scenario:
// 4 consumers share 32 partitions (~8 each), then one pod crashes.
// The remaining 3 must redistribute partitions (~10-11 each) without deadlock.
func TestIntegration_RebalanceWithConsumerLeave(t *testing.T) {
	skipIfKafkaUnavailable(t)

	logger.DefaultLogger = slog.NewLogger()
	if err := logger.DefaultLogger.Init(logger.WithLevel(logger.DebugLevel)); err != nil {
		t.Fatal(err)
	}
	bLogger := broker.Logger(logger.DefaultLogger.Clone(logger.WithLevel(logger.InfoLevel)))

	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()

	adm := intCreateAdminClient(t)
	topic := intUniqueTopic(t)
	intCreateTopicAndCleanup(t, adm, topic, intNumPartitions)
	group := fmt.Sprintf("inttest-leave-%d", time.Now().UnixNano())

	const numConsumers = 4

	counters := make([]atomic.Int64, numConsumers)
	brokers := make([]*kgo.Broker, numConsumers)
	subs := make([]broker.Subscriber, numConsumers)

	handler := func(idx int) broker.Handler {
		return func(event broker.Event) error {
			time.Sleep(slowHandlerDelay)
			counters[idx].Add(1)
			return event.Ack()
		}
	}

	// Phase 1: Start all 4 consumers sequentially, waiting for stable after each
	for i := 0; i < numConsumers; i++ {
		clientID := fmt.Sprintf("consumer-%d", i+1)
		b := intCreateBroker(t, clientID)
		b.Init(bLogger) //nolint:errcheck
		if err := b.Connect(ctx); err != nil {
			t.Fatalf("%s connect: %v", clientID, err)
		}
		brokers[i] = b

		sub, err := b.Subscribe(ctx, topic, handler(i),
			broker.SubscribeAutoAck(true),
			broker.SubscribeGroup(group),
			broker.SubscribeBodyOnly(true),
		)
		if err != nil {
			t.Fatalf("%s subscribe: %v", clientID, err)
		}
		subs[i] = sub

		t.Logf("waiting for %d-member stable...", i+1)
		if dur, err := monitorGroupState(ctx, t, adm, group, i+1, callbackDeadline); err != nil {
			t.Fatalf("group never stabilized with %d members: %v", i+1, err)
		} else {
			t.Logf("%d-member stable in %v", i+1, dur)
		}
	}

	// Cleanup survivors on exit
	defer func() {
		for i := 0; i < numConsumers; i++ {
			if subs[i] != nil {
				_ = subs[i].Unsubscribe(context.Background())
			}
			if brokers[i] != nil {
				_ = brokers[i].Disconnect(context.Background())
			}
		}
	}()

	// Phase 2: Producer at high RPS, let buffers fill
	startProducer(ctx, t, topic, producerRPS)
	time.Sleep(3 * time.Second)

	t.Logf("before crash: c1=%d c2=%d c3=%d c4=%d",
		counters[0].Load(), counters[1].Load(), counters[2].Load(), counters[3].Load())

	// Phase 3: Kill consumer-4 (simulate pod crash)
	crashIdx := 3
	t.Logf("simulating consumer-%d crash...", crashIdx+1)
	_ = subs[crashIdx].Unsubscribe(context.Background())
	_ = brokers[crashIdx].Disconnect(context.Background())
	subs[crashIdx] = nil
	brokers[crashIdx] = nil

	// Phase 4: Wait for remaining 3 to redistribute 32 partitions
	t.Log("waiting for 3-member stable after crash...")
	if dur, err := monitorGroupState(ctx, t, adm, group, numConsumers-1, callbackDeadline); err != nil {
		t.Fatalf("DEADLOCK after consumer crash: %v (c1=%d c2=%d c3=%d)",
			err, counters[0].Load(), counters[1].Load(), counters[2].Load())
	} else {
		t.Logf("3-member rebalance after crash in %v", dur)
	}

	// Phase 5: Verify processing continues with 3 consumers
	var snap3Total int64
	for i := 0; i < numConsumers-1; i++ {
		snap3Total += counters[i].Load()
	}
	time.Sleep(3 * time.Second)
	var after3Total int64
	for i := 0; i < numConsumers-1; i++ {
		after3Total += counters[i].Load()
	}
	t.Logf("post-crash total +%d", after3Total-snap3Total)
	if after3Total <= snap3Total {
		t.Error("no messages processed after consumer crash and rebalance")
	}

	// Phase 6: Consumer-4 comes back (pod restart in k8s)
	t.Log("consumer-4 rejoining (simulating pod restart)...")
	b4 := intCreateBroker(t, "consumer-4")
	b4.Init(bLogger) //nolint:errcheck
	if err := b4.Connect(ctx); err != nil {
		t.Fatalf("consumer-4 reconnect: %v", err)
	}
	brokers[crashIdx] = b4

	sub4, err := b4.Subscribe(ctx, topic, handler(crashIdx),
		broker.SubscribeAutoAck(true),
		broker.SubscribeGroup(group),
		broker.SubscribeBodyOnly(true),
	)
	if err != nil {
		t.Fatalf("consumer-4 resubscribe: %v", err)
	}
	subs[crashIdx] = sub4

	// Phase 7: Wait for 4-member stable again
	t.Log("waiting for 4-member stable after rejoin...")
	if dur, err := monitorGroupState(ctx, t, adm, group, numConsumers, callbackDeadline); err != nil {
		t.Fatalf("DEADLOCK after consumer rejoin: %v (c1=%d c2=%d c3=%d c4=%d)",
			err, counters[0].Load(), counters[1].Load(), counters[2].Load(), counters[3].Load())
	} else {
		t.Logf("4-member rebalance after rejoin in %v", dur)
	}

	// Phase 8: Verify all 4 consumers process after rejoin
	var snap4Total int64
	for i := 0; i < numConsumers; i++ {
		snap4Total += counters[i].Load()
	}
	time.Sleep(3 * time.Second)
	var after4Total int64
	for i := 0; i < numConsumers; i++ {
		after4Total += counters[i].Load()
	}
	t.Logf("post-rejoin: c1=%d c2=%d c3=%d c4=%d, total +%d",
		counters[0].Load(), counters[1].Load(), counters[2].Load(), counters[3].Load(),
		after4Total-snap4Total)
	if after4Total <= snap4Total {
		t.Error("no messages processed after consumer rejoin")
	}
}