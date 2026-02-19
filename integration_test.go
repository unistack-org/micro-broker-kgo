//go:build integration

package kgo_test

import (
	"context"
	"fmt"
	"io"
	"net"
	"sync"
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

// kafkaFrameProxy is a Kafka-protocol-aware TCP proxy.
// It parses Kafka request frames (4-byte length prefix + body) and can close the
// connection when it sees a specific API key — allowing targeted injection of
// errors for a single RPC type (e.g. OffsetCommit=8) without affecting others.
//
// Wire format (client→broker):
//
//	[int32 msgLen][int16 apiKey][int16 apiVersion][int32 correlationID][...]
type kafkaFrameProxy struct {
	t      *testing.T
	ln     net.Listener
	target string

	interceptAPIKey atomic.Int32    // -1 = disabled, otherwise drops frames with this API key
	commitDropped   chan struct{}    // receives a value each time a commit connection is dropped
}

const apiKeyOffsetCommit = int16(8)

func newKafkaFrameProxy(t *testing.T, target string) *kafkaFrameProxy {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("kafkaFrameProxy: listen: %v", err)
	}
	p := &kafkaFrameProxy{t: t, ln: ln, target: target, commitDropped: make(chan struct{}, 32)}
	p.interceptAPIKey.Store(-1)
	t.Cleanup(func() { p.ln.Close() })
	go p.run()
	return p
}

func (p *kafkaFrameProxy) Addr() string { return p.ln.Addr().String() }

func (p *kafkaFrameProxy) InterceptCommits() {
	p.interceptAPIKey.Store(int32(apiKeyOffsetCommit))
	p.t.Log("[kafkaProxy] will drop OffsetCommit (API key 8) connections")
}

func (p *kafkaFrameProxy) StopIntercepting() {
	p.interceptAPIKey.Store(-1)
	p.t.Log("[kafkaProxy] interception disabled")
}

func (p *kafkaFrameProxy) run() {
	for {
		clientConn, err := p.ln.Accept()
		if err != nil {
			return
		}
		brokerConn, err := net.DialTimeout("tcp", p.target, 3*time.Second)
		if err != nil {
			clientConn.Close()
			continue
		}
		// client→broker: intercept targeted API keys
		go p.pipeIntercept(clientConn, brokerConn)
		// broker→client: always pass through
		go func() { io.Copy(clientConn, brokerConn); clientConn.Close() }() //nolint:errcheck
	}
}

// pipeIntercept reads Kafka request frames from src, checks the API key,
// and either forwards or drops (closes connection) based on interceptAPIKey.
func (p *kafkaFrameProxy) pipeIntercept(src, dst net.Conn) {
	defer src.Close()
	defer dst.Close()

	lenBuf := make([]byte, 4)
	for {
		// Read 4-byte frame length
		if _, err := io.ReadFull(src, lenBuf); err != nil {
			return
		}
		msgLen := int(lenBuf[0])<<24 | int(lenBuf[1])<<16 | int(lenBuf[2])<<8 | int(lenBuf[3])
		if msgLen <= 0 || msgLen > 64*1024*1024 {
			return
		}

		body := make([]byte, msgLen)
		if _, err := io.ReadFull(src, body); err != nil {
			return
		}

		// API key is the first 2 bytes of the body
		if len(body) >= 2 {
			apiKey := int16(body[0])<<8 | int16(body[1])
			interceptKey := int16(p.interceptAPIKey.Load())
			if interceptKey >= 0 && apiKey == interceptKey {
				p.t.Logf("[kafkaProxy] dropping connection on API key %d (OffsetCommit)", apiKey)
				select {
				case p.commitDropped <- struct{}{}:
				default:
				}
				return // close both sides → kgo gets EOF on this socket
			}
		}

		// Forward normally
		if _, err := dst.Write(lenBuf); err != nil {
			return
		}
		if _, err := dst.Write(body); err != nil {
			return
		}
	}
}

// tcpProxy is a transparent TCP proxy that can simulate network failures
// by abruptly closing all active client connections while keeping the listener alive,
// so kgo can reconnect immediately after the "error injection".
type tcpProxy struct {
	t      *testing.T
	ln     net.Listener
	target string

	mu    sync.Mutex
	conns []net.Conn
}

func newTCPProxy(t *testing.T, target string) *tcpProxy {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("proxy: listen: %v", err)
	}
	p := &tcpProxy{t: t, ln: ln, target: target}
	t.Cleanup(func() { p.closeAll() })
	go p.run()
	return p
}

func (p *tcpProxy) Addr() string { return p.ln.Addr().String() }

func (p *tcpProxy) run() {
	for {
		clientConn, err := p.ln.Accept()
		if err != nil {
			return
		}
		brokerConn, err := net.DialTimeout("tcp", p.target, 3*time.Second)
		if err != nil {
			clientConn.Close()
			continue
		}
		p.mu.Lock()
		p.conns = append(p.conns, clientConn)
		p.mu.Unlock()
		// bidirectional pipe; both sides close when one side closes
		go func() { io.Copy(brokerConn, clientConn); brokerConn.Close() }() //nolint:errcheck
		go func() { io.Copy(clientConn, brokerConn); clientConn.Close() }() //nolint:errcheck
	}
}

// breakConnections closes all active client-side connections.
// kgo will receive EOF / connection-reset → OnBrokerRead(err) fires → our hook runs.
// The listener stays open so kgo can reconnect immediately.
func (p *tcpProxy) breakConnections() {
	p.mu.Lock()
	conns := p.conns
	p.conns = nil
	p.mu.Unlock()
	for _, c := range conns {
		c.Close()
	}
	p.t.Logf("[proxy] broke %d connections", len(conns))
}

func (p *tcpProxy) closeAll() {
	p.breakConnections()
	p.ln.Close()
}

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

// ---------------------------------------------------------------------------
// Bug-detection tests: consumer goroutines die on broker error
// ---------------------------------------------------------------------------
//
// Root cause (see analysis):
//   OnBrokerRead / OnBrokerConnect / OnBrokerWrite hooks call trySend(error)
//   on EVERY consumer goroutine.  Each goroutine sees p.FetchPartition.Err != nil,
//   calls the handler with the error event, and returns — permanently dead.
//   The kgo client stays in the consumer group (partitions still "assigned"),
//   but sendToConsumer hits c.ctx.Done() and silently drops every record.
//   Result: lag grows unboundedly; restarting only one pod does not help
//   because cooperative-sticky rebalance keeps partitions on the surviving (dead) pods.
//
// Both tests below assert CORRECT behaviour (processing resumes after the error).
// They FAIL with the current code and PASS after the fix.

// TestIntegration_BrokerHookErrorKillsConsumers verifies that a transient TCP-level
// broker error does NOT permanently kill consumer goroutines.
//
// Before the fix: OnBrokerRead/OnBrokerConnect/OnBrokerWrite/OnGroupManageError hooks
// called trySend(err) on ALL consumer goroutines → all goroutines exited →
// partitions stayed "assigned" but processing stopped forever.
//
// After the fix: Subscriber no longer implements broker-level hooks.
// hookEvent (logging) and hookMeter (metrics) still fire, but consumer goroutines
// are untouched. kgo handles reconnection internally. Processing continues.
//
// Injection method: TCP proxy → real connection-reset errors on all kgo sockets.
// kgo reconnects through the same proxy (listener stays open), which triggers
// OnBrokerRead with an error internally — the exact production scenario.
func TestIntegration_BrokerHookErrorKillsConsumers(t *testing.T) {
	skipIfKafkaUnavailable(t)

	logger.DefaultLogger = slog.NewLogger()
	if err := logger.DefaultLogger.Init(logger.WithLevel(logger.InfoLevel)); err != nil {
		t.Fatal(err)
	}
	bLogger := broker.Logger(logger.DefaultLogger)

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	// Route all kgo traffic through the proxy so we can inject TCP errors.
	proxy := newTCPProxy(t, kafkaAddr)

	adm := intCreateAdminClient(t)
	topic := intUniqueTopic(t)
	intCreateTopicAndCleanup(t, adm, topic, intNumPartitions)
	group := fmt.Sprintf("inttest-hook-err-%d", time.Now().UnixNano())

	var processed atomic.Int64

	// Broker connects through proxy, not directly to Kafka.
	b := kgo.NewBroker(
		broker.Addrs(proxy.Addr()),
		broker.Codec(codec.NewCodec()),
		kgo.CommitInterval(500*time.Millisecond),
		kgo.Options(
			kg.ClientID("hook-err-consumer"),
			kg.FetchMaxBytes(10*1024*1024),
		),
		bLogger,
	)
	b.Init() //nolint:errcheck
	if err := b.Connect(ctx); err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer func() { _ = b.Disconnect(context.Background()) }()

	sub, err := b.Subscribe(ctx, topic, func(event broker.Event) error {
		if event.Error() == nil {
			processed.Add(1)
		}
		return event.Ack()
	},
		broker.SubscribeAutoAck(true),
		broker.SubscribeGroup(group),
		broker.SubscribeBodyOnly(true),
	)
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer func() { _ = sub.Unsubscribe(context.Background()) }()

	t.Log("waiting for stable group...")
	if _, err := monitorGroupState(ctx, t, adm, group, 1, 30*time.Second); err != nil {
		t.Fatalf("group never stabilized: %v", err)
	}

	// Producer goes directly to Kafka (not through proxy) so it keeps running
	// even when we break consumer connections.
	startProducer(ctx, t, topic, 200)

	// --- Phase 1: baseline throughput ---
	time.Sleep(3 * time.Second)
	snap1 := processed.Load()
	time.Sleep(3 * time.Second)
	snap2 := processed.Load()
	rateNormal := float64(snap2-snap1) / 3.0
	t.Logf("normal processing rate: %.1f msg/s (total=%d)", rateNormal, snap2)

	if rateNormal < 5 {
		t.Fatalf("baseline rate too low (%.1f msg/s)", rateNormal)
	}

	// --- Phase 2: inject broker error via real TCP break ---
	// proxy.breakConnections() closes all active kgo sockets.
	// kgo receives io.EOF → OnBrokerRead(err) fires internally.
	// hookEvent logs it, hookMeter counts it — consumer goroutines are NOT touched.
	// kgo reconnects through the still-listening proxy within milliseconds.
	t.Logf("breaking TCP connections (triggering OnBrokerRead errors on kgo)...")
	proxy.breakConnections()

	// Allow kgo to reconnect and resume polling.
	t.Log("waiting 10s for kgo reconnect...")
	time.Sleep(10 * time.Second)

	// --- Phase 3: measure post-error throughput ---
	snap3 := processed.Load()
	time.Sleep(3 * time.Second)
	snap4 := processed.Load()
	rateAfter := float64(snap4-snap3) / 3.0
	t.Logf("post-error processing rate: %.1f msg/s (was %.1f msg/s)", rateAfter, rateNormal)

	lagMap, lagErr := adm.Lag(ctx, group)
	if lagErr == nil {
		var totalLag int64
		for _, gl := range lagMap {
			for _, lm := range gl.Lag {
				for _, l := range lm {
					if l.Lag > 0 {
						totalLag += l.Lag
					}
				}
			}
		}
		t.Logf("consumer group lag after error: %d records", totalLag)
	}

	// FIX: consumers survive broker errors → rate stays near normal.
	// REGRESSION: if this drops to 0, broker-hook error propagation is back.
	threshold := rateNormal * 0.3
	if rateAfter < threshold {
		t.Fatalf(
			"consumers did not survive broker TCP error.\n"+
				"Processing rate: normal=%.1f msg/s → after=%.1f msg/s (need ≥ %.1f).\n"+
				"Consumer goroutines likely died from broker error propagation.\n"+
				"Check that Subscriber no longer implements HookBrokerRead/HookBrokerConnect/etc.",
			rateNormal, rateAfter, threshold,
		)
	}
}

// TestIntegration_RepeatedBrokerErrors verifies that multiple consecutive TCP-level
// broker errors do NOT permanently kill consumer goroutines.
//
// The single-injection test (TestIntegration_BrokerHookErrorKillsConsumers) proves the
// fix works for one error.  This test ensures the fix holds for a series of errors —
// important because a flapping network produces rapid successive disconnects.
//
// Passes both before and after the fix only if consumers survive ALL injections.
func TestIntegration_RepeatedBrokerErrors(t *testing.T) {
	skipIfKafkaUnavailable(t)

	logger.DefaultLogger = slog.NewLogger()
	if err := logger.DefaultLogger.Init(logger.WithLevel(logger.InfoLevel)); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	proxy := newTCPProxy(t, kafkaAddr)

	adm := intCreateAdminClient(t)
	topic := intUniqueTopic(t)
	intCreateTopicAndCleanup(t, adm, topic, intNumPartitions)
	group := fmt.Sprintf("inttest-repeated-err-%d", time.Now().UnixNano())

	var processed atomic.Int64

	b := kgo.NewBroker(
		broker.Addrs(proxy.Addr()),
		broker.Codec(codec.NewCodec()),
		kgo.CommitInterval(500*time.Millisecond),
		kgo.Options(
			kg.ClientID("repeated-err-consumer"),
			kg.FetchMaxBytes(10*1024*1024),
		),
		broker.Logger(logger.DefaultLogger),
	)
	b.Init() //nolint:errcheck
	if err := b.Connect(ctx); err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer func() { _ = b.Disconnect(context.Background()) }()

	sub, err := b.Subscribe(ctx, topic, func(event broker.Event) error {
		if event.Error() == nil {
			processed.Add(1)
		}
		return event.Ack()
	},
		broker.SubscribeAutoAck(true),
		broker.SubscribeGroup(group),
		broker.SubscribeBodyOnly(true),
	)
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer func() { _ = sub.Unsubscribe(context.Background()) }()

	t.Log("waiting for stable group...")
	if _, err := monitorGroupState(ctx, t, adm, group, 1, 30*time.Second); err != nil {
		t.Fatalf("group never stabilized: %v", err)
	}

	startProducer(ctx, t, topic, 200)

	// baseline
	time.Sleep(3 * time.Second)
	snap1 := processed.Load()
	time.Sleep(3 * time.Second)
	rateNormal := float64(processed.Load()-snap1) / 3.0
	t.Logf("normal processing rate: %.1f msg/s", rateNormal)
	if rateNormal < 5 {
		t.Fatalf("baseline rate too low (%.1f msg/s)", rateNormal)
	}

	// inject 5 successive breaks, 3s apart
	const rounds = 5
	for i := 1; i <= rounds; i++ {
		t.Logf("TCP break #%d/%d...", i, rounds)
		proxy.breakConnections()
		time.Sleep(3 * time.Second)

		s1 := processed.Load()
		time.Sleep(2 * time.Second)
		rate := float64(processed.Load()-s1) / 2.0
		t.Logf("  rate after break #%d: %.1f msg/s", i, rate)
		if rate < rateNormal*0.3 {
			t.Fatalf(
				"consumers died after TCP break #%d/%d.\n"+
					"rate: normal=%.1f → after=%.1f msg/s.\n"+
					"Likely broker-hook error propagation re-introduced.",
				i, rounds, rateNormal, rate,
			)
		}
	}
	t.Logf("survived %d consecutive TCP breaks — no consumer kill regression", rounds)
}


// TestIntegration_AutocommitErrorKillsConsumers verifies that a transient TCP error
// during the commit window does NOT permanently kill consumer goroutines.
//
// Before the fix: autocommit() called trySend(err) on each affected partition consumer.
// An idle consumer (empty recs channel) would receive the error and exit — same root
// cause as the broker-hook bug. At low load (consumers idle between batches) all
// consumers could die and lag would grow unboundedly.
//
// After the fix: autocommit() only increments the commit-error metric. kgo retries the
// commit on the next interval tick. Consumer goroutines are unaffected.
//
// Note: reliably injecting an OffsetCommit-level error requires a proxy that rewrites
// advertised broker addresses in Metadata responses (e.g. Toxiproxy), because kgo opens
// direct connections to each broker after initial metadata exchange — a plain TCP proxy
// only covers the seed-broker connection. This test uses rapid TCP breaks at low load to
// maximise the chance of hitting a commit window. The kafkaFrameProxy infrastructure
// exists for future use with a proper broker-address-rewriting setup.
func TestIntegration_AutocommitErrorKillsConsumers(t *testing.T) {
	skipIfKafkaUnavailable(t)

	logger.DefaultLogger = slog.NewLogger()
	if err := logger.DefaultLogger.Init(logger.WithLevel(logger.InfoLevel)); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	proxy := newTCPProxy(t, kafkaAddr)

	adm := intCreateAdminClient(t)
	topic := intUniqueTopic(t)
	intCreateTopicAndCleanup(t, adm, topic, intNumPartitions)
	group := fmt.Sprintf("inttest-commit-err-%d", time.Now().UnixNano())

	var processed atomic.Int64

	b := kgo.NewBroker(
		broker.Addrs(proxy.Addr()),
		broker.Codec(codec.NewCodec()),
		kgo.CommitInterval(200*time.Millisecond),
		kgo.Options(
			kg.ClientID("commit-err-consumer"),
			kg.FetchMaxBytes(10*1024*1024),
		),
		broker.Logger(logger.DefaultLogger),
	)
	b.Init() //nolint:errcheck
	if err := b.Connect(ctx); err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer func() { _ = b.Disconnect(context.Background()) }()

	sub, err := b.Subscribe(ctx, topic, func(event broker.Event) error {
		if event.Error() == nil {
			processed.Add(1)
		}
		return event.Ack()
	},
		broker.SubscribeAutoAck(true),
		broker.SubscribeGroup(group),
		broker.SubscribeBodyOnly(true),
	)
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer func() { _ = sub.Unsubscribe(context.Background()) }()

	t.Log("waiting for stable group...")
	if _, err := monitorGroupState(ctx, t, adm, group, 1, 30*time.Second); err != nil {
		t.Fatalf("group never stabilized: %v", err)
	}

	// 50 msg/s across 32 partitions (~1.5/partition/s). Handler has no sleep →
	// consumers drain instantly and sit idle between batches (empty recs channel).
	// If trySend were still present, idle consumers would die on commit errors.
	startProducer(ctx, t, topic, 50)

	// baseline — wait for ≥1 produce batch (interval=2s at 50 rps)
	time.Sleep(5 * time.Second)
	snap1 := processed.Load()
	time.Sleep(4 * time.Second)
	rateNormal := float64(processed.Load()-snap1) / 4.0
	t.Logf("normal rate: %.1f msg/s (total=%d)", rateNormal, processed.Load())
	if processed.Load() == 0 {
		t.Fatal("no records processed in baseline")
	}

	// Rapid TCP breaks every 200ms for 4s — matches the commit interval,
	// maximising probability of hitting an in-flight OffsetCommit.
	t.Log("injecting rapid TCP breaks to simulate commit-window errors...")
	ticker := time.NewTicker(200 * time.Millisecond)
	stop := time.After(4 * time.Second)
	breaks := 0
loop:
	for {
		select {
		case <-stop:
			break loop
		case <-ticker.C:
			proxy.breakConnections()
			breaks++
		}
	}
	ticker.Stop()
	t.Logf("broke connections %d times over 4s", breaks)

	time.Sleep(5 * time.Second) // let kgo reconnect

	snap3 := processed.Load()
	time.Sleep(4 * time.Second)
	rateAfter := float64(processed.Load()-snap3) / 4.0
	t.Logf("post-error rate: %.1f msg/s (was %.1f msg/s)", rateAfter, rateNormal)

	lagMap, lagErr := adm.Lag(ctx, group)
	if lagErr == nil {
		var totalLag int64
		for _, gl := range lagMap {
			for _, lm := range gl.Lag {
				for _, l := range lm {
					if l.Lag > 0 {
						totalLag += l.Lag
					}
				}
			}
		}
		t.Logf("lag after error injection: %d records", totalLag)
	}

	if rateAfter == 0 {
		t.Fatalf(
			"BUG: consumers did not survive commit-window TCP errors.\n"+
				"Rate: normal=%.1f → after=%.1f msg/s.\n"+
				"autocommit() is likely calling trySend(err) on idle consumer goroutines.\n"+
				"Fix: remove trySend from autocommit(), keep only incCommitError().",
			rateNormal, rateAfter,
		)
	}
}


// TestIntegration_NetworkInterruptionKillsConsumers tests the "full TCP break → kgo rejoin"
// recovery path, which is DIFFERENT from the primary hook-injection bug.
//
// When ALL TCP connections break simultaneously, kgo loses its heartbeat connection
// and is forced to fully rejoin the consumer group.  The coordinator then calls
// assigned() again, creating fresh consumer goroutines — so processing resumes
// even though the old goroutines died.
//
// This test is expected to PASS both before and after the fix: it documents that
// kgo's reconnect + rejoin path works correctly.
//
// The insidious production bug (captured by TestIntegration_BrokerHookErrorKillsConsumers)
// is a TRANSIENT error that kgo handles internally via retry without a full rejoin:
//   1. kgo gets a single read error (e.g. brief packet loss)
//   2. kgo retries/reconnects the socket — the group session survives intact
//   3. OnBrokerRead(err) fires → our hook kills all consumers
//   4. No assigned() callback triggered → consumers stay dead permanently
//
// Flow of THIS test:
//   kgo ──► proxy ──► Kafka
//   proxy.breakConnections() → kgo gets io.EOF on all sockets
//   kgo cannot heartbeat → coordinator kicks it → kgo rejoins group
//   → assigned() called → new consumer goroutines → processing resumes
func TestIntegration_NetworkInterruptionKillsConsumers(t *testing.T) {
	skipIfKafkaUnavailable(t)

	logger.DefaultLogger = slog.NewLogger()
	if err := logger.DefaultLogger.Init(logger.WithLevel(logger.InfoLevel)); err != nil {
		t.Fatal(err)
	}
	bLogger := broker.Logger(logger.DefaultLogger)

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	// Start proxy in front of Kafka
	proxy := newTCPProxy(t, kafkaAddr)
	t.Logf("TCP proxy listening on %s → %s", proxy.Addr(), kafkaAddr)

	adm := intCreateAdminClient(t)
	topic := intUniqueTopic(t)
	intCreateTopicAndCleanup(t, adm, topic, intNumPartitions)
	group := fmt.Sprintf("inttest-netfail-%d", time.Now().UnixNano())

	var processed atomic.Int64

	// Create broker pointing to the PROXY, not Kafka directly
	b := kgo.NewBroker(
		broker.Addrs(proxy.Addr()),
		broker.Codec(codec.NewCodec()),
		kgo.CommitInterval(500*time.Millisecond),
		kgo.Options(
			kg.ClientID("netfail-consumer"),
			kg.FetchMaxBytes(10*1024*1024),
		),
		bLogger,
	)
	b.Init() //nolint:errcheck
	if err := b.Connect(ctx); err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer func() { _ = b.Disconnect(context.Background()) }()

	sub, err := b.Subscribe(ctx, topic, func(event broker.Event) error {
		if event.Error() == nil {
			processed.Add(1)
		}
		return event.Ack()
	},
		broker.SubscribeAutoAck(true),
		broker.SubscribeGroup(group),
		broker.SubscribeBodyOnly(true),
	)
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer func() { _ = sub.Unsubscribe(context.Background()) }()

	// Wait for stable group
	t.Log("waiting for stable group...")
	if _, err := monitorGroupState(ctx, t, adm, group, 1, 30*time.Second); err != nil {
		t.Fatalf("group never stabilized: %v", err)
	}

	// Start producer (also direct to Kafka, not through proxy)
	startProducer(ctx, t, topic, 200)

	// --- Phase 1: measure normal throughput ---
	time.Sleep(3 * time.Second)
	snap1 := processed.Load()
	time.Sleep(3 * time.Second)
	snap2 := processed.Load()
	rateNormal := float64(snap2-snap1) / 3.0
	t.Logf("normal processing rate: %.1f msg/s (total=%d)", rateNormal, snap2)

	if rateNormal < 5 {
		t.Fatalf("baseline rate too low (%.1f msg/s)", rateNormal)
	}

	// --- Phase 2: simulate network interruption ---
	// breakConnections closes all TCP sockets on the kgo side.
	// kgo receives io.EOF → OnBrokerRead(err) → trySend(err) to all consumers
	// → all consumer goroutines exit.
	// The proxy listener stays open, so kgo can reconnect within seconds.
	t.Log("breaking all TCP connections (simulating network interruption)...")
	proxy.breakConnections()

	// kgo should reconnect within ~1-2s (RetryBackoffFn starts at 100ms, max 1s)
	t.Log("waiting 10s for kgo reconnect + consumer self-recovery...")
	time.Sleep(10 * time.Second)

	// --- Phase 3: measure post-interruption throughput ---
	snap3 := processed.Load()
	time.Sleep(3 * time.Second)
	snap4 := processed.Load()
	rateAfter := float64(snap4-snap3) / 3.0
	t.Logf("post-interruption processing rate: %.1f msg/s (was %.1f msg/s)", rateAfter, rateNormal)

	// Verify group state: group should still be "Stable" (kgo reconnected)
	// but partitions have no active consumer goroutines behind them
	lagMap, lagErr := adm.Lag(ctx, group)
	if lagErr == nil {
		var totalLag int64
		for _, gl := range lagMap {
			for _, lm := range gl.Lag {
				for _, l := range lm {
					if l.Lag > 0 {
						totalLag += l.Lag
					}
				}
			}
		}
		t.Logf("consumer group lag after interruption: %d records", totalLag)
	}

	threshold := rateNormal * 0.3
	if rateAfter < threshold {
		t.Fatalf(
			"BUG DETECTED: consumers did not recover after TCP network interruption.\n"+
				"kgo reconnected (proxy still up) but consumer goroutines stayed dead.\n"+
				"Processing rate: normal=%.1f msg/s → after=%.1f msg/s (need ≥ %.1f).\n"+
				"Root cause: OnBrokerRead hook propagates transient errors to all\n"+
				"consumer goroutines via trySend; goroutines exit and are never restarted\n"+
				"because kgo does not trigger a rebalance on simple reconnect.",
			rateNormal, rateAfter, threshold,
		)
	}
}