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
	kgo "go.unistack.org/micro-broker-kgo/v4"
	"go.unistack.org/micro/v4/broker"
	"go.unistack.org/micro/v4/codec"
	"go.unistack.org/micro/v4/logger"
	"go.unistack.org/micro/v4/logger/slog"
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

	interceptAPIKey atomic.Int32  // -1 = disabled, otherwise drops frames with this API key
	commitDropped   chan struct{} // receives a value each time a commit connection is dropped
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
		go p.pipeIntercept(clientConn, brokerConn)
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

		if len(body) >= 2 {
			apiKey := int16(body[0])<<8 | int16(body[1])
			interceptKey := int16(p.interceptAPIKey.Load())
			if interceptKey >= 0 && apiKey == interceptKey {
				p.t.Logf("[kafkaProxy] dropping connection on API key %d (OffsetCommit)", apiKey)
				select {
				case p.commitDropped <- struct{}{}:
				default:
				}
				return
			}
		}

		if _, err := dst.Write(lenBuf); err != nil {
			return
		}
		if _, err := dst.Write(body); err != nil {
			return
		}
	}
}

// tcpProxy is a transparent TCP proxy that can simulate network failures
// by abruptly closing all active client connections while keeping the listener alive.
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
		go func() { io.Copy(brokerConn, clientConn); brokerConn.Close() }() //nolint:errcheck
		go func() { io.Copy(clientConn, brokerConn); clientConn.Close() }() //nolint:errcheck
	}
}

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

// msgHasError checks if a broker.Message carries an error (kgoMessage-specific).
func msgHasError(msg broker.Message) bool {
	if km, ok := msg.(interface{ Error() error }); ok {
		return km.Error() != nil
	}
	return false
}

// monitorGroupState polls kadm.DescribeGroups until the group reaches Stable with targetMembers.
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
				t.Logf("[%v] Stable but not all members have partitions, waiting...",
					time.Since(start).Round(time.Millisecond))
			}
		}
	}
}

// startProducer launches a goroutine producing messages at ~rps rate until ctx is cancelled.
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
// Before the fix: revoked() callback blocks on c.recs <- when buffer is full → deadlock.
// After the fix: trySend with default arm → no blocking → rebalance completes.
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

	var c1Count atomic.Int64
	b1 := intCreateBroker(t, "consumer-1")
	if err := b1.Init(bLogger); err != nil {
		t.Fatalf("b1 init: %v", err)
	}
	if err := b1.Connect(ctx); err != nil {
		t.Fatalf("b1 connect: %v", err)
	}
	defer func() { _ = b1.Disconnect(context.Background()) }()

	sub1, err := b1.Subscribe(ctx, topic, func(msg broker.Message) error {
		time.Sleep(slowHandlerDelay)
		if !msgHasError(msg) {
			c1Count.Add(1)
		}
		return msg.Ack()
	},
		broker.SubscribeAutoAck(true),
		broker.SubscribeGroup(group),
		broker.SubscribeBodyOnly(true),
	)
	if err != nil {
		t.Fatalf("subscribe b1: %v", err)
	}
	defer func() { _ = sub1.Unsubscribe(context.Background()) }()

	t.Log("waiting for consumer 1 to get all partitions...")
	if dur, err := monitorGroupState(ctx, t, adm, group, 1, 30*time.Second); err != nil {
		t.Fatalf("consumer 1 never stabilized: %v", err)
	} else {
		t.Logf("consumer 1 stable in %v", dur)
	}

	t.Log("starting producer at 2000 msg/s...")
	startProducer(ctx, t, topic, producerRPS)

	t.Log("waiting 3s for buffer pressure to build...")
	time.Sleep(3 * time.Second)
	t.Logf("consumer 1 processed %d messages before rebalance", c1Count.Load())

	var c2Count atomic.Int64
	b2 := intCreateBroker(t, "consumer-2")
	if err := b2.Init(bLogger); err != nil {
		t.Fatalf("b2 init: %v", err)
	}
	if err := b2.Connect(ctx); err != nil {
		t.Fatalf("b2 connect: %v", err)
	}
	defer func() { _ = b2.Disconnect(context.Background()) }()

	sub2, err := b2.Subscribe(ctx, topic, func(msg broker.Message) error {
		time.Sleep(slowHandlerDelay)
		if !msgHasError(msg) {
			c2Count.Add(1)
		}
		return msg.Ack()
	},
		broker.SubscribeAutoAck(true),
		broker.SubscribeGroup(group),
		broker.SubscribeBodyOnly(true),
	)
	if err != nil {
		t.Fatalf("subscribe b2: %v", err)
	}
	defer func() { _ = sub2.Unsubscribe(context.Background()) }()

	t.Log("consumer 2 joined, monitoring rebalance...")
	rebalanceDur, err := monitorGroupState(ctx, t, adm, group, 2, callbackDeadline)
	if err != nil {
		t.Fatalf("DEADLOCK DETECTED: rebalance did not complete in %v: %v\n"+
			"c1=%d c2=%d\n"+
			"The revoked() callback is likely stuck on a blocking send to a full buffer.",
			callbackDeadline, err, c1Count.Load(), c2Count.Load())
	}
	t.Logf("rebalance completed in %v — no deadlock", rebalanceDur)

	c1Before := c1Count.Load()
	c2Before := c2Count.Load()
	time.Sleep(3 * time.Second)
	c1After := c1Count.Load()
	c2After := c2Count.Load()

	t.Logf("post-rebalance: c1=%d→%d (+%d), c2=%d→%d (+%d)",
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
// triggering multiple rounds of cooperative-sticky rebalance.
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

	subscribe := func(b *kgo.Broker, counter *atomic.Int64) broker.Subscriber {
		sub, err := b.Subscribe(ctx, topic, func(msg broker.Message) error {
			time.Sleep(slowHandlerDelay)
			if !msgHasError(msg) {
				counter.Add(1)
			}
			return msg.Ack()
		},
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
		if err := b.Init(bLogger); err != nil {
			t.Fatalf("init %s: %v", clientID, err)
		}
		if err := b.Connect(ctx); err != nil {
			t.Fatalf("connect %s: %v", clientID, err)
		}
		return b
	}

	var c1, c2, c3 atomic.Int64
	b1 := connectBroker("consumer-1")
	defer func() { _ = b1.Disconnect(context.Background()) }()
	sub1 := subscribe(b1, &c1)
	defer func() { _ = sub1.Unsubscribe(context.Background()) }()

	if _, err := monitorGroupState(ctx, t, adm, group, 1, 30*time.Second); err != nil {
		t.Fatalf("consumer 1 never stabilized: %v", err)
	}

	startProducer(ctx, t, topic, producerRPS)
	time.Sleep(3 * time.Second)

	b2 := connectBroker("consumer-2")
	defer func() { _ = b2.Disconnect(context.Background()) }()
	sub2 := subscribe(b2, &c2)
	defer func() { _ = sub2.Unsubscribe(context.Background()) }()

	t.Log("waiting for 2-member stable...")
	if dur, err := monitorGroupState(ctx, t, adm, group, 2, callbackDeadline); err != nil {
		t.Fatalf("DEADLOCK after consumer 2 join: %v (c1=%d c2=%d)", err, c1.Load(), c2.Load())
	} else {
		t.Logf("2-member rebalance in %v", dur)
	}

	time.Sleep(2 * time.Second)

	b3 := connectBroker("consumer-3")
	defer func() { _ = b3.Disconnect(context.Background()) }()
	sub3 := subscribe(b3, &c3)
	defer func() { _ = sub3.Unsubscribe(context.Background()) }()

	t.Log("waiting for 3-member stable...")
	if dur, err := monitorGroupState(ctx, t, adm, group, 3, callbackDeadline); err != nil {
		t.Fatalf("DEADLOCK after consumer 3 join: %v (c1=%d c2=%d c3=%d)",
			err, c1.Load(), c2.Load(), c3.Load())
	} else {
		t.Logf("3-member rebalance in %v", dur)
	}

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

// TestIntegration_RebalanceWithConsumerLeave simulates pod crash:
// 4 consumers share 32 partitions, one crashes, remaining 3 redistribute.
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

	for i := 0; i < numConsumers; i++ {
		clientID := fmt.Sprintf("consumer-%d", i+1)
		b := intCreateBroker(t, clientID)
		if err := b.Init(bLogger); err != nil {
			t.Fatalf("%s init: %v", clientID, err)
		}
		if err := b.Connect(ctx); err != nil {
			t.Fatalf("%s connect: %v", clientID, err)
		}
		brokers[i] = b

		idx := i
		sub, err := b.Subscribe(ctx, topic, func(msg broker.Message) error {
			time.Sleep(slowHandlerDelay)
			if !msgHasError(msg) {
				counters[idx].Add(1)
			}
			return msg.Ack()
		},
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

	startProducer(ctx, t, topic, producerRPS)
	time.Sleep(3 * time.Second)

	t.Logf("before crash: c1=%d c2=%d c3=%d c4=%d",
		counters[0].Load(), counters[1].Load(), counters[2].Load(), counters[3].Load())

	crashIdx := 3
	t.Logf("simulating consumer-%d crash...", crashIdx+1)
	_ = subs[crashIdx].Unsubscribe(context.Background())
	_ = brokers[crashIdx].Disconnect(context.Background())
	subs[crashIdx] = nil
	brokers[crashIdx] = nil

	t.Log("waiting for 3-member stable after crash...")
	if dur, err := monitorGroupState(ctx, t, adm, group, numConsumers-1, callbackDeadline); err != nil {
		t.Fatalf("DEADLOCK after consumer crash: %v (c1=%d c2=%d c3=%d)",
			err, counters[0].Load(), counters[1].Load(), counters[2].Load())
	} else {
		t.Logf("3-member rebalance after crash in %v", dur)
	}

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

	t.Log("consumer-4 rejoining (simulating pod restart)...")
	b4 := intCreateBroker(t, "consumer-4")
	if err := b4.Init(bLogger); err != nil {
		t.Fatalf("consumer-4 init: %v", err)
	}
	if err := b4.Connect(ctx); err != nil {
		t.Fatalf("consumer-4 reconnect: %v", err)
	}
	brokers[crashIdx] = b4

	sub4, err := b4.Subscribe(ctx, topic, func(msg broker.Message) error {
		time.Sleep(slowHandlerDelay)
		if !msgHasError(msg) {
			counters[crashIdx].Add(1)
		}
		return msg.Ack()
	},
		broker.SubscribeAutoAck(true),
		broker.SubscribeGroup(group),
		broker.SubscribeBodyOnly(true),
	)
	if err != nil {
		t.Fatalf("consumer-4 resubscribe: %v", err)
	}
	subs[crashIdx] = sub4

	t.Log("waiting for 4-member stable after rejoin...")
	if dur, err := monitorGroupState(ctx, t, adm, group, numConsumers, callbackDeadline); err != nil {
		t.Fatalf("DEADLOCK after consumer rejoin: %v (c1=%d c2=%d c3=%d c4=%d)",
			err, counters[0].Load(), counters[1].Load(), counters[2].Load(), counters[3].Load())
	} else {
		t.Logf("4-member rebalance after rejoin in %v", dur)
	}

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

// TestIntegration_BrokerHookErrorKillsConsumers verifies that a transient TCP-level
// broker error does NOT permanently kill consumer goroutines.
//
// Before the fix: hooks called c.recs <- on all consumers → goroutines exited.
// After the fix: hooks use shouldSendErr + notifyConsumers → errs channel (non-fatal).
func TestIntegration_BrokerHookErrorKillsConsumers(t *testing.T) {
	skipIfKafkaUnavailable(t)

	logger.DefaultLogger = slog.NewLogger()
	if err := logger.DefaultLogger.Init(logger.WithLevel(logger.InfoLevel)); err != nil {
		t.Fatal(err)
	}
	bLogger := broker.Logger(logger.DefaultLogger)

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	proxy := newTCPProxy(t, kafkaAddr)

	adm := intCreateAdminClient(t)
	topic := intUniqueTopic(t)
	intCreateTopicAndCleanup(t, adm, topic, intNumPartitions)
	group := fmt.Sprintf("inttest-hook-err-%d", time.Now().UnixNano())

	var processed atomic.Int64

	b := intCreateBroker(t, "hook-err-consumer", proxy.Addr())
	if err := b.Init(bLogger); err != nil {
		t.Fatalf("init: %v", err)
	}
	if err := b.Connect(ctx); err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer func() { _ = b.Disconnect(context.Background()) }()

	sub, err := b.Subscribe(ctx, topic, func(msg broker.Message) error {
		if !msgHasError(msg) {
			processed.Add(1)
		}
		return msg.Ack()
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

	time.Sleep(3 * time.Second)
	snap1 := processed.Load()
	time.Sleep(3 * time.Second)
	snap2 := processed.Load()
	rateNormal := float64(snap2-snap1) / 3.0
	t.Logf("normal processing rate: %.1f msg/s (total=%d)", rateNormal, snap2)

	if rateNormal < 5 {
		t.Fatalf("baseline rate too low (%.1f msg/s)", rateNormal)
	}

	t.Logf("breaking TCP connections...")
	proxy.breakConnections()

	t.Log("waiting 10s for kgo reconnect...")
	time.Sleep(10 * time.Second)

	snap3 := processed.Load()
	time.Sleep(3 * time.Second)
	snap4 := processed.Load()
	rateAfter := float64(snap4-snap3) / 3.0
	t.Logf("post-error processing rate: %.1f msg/s (was %.1f msg/s)", rateAfter, rateNormal)

	if lagMap, lagErr := adm.Lag(ctx, group); lagErr == nil {
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

	threshold := rateNormal * 0.3
	if rateAfter < threshold {
		t.Fatalf(
			"consumers did not survive broker TCP error.\n"+
				"Rate: normal=%.1f msg/s → after=%.1f msg/s (need ≥ %.1f).\n"+
				"Consumer goroutines likely died from broker error propagation via recs channel.",
			rateNormal, rateAfter, threshold,
		)
	}
}

// TestIntegration_RepeatedBrokerErrors verifies that multiple consecutive TCP breaks
// do NOT permanently kill consumer goroutines.
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

	b := intCreateBroker(t, "repeated-err-consumer", proxy.Addr())
	if err := b.Init(broker.Logger(logger.DefaultLogger)); err != nil {
		t.Fatalf("init: %v", err)
	}
	if err := b.Connect(ctx); err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer func() { _ = b.Disconnect(context.Background()) }()

	sub, err := b.Subscribe(ctx, topic, func(msg broker.Message) error {
		if !msgHasError(msg) {
			processed.Add(1)
		}
		return msg.Ack()
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

	time.Sleep(3 * time.Second)
	snap1 := processed.Load()
	time.Sleep(3 * time.Second)
	rateNormal := float64(processed.Load()-snap1) / 3.0
	t.Logf("normal processing rate: %.1f msg/s", rateNormal)
	if rateNormal < 5 {
		t.Fatalf("baseline rate too low (%.1f msg/s)", rateNormal)
	}

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
					"rate: normal=%.1f → after=%.1f msg/s.",
				i, rounds, rateNormal, rate,
			)
		}
	}
	t.Logf("survived %d consecutive TCP breaks — no consumer kill regression", rounds)
}

// TestIntegration_AutocommitErrorKillsConsumers verifies that commit-window TCP errors
// do NOT permanently kill consumer goroutines.
//
// Before the fix: autocommit() called c.recs <- on idle consumers → goroutines exited.
// After the fix: autocommit() only increments metric (or uses notifyConsumers via errs).
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
		broker.ContentType("application/octet-stream"),
		broker.Codec("application/octet-stream", codec.NewCodec()),
		broker.Addrs(proxy.Addr()),
		kgo.CommitInterval(200*time.Millisecond),
		kgo.Options(
			kg.ClientID("commit-err-consumer"),
			kg.FetchMaxBytes(10*1024*1024),
		),
		broker.Logger(logger.DefaultLogger),
	)
	if err := b.Init(); err != nil {
		t.Fatalf("init: %v", err)
	}
	if err := b.Connect(ctx); err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer func() { _ = b.Disconnect(context.Background()) }()

	sub, err := b.Subscribe(ctx, topic, func(msg broker.Message) error {
		if !msgHasError(msg) {
			processed.Add(1)
		}
		return msg.Ack()
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

	startProducer(ctx, t, topic, 50)

	time.Sleep(5 * time.Second)
	snap1 := processed.Load()
	time.Sleep(4 * time.Second)
	rateNormal := float64(processed.Load()-snap1) / 4.0
	t.Logf("normal rate: %.1f msg/s (total=%d)", rateNormal, processed.Load())
	if processed.Load() == 0 {
		t.Fatal("no records processed in baseline")
	}

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

	time.Sleep(5 * time.Second)

	snap3 := processed.Load()
	time.Sleep(4 * time.Second)
	rateAfter := float64(processed.Load()-snap3) / 4.0
	t.Logf("post-error rate: %.1f msg/s (was %.1f msg/s)", rateAfter, rateNormal)

	if lagMap, lagErr := adm.Lag(ctx, group); lagErr == nil {
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
				"autocommit() is likely calling c.recs <- on idle consumer goroutines.",
			rateNormal, rateAfter,
		)
	}
}

// TestIntegration_NetworkInterruptionKillsConsumers tests the full TCP break → kgo rejoin path.
// This tests kgo's internal reconnect/rejoin mechanism (assigned() callback → new goroutines).
func TestIntegration_NetworkInterruptionKillsConsumers(t *testing.T) {
	skipIfKafkaUnavailable(t)

	logger.DefaultLogger = slog.NewLogger()
	if err := logger.DefaultLogger.Init(logger.WithLevel(logger.InfoLevel)); err != nil {
		t.Fatal(err)
	}
	bLogger := broker.Logger(logger.DefaultLogger)

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	proxy := newTCPProxy(t, kafkaAddr)
	t.Logf("TCP proxy listening on %s → %s", proxy.Addr(), kafkaAddr)

	adm := intCreateAdminClient(t)
	topic := intUniqueTopic(t)
	intCreateTopicAndCleanup(t, adm, topic, intNumPartitions)
	group := fmt.Sprintf("inttest-netfail-%d", time.Now().UnixNano())

	var processed atomic.Int64

	b := intCreateBroker(t, "netfail-consumer", proxy.Addr())
	if err := b.Init(bLogger); err != nil {
		t.Fatalf("init: %v", err)
	}
	if err := b.Connect(ctx); err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer func() { _ = b.Disconnect(context.Background()) }()

	sub, err := b.Subscribe(ctx, topic, func(msg broker.Message) error {
		if !msgHasError(msg) {
			processed.Add(1)
		}
		return msg.Ack()
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

	time.Sleep(3 * time.Second)
	snap1 := processed.Load()
	time.Sleep(3 * time.Second)
	snap2 := processed.Load()
	rateNormal := float64(snap2-snap1) / 3.0
	t.Logf("normal processing rate: %.1f msg/s (total=%d)", rateNormal, snap2)

	if rateNormal < 5 {
		t.Fatalf("baseline rate too low (%.1f msg/s)", rateNormal)
	}

	t.Log("breaking all TCP connections (simulating network interruption)...")
	proxy.breakConnections()

	t.Log("waiting 10s for kgo reconnect + consumer self-recovery...")
	time.Sleep(10 * time.Second)

	snap3 := processed.Load()
	time.Sleep(3 * time.Second)
	snap4 := processed.Load()
	rateAfter := float64(snap4-snap3) / 3.0
	t.Logf("post-interruption processing rate: %.1f msg/s (was %.1f msg/s)", rateAfter, rateNormal)

	if lagMap, lagErr := adm.Lag(ctx, group); lagErr == nil {
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
			"consumers did not recover after TCP network interruption.\n"+
				"Rate: normal=%.1f msg/s → after=%.1f msg/s (need ≥ %.1f).",
			rateNormal, rateAfter, threshold,
		)
	}
}
