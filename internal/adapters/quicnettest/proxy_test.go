package quicnettest

import (
	"encoding/binary"
	"net"
	"reflect"
	"sync"
	"testing"
	"time"
)

const awaitTimeout = 3 * time.Second

// scriptedRNG returns pre-programmed values so tests can force impairment
// decisions without depending on a seed's random stream.
type scriptedRNG struct {
	mu     sync.Mutex
	values []int
	next   int
}

func (s *scriptedRNG) Intn(n int) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.values) == 0 || n <= 0 {
		return 0
	}
	value := s.values[s.next%len(s.values)]
	s.next++
	return value % n
}

type datagram struct {
	payload []byte
	from    *net.UDPAddr
}

func startSinkServer(t *testing.T) (*net.UDPAddr, <-chan datagram) {
	t.Helper()
	conn, packets := serveUDP(t, false)
	return conn.LocalAddr().(*net.UDPAddr), packets
}

func startEchoServer(t *testing.T) (*net.UDPAddr, <-chan datagram) {
	t.Helper()
	conn, packets := serveUDP(t, true)
	return conn.LocalAddr().(*net.UDPAddr), packets
}

func serveUDP(t *testing.T, echo bool) (*net.UDPConn, <-chan datagram) {
	t.Helper()
	conn, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatalf("listen udp server: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	packets := make(chan datagram, 4096)
	go func() {
		buf := make([]byte, 2048)
		for {
			n, from, err := conn.ReadFromUDP(buf)
			if err != nil {
				return
			}
			payload := append([]byte(nil), buf[:n]...)
			packets <- datagram{payload: payload, from: from}
			if echo {
				if _, err := conn.WriteToUDP(payload, from); err != nil {
					return
				}
			}
		}
	}()
	return conn, packets
}

func newUDPClient(t *testing.T) *net.UDPConn {
	t.Helper()
	conn, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatalf("listen udp client: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

func newTestProxy(t *testing.T, cfg Config) (*Proxy, *ManualClock, <-chan Event) {
	t.Helper()
	clock := NewManualClock(time.Unix(1000, 0))
	events := make(chan Event, 4096)
	cfg.Clock = clock
	cfg.OnPacket = func(event Event) { events <- event }
	proxy, err := New(cfg)
	if err != nil {
		t.Fatalf("new proxy: %v", err)
	}
	t.Cleanup(func() { _ = proxy.Close() })
	return proxy, clock, events
}

func send(t *testing.T, conn *net.UDPConn, dst *net.UDPAddr, payload []byte) {
	t.Helper()
	if _, err := conn.WriteToUDP(payload, dst); err != nil {
		t.Fatalf("send %d-byte packet: %v", len(payload), err)
	}
}

// sendPackets sends count datagrams in bounded bursts and returns the events
// observed for them, so the client never outruns the proxy's receive buffer.
func sendPackets(t *testing.T, client *net.UDPConn, dst *net.UDPAddr, events <-chan Event, count, size int) []Event {
	t.Helper()
	const burst = 20
	payload := make([]byte, size)
	observed := make([]Event, 0, count)
	for sent := 0; sent < count; {
		stop := sent + burst
		if stop > count {
			stop = count
		}
		for i := sent; i < stop; i++ {
			binary.BigEndian.PutUint32(payload, uint32(i))
			send(t, client, dst, payload)
		}
		for i := sent; i < stop; i++ {
			observed = append(observed, awaitEvent(t, events))
		}
		sent = stop
	}
	return observed
}

func readPacket(t *testing.T, conn *net.UDPConn) []byte {
	t.Helper()
	if err := conn.SetReadDeadline(time.Now().Add(awaitTimeout)); err != nil {
		t.Fatalf("set read deadline: %v", err)
	}
	buf := make([]byte, 2048)
	n, _, err := conn.ReadFromUDP(buf)
	if err != nil {
		t.Fatalf("read reply: %v", err)
	}
	return append([]byte(nil), buf[:n]...)
}

func awaitPacket(t *testing.T, packets <-chan datagram) datagram {
	t.Helper()
	select {
	case packet := <-packets:
		return packet
	case <-time.After(awaitTimeout):
		t.Fatal("timed out waiting for a server receipt")
	}
	return datagram{}
}

func awaitEvent(t *testing.T, events <-chan Event) Event {
	t.Helper()
	select {
	case event := <-events:
		return event
	case <-time.After(awaitTimeout):
		t.Fatal("timed out waiting for an impairment event")
	}
	return Event{}
}

func expectNoDatagram(t *testing.T, packets <-chan datagram) {
	t.Helper()
	select {
	case packet := <-packets:
		t.Fatalf("unexpected server receipt %q", packet.payload)
	default:
	}
}

func TestNewRejectsInvalidConfig(t *testing.T) {
	if _, err := New(Config{}); err == nil {
		t.Fatal("New accepted a config without ServerAddr")
	}
	server := &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 9}
	if _, err := New(Config{ServerAddr: server, ToServer: LinkConfig{LossPercent: 101}}); err == nil {
		t.Fatal("New accepted LossPercent 101")
	}
	if _, err := New(Config{ServerAddr: server, ToClient: LinkConfig{Blackouts: []Blackout{{Start: time.Second, End: time.Second}}}}); err == nil {
		t.Fatal("New accepted an empty blackout window")
	}
}

func TestProxyForwardsTrafficBothDirections(t *testing.T) {
	serverAddr, serverPackets := startEchoServer(t)
	proxy, _, events := newTestProxy(t, Config{ServerAddr: serverAddr, Seed: 1})
	client := newUDPClient(t)

	send(t, client, proxy.Addr(), []byte("hello quic"))

	up := awaitEvent(t, events)
	if up.Direction != ToServer {
		t.Fatalf("first event direction = %s, want to-server", up.Direction)
	}
	if up.Decision.Drop {
		t.Fatalf("to-server decision = %+v, want forward", up.Decision)
	}
	if got := string(awaitPacket(t, serverPackets).payload); got != "hello quic" {
		t.Fatalf("server received %q, want %q", got, "hello quic")
	}

	down := awaitEvent(t, events)
	if down.Direction != ToClient {
		t.Fatalf("second event direction = %s, want to-client", down.Direction)
	}
	if got := string(readPacket(t, client)); got != "hello quic" {
		t.Fatalf("client received %q, want %q", got, "hello quic")
	}

	stats := proxy.Stats()
	if stats.ToServer.Received != 1 || stats.ToServer.Sent != 1 {
		t.Fatalf("to-server stats = %s, want one received and sent packet", stats.ToServer)
	}
	if stats.ToClient.Received != 1 || stats.ToClient.Sent != 1 {
		t.Fatalf("to-client stats = %s, want one received and sent packet", stats.ToClient)
	}
	if stats.StrayPackets != 0 || stats.Rebinds != 0 {
		t.Fatalf("unexpected stray/rebind counters: %s", stats)
	}
}

func TestProxySeededLossIsReproducible(t *testing.T) {
	const packets = 100
	run := func(t *testing.T) (Stats, []Event) {
		t.Helper()
		serverAddr, serverPackets := startSinkServer(t)
		proxy, clock, events := newTestProxy(t, Config{
			ServerAddr: serverAddr,
			Seed:       42,
			ToServer:   LinkConfig{BaseLatency: time.Millisecond, LossPercent: 25},
		})
		client := newUDPClient(t)
		observed := sendPackets(t, client, proxy.Addr(), events, packets, 64)

		expected := 0
		for _, event := range observed {
			if !event.Decision.Drop {
				expected++
			}
		}
		clock.Advance(time.Millisecond)
		for i := 0; i < expected; i++ {
			awaitPacket(t, serverPackets)
		}
		stats := proxy.Stats()
		if stats.ToServer.Received != packets {
			t.Fatalf("received = %d, want %d", stats.ToServer.Received, packets)
		}
		return stats, observed
	}

	firstStats, firstEvents := run(t)
	secondStats, secondEvents := run(t)
	if firstStats.ToServer != secondStats.ToServer {
		t.Fatalf("same seed produced different stats:\nfirst  = %s\nsecond = %s", firstStats.ToServer, secondStats.ToServer)
	}
	if !reflect.DeepEqual(firstEvents, secondEvents) {
		t.Fatal("same seed produced different impairment decisions")
	}
	if firstStats.ToServer.LossDrops == 0 || firstStats.ToServer.LossDrops >= packets {
		t.Fatalf("loss drops = %d, want a partial loss sample", firstStats.ToServer.LossDrops)
	}
	if want := uint64(packets) - firstStats.ToServer.LossDrops; firstStats.ToServer.Sent != want {
		t.Fatalf("sent = %d, want %d", firstStats.ToServer.Sent, want)
	}
}

func TestProxyDuplicationRepeatsPackets(t *testing.T) {
	const packets = 3
	serverAddr, serverPackets := startSinkServer(t)
	proxy, _, events := newTestProxy(t, Config{
		ServerAddr: serverAddr,
		Seed:       3,
		ToServer:   LinkConfig{DuplicatePercent: 100},
	})
	client := newUDPClient(t)

	observed := sendPackets(t, client, proxy.Addr(), events, packets, 32)
	for i, event := range observed {
		if event.Decision.Duplicates != 1 {
			t.Fatalf("packet %d decision = %+v, want one duplicate", i, event.Decision)
		}
	}
	for i := 0; i < packets*2; i++ {
		awaitPacket(t, serverPackets)
	}
	stats := proxy.Stats()
	if stats.ToServer.Duplicates != packets {
		t.Fatalf("duplicates = %d, want %d", stats.ToServer.Duplicates, packets)
	}
	if stats.ToServer.Sent != packets*2 {
		t.Fatalf("sent = %d, want %d", stats.ToServer.Sent, packets*2)
	}
}

func TestProxyReorderingDeliversOutOfOrder(t *testing.T) {
	serverAddr, serverPackets := startSinkServer(t)
	proxy, clock, events := newTestProxy(t, Config{
		ServerAddr: serverAddr,
		RNG:        &scriptedRNG{values: []int{0, 99}},
		ToServer:   LinkConfig{ReorderPercent: 50, ReorderDelay: 5 * time.Millisecond},
	})
	client := newUDPClient(t)

	send(t, client, proxy.Addr(), []byte("first"))
	send(t, client, proxy.Addr(), []byte("second"))

	first := awaitEvent(t, events)
	if !first.Decision.Reordered {
		t.Fatalf("first decision = %+v, want reordered", first.Decision)
	}
	second := awaitEvent(t, events)
	if second.Decision.Reordered {
		t.Fatalf("second decision = %+v, want not reordered", second.Decision)
	}
	if !second.Due.Before(first.Due) {
		t.Fatalf("second due %s is not before first due %s", second.Due, first.Due)
	}

	clock.Advance(5 * time.Millisecond)
	if got := string(awaitPacket(t, serverPackets).payload); got != "second" {
		t.Fatalf("first server receipt = %q, want %q", got, "second")
	}
	if got := string(awaitPacket(t, serverPackets).payload); got != "first" {
		t.Fatalf("second server receipt = %q, want %q", got, "first")
	}
	if stats := proxy.Stats(); stats.ToServer.Reordered != 1 {
		t.Fatalf("reordered = %d, want 1", stats.ToServer.Reordered)
	}
}

func TestProxyLatencyHoldsPacketsUntilClockAdvances(t *testing.T) {
	serverAddr, serverPackets := startSinkServer(t)
	proxy, clock, events := newTestProxy(t, Config{
		ServerAddr: serverAddr,
		Seed:       4,
		ToServer:   LinkConfig{BaseLatency: 3 * time.Millisecond},
	})
	client := newUDPClient(t)

	send(t, client, proxy.Addr(), []byte("held"))
	awaitEvent(t, events)
	expectNoDatagram(t, serverPackets)

	clock.Advance(2 * time.Millisecond)
	expectNoDatagram(t, serverPackets)

	clock.Advance(time.Millisecond)
	if got := string(awaitPacket(t, serverPackets).payload); got != "held" {
		t.Fatalf("server received %q, want %q", got, "held")
	}
	if stats := proxy.Stats(); stats.ToServer.Sent != 1 {
		t.Fatalf("sent = %d, want 1", stats.ToServer.Sent)
	}
}

func TestProxyBlackoutWindowDropsDuringInterval(t *testing.T) {
	serverAddr, serverPackets := startSinkServer(t)
	proxy, clock, events := newTestProxy(t, Config{
		ServerAddr: serverAddr,
		Seed:       5,
		ToServer: LinkConfig{Blackouts: []Blackout{{
			Start: 5 * time.Millisecond,
			End:   10 * time.Millisecond,
		}}},
	})
	client := newUDPClient(t)

	send(t, client, proxy.Addr(), []byte("before"))
	if event := awaitEvent(t, events); event.Decision.Drop {
		t.Fatalf("packet before blackout dropped: %+v", event.Decision)
	}
	if got := string(awaitPacket(t, serverPackets).payload); got != "before" {
		t.Fatalf("server received %q, want %q", got, "before")
	}

	clock.Advance(5 * time.Millisecond)
	send(t, client, proxy.Addr(), []byte("during"))
	during := awaitEvent(t, events)
	if !during.Decision.Drop || !during.Decision.Blackout {
		t.Fatalf("during decision = %+v, want a blackout drop", during.Decision)
	}

	clock.Advance(6 * time.Millisecond)
	send(t, client, proxy.Addr(), []byte("after"))
	if event := awaitEvent(t, events); event.Decision.Drop {
		t.Fatalf("packet after blackout dropped: %+v", event.Decision)
	}
	if got := string(awaitPacket(t, serverPackets).payload); got != "after" {
		t.Fatalf("server received %q, want %q", got, "after")
	}

	stats := proxy.Stats()
	if stats.ToServer.BlackoutDrops != 1 {
		t.Fatalf("blackout drops = %d, want 1", stats.ToServer.BlackoutDrops)
	}
	if stats.ToServer.Received != 3 || stats.ToServer.Sent != 2 {
		t.Fatalf("to-server stats = %s, want 3 received and 2 sent", stats.ToServer)
	}
}

func TestProxyManualBlackoutToggles(t *testing.T) {
	serverAddr, serverPackets := startSinkServer(t)
	proxy, _, events := newTestProxy(t, Config{ServerAddr: serverAddr, Seed: 6})
	client := newUDPClient(t)

	proxy.SetBlackout(true)
	send(t, client, proxy.Addr(), []byte("blacked"))
	if event := awaitEvent(t, events); !event.Decision.Blackout {
		t.Fatalf("decision = %+v, want a blackout drop", event.Decision)
	}
	expectNoDatagram(t, serverPackets)

	proxy.SetBlackout(false)
	send(t, client, proxy.Addr(), []byte("cleared"))
	if event := awaitEvent(t, events); event.Decision.Drop {
		t.Fatalf("decision = %+v, want forward", event.Decision)
	}
	if got := string(awaitPacket(t, serverPackets).payload); got != "cleared" {
		t.Fatalf("server received %q, want %q", got, "cleared")
	}
}

func TestProxyQueueCapacityBoundsInFlightPackets(t *testing.T) {
	serverAddr, serverPackets := startSinkServer(t)
	proxy, clock, events := newTestProxy(t, Config{
		ServerAddr:    serverAddr,
		Seed:          11,
		QueueCapacity: 2,
		ToServer:      LinkConfig{BaseLatency: time.Second},
	})
	client := newUDPClient(t)

	observed := sendPackets(t, client, proxy.Addr(), events, 10, 32)
	for _, event := range observed {
		if event.Decision.Drop {
			t.Fatalf("unexpected impairment drop: %+v", event.Decision)
		}
	}
	stats := proxy.Stats()
	if stats.ToServer.OverflowDrops != 8 {
		t.Fatalf("overflow drops = %d, want 8", stats.ToServer.OverflowDrops)
	}
	if stats.ToServer.Sent != 0 {
		t.Fatalf("sent = %d, want 0 before the latency elapses", stats.ToServer.Sent)
	}
	expectNoDatagram(t, serverPackets)

	clock.Advance(time.Second)
	for i := 0; i < 2; i++ {
		awaitPacket(t, serverPackets)
	}
	if got := proxy.Stats().ToServer.Sent; got != 2 {
		t.Fatalf("sent = %d, want 2", got)
	}
}

func TestProxyDropsOversizeDatagrams(t *testing.T) {
	serverAddr, serverPackets := startSinkServer(t)
	proxy, _, events := newTestProxy(t, Config{
		ServerAddr:    serverAddr,
		Seed:          8,
		MaxPacketSize: 64,
	})
	client := newUDPClient(t)

	send(t, client, proxy.Addr(), make([]byte, 65))
	event := awaitEvent(t, events)
	if !event.Decision.Drop || !event.Decision.Oversize {
		t.Fatalf("decision = %+v, want an oversize drop", event.Decision)
	}
	expectNoDatagram(t, serverPackets)

	stats := proxy.Stats()
	if stats.ToServer.OversizeDrops != 1 || stats.ToServer.Received != 0 {
		t.Fatalf("to-server stats = %s, want one oversize drop and no receipt", stats.ToServer)
	}
}

func TestProxyRebindChangesUpstreamSourceAddress(t *testing.T) {
	serverAddr, serverPackets := startSinkServer(t)
	proxy, _, events := newTestProxy(t, Config{
		ServerAddr:            serverAddr,
		Seed:                  13,
		NATRebindAfterPackets: 1,
	})
	client := newUDPClient(t)

	send(t, client, proxy.Addr(), []byte("one"))
	awaitEvent(t, events)
	first := awaitPacket(t, serverPackets)

	send(t, client, proxy.Addr(), []byte("two"))
	awaitEvent(t, events)
	second := awaitPacket(t, serverPackets)

	if first.from.Port == second.from.Port {
		t.Fatalf("upstream source port stayed %d across a NAT rebind", first.from.Port)
	}
	if got := proxy.Stats().Rebinds; got < 1 {
		t.Fatalf("rebinds = %d, want at least 1", got)
	}
}

func TestProxyExplicitRebindKeepsRoundTripWorking(t *testing.T) {
	serverAddr, serverPackets := startEchoServer(t)
	proxy, _, events := newTestProxy(t, Config{ServerAddr: serverAddr, Seed: 17})
	client := newUDPClient(t)

	send(t, client, proxy.Addr(), []byte("one"))
	awaitEvent(t, events)
	awaitEvent(t, events)
	if got := string(readPacket(t, client)); got != "one" {
		t.Fatalf("client received %q, want %q", got, "one")
	}
	before := awaitPacket(t, serverPackets)

	if err := proxy.Rebind(); err != nil {
		t.Fatalf("rebind: %v", err)
	}

	send(t, client, proxy.Addr(), []byte("two"))
	awaitEvent(t, events)
	awaitEvent(t, events)
	if got := string(readPacket(t, client)); got != "two" {
		t.Fatalf("client received %q, want %q", got, "two")
	}
	after := awaitPacket(t, serverPackets)

	if before.from.Port == after.from.Port {
		t.Fatalf("upstream source port stayed %d after Rebind", before.from.Port)
	}
	stats := proxy.Stats()
	if stats.Rebinds != 1 || stats.StrayPackets != 0 {
		t.Fatalf("unexpected stats after rebind: %s", stats)
	}
}

func TestProxyCloseIsIdempotent(t *testing.T) {
	serverAddr, _ := startSinkServer(t)
	proxy, _, _ := newTestProxy(t, Config{ServerAddr: serverAddr})

	if err := proxy.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}
	if err := proxy.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
	if err := proxy.Rebind(); err != ErrClosed {
		t.Fatalf("Rebind after Close = %v, want ErrClosed", err)
	}
}

// TestProxyQUICLikeFlowUnderImpairment drives a QUIC-like packet flow (1.2 KiB
// MTU-sized packets in bursts, with a blackout window in the middle) through a
// fully impaired link and checks that every packet is accounted for exactly
// once: received + duplicates == sent + drops.
func TestProxyQUICLikeFlowUnderImpairment(t *testing.T) {
	const (
		phaseA     = 120
		phaseB     = 30
		phaseC     = 150
		packetSize = 1200
	)
	serverAddr, serverPackets := startSinkServer(t)
	proxy, clock, events := newTestProxy(t, Config{
		ServerAddr:    serverAddr,
		Seed:          7,
		QueueCapacity: 1024,
		ToServer: LinkConfig{
			BaseLatency:      2 * time.Millisecond,
			Jitter:           time.Millisecond,
			LossPercent:      10,
			DuplicatePercent: 5,
			DuplicateDelay:   time.Millisecond,
			ReorderPercent:   5,
			ReorderDelay:     3 * time.Millisecond,
			Blackouts:        []Blackout{{Start: 10 * time.Millisecond, End: 20 * time.Millisecond}},
		},
	})
	client := newUDPClient(t)

	observed := sendPackets(t, client, proxy.Addr(), events, phaseA, packetSize)
	clock.Advance(10 * time.Millisecond)

	// Every packet inside the blackout window must be dropped.
	blacked := sendPackets(t, client, proxy.Addr(), events, phaseB, packetSize)
	for _, event := range blacked {
		if !event.Decision.Blackout {
			t.Fatalf("blackout phase decision = %+v, want a blackout drop", event.Decision)
		}
	}
	clock.Advance(10 * time.Millisecond)

	observed = append(observed, blacked...)
	observed = append(observed, sendPackets(t, client, proxy.Addr(), events, phaseC, packetSize)...)
	clock.Advance(20 * time.Millisecond)

	var expected, dropped, duplicates, reordered uint64
	for _, event := range observed {
		if event.Decision.Drop {
			dropped++
			continue
		}
		expected += 1 + uint64(event.Decision.Duplicates)
		duplicates += uint64(event.Decision.Duplicates)
		if event.Decision.Reordered {
			reordered++
		}
	}
	for i := uint64(0); i < expected; i++ {
		awaitPacket(t, serverPackets)
	}

	stats := proxy.Stats()
	if stats.ToServer.Received != phaseA+phaseB+phaseC {
		t.Fatalf("received = %d, want %d", stats.ToServer.Received, phaseA+phaseB+phaseC)
	}
	if stats.ToServer.BlackoutDrops != phaseB {
		t.Fatalf("blackout drops = %d, want %d", stats.ToServer.BlackoutDrops, phaseB)
	}
	if stats.ToServer.OverflowDrops != 0 || stats.ToServer.OversizeDrops != 0 {
		t.Fatalf("unexpected overflow/oversize: %s", stats.ToServer)
	}
	if stats.ToServer.Duplicates != duplicates {
		t.Fatalf("duplicates = %d, want %d", stats.ToServer.Duplicates, duplicates)
	}
	if stats.ToServer.Reordered != reordered || reordered == 0 {
		t.Fatalf("reordered = %d (want %d and non-zero)", stats.ToServer.Reordered, reordered)
	}
	if duplicates == 0 {
		t.Fatal("flow produced no duplicates, so duplication was not exercised")
	}
	if stats.ToServer.LossDrops == 0 {
		t.Fatal("flow produced no loss drops, so loss was not exercised")
	}
	if drops := stats.ToServer.LossDrops + stats.ToServer.BlackoutDrops + stats.ToServer.OversizeDrops + stats.ToServer.OverflowDrops; drops != dropped {
		t.Fatalf("drops = %d, want %d", drops, dropped)
	}
	if stats.ToServer.Sent != expected {
		t.Fatalf("sent = %d, want %d", stats.ToServer.Sent, expected)
	}
	// Oversize datagrams are rejected before Received is incremented and
	// therefore don't participate in the accepted-packet identity.
	total := stats.ToServer.LossDrops + stats.ToServer.BlackoutDrops + stats.ToServer.OverflowDrops
	if got := stats.ToServer.Sent + total; got != stats.ToServer.Received+stats.ToServer.Duplicates {
		t.Fatalf("accounting invariant broken: received(%d) + duplicates(%d) != sent(%d) + drops(%d)",
			stats.ToServer.Received, stats.ToServer.Duplicates, stats.ToServer.Sent, total)
	}
	if got := stats.String(); got == "" {
		t.Fatal("Stats.String returned empty")
	}
}
