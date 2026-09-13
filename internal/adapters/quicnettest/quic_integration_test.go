// Package quicnettest_test holds the end-to-end integration tests for the
// adverse-network proxy. It is deliberately an external test package: the
// concrete production adapters (internal/adapters/quic, sessionwire, protocol,
// ports) are imported only from these _test.go files, so the simulator package
// itself stays free of production dependencies and the hexagonal boundary
// policy still holds.
package quicnettest_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"net"
	"reflect"
	"runtime"
	"testing"
	"time"

	quicadapter "github.com/bnema/vev/internal/adapters/quic"
	"github.com/bnema/vev/internal/adapters/quicnettest"
	"github.com/bnema/vev/internal/adapters/sessionwire"
	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/protocol/wire"
)

// These tests run the real internal/adapters/quic transport through the proxy
// and drive a real typed sessionwire conversation over it. The proxy is the
// only impairment source: no netem, no root, no hooks in production code.

const typedConversationTimeout = 25 * time.Second

type acceptResult struct {
	transport wire.Transport
	err       error
}

// trainProfile is the deterministic "4G/train" adverse link: tens of
// milliseconds of latency with jitter, a few percent loss, duplication, and
// delay-based reordering. The temporary blackout is toggled while the typed
// transcript is in flight.
func trainProfile() quicnettest.LinkConfig {
	return quicnettest.LinkConfig{
		BaseLatency:      25 * time.Millisecond,
		Jitter:           15 * time.Millisecond,
		LossPercent:      4,
		DuplicatePercent: 6,
		DuplicateDelay:   2 * time.Millisecond,
		ReorderPercent:   10,
		ReorderDelay:     12 * time.Millisecond,
	}
}

// startQUICFixture starts a real QUIC listener, puts a proxy in front of it,
// and returns the proxy plus the pinned fingerprint the client must dial with.
func startQUICFixture(t *testing.T, cfg quicnettest.Config) (*quicnettest.Proxy, *quicadapter.Listener, []byte) {
	t.Helper()
	cert, fingerprint, err := quicadapter.GenerateEphemeralCert()
	if err != nil {
		t.Fatalf("generate cert: %v", err)
	}
	listener, err := quicadapter.ListenConfig("127.0.0.1:0", cert, quicadapter.Config{}, 4)
	if err != nil {
		t.Fatalf("listen quic: %v", err)
	}
	t.Cleanup(func() { _ = listener.Close() })

	serverAddr, err := net.ResolveUDPAddr("udp", listener.Addr())
	if err != nil {
		t.Fatalf("resolve listener address %q: %v", listener.Addr(), err)
	}
	// The event sink must never block a reader goroutine; the deterministic
	// counters in Stats are what the assertions use.
	events := make(chan quicnettest.Event, 1<<16)
	cfg.ServerAddr = serverAddr
	cfg.OnPacket = func(event quicnettest.Event) {
		select {
		case events <- event:
		default:
		}
	}
	proxy, err := quicnettest.New(cfg)
	if err != nil {
		t.Fatalf("new proxy: %v", err)
	}
	t.Cleanup(func() { _ = proxy.Close() })
	return proxy, listener, fingerprint
}

func goAccept(listener *quicadapter.Listener) <-chan acceptResult {
	accepted := make(chan acceptResult, 1)
	go func() {
		transport, err := listener.Accept()
		accepted <- acceptResult{transport: transport, err: err}
	}()
	return accepted
}

// dialRealQUIC accepts on the listener and dials the proxy with the pinned
// fingerprint, returning both ends of the real QUIC carriage.
func dialRealQUIC(t *testing.T, proxy *quicnettest.Proxy, listener *quicadapter.Listener, fingerprint []byte) (wire.Transport, wire.Transport) {
	t.Helper()
	accepted := goAccept(listener)
	dialer := quicadapter.DialConfig(proxy.Addr().String(), "vev-bootstrap", fingerprint, quicadapter.Config{}, 15*time.Second)
	ctx, cancel := context.WithTimeout(context.Background(), typedConversationTimeout)
	defer cancel()
	client, err := dialer.Dial(ctx)
	if err != nil {
		t.Fatalf("dial through proxy: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })

	select {
	case result := <-accepted:
		if result.err != nil {
			t.Fatalf("accept: %v", result.err)
		}
		t.Cleanup(func() { _ = result.transport.Close() })
		return client, result.transport
	case <-time.After(typedConversationTimeout):
		t.Fatal("timed out waiting for the QUIC listener to accept")
		return nil, nil
	}
}

type serverReceive struct {
	message protocol.ClientMessage
	err     error
}

// exchangeWithReply sends one typed client message, asserts the server received
// exactly that message, and returns the typed reply the server sends back.
func exchangeWithReply(t *testing.T, client ports.ClientConnection, server ports.ServerConnection, message protocol.ClientMessage, reply protocol.ServerMessage) protocol.ServerMessage {
	t.Helper()
	received := make(chan serverReceive, 1)
	go func() {
		got, err := server.ReceiveClient()
		received <- serverReceive{message: got, err: err}
	}()
	if err := client.SendClient(message); err != nil {
		t.Fatalf("send %T: %v", message, err)
	}
	result := awaitServerReceive(t, received)
	if result.err != nil {
		t.Fatalf("server receive: %v", result.err)
	}
	if !reflect.DeepEqual(result.message, message) {
		t.Fatalf("server received %#v, want %#v", result.message, message)
	}

	sent := make(chan error, 1)
	go func() { sent <- server.SendServer(reply) }()
	got, err := client.ReceiveServer()
	if err != nil {
		t.Fatalf("client receive reply: %v", err)
	}
	if err := <-sent; err != nil {
		t.Fatalf("server send %T: %v", reply, err)
	}
	return got
}

func awaitServerReceive(t *testing.T, received <-chan serverReceive) serverReceive {
	t.Helper()
	select {
	case result := <-received:
		return result
	case <-time.After(typedConversationTimeout):
		t.Fatal("timed out waiting for the typed client message")
		return serverReceive{}
	}
}

func serverReceiveFrom(server ports.ServerConnection) <-chan serverReceive {
	received := make(chan serverReceive, 1)
	go func() {
		message, err := server.ReceiveClient()
		received <- serverReceive{message: message, err: err}
	}()
	return received
}

// startTypedConversation performs the real QUIC handshake through the proxy and
// completes the typed session preamble with a Hello/Welcome exchange.
func startTypedConversation(t *testing.T, proxy *quicnettest.Proxy, listener *quicadapter.Listener, fingerprint []byte) (ports.ClientConnection, ports.ServerConnection) {
	t.Helper()
	clientRaw, serverRaw := dialRealQUIC(t, proxy, listener, fingerprint)
	client := sessionwire.NewClientConnection(clientRaw)
	server := sessionwire.NewServerConnection(serverRaw)
	t.Cleanup(func() { _ = client.Close() })
	t.Cleanup(func() { _ = server.Close() })

	hello := protocol.Hello{
		Version:  protocol.Version,
		ClientID: [16]byte{0x4d, 0x79, 0x4e, 0x41, 0x54, 0x2d, 0x34, 0x47},
		Name:     "train-4g",
		Size:     domain.Size{Cols: 80, Rows: 24},
		TermEnv:  "xterm-256color",
	}
	reply := exchangeWithReply(t, client, server, hello, protocol.Welcome{SessionID: "train-4g", SessionName: "train", Capabilities: 1})
	welcome, ok := reply.(protocol.Welcome)
	if !ok {
		t.Fatalf("welcome reply is %T, want protocol.Welcome", reply)
	}
	if welcome.SessionID != "train-4g" {
		t.Fatalf("welcome session id = %q, want %q", welcome.SessionID, "train-4g")
	}
	return client, server
}

func inputPayload(seq uint64) []byte {
	payload := make([]byte, 512)
	binary.BigEndian.PutUint64(payload, seq)
	for i := 8; i < len(payload); i++ {
		payload[i] = byte(seq)
	}
	return payload
}

// TestRealQUICTypedConversationOverTrainProfile runs the real vev QUIC
// transport through the proxy under a deterministic 4G/train profile and drives
// a real typed sessionwire conversation over it: Hello/Welcome, then 200
// sequenced Input records (each 512 bytes) in one pipelined transcript, then
// 200 Pong replies. It requires that
//
//   - every semantic payload arrives exactly once, in order, byte-for-byte,
//     with no gaps and no duplicates,
//   - a 500ms blackout that starts mid-transcript drops packets in both
//     directions without losing or duplicating an application message,
//   - loss, duplication, and reordering actually occurred,
//   - the bounded queue never overflowed and every accepted packet is
//     accounted for (received + duplicates == sent + drops),
//   - cleanup and goroutine exit are bounded.
func TestRealQUICTypedConversationOverTrainProfile(t *testing.T) {
	const total = 200

	before := runtime.NumGoroutine()
	profile := trainProfile()
	proxy, listener, fingerprint := startQUICFixture(t, quicnettest.Config{
		Seed:          20260913,
		QueueCapacity: 1024,
		ToServer:      profile,
		ToClient:      profile,
	})
	client, server := startTypedConversation(t, proxy, listener, fingerprint)

	// Forward direction: the client pipelines the whole sequenced transcript
	// while the server drains it. The blackout starts once the transcript is
	// under way and clears on a bounded real-time timer, because the real QUIC
	// stack, not a test clock, owns retransmission here.
	sendErr := make(chan error, 1)
	go func() {
		for seq := uint64(1); seq <= total; seq++ {
			err := client.SendClient(protocol.Input{InputSeq: seq, ActionID: seq, Data: inputPayload(seq)})
			if err != nil {
				sendErr <- err
				return
			}
		}
		sendErr <- nil
	}()

	blackoutStarted := false
	for index := 0; index < total; index++ {
		if index == total/5 && !blackoutStarted {
			blackoutStarted = true
			proxy.SetBlackout(true)
			time.AfterFunc(500*time.Millisecond, func() { proxy.SetBlackout(false) })
		}
		result := awaitServerReceive(t, serverReceiveFrom(server))
		if result.err != nil {
			t.Fatalf("server receive %d: %v", index, result.err)
		}
		input, ok := result.message.(protocol.Input)
		if !ok {
			t.Fatalf("server message %d is %T, want protocol.Input", index, result.message)
		}
		wantSeq := uint64(index + 1)
		if input.InputSeq != wantSeq || input.ActionID != wantSeq {
			t.Fatalf("server message %d has seq/action %d/%d, want %d/%d", index, input.InputSeq, input.ActionID, wantSeq, wantSeq)
		}
		if !bytes.Equal(input.Data, inputPayload(wantSeq)) {
			t.Fatalf("server message %d payload differs from the sent bytes", index)
		}
	}
	if err := <-sendErr; err != nil {
		t.Fatalf("client transcript send: %v", err)
	}

	// Reverse direction: the server answers every Input with exactly one Pong.
	replyErr := make(chan error, 1)
	go func() {
		for index := 0; index < total; index++ {
			if err := server.SendServer(protocol.Pong{}); err != nil {
				replyErr <- err
				return
			}
		}
		replyErr <- nil
	}()
	for index := 0; index < total; index++ {
		message, err := client.ReceiveServer()
		if err != nil {
			t.Fatalf("client receive reply %d: %v", index, err)
		}
		if _, ok := message.(protocol.Pong); !ok {
			t.Fatalf("client reply %d is %T, want protocol.Pong", index, message)
		}
	}
	if err := <-replyErr; err != nil {
		t.Fatalf("server reply transcript: %v", err)
	}

	stats := waitForAccounting(t, proxy, 10*time.Second)
	if stats.ToServer.LossDrops == 0 || stats.ToServer.Duplicates == 0 || stats.ToServer.Reordered == 0 {
		t.Fatalf("train profile was not exercised: %s", stats)
	}
	if stats.ToServer.BlackoutDrops == 0 || stats.ToClient.BlackoutDrops == 0 {
		t.Fatalf("blackout did not drop packets in both directions: %s", stats)
	}
	if stats.ToServer.OverflowDrops != 0 || stats.ToClient.OverflowDrops != 0 {
		t.Fatalf("bounded queue overflowed: %s", stats)
	}
	if stats.ToServer.DeliveryErrors != 0 || stats.ToClient.DeliveryErrors != 0 {
		t.Fatalf("proxy delivery failed: %s", stats)
	}
	if stats.ToServer.Received == 0 || stats.ToServer.Sent == 0 || stats.ToClient.Sent == 0 {
		t.Fatalf("proxy carried no traffic: %s", stats)
	}

	assertBoundedCleanup(t, proxy, listener, client, server, before)
}

// TestRealQUICNATRebindKeepsTypedConversation proves the address-change path:
// after the proxy rebinds its upstream socket, the real quic-go server must
// follow the client's new address (RFC 9000 section 9 passive migration) so the
// typed conversation keeps flowing. If quic-go ever stops honouring passive
// migration, this test fails instead of silently dropping the scenario.
func TestRealQUICNATRebindKeepsTypedConversation(t *testing.T) {
	before := runtime.NumGoroutine()
	proxy, listener, fingerprint := startQUICFixture(t, quicnettest.Config{
		Seed:          20260914,
		QueueCapacity: 1024,
		ToServer:      quicnettest.LinkConfig{BaseLatency: 5 * time.Millisecond},
		ToClient:      quicnettest.LinkConfig{BaseLatency: 5 * time.Millisecond},
	})
	client, server := startTypedConversation(t, proxy, listener, fingerprint)

	// Baseline typed round trip before the address change.
	if reply := exchangeWithReply(t, client, server, protocol.Ping{}, protocol.Pong{}); reply == nil {
		t.Fatal("baseline exchange returned no reply")
	}
	beforeAddr := proxy.UpstreamAddr()

	if err := proxy.Rebind(); err != nil {
		t.Fatalf("rebind: %v", err)
	}
	afterAddr := proxy.UpstreamAddr()
	if beforeAddr == nil || afterAddr == nil || beforeAddr.Port == afterAddr.Port {
		t.Fatalf("upstream address did not change: %v -> %v", beforeAddr, afterAddr)
	}

	// Every further typed round trip must survive the server-observed address
	// change; the old upstream socket is closed, so a server that ignored the
	// rebind would stall these exchanges.
	for sequence := uint64(1); sequence <= 5; sequence++ {
		message := protocol.Input{InputSeq: sequence, ActionID: sequence, Data: inputPayload(sequence)}
		reply := exchangeWithReply(t, client, server, message, protocol.Pong{})
		if _, ok := reply.(protocol.Pong); !ok {
			t.Fatalf("round trip %d after rebind returned %T, want protocol.Pong", sequence, reply)
		}
	}

	stats := waitForAccounting(t, proxy, 10*time.Second)
	if stats.Rebinds != 1 {
		t.Fatalf("rebinds = %d, want 1", stats.Rebinds)
	}
	if stats.StrayPackets != 0 {
		t.Fatalf("stray packets after rebind = %d, want 0", stats.StrayPackets)
	}
	if stats.ToServer.OverflowDrops != 0 || stats.ToClient.OverflowDrops != 0 {
		t.Fatalf("bounded queue overflowed: %s", stats)
	}
	if stats.ToServer.DeliveryErrors != 0 || stats.ToClient.DeliveryErrors != 0 {
		t.Fatalf("proxy delivery failed after rebind: %s", stats)
	}

	assertBoundedCleanup(t, proxy, listener, client, server, before)
}

// waitForAccounting polls the proxy counters until every accepted packet is
// either sent or dropped in both directions, proving no packet is stuck in the
// bounded queue, then returns the settled snapshot.
func waitForAccounting(t *testing.T, proxy *quicnettest.Proxy, timeout time.Duration) quicnettest.Stats {
	t.Helper()
	deadline := time.Now().Add(timeout)
	stats := proxy.Stats()
	for {
		stats = proxy.Stats()
		if accounted(stats.ToServer) && accounted(stats.ToClient) {
			return stats
		}
		if time.Now().After(deadline) {
			t.Fatalf("proxy accounting did not settle within %s: %s", timeout, stats)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// accounted reports that a direction has no packet in flight inside the proxy.
func accounted(link quicnettest.LinkStats) bool {
	// Oversize datagrams are rejected before Received is incremented, so
	// they are intentionally outside the accepted-packet identity.
	drops := link.LossDrops + link.BlackoutDrops + link.OverflowDrops
	return link.Received+link.Duplicates == link.Sent+drops
}

// assertBoundedCleanup closes every resource with a deadline and requires the
// goroutine count to return near its pre-test baseline.
func assertBoundedCleanup(t *testing.T, proxy *quicnettest.Proxy, listener *quicadapter.Listener, client, server interface{ Close() error }, baseline int) {
	t.Helper()
	closed := make(chan error, 1)
	go func() {
		closed <- errors.Join(client.Close(), server.Close(), listener.Close(), proxy.Close())
	}()
	select {
	case err := <-closed:
		if err != nil {
			t.Fatalf("cleanup: %v", err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("cleanup did not complete within 15s")
	}

	deadline := time.Now().Add(5 * time.Second)
	const slack = 8
	for {
		current := runtime.NumGoroutine()
		if current <= baseline+slack {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("goroutines did not drain: baseline %d, still %d", baseline, current)
		}
		time.Sleep(25 * time.Millisecond)
	}
}
