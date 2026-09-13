package quicnettest_test

// This file proves the application-level QUIC reconnect path end to end with
// no fake carriage anywhere:
//
//   - the real thin-client use case (internal/usecase/client.Run) drives the
//     attach lifecycle, including its reconnect loop and resume-token handling;
//   - the real sessionwire typed adapter wraps both ends;
//   - two successive, genuinely separate QUIC connections run over the real
//     internal/adapters/quic transport through the real UDP proxy;
//   - the first connection is killed irreversibly (QUIC has no resumption here,
//     0-RTT is disabled) by closing the accepted server transport;
//   - the resumed connection must carry a second preamble+handshake, a Hello
//     with IntentResume, the same ClientID and the previous resume token, a
//     Welcome with a renewed token, and a fresh full Output that the client
//     accepts and ACKs.
//
// The daemon side is a scripted typed responder: it speaks only the real
// sessionwire protocol over the real QUIC carriage. No sshd, no netem, no root,
// and no fake QUIC transport is involved.

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bnema/vev/internal/adapters/clock"
	quicadapter "github.com/bnema/vev/internal/adapters/quic"
	"github.com/bnema/vev/internal/adapters/quicnettest"
	"github.com/bnema/vev/internal/adapters/sessionwire"
	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/usecase/client"
)

const (
	reconnectProofTimeout = 30 * time.Second
	reconnectSessionName  = "work"
)

func TestRealQUICApplicationReconnectResumesTypedSession(t *testing.T) {
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
	proxy, err := quicnettest.New(quicnettest.Config{
		ServerAddr: serverAddr,
		Seed:       20260915,
	})
	if err != nil {
		t.Fatalf("new proxy: %v", err)
	}
	t.Cleanup(func() { _ = proxy.Close() })

	// The dialer is the production composition: a real QUIC dialer with the
	// pinned certificate fingerprint, wrapped by the typed sessionwire dialer.
	// The counter is observational only; every dial still opens a real QUIC
	// connection through the proxy.
	rawDialer := quicadapter.DialConfig(proxy.Addr().String(), "vev-reconnect", fingerprint, quicadapter.Config{}, 15*time.Second)
	dialer := &reconnectCountingDialer{inner: sessionwire.NewClientDialer(rawDialer)}

	term := newReconnectProofTerminal()
	t.Cleanup(term.unblock)

	deps := client.Dependencies{
		Dialer:                 dialer,
		Terminal:               term,
		Clock:                  clock.New(),
		DisableCapabilityProbe: true,
		Logger:                 slog.New(slog.DiscardHandler),
	}

	obs := &reconnectProofObservations{}
	var obsMu sync.Mutex
	daemonDone := make(chan error, 1)
	go func() { daemonDone <- serveReconnectProof(listener, obs, &obsMu) }()

	ctx, cancel := context.WithTimeout(context.Background(), reconnectProofTimeout)
	defer cancel()
	runResult := make(chan error, 1)
	go func() {
		runResult <- client.NewRunner(deps).Run(ctx, client.AttachRequest{
			Intent:      protocol.IntentAttach,
			SessionName: reconnectSessionName,
		})
	}()

	select {
	case err := <-daemonDone:
		if err != nil {
			t.Fatalf("scripted resume daemon: %v", err)
		}
	case <-time.After(reconnectProofTimeout):
		t.Fatal("timed out waiting for the resumed session to complete")
	}
	select {
	case err := <-runResult:
		if err != nil {
			t.Fatalf("client run: %v", err)
		}
	case <-time.After(reconnectProofTimeout):
		t.Fatal("timed out waiting for the client run to return")
	}

	obsMu.Lock()
	got := *obs
	obsMu.Unlock()

	if got.accepts != 2 {
		t.Fatalf("listener accepted %d QUIC connections, want 2 (first must die irreversibly)", got.accepts)
	}
	if dialer.calls.Load() != 2 {
		t.Fatalf("client dialed %d times, want exactly 2 real QUIC connections", dialer.calls.Load())
	}

	// First connection: a fresh attach, no resume token.
	if got.firstHello.Intent != protocol.IntentAttach {
		t.Fatalf("first Hello.Intent = %d, want IntentAttach(%d)", got.firstHello.Intent, protocol.IntentAttach)
	}
	if got.firstHello.ResumeToken != 0 {
		t.Fatalf("first Hello.ResumeToken = %d, want 0", got.firstHello.ResumeToken)
	}
	if got.firstHello.Name != reconnectSessionName {
		t.Fatalf("first Hello.Name = %q, want %q", got.firstHello.Name, reconnectSessionName)
	}

	// Second connection: the client must resume, not start over.
	if got.secondHello.Intent != protocol.IntentResume {
		t.Fatalf("second Hello.Intent = %d, want IntentResume(%d)", got.secondHello.Intent, protocol.IntentResume)
	}
	if got.secondHello.ClientID != got.firstHello.ClientID {
		t.Fatalf("second Hello.ClientID = %x, want the first %x", got.secondHello.ClientID, got.firstHello.ClientID)
	}
	if got.secondHello.ResumeToken != got.firstWelcome.ResumeToken {
		t.Fatalf("second Hello.ResumeToken = %d, want the first Welcome token %d", got.secondHello.ResumeToken, got.firstWelcome.ResumeToken)
	}
	if got.secondHello.Name != reconnectSessionName {
		t.Fatalf("second Hello.Name = %q, want %q", got.secondHello.Name, reconnectSessionName)
	}
	if got.secondWelcome.ResumeToken == got.firstWelcome.ResumeToken || got.secondWelcome.ResumeToken == 0 {
		t.Fatalf("second Welcome.ResumeToken = %d, want a renewal distinct from %d", got.secondWelcome.ResumeToken, got.firstWelcome.ResumeToken)
	}

	// Fresh, resynchronised first Output: the resumed attachment starts from a
	// full publication, the client accepts it (no reset request), and ACKs it.
	if got.secondOutputResetRequested {
		t.Fatal("client requested an output reset instead of accepting the fresh resumed Output")
	}
	if got.secondAck.Epoch != got.secondOutputEpoch || got.secondAck.State != got.secondOutputState {
		t.Fatalf("second Ack = {%d,%d}, want {%d,%d}", got.secondAck.Epoch, got.secondAck.State, got.secondOutputEpoch, got.secondOutputState)
	}
	if !bytes.Contains([]byte(term.output()), []byte(reconnectResumedFrame)) {
		t.Fatalf("client terminal never received the resumed fresh Output %q", reconnectResumedFrame)
	}

	stats := waitForAccounting(t, proxy, 10*time.Second)
	if stats.ToServer.Received == 0 || stats.ToClient.Received == 0 {
		t.Fatalf("proxy carried no QUIC traffic: %s", stats)
	}
	if stats.ToServer.OverflowDrops != 0 || stats.ToClient.OverflowDrops != 0 {
		t.Fatalf("bounded proxy queue overflowed: %s", stats)
	}
	if stats.ToServer.DeliveryErrors != 0 || stats.ToClient.DeliveryErrors != 0 {
		t.Fatalf("proxy delivery failed: %s", stats)
	}
	if stats.StrayPackets != 0 {
		t.Fatalf("proxy observed %d stray packets, want 0", stats.StrayPackets)
	}
}

const (
	reconnectInitialToken = uint64(0x1111)
	reconnectRenewedToken = uint64(0x2222)
	reconnectFirstFrame   = "first frame"
	reconnectResumedFrame = "resumed frame"
)

// reconnectProofObservations is the daemon-side record, written by the daemon
// goroutine and read by the test goroutine after daemonDone closes.
type reconnectProofObservations struct {
	accepts int

	firstHello   protocol.Hello
	firstWelcome protocol.Welcome
	firstAck     protocol.Ack

	secondHello   protocol.Hello
	secondWelcome protocol.Welcome
	secondAck     protocol.Ack
	// secondOutputEpoch/State are the boundary of the fresh resumed Output the
	// daemon sent; the Ack must match them.
	secondOutputEpoch          uint64
	secondOutputState          uint64
	secondOutputResetRequested bool
}

// serveReconnectProof drives two successive real QUIC connections as a typed
// sessionwire server: a first attach that publishes output and is then killed
// irreversibly, and a second resume that must carry the old token.
func serveReconnectProof(listener *quicadapter.Listener, obs *reconnectProofObservations, mu *sync.Mutex) error {
	// --- First connection: fresh attach, then irreversible death. ---
	firstRaw, err := listener.Accept()
	if err != nil {
		return fmt.Errorf("accept first QUIC connection: %w", err)
	}
	first := sessionwire.NewServerConnection(firstRaw)
	firstHello, err := receiveProofHello(first)
	if err != nil {
		_ = first.Close()
		return fmt.Errorf("first connection: %w", err)
	}
	mu.Lock()
	obs.accepts++
	obs.firstHello = firstHello
	mu.Unlock()

	firstWelcome := protocol.Welcome{
		SessionID:    "work-session",
		SessionName:  reconnectSessionName,
		ResumeToken:  reconnectInitialToken,
		Capabilities: protocol.CapabilityResume,
	}
	if err := first.SendServer(firstWelcome); err != nil {
		_ = first.Close()
		return fmt.Errorf("first connection: send welcome: %w", err)
	}
	if err := first.SendOutput(reconnectProofOutput(1, []byte(reconnectFirstFrame))); err != nil {
		_ = first.Close()
		return fmt.Errorf("first connection: send output: %w", err)
	}
	firstAck, requestedReset, err := awaitProofAck(first)
	if err != nil {
		_ = first.Close()
		return fmt.Errorf("first connection: %w", err)
	}
	mu.Lock()
	obs.firstWelcome = firstWelcome
	obs.firstAck = firstAck
	mu.Unlock()
	if requestedReset {
		_ = first.Close()
		return errors.New("first connection: client requested output reset for a fresh publication")
	}
	// Irreversible death: QUIC resumption/0-RTT is disabled, so closing the
	// accepted connection can never be undone by a later packet.
	if err := first.Close(); err != nil {
		return fmt.Errorf("first connection: irreversible close: %w", err)
	}

	// --- Second connection: resume with the previous token. ---
	secondRaw, err := listener.Accept()
	if err != nil {
		return fmt.Errorf("accept second QUIC connection: %w", err)
	}
	second := sessionwire.NewServerConnection(secondRaw)
	secondHello, err := receiveProofHello(second)
	if err != nil {
		_ = second.Close()
		return fmt.Errorf("second connection: %w", err)
	}

	secondWelcome := protocol.Welcome{
		SessionID:    "work-session",
		SessionName:  reconnectSessionName,
		ResumeToken:  reconnectRenewedToken,
		Capabilities: protocol.CapabilityResume,
	}
	if err := second.SendServer(secondWelcome); err != nil {
		_ = second.Close()
		return fmt.Errorf("second connection: send welcome: %w", err)
	}
	resumed := reconnectProofOutput(7, []byte(reconnectResumedFrame))
	if err := second.SendOutput(resumed); err != nil {
		_ = second.Close()
		return fmt.Errorf("second connection: send resumed output: %w", err)
	}
	secondAck, requestedReset, err := awaitProofAck(second)
	if err != nil {
		_ = second.Close()
		return fmt.Errorf("second connection: %w", err)
	}

	mu.Lock()
	obs.accepts++
	obs.secondHello = secondHello
	obs.secondWelcome = secondWelcome
	obs.secondAck = secondAck
	obs.secondOutputEpoch = resumed.Epoch
	obs.secondOutputState = resumed.New
	obs.secondOutputResetRequested = requestedReset
	mu.Unlock()

	// End the run cleanly so the client use case returns nil. The connection
	// must stay open until the client has consumed the Detached and closed its
	// own side: closing first can drop the frame, which the client would read as
	// an unrecoverable link loss and answer with yet another resume dial.
	if err := second.SendServer(protocol.Detached{Reason: protocol.ReasonDetach}); err != nil {
		_ = second.Close()
		return fmt.Errorf("second connection: send detached: %w", err)
	}
	for {
		if _, err := second.ReceiveClient(); err != nil {
			break
		}
	}
	return second.Close()
}

func receiveProofHello(conn ports.ServerConnection) (protocol.Hello, error) {
	message, err := conn.ReceiveClient()
	if err != nil {
		return protocol.Hello{}, fmt.Errorf("receive hello: %w", err)
	}
	hello, ok := message.(protocol.Hello)
	if !ok {
		return protocol.Hello{}, fmt.Errorf("first message is %T, want protocol.Hello", message)
	}
	return hello, nil
}

// awaitProofAck reads client messages until the first Ack, reporting whether a
// reset request was seen first. The bounded skip count keeps a misbehaving peer
// from hiding the Ack behind an unbounded control stream.
func awaitProofAck(conn ports.ServerConnection) (ack protocol.Ack, resetRequested bool, err error) {
	for skipped := 0; skipped < 32; skipped++ {
		message, recvErr := conn.ReceiveClient()
		if recvErr != nil {
			return protocol.Ack{}, false, fmt.Errorf("receive ack: %w", recvErr)
		}
		switch typed := message.(type) {
		case protocol.Ack:
			return typed, false, nil
		case protocol.OutputResetRequest:
			return protocol.Ack{}, true, nil
		}
	}
	return protocol.Ack{}, false, errors.New("no ack within 32 client messages")
}

// reconnectProofOutput builds a valid fresh full publication for the scripted
// daemon. Its data is unique per connection so the client terminal proves which
// frame was applied.
func reconnectProofOutput(publication uint64, data []byte) protocol.Output {
	return protocol.Output{
		Epoch:        1,
		Base:         0,
		New:          1,
		ViewRevision: 1,
		Size:         domain.Size{Cols: 80, Rows: 24},
		Full:         true,
		Context: &protocol.ViewContext{
			Publication: publication,
			Route: protocol.CommittedRouteIdentity{Target: protocol.ExactSessionTarget{
				LifecycleID: domain.SessionLifecycleID{
					0x52, 0x45, 0x53, 0x55, 0x4d, 0x45, 0x00, 0x01,
					0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01,
				},
				SessionName: reconnectSessionName,
			}},
			TabID:         domain.TabStableID("tab-1"),
			FocusedPaneID: domain.PaneStableID("pane-1"),
		},
		Data: data,
	}
}

// reconnectCountingDialer counts real dials without replacing the carriage.
type reconnectCountingDialer struct {
	inner ports.ClientDialer
	calls atomic.Int64
}

func (d *reconnectCountingDialer) Dial(ctx context.Context) (ports.ClientConnection, error) {
	d.calls.Add(1)
	return d.inner.Dial(ctx)
}

// reconnectProofTerminal is a headless ports.Terminal. Its input never
// produces bytes, so the client's input pump stays parked for the run.
type reconnectProofTerminal struct {
	in     *reconnectProofInput
	mu     sync.Mutex
	out    bytes.Buffer
	resize chan domain.Geometry
}

func newReconnectProofTerminal() *reconnectProofTerminal {
	return &reconnectProofTerminal{
		in:     &reconnectProofInput{closed: make(chan struct{})},
		resize: make(chan domain.Geometry),
	}
}

func (t *reconnectProofTerminal) EnterRaw() (func() error, error) {
	return func() error { return nil }, nil
}

func (t *reconnectProofTerminal) Geometry() (domain.Geometry, error) {
	return domain.Geometry{Size: domain.Size{Cols: 80, Rows: 24}}, nil
}

func (t *reconnectProofTerminal) ResizeEvents() <-chan domain.Geometry { return t.resize }
func (t *reconnectProofTerminal) In() io.Reader                        { return t.in }
func (t *reconnectProofTerminal) Out() io.Writer {
	return reconnectProofWriter{mu: &t.mu, buf: &t.out}
}
func (t *reconnectProofTerminal) Flush() error { return nil }

func (t *reconnectProofTerminal) output() string {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.out.String()
}

func (t *reconnectProofTerminal) unblock() { t.in.unblock() }

type reconnectProofWriter struct {
	mu  *sync.Mutex
	buf *bytes.Buffer
}

func (w reconnectProofWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.buf.Write(p)
}

type reconnectProofInput struct {
	closed chan struct{}
	once   sync.Once
}

func (r *reconnectProofInput) Read([]byte) (int, error) {
	<-r.closed
	return 0, io.EOF
}

func (r *reconnectProofInput) unblock() {
	r.once.Do(func() { close(r.closed) })
}
