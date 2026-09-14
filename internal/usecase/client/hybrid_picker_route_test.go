package client_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/protocol/wire"
	"github.com/bnema/vev/internal/usecase/client"
)

// This file drives the hybrid route-switch journeys through the client-owned
// picker: a client attached to a remote serving daemon opens the picker from
// the daemon's offer, commits a row, and then follows whichever handoff the
// serving daemon authorises. It replaces the deleted parked-route suites,
// whose journeys are now the daemon's decision plus the client's same-peer
// confirmation or endpoint dial.

// hybridPickerTransport is a live wire.Transport the test drives frame by
// frame. Recv yields queued envelopes in order and parks when the queue is
// empty, so the journey can react to what the client actually sent.
type hybridPickerTransport struct {
	mu        sync.Mutex
	queue     []wire.Envelope
	receiving bool
	sends     []wire.Envelope
	signal    chan struct{}
	done      chan struct{}
	closeOnce sync.Once
	closed    atomic.Int32
}

func newHybridPickerTransport() *hybridPickerTransport {
	return &hybridPickerTransport{signal: make(chan struct{}, 1), done: make(chan struct{})}
}

func (t *hybridPickerTransport) Send(frame wire.Envelope) error {
	t.mu.Lock()
	t.sends = append(t.sends, frame)
	t.mu.Unlock()
	return nil
}

func (t *hybridPickerTransport) Recv() (wire.Envelope, error) {
	for {
		t.mu.Lock()
		if len(t.queue) > 0 {
			t.receiving = false
			frame := t.queue[0]
			t.queue = t.queue[1:]
			t.mu.Unlock()
			return frame, nil
		}
		t.receiving = true
		t.mu.Unlock()
		select {
		case <-t.signal:
		case <-t.done:
			return wire.Envelope{}, io.EOF
		}
	}
}

func (t *hybridPickerTransport) Close() error {
	t.closed.Add(1)
	t.closeOnce.Do(func() { close(t.done) })
	return nil
}

func (t *hybridPickerTransport) push(frame wire.Envelope) {
	t.mu.Lock()
	t.queue = append(t.queue, frame)
	t.receiving = false
	t.mu.Unlock()
	select {
	case t.signal <- struct{}{}:
	default:
	}
}

// awaitReceived waits until the reader has delivered every queued frame to its
// inbox and entered the next Recv. Merely pushing invalid dormant output does
// not establish that it arrived before the activation boundary.
func (t *hybridPickerTransport) awaitReceived(tb *testing.T) {
	tb.Helper()
	require.Eventually(tb, func() bool {
		t.mu.Lock()
		defer t.mu.Unlock()
		return len(t.queue) == 0 && t.receiving
	}, time.Second, time.Millisecond)
}

func (t *hybridPickerTransport) sent() []wire.Envelope {
	t.mu.Lock()
	defer t.mu.Unlock()
	return append([]wire.Envelope(nil), t.sends...)
}

// awaitSend waits for one client message of the requested kind.
func (t *hybridPickerTransport) awaitSend(tb *testing.T, name string) (protocol.ClientMessage, bool) {
	tb.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		for _, frame := range t.sent() {
			if clientMessageName(tb, frame) == name {
				return decodeClientMessageForTest(tb, frame), true
			}
		}
		time.Sleep(time.Millisecond)
	}
	return nil, false
}

func (t *hybridPickerTransport) sentType(tb *testing.T, name string) bool {
	tb.Helper()
	for _, frame := range t.sent() {
		if clientMessageName(tb, frame) == name {
			return true
		}
	}
	return false
}

// hybridPickerReader yields one carriage return only when the test releases
// it, so a keypress can never reach the session before the picker owns input.
type hybridPickerReader struct {
	mu        sync.Mutex
	delivered bool
	press     chan struct{}
	done      chan struct{}
	once      sync.Once
	closeOnce sync.Once
}

func newHybridPickerReader() *hybridPickerReader {
	return &hybridPickerReader{press: make(chan struct{}), done: make(chan struct{})}
}

func (r *hybridPickerReader) pressEnter() { r.once.Do(func() { close(r.press) }) }
func (r *hybridPickerReader) unblock()    { r.closeOnce.Do(func() { close(r.done) }) }

func (r *hybridPickerReader) Read(p []byte) (int, error) {
	select {
	case <-r.press:
		r.mu.Lock()
		deliver := !r.delivered
		r.delivered = true
		r.mu.Unlock()
		if deliver {
			return copy(p, "\r"), nil
		}
	case <-r.done:
		return 0, io.EOF
	}
	<-r.done
	return 0, io.EOF
}

// hybridPickerTerminal is the headless terminal the runner drives: the picker
// frame is written to its buffer and one scripted keypress is its input.
type hybridPickerTerminal struct {
	reader *hybridPickerReader
	mu     sync.Mutex
	out    bytes.Buffer
	resize chan domain.Geometry
}

func newHybridPickerTerminal() *hybridPickerTerminal {
	return &hybridPickerTerminal{reader: newHybridPickerReader(), resize: make(chan domain.Geometry)}
}

func (*hybridPickerTerminal) EnterRaw() (func() error, error) {
	return func() error { return nil }, nil
}
func (*hybridPickerTerminal) Geometry() (domain.Geometry, error) {
	return domain.Geometry{Size: domain.Size{Cols: 80, Rows: 24}}, nil
}
func (t *hybridPickerTerminal) ResizeEvents() <-chan domain.Geometry { return t.resize }
func (t *hybridPickerTerminal) In() io.Reader                        { return t.reader }
func (t *hybridPickerTerminal) Out() io.Writer                       { return hybridPickerWriter{t} }
func (*hybridPickerTerminal) Flush() error                           { return nil }

func (t *hybridPickerTerminal) screen() string {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.out.String()
}

type hybridPickerWriter struct{ t *hybridPickerTerminal }

func (w hybridPickerWriter) Write(p []byte) (int, error) {
	w.t.mu.Lock()
	defer w.t.mu.Unlock()
	return w.t.out.Write(p)
}

// awaitDisplay waits until the composed picker frame is on the terminal: the
// row labels are rendered verbatim.
func (term *hybridPickerTerminal) awaitDisplay(t *testing.T, label string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if bytes.Contains([]byte(term.screen()), []byte(label)) {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("the picker frame never showed %q; terminal output was %q", label, term.screen())
}

func hybridPickerWelcome(name string, lifecycle domain.SessionLifecycleID) wire.Envelope {
	return frameOfMessage(protocol.Welcome{
		SessionID: name + "-id", SessionName: name, ResumeToken: 1,
		Capabilities: protocol.CapabilityResume,
		CommittedIdentity: &protocol.CommittedRouteIdentity{
			Target: protocol.ExactSessionTarget{LifecycleID: lifecycle, SessionName: name},
		},
	})
}

// hybridPickerPaint pushes one authoritative full paint, which is both the
// output boundary the picker barrier names and the release paint.
func hybridPickerPaint(t *testing.T, transport *hybridPickerTransport, epoch uint64, target protocol.ExactSessionTarget, text string) {
	t.Helper()
	view := protocol.ViewContext{Publication: epoch, Route: protocol.CommittedRouteIdentity{Target: target}, TabID: "t_abc123", FocusedPaneID: "p_def456"}
	transport.push(mustServerEnvelope(protocol.Output{
		Epoch: epoch, New: 1, Full: true, Size: domain.Size{Cols: 80, Rows: 24}, Context: &view,
		Data: []byte("\x1b[2J\x1b[H" + text),
	}))
}

const hybridPickerInteraction = uint64(7)

func hybridPickerOffer() protocol.PickerOffer {
	return protocol.PickerOffer{
		InteractionID: hybridPickerInteraction, Intent: protocol.PickerIntentNavigation,
		Title: " Sessions · recent ", BarrierEpoch: 1, BarrierState: 1, SizeEpoch: 1,
	}
}

func hybridPickerSnapshot() protocol.PickerSnapshot {
	return protocol.PickerSnapshot{
		InteractionID: hybridPickerInteraction, SourceID: "serving", SourceRevision: 1, Status: protocol.PickerSourceOK,
		Lines: []protocol.PickerLine{
			{Key: "aa/first", Kind: protocol.PickerLineSession, Label: "first", Focusable: true, Actions: protocol.PickerCanNavigate},
			{Key: "bb/second", Kind: protocol.PickerLineSession, Label: "second", Focusable: true, Actions: protocol.PickerCanNavigate},
		},
		Cursor: protocol.PickerCursor{Key: "aa/first", Index: 0},
	}
}

// openHybridPicker scripts the serving daemon's offer and first snapshot after
// the barrier paint, so the client takes presentation over.
func openHybridPicker(t *testing.T, transport *hybridPickerTransport, serving protocol.ExactSessionTarget) {
	t.Helper()
	hybridPickerPaint(t, transport, 1, serving, "ready")
	transport.push(frameOfMessage(hybridPickerOffer()))
	transport.push(frameOfMessage(hybridPickerSnapshot()))
}

func hybridPickerTargets() (domain.RemoteSessionTarget, protocol.ExactSessionTarget) {
	source := domain.RemoteSessionTarget{
		Endpoint: "remote", DisplayOrigin: "remote", LifecycleID: domain.SessionLifecycleID{2},
		SessionName: "source", LiveTabID: "source-tab",
	}
	target := protocol.ExactSessionTarget{LifecycleID: domain.SessionLifecycleID{3}, SessionName: "target"}
	return source, target
}

// TestHybridPickerSameHostSwitchKeepsTheServingTransport pins the hybrid
// same-host journey through the client picker: the client commits a row to a
// remote serving daemon, the daemon retires the interaction and offers the
// same-host target, and the client confirms it on the authenticated
// connection instead of dialing a replacement.
func TestHybridPickerSameHostSwitchKeepsTheServingTransport(t *testing.T) {
	term := newHybridPickerTerminal()
	defer term.reader.unblock()

	source, target := hybridPickerTargets()
	local := newHybridPickerTransport()
	remote := newHybridPickerTransport()
	handoffEndpoints := make(chan string, 4)

	// The bootstrap attaches locally, then hands the client to the remote
	// endpoint the serving daemon names.
	local.push(hybridPickerWelcome("local", domain.SessionLifecycleID{1}))
	local.push(frameOfMessage(protocol.AttachTarget{
		Endpoint: "remote", Session: "source", Intent: protocol.IntentAttach, RemoteTarget: &source,
		EnvironmentPolicy: protocol.EnvironmentPolicyDaemonOwned,
	}))

	remoteDialer := &sequenceDialer{trs: []wire.Transport{remote}}
	localDialer := &sequenceDialer{trs: []wire.Transport{local}}
	deps := testDependencies(localDialer, term, realClock{}, nil, nil)
	deps.HostRegistry = stubHostRegistry{resolve: func(endpoint string) (ports.RemoteEndpointBinding, error) {
		handoffEndpoints <- endpoint
		return ports.RemoteEndpointBinding{Dialer: remoteDialer}, nil
	}}

	done := make(chan error, 1)
	go func() {
		done <- runTestClient(context.Background(), deps, client.AttachRequest{
			Intent: protocol.IntentAttach, SessionName: "local", Origin: protocol.RouteOriginLocal, OriginKey: "local",
		})
	}()

	remote.push(hybridPickerWelcome("source", source.LifecycleID))
	openHybridPicker(t, remote, protocol.ExactSessionTarget{LifecycleID: source.LifecycleID, SessionName: "source"})
	term.awaitDisplay(t, "second")
	require.Contains(t, term.screen(), "\x1b[?25l", "the client-owned picker must hide the hardware cursor")

	// The commit crosses as a typed selection; the daemon then retires the
	// interaction and offers the same-host target on the same connection.
	term.reader.pressEnter()
	selection := mustAwaitSend(t, remote, "PickerSelection").(protocol.PickerSelection)
	require.Equal(t, hybridPickerInteraction, selection.InteractionID)

	remote.push(frameOfMessage(protocol.PickerClosed{
		InteractionID: hybridPickerInteraction, BarrierEpoch: 1, BarrierState: 1,
	}))
	remote.push(frameOfMessage(protocol.AttachTarget{
		Session: target.SessionName, Intent: protocol.IntentAttach, ExactTarget: &target,
		EnvironmentPolicy: protocol.EnvironmentPolicyDaemonOwned, SamePeer: true, CauseActionID: selection.CauseActionID,
	}))

	switchRequest := mustAwaitSend(t, remote, "SamePeerSwitchRequest").(protocol.SamePeerSwitchRequest)
	require.Equal(t, target, switchRequest.Target)

	remote.push(frameOfMessage(protocol.CommittedRouteIdentity{Target: target}))
	hybridPickerPaint(t, remote, 2, target, "target frame")

	// The released destination paint is displayed while the client is still on
	// the authenticated serving connection: nothing dialed a replacement and
	// nothing closed the source to get here.
	term.awaitDisplay(t, "target frame")
	require.Equal(t, int32(1), remoteDialer.calls.Load(), "a same-host switch must keep the authenticated serving transport")
	require.Zero(t, remote.closed.Load(), "a same-host switch must not close the serving transport")

	remote.push(frameOfMessage(protocol.Detached{Reason: protocol.ReasonDetach}))

	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("client did not finish the same-host picker switch")
	}
	require.Equal(t, []string{"remote"}, drainStrings(handoffEndpoints))
	require.False(t, local.sentType(t, "SamePeerSwitchRequest"), "the switch belongs to the serving connection")
}

// TestHybridPickerDifferentHostDialsTheTargetEndpoint pins the cross-host
// journey through the client picker: the daemon retires the interaction and
// hands the client to another endpoint, which the runner dials through the
// composed handoff while the source attachment closes.
func TestHybridPickerDifferentHostDialsTheTargetEndpoint(t *testing.T) {
	term := newHybridPickerTerminal()
	defer term.reader.unblock()

	source, _ := hybridPickerTargets()
	targetHost := domain.RemoteSessionTarget{
		Endpoint: "target-host", DisplayOrigin: "target-host", LifecycleID: domain.SessionLifecycleID{4},
		SessionName: "target", LiveTabID: "target-tab",
	}
	local := newHybridPickerTransport()
	remote := newHybridPickerTransport()
	targetTransport := newHybridPickerTransport()
	handoffEndpoints := make(chan string, 4)

	local.push(hybridPickerWelcome("local", domain.SessionLifecycleID{1}))
	local.push(frameOfMessage(protocol.AttachTarget{
		Endpoint: "remote", Session: "source", Intent: protocol.IntentAttach, RemoteTarget: &source,
		EnvironmentPolicy: protocol.EnvironmentPolicyDaemonOwned,
	}))

	remoteDialer := &sequenceDialer{trs: []wire.Transport{remote}}
	targetDialer := &sequenceDialer{trs: []wire.Transport{targetTransport}}
	localDialer := &sequenceDialer{trs: []wire.Transport{local}}
	deps := testDependencies(localDialer, term, realClock{}, nil, nil)
	deps.HostRegistry = stubHostRegistry{resolve: func(endpoint string) (ports.RemoteEndpointBinding, error) {
		handoffEndpoints <- endpoint
		if endpoint == targetHost.Endpoint {
			return ports.RemoteEndpointBinding{Dialer: targetDialer}, nil
		}
		return ports.RemoteEndpointBinding{Dialer: remoteDialer}, nil
	}}

	done := make(chan error, 1)
	go func() {
		done <- runTestClient(context.Background(), deps, client.AttachRequest{
			Intent: protocol.IntentAttach, SessionName: "local", Origin: protocol.RouteOriginLocal, OriginKey: "local",
		})
	}()

	remote.push(hybridPickerWelcome("source", source.LifecycleID))
	openHybridPicker(t, remote, protocol.ExactSessionTarget{LifecycleID: source.LifecycleID, SessionName: "source"})
	term.awaitDisplay(t, "second")

	term.reader.pressEnter()
	selection := mustAwaitSend(t, remote, "PickerSelection").(protocol.PickerSelection)

	remote.push(frameOfMessage(protocol.PickerClosed{
		InteractionID: hybridPickerInteraction, BarrierEpoch: 1, BarrierState: 1,
	}))
	remote.push(frameOfMessage(protocol.AttachTarget{
		Endpoint: targetHost.Endpoint, Session: targetHost.SessionName, Intent: protocol.IntentAttach,
		RemoteTarget: &targetHost, EnvironmentPolicy: protocol.EnvironmentPolicyDaemonOwned,
		CauseActionID: selection.CauseActionID,
	}))

	suspend := mustAwaitSend(t, remote, "SuspendAttachment").(protocol.SuspendAttachment)
	remote.push(frameOfMessage(protocol.AttachmentSuspended{RequestID: suspend.RequestID, Target: protocol.ExactSessionTarget{LifecycleID: source.LifecycleID, SessionName: source.SessionName}}))

	targetTransport.push(hybridPickerWelcome("target", targetHost.LifecycleID))
	targetTransport.push(frameOfMessage(protocol.Detached{Reason: protocol.ReasonDetach}))

	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("client did not finish the cross-host picker handoff")
	}
	require.Equal(t, []string{"remote", "target-host"}, drainStrings(handoffEndpoints))
	require.Equal(t, int32(1), targetDialer.calls.Load(), "the cross-host target must be dialed once")
	require.Positive(t, remote.closed.Load(), "the source attachment must close after the cross-host handoff")
	require.False(t, remote.sentType(t, "SamePeerSwitchRequest"), "a cross-host target is never confirmed on the source connection")
	hello := mustAwaitSend(t, targetTransport, "Hello").(protocol.Hello)
	require.Equal(t, "target", hello.Name)
	require.NotNil(t, hello.RemoteTarget)
	require.Equal(t, targetHost, *hello.RemoteTarget)
}

func drainStrings(ch chan string) []string {
	var out []string
	for {
		select {
		case value := <-ch:
			out = append(out, value)
		default:
			return out
		}
	}
}

// TestHybridPickerKeepsOneEndpointBindingAcrossServingPeers pins the registry
// invariant the client now owns: a picker route that moves to another serving
// peer resolves that endpoint once, and returning to the first peer reuses the
// binding it already had instead of rebuilding the carriage.
func TestHybridPickerKeepsOneEndpointBindingAcrossServingPeers(t *testing.T) {
	term := newHybridPickerTerminal()
	defer term.reader.unblock()

	source, _ := hybridPickerTargets()
	otherHost := domain.RemoteSessionTarget{
		Endpoint: "target-host", DisplayOrigin: "target-host", LifecycleID: domain.SessionLifecycleID{4},
		SessionName: "other", LiveTabID: "other-tab",
	}
	local := newHybridPickerTransport()
	remote := newHybridPickerTransport()
	other := newHybridPickerTransport()
	resolutions := make(chan string, 8)

	local.push(hybridPickerWelcome("local", domain.SessionLifecycleID{1}))
	local.push(frameOfMessage(protocol.AttachTarget{
		Endpoint: "remote", Session: "source", Intent: protocol.IntentAttach, RemoteTarget: &source,
		EnvironmentPolicy: protocol.EnvironmentPolicyDaemonOwned,
	}))

	remoteDialer := &sequenceDialer{trs: []wire.Transport{remote}}
	otherDialer := &sequenceDialer{trs: []wire.Transport{other}}
	localDialer := &sequenceDialer{trs: []wire.Transport{local}}
	dialers := map[string]ports.ClientDialer{"remote": remoteDialer, "target-host": otherDialer}
	deps := testDependencies(localDialer, term, realClock{}, nil, nil)
	deps.HostRegistry = stubHostRegistry{resolve: func(endpoint string) (ports.RemoteEndpointBinding, error) {
		resolutions <- endpoint
		dialer := dialers[endpoint]
		if dialer == nil {
			return ports.RemoteEndpointBinding{}, errors.New("unknown endpoint")
		}
		return ports.RemoteEndpointBinding{Dialer: dialer}, nil
	}}

	done := make(chan error, 1)
	go func() {
		done <- runTestClient(context.Background(), deps, client.AttachRequest{
			Intent: protocol.IntentAttach, SessionName: "local", Origin: protocol.RouteOriginLocal, OriginKey: "local",
		})
	}()

	remote.push(hybridPickerWelcome("source", source.LifecycleID))
	openHybridPicker(t, remote, protocol.ExactSessionTarget{LifecycleID: source.LifecycleID, SessionName: "source"})
	term.awaitDisplay(t, "second")

	// The serving peer hands the client to another host.
	term.reader.pressEnter()
	selection := mustAwaitSend(t, remote, "PickerSelection").(protocol.PickerSelection)
	remote.push(frameOfMessage(protocol.PickerClosed{
		InteractionID: hybridPickerInteraction, BarrierEpoch: 1, BarrierState: 1,
	}))
	remote.push(frameOfMessage(protocol.AttachTarget{
		Endpoint: otherHost.Endpoint, Session: otherHost.SessionName, Intent: protocol.IntentAttach,
		RemoteTarget: &otherHost, EnvironmentPolicy: protocol.EnvironmentPolicyDaemonOwned,
		CauseActionID: selection.CauseActionID,
	}))
	suspend := mustAwaitSend(t, remote, "SuspendAttachment").(protocol.SuspendAttachment)
	remote.push(frameOfMessage(protocol.AttachmentSuspended{RequestID: suspend.RequestID, Target: protocol.ExactSessionTarget{LifecycleID: source.LifecycleID, SessionName: source.SessionName}}))
	other.push(hybridPickerWelcome("other", otherHost.LifecycleID))
	// The new peer offers the return route to the first host.
	other.push(frameOfMessage(protocol.AttachTarget{
		Endpoint: "remote", Session: source.SessionName, Intent: protocol.IntentAttach,
		RemoteTarget: &source, EnvironmentPolicy: protocol.EnvironmentPolicyDaemonOwned,
	}))
	otherSuspend := mustAwaitSend(t, other, "SuspendAttachment").(protocol.SuspendAttachment)
	other.push(frameOfMessage(protocol.AttachmentSuspended{RequestID: otherSuspend.RequestID, Target: protocol.ExactSessionTarget{LifecycleID: otherHost.LifecycleID, SessionName: otherHost.SessionName}}))
	activation := mustAwaitSend(t, remote, "ActivateAttachment").(protocol.ActivateAttachment)
	require.Equal(t, source.LifecycleID, activation.Target.LifecycleID)
	hybridPickerPaint(t, remote, 2, activation.Target, "warm return")
	remote.push(frameOfMessage(protocol.RoutePosition{Target: activation.Target, ActiveTabID: "tab-2"}))
	remote.push(frameOfMessage(protocol.AttachmentActivated{RequestID: activation.RequestID, Identity: protocol.CommittedRouteIdentity{Target: activation.Target}, Epoch: 2, State: 1, ViewPublication: 2}))
	term.awaitDisplay(t, "warm return")
	remote.push(frameOfMessage(protocol.Detached{Reason: protocol.ReasonDetach}))

	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("client did not finish the cross-peer picker route")
	}
	// Resolution remains registry-owned, but activation reuses the authenticated
	// attachment and emits no second Hello or dial.
	require.Equal(t, int32(1), remoteDialer.calls.Load())
	require.Equal(t, int32(1), otherDialer.calls.Load())
	require.Equal(t, []string{"remote", "target-host", "remote"}, drainStrings(resolutions))
}

// mustAwaitSend waits for one client message of the requested kind.
func mustAwaitSend(t *testing.T, transport *hybridPickerTransport, name string) protocol.ClientMessage {
	t.Helper()
	message, ok := transport.awaitSend(t, name)
	require.True(t, ok, "client message %s never crossed the wire", name)
	return message
}

// The route ledger chooses local/home; the cache only transfers transport
// ownership. The activation frame must be displayed before queued input resumes.
func TestWarmAttachmentRemoteLocalRemoteReusesDial(t *testing.T) {
	term := newHybridPickerTerminal()
	defer term.reader.unblock()
	local1, local2, remote := newHybridPickerTransport(), newHybridPickerTransport(), newHybridPickerTransport()
	localTarget := protocol.ExactSessionTarget{LifecycleID: domain.SessionLifecycleID{1}, SessionName: "local"}
	remoteTarget := protocol.ExactSessionTarget{LifecycleID: domain.SessionLifecycleID{2}, SessionName: "work"}
	local1.push(hybridPickerWelcome("local", localTarget.LifecycleID))
	local1.push(frameOfMessage(protocol.AttachTarget{Endpoint: "remote", Session: "work", Intent: protocol.IntentAttach, ExactTarget: &remoteTarget, EnvironmentPolicy: protocol.EnvironmentPolicyDaemonOwned}))
	localDialer := &sequenceDialer{trs: []wire.Transport{local1, local2}}
	remoteDialer := &sequenceDialer{trs: []wire.Transport{remote}}
	deps := testDependencies(localDialer, term, realClock{}, nil, nil)
	deps.HostRegistry = stubHostRegistry{resolve: func(string) (ports.RemoteEndpointBinding, error) {
		return ports.RemoteEndpointBinding{Dialer: remoteDialer}, nil
	}}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- runTestClient(ctx, deps, client.AttachRequest{Intent: protocol.IntentAttach, SessionName: "local", Origin: protocol.RouteOriginLocal, OriginKey: "local"})
	}()
	remote.push(hybridPickerWelcome("work", remoteTarget.LifecycleID))
	hybridPickerPaint(t, remote, 1, remoteTarget, "remote first")
	term.awaitDisplay(t, "remote first")
	// The committed cursor differs from the original (empty) attach hint.
	// Both the route ledger and retained cache request must remember it.
	remote.push(frameOfMessage(protocol.RoutePosition{Target: remoteTarget, ActiveTabID: "tab-2"}))
	remote.push(frameOfMessage(protocol.RouteNavigationAction{SnapshotGeneration: 2, Key: 1, Generation: 1}))
	suspension := mustAwaitSend(t, remote, "SuspendAttachment").(protocol.SuspendAttachment)
	remote.push(frameOfMessage(protocol.AttachmentSuspended{RequestID: suspension.RequestID, Target: remoteTarget}))
	local2.push(hybridPickerWelcome("local", localTarget.LifecycleID))
	hybridPickerPaint(t, local2, 1, localTarget, "local return")
	term.awaitDisplay(t, "local return")
	// Ordinary dormant output cannot overwrite the local foreground.
	hybridPickerPaint(t, remote, 1, remoteTarget, "forbidden dormant output")
	remote.awaitReceived(t)
	local2.push(frameOfMessage(protocol.RouteNavigationAction{SnapshotGeneration: 3, Key: 2, Generation: 2}))
	activation := mustAwaitSend(t, remote, "ActivateAttachment").(protocol.ActivateAttachment)
	hybridPickerPaint(t, remote, 2, remoteTarget, "reactivated full")
	remote.push(frameOfMessage(protocol.RoutePosition{Target: activation.Target, ActiveTabID: "tab-2"}))
	remote.push(frameOfMessage(protocol.AttachmentActivated{RequestID: activation.RequestID, Identity: protocol.CommittedRouteIdentity{Target: remoteTarget}, Epoch: 2, State: 1, ViewPublication: 2}))
	term.awaitDisplay(t, "reactivated full")
	require.NotContains(t, term.screen(), "forbidden dormant output")
	require.Equal(t, int32(1), remoteDialer.calls.Load())
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("runner failed to close cached transports")
	}
	require.Positive(t, remote.closed.Load())
}

func TestWarmAttachmentRepairsDisappearedTab(t *testing.T) {
	for _, scenario := range []string{"reconnect", "route return"} {
		t.Run(scenario, func(t *testing.T) {
			term := newHybridPickerTerminal()
			defer term.reader.unblock()
			local1, local2, remote := newHybridPickerTransport(), newHybridPickerTransport(), newHybridPickerTransport()
			local3, remote2 := newHybridPickerTransport(), newHybridPickerTransport()
			localTarget := protocol.ExactSessionTarget{LifecycleID: domain.SessionLifecycleID{1}, SessionName: "local"}
			remoteTarget := protocol.ExactSessionTarget{LifecycleID: domain.SessionLifecycleID{2}, SessionName: "work"}
			local1.push(hybridPickerWelcome("local", localTarget.LifecycleID))
			local1.push(frameOfMessage(protocol.AttachTarget{Endpoint: "remote", Session: "work", Intent: protocol.IntentAttach, ExactTarget: &remoteTarget, EnvironmentPolicy: protocol.EnvironmentPolicyDaemonOwned}))
			localDialer := &sequenceDialer{trs: []wire.Transport{local1, local2, local3}}
			remoteDialer := &sequenceDialer{trs: []wire.Transport{remote, remote2}}
			deps := testDependencies(localDialer, term, realClock{}, nil, nil)
			deps.HostRegistry = stubHostRegistry{resolve: func(string) (ports.RemoteEndpointBinding, error) {
				return ports.RemoteEndpointBinding{Dialer: remoteDialer}, nil
			}}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan error, 1)
			go func() {
				done <- runTestClient(ctx, deps, client.AttachRequest{Intent: protocol.IntentAttach, SessionName: "local", Origin: protocol.RouteOriginLocal, OriginKey: "local"})
			}()
			remote.push(hybridPickerWelcome("work", remoteTarget.LifecycleID))
			hybridPickerPaint(t, remote, 1, remoteTarget, "remote first")
			term.awaitDisplay(t, "remote first")
			// The committed cursor differs from the original (empty) attach hint.
			// Both the route ledger and retained cache request must remember it.
			remote.push(frameOfMessage(protocol.RoutePosition{Target: remoteTarget, ActiveTabID: "tab-2"}))
			remote.push(frameOfMessage(protocol.RouteNavigationAction{SnapshotGeneration: 2, Key: 1, Generation: 1}))
			suspension := mustAwaitSend(t, remote, "SuspendAttachment").(protocol.SuspendAttachment)
			remote.push(frameOfMessage(protocol.AttachmentSuspended{RequestID: suspension.RequestID, Target: remoteTarget}))
			local2.push(hybridPickerWelcome("local", localTarget.LifecycleID))
			hybridPickerPaint(t, local2, 1, localTarget, "local return")
			term.awaitDisplay(t, "local return")
			// Ordinary dormant output cannot overwrite the local foreground.
			hybridPickerPaint(t, remote, 1, remoteTarget, "forbidden dormant output")
			remote.awaitReceived(t)
			if scenario == "reconnect" {
				local2.push(frameOfMessage(protocol.AttachTarget{Endpoint: "remote", Session: "work", Intent: protocol.IntentAttach, ExactTarget: &remoteTarget, EnvironmentPolicy: protocol.EnvironmentPolicyDaemonOwned}))
			} else {
				local2.push(frameOfMessage(protocol.RouteNavigationAction{SnapshotGeneration: 3, Key: 2, Generation: 2}))
			}
			activation := mustAwaitSend(t, remote, "ActivateAttachment").(protocol.ActivateAttachment)
			hybridPickerPaint(t, remote, 2, remoteTarget, "reactivated full")
			remote.push(frameOfMessage(protocol.RoutePosition{Target: activation.Target, ActiveTabID: "tab-1"}))
			remote.push(frameOfMessage(protocol.AttachmentActivated{RequestID: activation.RequestID, Identity: protocol.CommittedRouteIdentity{Target: remoteTarget}, Epoch: 2, State: 1, ViewPublication: 2}))
			term.awaitDisplay(t, "reactivated full")
			require.NotContains(t, term.screen(), "forbidden dormant output")
			require.Equal(t, int32(1), remoteDialer.calls.Load())
			// While suspended tab-2 disappeared; the daemon repaired its retained
			// cursor to tab-1 in the activation publication above, with no later update.
			if scenario == "route return" {
				remote.push(frameOfMessage(protocol.RouteNavigationAction{SnapshotGeneration: 4, Key: 1, Generation: 3}))
				suspend := mustAwaitSend(t, remote, "SuspendAttachment").(protocol.SuspendAttachment)
				remote.push(frameOfMessage(protocol.AttachmentSuspended{RequestID: suspend.RequestID, Target: remoteTarget}))
				local3.push(hybridPickerWelcome("local", localTarget.LifecycleID))
				hybridPickerPaint(t, local3, 1, localTarget, "local third")
				term.awaitDisplay(t, "local third")
				// Evict the parked connection so the next Hello exposes the ledger cursor.
				remote.push(frameOfMessage(protocol.Detached{Reason: protocol.ReasonDetach}))
				require.Eventually(t, func() bool { return remote.closed.Load() > 0 }, time.Second, time.Millisecond)
				local3.push(frameOfMessage(protocol.RouteNavigationAction{SnapshotGeneration: 5, Key: 2, Generation: 4}))
			} else {
				// A foreground transport failure exposes the attempt request cursor.
				remote.closeOnce.Do(func() { close(remote.done) })
			}
			hello := mustAwaitSend(t, remote2, "Hello").(protocol.Hello)
			require.Equal(t, &remoteTarget, hello.ExactTarget)
			require.Equal(t, domain.TabStableID("tab-1"), hello.PreferredTabID)
			remote2.push(hybridPickerWelcome("work", remoteTarget.LifecycleID))
			hybridPickerPaint(t, remote2, 1, remoteTarget, "repaired return")
			term.awaitDisplay(t, "repaired return")
			cancel()
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				t.Fatal("runner failed to close cached transports")
			}
			require.Positive(t, remote.closed.Load())
		})
	}
}
