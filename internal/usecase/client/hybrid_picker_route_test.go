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
// frame. Recv yields queued frames in order and parks when the queue is
// empty, so the journey can react to what the client actually sent.
type hybridPickerTransport struct {
	mu        sync.Mutex
	queue     []wire.Frame
	sends     []wire.Frame
	signal    chan struct{}
	done      chan struct{}
	closeOnce sync.Once
	closed    atomic.Int32
}

func newHybridPickerTransport() *hybridPickerTransport {
	return &hybridPickerTransport{signal: make(chan struct{}, 1), done: make(chan struct{})}
}

func (t *hybridPickerTransport) Send(frame wire.Frame) error {
	t.mu.Lock()
	t.sends = append(t.sends, frame)
	t.mu.Unlock()
	return nil
}

func (t *hybridPickerTransport) Recv() (wire.Frame, error) {
	for {
		t.mu.Lock()
		if len(t.queue) > 0 {
			frame := t.queue[0]
			t.queue = t.queue[1:]
			t.mu.Unlock()
			return frame, nil
		}
		t.mu.Unlock()
		select {
		case <-t.signal:
		case <-t.done:
			return wire.Frame{}, io.EOF
		}
	}
}

func (t *hybridPickerTransport) Close() error {
	t.closed.Add(1)
	t.closeOnce.Do(func() { close(t.done) })
	return nil
}

func (t *hybridPickerTransport) push(frame wire.Frame) {
	t.mu.Lock()
	t.queue = append(t.queue, frame)
	t.mu.Unlock()
	select {
	case t.signal <- struct{}{}:
	default:
	}
}

func (t *hybridPickerTransport) sent() []wire.Frame {
	t.mu.Lock()
	defer t.mu.Unlock()
	return append([]wire.Frame(nil), t.sends...)
}

// awaitSend waits for one client frame of the requested type and returns its
// payload.
func (t *hybridPickerTransport) awaitSend(typ wire.MsgType) ([]byte, bool) {
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		for _, frame := range t.sent() {
			if frame.Type == typ {
				return frame.Payload, true
			}
		}
		time.Sleep(time.Millisecond)
	}
	return nil, false
}

func (t *hybridPickerTransport) sentType(typ wire.MsgType) bool {
	for _, frame := range t.sent() {
		if frame.Type == typ {
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

func hybridPickerWelcome(name string, lifecycle domain.SessionLifecycleID) wire.Frame {
	return frameOf(wire.MsgWelcome, wire.MarshalWelcome(protocol.Welcome{
		SessionID: name + "-id", SessionName: name, ResumeToken: 1,
		Capabilities: protocol.CapabilityResume,
		CommittedIdentity: &protocol.CommittedRouteIdentity{
			Target: protocol.ExactSessionTarget{LifecycleID: lifecycle, SessionName: name},
		},
	}))
}

// hybridPickerPaint pushes one authoritative full paint, which is both the
// output boundary the picker barrier names and the release paint.
func hybridPickerPaint(t *testing.T, transport *hybridPickerTransport, epoch uint64, target protocol.ExactSessionTarget, text string) {
	t.Helper()
	view := protocol.ViewContext{Publication: epoch, Route: protocol.CommittedRouteIdentity{Target: target}, TabID: "t_abc123", FocusedPaneID: "p_def456"}
	payload, err := wire.MarshalOutput(protocol.Output{
		Epoch: epoch, New: 1, Full: true, Size: domain.Size{Cols: 80, Rows: 24}, Context: &view,
		Data: []byte("\x1b[2J\x1b[H" + text),
	})
	require.NoError(t, err)
	transport.push(frameOf(wire.MsgOutput, payload))
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
	transport.push(frameOf(wire.MsgPickerOffer, wire.MarshalPickerOffer(hybridPickerOffer())))
	transport.push(frameOf(wire.MsgPickerSnapshot, wire.MarshalPickerSnapshot(hybridPickerSnapshot())))
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
	local.push(frameOf(wire.MsgAttachTarget, wire.MarshalAttachTarget(protocol.AttachTarget{
		Endpoint: "remote", Session: "source", Intent: protocol.IntentAttach, RemoteTarget: &source,
		EnvironmentPolicy: protocol.EnvironmentPolicyDaemonOwned,
	})))

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

	// The commit crosses as a typed selection; the daemon then retires the
	// interaction and offers the same-host target on the same connection.
	term.reader.pressEnter()
	selectionPayload, ok := remote.awaitSend(wire.MsgPickerSelection)
	require.True(t, ok, "the picker commit never crossed the wire")
	selection, err := wire.UnmarshalPickerSelection(selectionPayload)
	require.NoError(t, err)
	require.Equal(t, hybridPickerInteraction, selection.InteractionID)

	remote.push(frameOf(wire.MsgPickerClosedServer, wire.MarshalPickerClosed(protocol.PickerClosed{
		InteractionID: hybridPickerInteraction, BarrierEpoch: 1, BarrierState: 1,
	})))
	remote.push(frameOf(wire.MsgAttachTarget, wire.MarshalAttachTarget(protocol.AttachTarget{
		Session: target.SessionName, Intent: protocol.IntentAttach, ExactTarget: &target,
		EnvironmentPolicy: protocol.EnvironmentPolicyDaemonOwned, SamePeer: true, CauseActionID: selection.CauseActionID,
	})))

	switchPayload, ok := remote.awaitSend(wire.MsgSamePeerSwitchRequest)
	require.True(t, ok, "the same-host target must be confirmed on the serving connection")
	switchRequest, err := wire.UnmarshalSamePeerSwitchRequest(switchPayload)
	require.NoError(t, err)
	require.Equal(t, target, switchRequest.Target)

	remote.push(frameOf(wire.MsgCommittedRouteIdentity, mustMarshalCommittedIdentity(protocol.CommittedRouteIdentity{Target: target})))
	hybridPickerPaint(t, remote, 2, target, "target frame")

	// The released destination paint is displayed while the client is still on
	// the authenticated serving connection: nothing dialed a replacement and
	// nothing closed the source to get here.
	term.awaitDisplay(t, "target frame")
	require.Equal(t, int32(1), remoteDialer.calls.Load(), "a same-host switch must keep the authenticated serving transport")
	require.Zero(t, remote.closed.Load(), "a same-host switch must not close the serving transport")

	remote.push(frameOf(wire.MsgDetached, wire.MarshalDetached(protocol.Detached{Reason: protocol.ReasonDetach})))

	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("client did not finish the same-host picker switch")
	}
	require.Equal(t, []string{"remote"}, drainStrings(handoffEndpoints))
	require.False(t, local.sentType(wire.MsgSamePeerSwitchRequest), "the switch belongs to the serving connection")
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
	local.push(frameOf(wire.MsgAttachTarget, wire.MarshalAttachTarget(protocol.AttachTarget{
		Endpoint: "remote", Session: "source", Intent: protocol.IntentAttach, RemoteTarget: &source,
		EnvironmentPolicy: protocol.EnvironmentPolicyDaemonOwned,
	})))

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
	selectionPayload, ok := remote.awaitSend(wire.MsgPickerSelection)
	require.True(t, ok, "the picker commit never crossed the wire")
	selection, err := wire.UnmarshalPickerSelection(selectionPayload)
	require.NoError(t, err)

	remote.push(frameOf(wire.MsgPickerClosedServer, wire.MarshalPickerClosed(protocol.PickerClosed{
		InteractionID: hybridPickerInteraction, BarrierEpoch: 1, BarrierState: 1,
	})))
	remote.push(frameOf(wire.MsgAttachTarget, wire.MarshalAttachTarget(protocol.AttachTarget{
		Endpoint: targetHost.Endpoint, Session: targetHost.SessionName, Intent: protocol.IntentAttach,
		RemoteTarget: &targetHost, EnvironmentPolicy: protocol.EnvironmentPolicyDaemonOwned,
		CauseActionID: selection.CauseActionID,
	})))

	targetTransport.push(hybridPickerWelcome("target", targetHost.LifecycleID))
	targetTransport.push(frameOf(wire.MsgDetached, wire.MarshalDetached(protocol.Detached{Reason: protocol.ReasonDetach})))

	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("client did not finish the cross-host picker handoff")
	}
	require.Equal(t, []string{"remote", "target-host"}, drainStrings(handoffEndpoints))
	require.Equal(t, int32(1), targetDialer.calls.Load(), "the cross-host target must be dialed once")
	require.Positive(t, remote.closed.Load(), "the source attachment must close after the cross-host handoff")
	require.False(t, remote.sentType(wire.MsgSamePeerSwitchRequest), "a cross-host target is never confirmed on the source connection")
	helloPayload, ok := targetTransport.awaitSend(wire.MsgHello)
	require.True(t, ok, "the target attachment never sent a Hello")
	hello, err := wire.UnmarshalHello(helloPayload)
	require.NoError(t, err)
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
	remoteReturn := newHybridPickerTransport()
	other := newHybridPickerTransport()
	resolutions := make(chan string, 8)

	local.push(hybridPickerWelcome("local", domain.SessionLifecycleID{1}))
	local.push(frameOf(wire.MsgAttachTarget, wire.MarshalAttachTarget(protocol.AttachTarget{
		Endpoint: "remote", Session: "source", Intent: protocol.IntentAttach, RemoteTarget: &source,
		EnvironmentPolicy: protocol.EnvironmentPolicyDaemonOwned,
	})))

	remoteDialer := &sequenceDialer{trs: []wire.Transport{remote, remoteReturn}}
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
	selection, err := wire.UnmarshalPickerSelection(mustAwaitSend(t, remote, wire.MsgPickerSelection))
	require.NoError(t, err)
	remote.push(frameOf(wire.MsgPickerClosedServer, wire.MarshalPickerClosed(protocol.PickerClosed{
		InteractionID: hybridPickerInteraction, BarrierEpoch: 1, BarrierState: 1,
	})))
	remote.push(frameOf(wire.MsgAttachTarget, wire.MarshalAttachTarget(protocol.AttachTarget{
		Endpoint: otherHost.Endpoint, Session: otherHost.SessionName, Intent: protocol.IntentAttach,
		RemoteTarget: &otherHost, EnvironmentPolicy: protocol.EnvironmentPolicyDaemonOwned,
		CauseActionID: selection.CauseActionID,
	})))
	other.push(hybridPickerWelcome("other", otherHost.LifecycleID))
	// The new peer offers the return route to the first host.
	other.push(frameOf(wire.MsgAttachTarget, wire.MarshalAttachTarget(protocol.AttachTarget{
		Endpoint: "remote", Session: source.SessionName, Intent: protocol.IntentAttach,
		RemoteTarget: &source, EnvironmentPolicy: protocol.EnvironmentPolicyDaemonOwned,
	})))
	remoteReturn.push(hybridPickerWelcome("source", source.LifecycleID))
	remoteReturn.push(frameOf(wire.MsgDetached, wire.MarshalDetached(protocol.Detached{Reason: protocol.ReasonDetach})))

	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("client did not finish the cross-peer picker route")
	}
	// Returning to the first peer resolves that endpoint again and dials a fresh
	// connection through it: the registry owns binding reuse, and a route
	// crossing serving peers never invalidates the client's own resolution.
	require.Equal(t, int32(2), remoteDialer.calls.Load())
	require.Equal(t, int32(1), otherDialer.calls.Load())
	require.Equal(t, []string{"remote", "target-host", "remote"}, drainStrings(resolutions))
}

// mustAwaitSend waits for one client frame of the requested type.
func mustAwaitSend(t *testing.T, transport *hybridPickerTransport, typ wire.MsgType) []byte {
	t.Helper()
	payload, ok := transport.awaitSend(typ)
	require.True(t, ok, "frame %d never crossed the wire", typ)
	return payload
}
