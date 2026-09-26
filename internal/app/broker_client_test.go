//go:build linux

package app

import (
	"context"
	"errors"
	"io"
	"os"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/adapters/brokeripc"
	"github.com/bnema/vev/internal/adapters/clock"
	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/protocol/catalogue"
	"github.com/bnema/vev/internal/usecase/client"
)

// Terminal composition tests (Plan 001 P7.4a).
//
// runAttach is the ordinary terminal entry point. These tests drive it through
// the composition-root seams only: connectProductionClientBroker supplies one
// scripted broker connection and terminalForAttach supplies a recording
// terminal, so the whole client supervisor runs without a production broker
// process, a daemon dialer, or a controlling terminal. Every assertion is about
// the composition contract: which broker stream the parsed CLI target resolves
// to, that a refused target opens nothing, and that an admitted attachment
// commits a real session publication to the terminal.

const (
	terminalCompositionEpoch      = ports.BrokerEpoch(41)
	terminalCompositionGeneration = domain.RemoteGeneration(3)
)

func terminalCompositionPolicy() ports.BrokerPolicy {
	return ports.BrokerPolicy{
		ProtocolVersion:      protocol.Version,
		CatalogSchemaVersion: catalogue.RemoteCatalogSchemaVersion,
		EnvironmentPolicy:    protocol.EnvironmentPolicyDaemonOwned,
		Transport:            "unix-mux",
		Trust:                "same-user",
		Launch:               "explicit",
		Isolation:            "per-user",
	}
}

func terminalCompositionRegistration(endpoint string) domain.RemoteRegistration {
	var incarnation [16]byte
	for i := range incarnation {
		incarnation[i] = byte(i + 1)
	}
	return domain.RemoteRegistration{Endpoint: endpoint, Incarnation: incarnation, Generation: terminalCompositionGeneration}
}

// terminalCompositionSnapshot publishes the local daemon plus the given remote
// endpoints, each carrying exactly the sessions named.
func terminalCompositionSnapshot(localSessions []string, remotes map[string][]string) ports.BrokerSnapshot {
	policy := terminalCompositionPolicy()
	observations := []ports.BrokerDaemonObservation{{
		Local:           true,
		DisplayOrigin:   "local",
		Policy:          policy,
		Identity:        "local-daemon",
		Incarnation:     ports.BrokerDaemonIncarnation{9},
		ProtocolVersion: protocol.Version,
		Availability:    domain.RemoteAvailabilityReachable,
		LastSuccess:     time.Now(),
		InventoryKnown:  true,
		Sessions:        terminalCompositionSessions(localSessions),
	}}
	endpoints := make([]string, 0, len(remotes))
	for endpoint := range remotes {
		endpoints = append(endpoints, endpoint)
	}
	sortStrings(endpoints)
	for _, endpoint := range endpoints {
		observations = append(observations, ports.BrokerDaemonObservation{
			Endpoint:        endpoint,
			DisplayOrigin:   endpoint,
			Policy:          policy,
			Registration:    terminalCompositionRegistration(endpoint),
			Identity:        ports.BrokerDaemonIdentity("identity-" + endpoint),
			Incarnation:     ports.BrokerDaemonIncarnation{7},
			ProtocolVersion: protocol.Version,
			Availability:    domain.RemoteAvailabilityReachable,
			LastSuccess:     time.Now(),
			InventoryKnown:  true,
			Sessions:        terminalCompositionSessions(remotes[endpoint]),
		})
	}
	return ports.BrokerSnapshot{Epoch: terminalCompositionEpoch, Revision: 1, Daemons: observations}
}

func terminalCompositionSessions(names []string) []catalogue.RemoteCatalogSession {
	sessions := make([]catalogue.RemoteCatalogSession, 0, len(names))
	for i, name := range names {
		var lifecycle domain.SessionLifecycleID
		lifecycle[0] = byte(i + 3)
		lifecycle[1] = 0x5a
		sessions = append(sessions, catalogue.RemoteCatalogSession{
			LifecycleID: lifecycle, Name: name, State: catalogue.RemoteCatalogSessionUp,
		})
	}
	return sessions
}

func sortStrings(values []string) {
	for i := 1; i < len(values); i++ {
		for j := i; j > 0 && values[j] < values[j-1]; j-- {
			values[j], values[j-1] = values[j-1], values[j]
		}
	}
}

// terminalCompositionService is the scripted broker service one terminal client
// adopts. It records every opened stream request, serves one committed
// publication, and answers each admitted attachment with a Welcome and one
// initial full session frame so the supervisor reaches its attached
// presentation.
type terminalCompositionService struct {
	mu       sync.Mutex
	snapshot ports.BrokerSnapshot
	// daemonSessions, when set, is what the local daemon owns regardless of
	// what the broker catalogue has observed.
	daemonSessions []string
	closed         bool
	next           ports.BrokerStreamID
	opened         []ports.BrokerOpenStreamRequest
	streams        []*terminalCompositionStream
	done           chan struct{}
	changed        chan struct{}
}

func newTerminalCompositionService(snapshot ports.BrokerSnapshot) *terminalCompositionService {
	return &terminalCompositionService{
		snapshot: snapshot,
		done:     make(chan struct{}),
		changed:  make(chan struct{}, 1),
	}
}

func (s *terminalCompositionService) ConnectionID() ports.BrokerConnectionID {
	return ports.BrokerConnectionID{4}
}

func (s *terminalCompositionService) NextStreamID() (ports.BrokerStreamID, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return 0, ports.BrokerAdmissionClosed
	}
	s.next++
	return s.next, nil
}

func (s *terminalCompositionService) Done() <-chan struct{} { return s.done }
func (s *terminalCompositionService) Err() error            { return nil }

func (s *terminalCompositionService) Snapshot() ports.BrokerSnapshot { return s.snapshot.Clone() }

func (s *terminalCompositionService) Subscribe() (ports.BrokerSubscription, error) {
	return terminalCompositionSubscription{changed: s.changed}, nil
}

func (s *terminalCompositionService) SubscribePreview(ports.BrokerPreviewRequest) (ports.BrokerPreviewSubscription, error) {
	return nil, errors.New("terminal composition service does not support preview")
}

func (s *terminalCompositionService) OpenStream(_ context.Context, request ports.BrokerOpenStreamRequest) (ports.BrokerLogicalConnection, error) {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil, ports.BrokerAdmissionClosed
	}
	s.opened = append(s.opened, request)
	sessions := terminalCompositionDaemonSessions(s.snapshot, request)
	if request.Local && s.daemonSessions != nil {
		sessions = terminalCompositionSessions(s.daemonSessions)
	}
	stream := newTerminalCompositionStream(request, sessions)
	s.streams = append(s.streams, stream)
	s.mu.Unlock()
	return stream, nil
}

func (s *terminalCompositionService) CloseStream(ports.BrokerConnectionID, ports.BrokerStreamID) error {
	return nil
}

func (s *terminalCompositionService) AddHost(context.Context, string, ports.BrokerPolicy) (domain.RemoteRegistration, error) {
	return domain.RemoteRegistration{}, nil
}

func (s *terminalCompositionService) RemoveHost(context.Context, domain.RemoteRegistration) (bool, error) {
	return false, nil
}

func (s *terminalCompositionService) UpdateHostPolicy(context.Context, domain.RemoteRegistration, ports.BrokerPolicy) (domain.RemoteRegistration, error) {
	return domain.RemoteRegistration{}, nil
}

func (s *terminalCompositionService) RequestReconcile(string) {}

func (s *terminalCompositionService) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.closed {
		s.closed = true
		close(s.done)
	}
	return nil
}

// openedRequests returns the attachment streams opened; the preflight's
// control listings are reported by openedControls.
func (s *terminalCompositionService) openedRequests() []ports.BrokerOpenStreamRequest {
	return s.openedWithPurpose(ports.BrokerStreamAttachment)
}

func (s *terminalCompositionService) openedControls() []ports.BrokerOpenStreamRequest {
	return s.openedWithPurpose(ports.BrokerStreamControl)
}

func (s *terminalCompositionService) openedWithPurpose(purpose ports.BrokerStreamPurpose) []ports.BrokerOpenStreamRequest {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []ports.BrokerOpenStreamRequest
	for _, request := range s.opened {
		if request.Purpose == purpose {
			out = append(out, request)
		}
	}
	return out
}

// streamHello returns the Hello the supervisor sent on the single admitted
// stream, proving the composition carried the CLI intent through to the typed
// session handshake.
func (s *terminalCompositionService) streamHello(t *testing.T) protocol.Hello {
	t.Helper()
	s.mu.Lock()
	var streams []*terminalCompositionStream
	for _, stream := range s.streams {
		if stream.request.Purpose == ports.BrokerStreamAttachment {
			streams = append(streams, stream)
		}
	}
	s.mu.Unlock()
	require.Len(t, streams, 1)
	for _, message := range streams[0].sentMessages() {
		if hello, ok := message.(protocol.Hello); ok {
			return hello
		}
	}
	t.Fatal("the composed client never sent a session Hello")
	return protocol.Hello{}
}

type terminalCompositionSubscription struct{ changed chan struct{} }

func (s terminalCompositionSubscription) Changed() <-chan struct{} { return s.changed }
func (s terminalCompositionSubscription) Close()                   {}

// terminalCompositionStream answers one admitted attachment with a Welcome and
// one initial full session frame carrying real terminal bytes, so a test can
// prove the committed publication reached the terminal.
type terminalCompositionStream struct {
	request ports.BrokerOpenStreamRequest

	mu     sync.Mutex
	sent   []protocol.ClientMessage
	queue  []protocol.ServerMessage
	closed bool
	done   chan struct{}
}

// terminalCompositionDaemonSessions is the session list the scripted
// destination daemon owns: the sessions its observation publishes.
func terminalCompositionDaemonSessions(snapshot ports.BrokerSnapshot, request ports.BrokerOpenStreamRequest) []catalogue.RemoteCatalogSession {
	for _, observation := range snapshot.Daemons {
		if observation.Local == request.Local && (request.Local || observation.Endpoint == request.Endpoint) {
			return observation.Sessions
		}
	}
	return nil
}

func newTerminalCompositionStream(request ports.BrokerOpenStreamRequest, sessions []catalogue.RemoteCatalogSession) *terminalCompositionStream {
	stream := &terminalCompositionStream{request: request, done: make(chan struct{})}
	if request.Purpose == ports.BrokerStreamControl {
		// The scripted daemon answers a List with its own sessions.
		infos := make([]protocol.SessionInfo, 0, len(sessions))
		for _, session := range sessions {
			infos = append(infos, protocol.SessionInfo{Name: session.Name})
		}
		stream.queue = append(stream.queue, protocol.Sessions{Sessions: infos})
		return stream
	}
	if request.Admission == ports.BrokerAdmissionAttachNamed && !slices.ContainsFunc(sessions, func(session catalogue.RemoteCatalogSession) bool { return session.Name == request.Name }) {
		// Like the real daemon, an unknown name is refused, never created.
		stream.queue = append(stream.queue, protocol.ErrorMsg{Code: protocol.ErrNoSuchSession, Text: "no such resumable session: " + request.Name})
		return stream
	}
	target := terminalCompositionCommittedTarget(request)
	stream.queue = append(stream.queue, protocol.Welcome{
		SessionID: "sess", SessionName: request.Name,
		CommittedIdentity: &protocol.CommittedRouteIdentity{Target: target},
	})
	stream.queue = append(stream.queue, protocol.Output{
		Epoch: 1, Full: true, New: 1,
		Size: domain.Size{Cols: 80, Rows: 24},
		Context: &protocol.ViewContext{
			Publication:   1,
			Route:         protocol.CommittedRouteIdentity{Target: target},
			TabID:         domain.TabStableID("tab-1"),
			FocusedPaneID: domain.PaneStableID("pane-1"),
		},
		Data: []byte("terminal-composition-ready\r\n"),
	})
	return stream
}

// terminalCompositionCommittedTarget is the exact identity the scripted daemon
// commits for one admitted request. An ephemeral creation has no requested name,
// so the scripted daemon allocates one exactly as the real daemon does.
func terminalCompositionCommittedTarget(request ports.BrokerOpenStreamRequest) protocol.ExactSessionTarget {
	if request.Admission == ports.BrokerAdmissionExact {
		return request.Target
	}
	name := request.Name
	if name == "" {
		name = "1"
	}
	var lifecycle domain.SessionLifecycleID
	lifecycle[0] = 0x11
	return protocol.ExactSessionTarget{LifecycleID: lifecycle, SessionName: name}
}

func (s *terminalCompositionStream) SendClient(message protocol.ClientMessage) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sent = append(s.sent, message)
	return nil
}

func (s *terminalCompositionStream) ReceiveServer() (protocol.ServerMessage, error) {
	s.mu.Lock()
	if len(s.queue) != 0 {
		message := s.queue[0]
		s.queue = s.queue[1:]
		s.mu.Unlock()
		return message, nil
	}
	s.mu.Unlock()
	<-s.done
	return nil, io.EOF
}

func (s *terminalCompositionStream) Capabilities() protocol.ConnectionCapabilities {
	return protocol.ConnectionCapabilities{}
}

func (s *terminalCompositionStream) LinkState() ports.LinkState { return ports.LinkStateConnected }
func (s *terminalCompositionStream) LinkEvents() <-chan ports.LinkEvent {
	return nil
}

func (s *terminalCompositionStream) Done() <-chan struct{} { return s.done }
func (s *terminalCompositionStream) Err() error            { return nil }

func (s *terminalCompositionStream) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.closed {
		s.closed = true
		close(s.done)
	}
	return nil
}

func (s *terminalCompositionStream) sentMessages() []protocol.ClientMessage {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]protocol.ClientMessage(nil), s.sent...)
}

// terminalCompositionTerminal is the terminal double: it records written bytes,
// flushes, raw-mode entries/restores, and every UI publication so a test can
// prove a committed attachment published a real frame.
type terminalCompositionTerminal struct {
	mu       sync.Mutex
	out      []byte
	flushes  int
	restores int
	statuses []ports.UIPresentationStatus
	changes  chan struct{}
	input    *terminalCompositionInput
}

func newTerminalCompositionTerminal() *terminalCompositionTerminal {
	return &terminalCompositionTerminal{changes: make(chan struct{}), input: newTerminalCompositionInput()}
}

// terminalCompositionInput is the supervisor's single terminal reader. It
// yields bytes only when a test pushes them and reaches EOF when it closes, so
// a client with no pushed input stays live until the test ends the run.
type terminalCompositionInput struct {
	chunks chan []byte
}

func newTerminalCompositionInput() *terminalCompositionInput {
	return &terminalCompositionInput{chunks: make(chan []byte)}
}

func (i *terminalCompositionInput) Read(buffer []byte) (int, error) {
	chunk, ok := <-i.chunks
	if !ok {
		return 0, io.EOF
	}
	return copy(buffer, chunk), nil
}

func (t *terminalCompositionTerminal) EnterRaw() (func() error, error) {
	return func() error {
		t.mu.Lock()
		t.restores++
		t.mu.Unlock()
		return nil
	}, nil
}

func (t *terminalCompositionTerminal) Geometry() (domain.Geometry, error) {
	return domain.Geometry{Size: domain.Size{Cols: 80, Rows: 24}}, nil
}

func (t *terminalCompositionTerminal) ResizeEvents() <-chan domain.Geometry { return nil }

// In blocks until the run ends: the supervisor owns exactly one terminal read
// lifetime, and an immediately-empty reader would look like a terminal EOF and
// end an otherwise live client.
func (t *terminalCompositionTerminal) In() io.Reader { return t.input }

func (t *terminalCompositionTerminal) Out() io.Writer { return t }

func (t *terminalCompositionTerminal) Write(data []byte) (int, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.out = append(t.out, data...)
	return len(data), nil
}

func (t *terminalCompositionTerminal) Flush() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.flushes++
	return nil
}

func (t *terminalCompositionTerminal) BeginOutput(ports.UIContext) {}
func (t *terminalCompositionTerminal) EndOutput(bool)              {}

func (t *terminalCompositionTerminal) PublishContext(context ports.UIContext) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.statuses = append(t.statuses, context.Status)
	return nil
}

func (t *terminalCompositionTerminal) written() string {
	t.mu.Lock()
	defer t.mu.Unlock()
	return string(t.out)
}

func (t *terminalCompositionTerminal) publishes() []ports.UIPresentationStatus {
	t.mu.Lock()
	defer t.mu.Unlock()
	return append([]ports.UIPresentationStatus(nil), t.statuses...)
}

// startTerminalComposition drives the real runAttach entry point over one
// scripted broker service and recording terminal. It substitutes only the two
// composition seams (connect-or-spawn and the terminal) plus the callback seam,
// so every assertion holds for the production terminal path itself and not for
// a parallel test harness.
type terminalCompositionRun struct {
	service  *terminalCompositionService
	terminal *terminalCompositionTerminal
	states   chan client.State
	notices  chan client.LifecycleNotice
	failures chan error
	finished chan struct{}
	runMu    sync.Mutex
	runErr   error
	cancel   context.CancelFunc
}

// wait returns the run's terminal error. It is idempotent: every caller waits on
// the same closed channel, so a failure observed by one helper never consumes the
// result another helper needs.
func (r *terminalCompositionRun) wait(t *testing.T) error {
	t.Helper()
	select {
	case <-r.finished:
	case <-time.After(brokerTestWait):
		t.Fatal("terminal client did not stop")
	}
	r.runMu.Lock()
	defer r.runMu.Unlock()
	return r.runErr
}

func startTerminalComposition(t *testing.T, intent uint8, name, remoteTarget string, snapshot ports.BrokerSnapshot) *terminalCompositionRun {
	t.Helper()
	return startTerminalCompositionWithDaemon(t, intent, name, remoteTarget, snapshot, nil)
}

// startTerminalCompositionWithDaemon is startTerminalComposition whose local
// daemon owns daemonSessions even when the catalogue has not observed them.
func startTerminalCompositionWithDaemon(t *testing.T, intent uint8, name, remoteTarget string, snapshot ports.BrokerSnapshot, daemonSessions []string) *terminalCompositionRun {
	t.Helper()
	// The ordinary terminal path reads the nested-session and remote-transport
	// environment; a test drives the broker composition with both unset.
	t.Setenv("VEV", "")
	t.Setenv(envRemoteTransport, "")
	service := newTerminalCompositionService(snapshot)
	service.daemonSessions = daemonSessions
	terminal := newTerminalCompositionTerminal()
	previousConnect := connectProductionClientBroker
	previousTerminal := terminalForAttach
	previousCallbacks := terminalBrokerCallbacks
	connectProductionClientBroker = func(context.Context) (ports.BrokerService, error) {
		return service, nil
	}
	terminalForAttach = func() ports.Terminal { return terminal }

	run := &terminalCompositionRun{
		service:  service,
		terminal: terminal,
		states:   make(chan client.State, 64),
		notices:  make(chan client.LifecycleNotice, 16),
		failures: make(chan error, 16),
		finished: make(chan struct{}),
	}
	terminalBrokerCallbacks = func() terminalBrokerSeams {
		return terminalBrokerSeams{
			OnState: func(state client.State) {
				select {
				case run.states <- state:
				default:
				}
			},
			OnLifecycle: func(notice client.LifecycleNotice) {
				select {
				case run.notices <- notice:
				default:
				}
			},
			OnFailure: func(err error) {
				select {
				case run.failures <- err:
				default:
				}
			},
		}
	}
	t.Cleanup(func() {
		connectProductionClientBroker = previousConnect
		terminalForAttach = previousTerminal
		terminalBrokerCallbacks = previousCallbacks
	})

	ctx, cancel := context.WithCancel(context.Background())
	run.cancel = cancel
	go func() {
		err := runAttach(ctx, intent, name, remoteTarget)
		run.runMu.Lock()
		run.runErr = err
		run.runMu.Unlock()
		close(run.finished)
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-run.finished:
		case <-time.After(brokerTestWait):
			t.Error("terminal client did not stop")
		}
	})
	return run
}

// awaitTerminalCompositionAttachment waits for the supervisor to present an
// attached state and returns it.
func awaitTerminalCompositionAttachment(t *testing.T, run *terminalCompositionRun) client.State {
	t.Helper()
	deadline := time.NewTimer(brokerTestWait)
	defer deadline.Stop()
	for {
		select {
		case state := <-run.states:
			if state.Presentation == client.PresentAttached {
				return state
			}
		case <-run.finished:
			t.Fatalf("terminal client ended before attaching: %v", run.wait(t))
		case <-deadline.C:
			t.Fatal("terminal client never attached")
		}
	}
}

// awaitTerminalCompositionNotice waits for one bounded lifecycle notice.
func awaitTerminalCompositionNotice(t *testing.T, run *terminalCompositionRun) client.LifecycleNotice {
	t.Helper()
	select {
	case notice := <-run.notices:
		return notice
	case <-time.After(brokerTestWait):
		t.Fatal("terminal client reported no lifecycle notice")
		return client.LifecycleNotice{}
	}
}

func TestTerminalNavigationTranslationClosesTheIntent(t *testing.T) {
	tests := []struct {
		name         string
		intent       uint8
		session      string
		remote       string
		wantKind     client.InitialNavigationKind
		wantLocal    bool
		wantName     string
		wantResolver bool
		wantError    bool
	}{
		{name: "no arguments creates a local ephemeral session", intent: protocol.IntentEphemeral, wantKind: client.InitialNavigationCreateEphemeral, wantLocal: true},
		{name: "new creates a local named session", intent: protocol.IntentNew, session: "scratch", wantKind: client.InitialNavigationCreateNamed, wantLocal: true, wantName: "scratch"},
		{name: "attach names a local session for the daemon to resolve", intent: protocol.IntentAttach, session: "work", wantKind: client.InitialNavigationAttachNamed, wantLocal: true, wantName: "work"},
		{name: "remote attach resolves the remote registration", intent: protocol.IntentAttach, session: "work", remote: "user@example.test", wantResolver: true},
		{name: "remote new resolves a named remote creation", intent: protocol.IntentNew, session: "work", remote: "user@example.test", wantResolver: true},
		{name: "remote ephemeral attach resolves a remote creation", intent: protocol.IntentEphemeral, remote: "user@example.test", wantResolver: true},
		{name: "invalid local name is refused", intent: protocol.IntentNew, session: "bad name", wantError: true},
		{name: "invalid remote target is refused", intent: protocol.IntentAttach, session: "work", remote: "bad target", wantError: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			navigation, resolver, err := terminalBrokerNavigation(tc.intent, tc.session, tc.remote)
			if tc.wantError {
				require.Error(t, err)
				require.Nil(t, resolver)
				require.Equal(t, client.InitialNavigation{}, navigation)
				return
			}
			require.NoError(t, err)
			if tc.wantResolver {
				require.NotNil(t, resolver)
				require.Equal(t, client.InitialNavigation{}, navigation)
				return
			}
			require.Nil(t, resolver)
			require.Equal(t, tc.wantKind, navigation.Kind)
			require.Equal(t, tc.wantLocal, navigation.Destination.Local)
			require.Equal(t, tc.wantName, navigation.Name)
			require.NoError(t, navigation.Validate())
		})
	}
}

// TestTerminalCompositionOpensTheExactBrokerStream pins the ordinary terminal
// entry point to the broker: each parsed CLI target resolves to exactly one
// admission on one broker logical stream, and the composition dials no daemon.
func TestTerminalCompositionOpensTheExactBrokerStream(t *testing.T) {
	localSnapshot := func() ports.BrokerSnapshot {
		return terminalCompositionSnapshot([]string{"work"}, nil)
	}
	remoteSnapshot := func() ports.BrokerSnapshot {
		return terminalCompositionSnapshot(nil, map[string][]string{"user@example.test": {"work"}})
	}
	tests := []struct {
		name           string
		intent         uint8
		session        string
		remote         string
		snapshot       ports.BrokerSnapshot
		daemonSessions []string
		wantAdmission  ports.BrokerStreamAdmission
		wantLocal      bool
		wantName       string
		wantHello      uint8
	}{
		{name: "no arguments creates a local ephemeral session", intent: protocol.IntentEphemeral, snapshot: localSnapshot(), wantAdmission: ports.BrokerAdmissionCreateEphemeral, wantLocal: true, wantHello: protocol.IntentEphemeral},
		{name: "new creates a local named session", intent: protocol.IntentNew, session: "scratch", snapshot: localSnapshot(), wantAdmission: ports.BrokerAdmissionCreateNamed, wantLocal: true, wantName: "scratch", wantHello: protocol.IntentNew},
		{name: "attach names the local session", intent: protocol.IntentAttach, session: "work", snapshot: localSnapshot(), wantAdmission: ports.BrokerAdmissionAttachNamed, wantLocal: true, wantName: "work", wantHello: protocol.IntentAttach},
		{name: "remote attach names the remote session", intent: protocol.IntentAttach, session: "work", remote: "user@example.test", snapshot: remoteSnapshot(), wantAdmission: ports.BrokerAdmissionAttachNamed, wantName: "work", wantHello: protocol.IntentAttach},
		{name: "attach reaches a session the catalogue has not observed yet", intent: protocol.IntentAttach, session: "work", snapshot: terminalCompositionUnobservedLocal(), daemonSessions: []string{"work"}, wantAdmission: ports.BrokerAdmissionAttachNamed, wantLocal: true, wantName: "work", wantHello: protocol.IntentAttach},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			run := startTerminalCompositionWithDaemon(t, tc.intent, tc.session, tc.remote, tc.snapshot, tc.daemonSessions)
			awaitTerminalCompositionAttachment(t, run)
			opened := run.service.openedRequests()
			require.Len(t, opened, 1, "one CLI target opens exactly one broker stream")
			request := opened[0]
			require.NoError(t, request.Validate())
			require.Equal(t, terminalCompositionEpoch, request.Epoch)
			require.Equal(t, ports.BrokerStreamAttachment, request.Purpose)
			require.Equal(t, tc.wantAdmission, request.Admission)
			require.Equal(t, tc.wantLocal, request.Local)
			require.Equal(t, tc.wantName, request.Name)
			require.Equal(t, ports.BrokerConnectionID{4}, request.Connection)
			require.NotZero(t, request.Stream)
			require.NotEmpty(t, request.Policy.Transport)
			require.Equal(t, ports.BrokerDaemonStartIfNeeded, request.StartMode)
			if !tc.wantLocal {
				require.Equal(t, "user@example.test", request.Endpoint)
				require.Equal(t, terminalCompositionRegistration("user@example.test"), request.Registration)
			}
			if tc.wantAdmission == ports.BrokerAdmissionExact {
				require.NoError(t, request.Target.Validate())
				require.Equal(t, tc.session, request.Target.SessionName)
			}
			// The same intent reaches the typed session handshake, so the
			// broker admission and the daemon Hello can never disagree.
			hello := run.service.streamHello(t)
			require.Equal(t, tc.wantHello, hello.Intent)
			require.Equal(t, tc.session, hello.Name)
			require.Equal(t, tc.remote != "", hello.Remote)
			if tc.wantAdmission == ports.BrokerAdmissionExact {
				require.NotNil(t, hello.ExactTarget)
				require.Equal(t, request.Target, *hello.ExactTarget)
			}
			if tc.wantAdmission == ports.BrokerAdmissionAttachNamed {
				require.Nil(t, hello.ExactTarget, "a named attach leaves identity to the daemon")
				controls := run.service.openedControls()
				require.Len(t, controls, 1, "the preflight asks the daemon once")
				require.Equal(t, ports.BrokerDaemonStartIfNeeded, controls[0].StartMode, "the preflight may start a stopped daemon")
			}
		})
	}
}

// terminalCompositionUnobservedLocal is the publication a freshly started
// broker commits right after `kill --all`: the local daemon is known but its
// sessions are not observed yet, while the daemon itself restores them.
func terminalCompositionUnobservedLocal() ports.BrokerSnapshot {
	snapshot := terminalCompositionSnapshot(nil, nil)
	snapshot.Daemons[0].InventoryKnown = false
	return snapshot
}

// TestTerminalCompositionPublishesRealSessionOutput proves an admitted
// attachment is a committed publication: the daemon's initial full frame reached
// the terminal through a committed UI output transaction, and the supervisor
// presented attached only after that commit.
func TestTerminalCompositionPublishesRealSessionOutput(t *testing.T) {
	run := startTerminalComposition(t, protocol.IntentEphemeral, "", "", terminalCompositionSnapshot([]string{"work"}, nil))
	awaitTerminalCompositionAttachment(t, run)
	require.Contains(t, run.terminal.written(), "terminal-composition-ready")
	// The initial publication is committed before the attached marker, so its
	// recorded boundary status is the pre-attachment Connecting presentation.
	require.Contains(t, run.terminal.publishes(), ports.UIStatusConnecting)
	// The attached presentation is published just after the attached state, on
	// the attachment goroutine, so wait for it instead of racing it.
	require.Eventually(t, func() bool {
		return slices.Contains(run.terminal.publishes(), ports.UIStatusAttached)
	}, brokerTestWait, 5*time.Millisecond)
	_, _, restores := run.terminalCounts()
	require.Zero(t, restores)
}

func (r *terminalCompositionRun) terminalCounts() (int, int, int) {
	r.terminal.mu.Lock()
	defer r.terminal.mu.Unlock()
	return len(r.terminal.out), r.terminal.flushes, r.terminal.restores
}

// TestTerminalCompositionRefusedTargetOpensNothingAndKeepsThePicker pins the
// failure contract: a target the committed catalogue does not carry is a local
// refusal. No stream is opened, the supervisor reports a bounded
// selection-unavailable notice, and the process stays in the picker instead of
// attaching a fallback or creating implicitly.
func TestTerminalCompositionRefusedTargetOpensNothingAndKeepsThePicker(t *testing.T) {
	tests := []struct {
		name     string
		intent   uint8
		session  string
		remote   string
		snapshot ports.BrokerSnapshot
	}{
		{name: "local daemon absent", intent: protocol.IntentEphemeral, snapshot: ports.BrokerSnapshot{Epoch: terminalCompositionEpoch, Revision: 1}},
		{name: "unconfigured remote host", intent: protocol.IntentAttach, session: "work", remote: "user@elsewhere.test", snapshot: terminalCompositionSnapshot([]string{"work"}, nil)},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			run := startTerminalComposition(t, tc.intent, tc.session, tc.remote, tc.snapshot)
			require.Equal(t, client.LifecycleNoticeSelectionUnavailable, awaitTerminalCompositionNotice(t, run).Kind)
			// A local refusal is surfaced once, with the bounded text the supervisor
			// classifies; the resolver's own sentinel is deliberately not propagated
			// past that taxonomy, so only the presence and the classification are
			// asserted.
			select {
			case failure := <-run.failures:
				require.Error(t, failure)
			case <-time.After(brokerTestWait):
				t.Fatal("the refused target reported no failure")
			}
			require.Empty(t, run.service.openedRequests(), "a refused target never opens a stream")
			// The pending navigation presents Connecting, never the picker
			// first; the refusal then returns to the picker and never attaches.
			last := client.PresentConnecting
			for {
				select {
				case state := <-run.states:
					require.NotEqual(t, client.PresentAttached, state.Presentation, "a refusal never attaches")
					last = state.Presentation
				case <-time.After(100 * time.Millisecond):
					require.Equal(t, client.PresentPicker, last, "a refusal ends on the picker")
					return
				}
			}
		})
	}
}

// TestTerminalCompositionDaemonRefusesMissingName pins that existence is the
// daemon's decision: a declined (non-interactive) create prompt keeps the
// named attach, the daemon answers ErrNoSuchSession, and the client reports
// that refusal without attaching or creating.
func TestTerminalCompositionDaemonRefusesMissingName(t *testing.T) {
	tests := []struct {
		name     string
		remote   string
		snapshot ports.BrokerSnapshot
	}{
		{name: "local", snapshot: terminalCompositionSnapshot([]string{"work"}, nil)},
		{name: "remote", remote: "user@example.test", snapshot: terminalCompositionSnapshot(nil, map[string][]string{"user@example.test": {"work"}})},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			withAttachInteractiveConsole(t, false)
			run := startTerminalComposition(t, protocol.IntentAttach, "gone", tc.remote, tc.snapshot)
			require.Equal(t, client.LifecycleNoticeDestinationFailed, awaitTerminalCompositionNotice(t, run).Kind)
			select {
			case failure := <-run.failures:
				var protocolErr *client.ProtocolError
				require.ErrorAs(t, failure, &protocolErr)
				require.Equal(t, protocol.ErrNoSuchSession, protocolErr.Code)
			case <-time.After(brokerTestWait):
				t.Fatal("the daemon refusal was not reported")
			}
			opened := run.service.openedRequests()
			require.Len(t, opened, 1)
			require.Equal(t, ports.BrokerAdmissionAttachNamed, opened[0].Admission, "a missing name is never created implicitly")
		})
	}
}

// TestTerminalCompositionClosesRawModeOnTerminalEnd pins that the shared
// composition restores the terminal exactly once when the run ends, however the
// attachment settled.
func TestTerminalCompositionClosesRawModeOnTerminalEnd(t *testing.T) {
	run := startTerminalComposition(t, protocol.IntentEphemeral, "", "", terminalCompositionSnapshot([]string{"work"}, nil))
	awaitTerminalCompositionAttachment(t, run)
	run.cancel()
	require.NoError(t, ignoreContextCancellation(run.wait(t)))
	_, _, restores := run.terminalCounts()
	require.Equal(t, 1, restores, "raw mode is restored exactly once")
}

// TestTerminalCompositionCapturesSessionEnvironmentDefensively proves the test
// terminal composition keeps its process snapshot defensive: it captures
// env/Cwd, mutates the process seam after construction, and the supervisor
// still creates without failure.
func TestTerminalCompositionCapturesSessionEnvironmentDefensively(t *testing.T) {
	t.Setenv("VEV_TEST_SESSION_ENV", "composition-value")
	snapshot := terminalSessionEnvironment()
	require.Equal(t, client.SessionEnvironmentLocalCLI, snapshot.Provenance)
	require.NoError(t, snapshot.Validate())
	require.Contains(t, snapshot.Env, "VEV_TEST_SESSION_ENV=composition-value")
	// Mutate the process seam after capture: the snapshot must keep the
	// captured value rather than aliasing live process state.
	require.NoError(t, os.Setenv("VEV_TEST_SESSION_ENV", "mutated-after-capture"))
	require.Contains(t, snapshot.Env, "VEV_TEST_SESSION_ENV=composition-value")
	require.NotContains(t, snapshot.Env, "VEV_TEST_SESSION_ENV=mutated-after-capture")
	clone := snapshot.Clone()
	// Mutate the clone's slice after capture: the supervisor creation must
	// not fail and the source slice stays intact, so the snapshot is a copy
	// rather than a live alias of the process environment.
	for i := range clone.Env {
		clone.Env[i] = "MUTATED=x"
	}
	require.NotEqual(t, snapshot.Env, clone.Env)
	servicesnapshot := terminalCompositionSnapshot([]string{"work"}, nil)
	service := newTerminalCompositionService(servicesnapshot)
	terminal := newTerminalCompositionTerminal()
	navigation := localEphemeralNavigation()
	_, err := client.NewSupervisor(client.SupervisorConfig{
		Connector: scriptedBrokerConnector{service: service},
		Terminal:  terminal,
		Clock:     clock.New(),
		// The picker is omitted: creation alone proves the snapshot does not
		// fail supervisor validation.
		InitialNavigation:  navigation,
		SessionEnvironment: snapshot,
	})
	require.NoError(t, err)
}

// TestTerminalCompositionReportsBrokerFailureInThePicker pins that a broker the
// composition cannot connect to (or start) is no longer a fatal startup error:
// the run enters raw mode, stays in the picker, and reports the typed failure
// without opening any stream. It remains live until the terminal ends.
func TestTerminalCompositionReportsBrokerFailureInThePicker(t *testing.T) {
	t.Setenv("VEV", "")
	t.Setenv(envRemoteTransport, "")
	startupErr := errors.New("broker not reachable")
	previousConnect := connectProductionClientBroker
	terminal := newTerminalCompositionTerminal()
	previousTerminal := terminalForAttach
	connectProductionClientBroker = func(context.Context) (ports.BrokerService, error) { return nil, startupErr }
	terminalForAttach = func() ports.Terminal { return terminal }
	t.Cleanup(func() {
		connectProductionClientBroker = previousConnect
		terminalForAttach = previousTerminal
	})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- runAttach(ctx, protocol.IntentEphemeral, "", "") }()

	// A broker failure is surfaced, not fatal, and no stream is opened.
	require.Eventually(t, func() bool {
		for _, status := range terminal.publishes() {
			if status == ports.UIStatusConnecting || status == ports.UIStatusAttached {
				return false
			}
		}
		return true
	}, 200*time.Millisecond, 10*time.Millisecond)
	select {
	case err := <-done:
		t.Fatalf("the composition ended on a broker failure: %v", err)
	case <-time.After(200 * time.Millisecond):
	}
	_, _, restores := terminalCounts(terminal)
	require.Zero(t, restores, "the run is still live, so raw mode is not yet restored")

	cancel()
	require.NoError(t, ignoreContextCancellation(<-done))
	_, _, restores = terminalCounts(terminal)
	require.Equal(t, 1, restores, "the cancelled run restores raw mode exactly once")
}

// TestRunAttachNestedSessionBehavior pins the nested-session product contract of
// the terminal path: an explicit `new <name>` inside an existing session still
// creates the session without attaching, and every other intent is refused
// without reaching the broker.
func TestRunAttachNestedSessionBehavior(t *testing.T) {
	t.Setenv("VEV", "outer")
	t.Setenv(envRemoteTransport, "")
	connectCalls := 0
	previousConnect := connectProductionClientBroker
	previousCreate := createDetachedTerminalSession
	var createdName string
	connectProductionClientBroker = func(context.Context) (ports.BrokerService, error) {
		connectCalls++
		return nil, errors.New("the broker must not be reached")
	}
	createDetachedTerminalSession = func(_ context.Context, name string) error {
		createdName = name
		return nil
	}
	t.Cleanup(func() {
		connectProductionClientBroker = previousConnect
		createDetachedTerminalSession = previousCreate
	})

	require.NoError(t, runAttach(context.Background(), protocol.IntentNew, "scratch", ""))
	require.Equal(t, "scratch", createdName)

	for _, intent := range []uint8{protocol.IntentEphemeral, protocol.IntentAttach} {
		err := runAttach(context.Background(), intent, "scratch", "")
		require.ErrorContains(t, err, "sessions should be nested with care")
	}
	require.Zero(t, connectCalls, "a nested invocation never connects to the broker")
}

func terminalCounts(terminal *terminalCompositionTerminal) (int, int, int) {
	terminal.mu.Lock()
	defer terminal.mu.Unlock()
	return len(terminal.out), terminal.flushes, terminal.restores
}

// TestTerminalCompositionUsesTheBrokerConnector pins the production terminal
// path source: runAttach composes the shared broker client over the process'
// production connector and keeps no direct-dialer attach composition, attach
// preflight, or client host registry. The old pre-cutover symbols are removed
// outright, so this fails if any is reintroduced as a bypass.
func TestTerminalCompositionUsesTheBrokerConnector(t *testing.T) {
	sources := loadAppSources(t, false)
	body := sources["run.go"]
	require.Contains(t, body, "runBrokerClient(ctx, brokerClientConfig{")
	require.Contains(t, body, "newProductionBrokerConnector()")
	for _, forbidden := range []string{
		"runAttachWithDeps", "runAttachDeps", "localDaemonDialer", "dialOnlyLocalDialer",
		"defaultLocalDialer", "resolveMissingSessionAttach", "newClientHostRegistry",
		"listSessionsWithDialer", "clientEndpointFactory",
	} {
		require.NotContains(t, body, forbidden, "the terminal composition must not keep %q", forbidden)
	}
}

// TestTerminalExactAttachAcceptedByRealDaemon is the end-to-end regression for
// the exact-attach wire shape: against the real broker and a real daemon, the
// Hello the client supervisor builds for an exact admission (Intent=attach,
// Name equal to the exact target's session name, ExactTarget set) is accepted
// and answered with a committed Welcome. It fails if the strict Hello contract
// and the worker disagree about the name.
func TestTerminalExactAttachAcceptedByRealDaemon(t *testing.T) {
	fixture := startOfflineClientFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), brokerTestWait)
	defer cancel()
	service, err := brokeripc.NewConnector(fixture.socket, brokeripc.Config{}).Connect(ctx)
	require.NoError(t, err)
	defer service.Close()
	require.NotZero(t, awaitTerminalCompositionEpoch(t, service))

	createID, err := service.NextStreamID()
	require.NoError(t, err)
	create, err := service.OpenStream(ctx, ports.BrokerOpenStreamRequest{
		Purpose: ports.BrokerStreamAttachment, Stream: createID, Local: true, Policy: fixture.policy,
		Admission: ports.BrokerAdmissionCreateNamed, Name: "attach-exact", StartMode: ports.BrokerDaemonStartIfNeeded,
	})
	require.NoError(t, err)
	require.NoError(t, create.SendClient(protocol.Hello{Version: protocol.Version, Intent: protocol.IntentNew, Name: "attach-exact", Size: domain.Size{Cols: 80, Rows: 24}}))
	target := receiveOfflineWelcome(t, create).CommittedIdentity.Target
	require.NoError(t, create.Close())

	exactID, err := service.NextStreamID()
	require.NoError(t, err)
	exact, err := service.OpenStream(ctx, ports.BrokerOpenStreamRequest{
		Purpose: ports.BrokerStreamAttachment, Stream: exactID, Local: true, Policy: fixture.policy,
		Admission: ports.BrokerAdmissionExact, Target: target, StartMode: ports.BrokerDaemonStartIfNeeded,
	})
	require.NoError(t, err)
	defer exact.Close()

	// The exact Hello shape the client supervisor's worker builds for this
	// request; it must satisfy the protocol and be accepted by the daemon.
	hello := protocol.Hello{
		Version: protocol.Version, Intent: protocol.IntentAttach, Name: target.SessionName,
		ExactTarget: &target, Size: domain.Size{Cols: 80, Rows: 24},
	}
	require.NoError(t, protocol.ValidateHello(hello))
	require.NoError(t, exact.SendClient(hello))
	require.Equal(t, "attach-exact", receiveOfflineWelcome(t, exact).SessionName)
}

// TestTerminalCompositionExactAttachReportsMissingLocalInventory documents the
// composition's behavior when the committed broker publication carries no local
// session inventory: an exact attach is refused locally as a bounded selection
// notice, opens no stream, and never invents a creation or a same-name
// fallback. It pins the honest refusal so a future broker-side local catalogue
// is the only thing that can make this navigation succeed.
func TestTerminalCompositionExactAttachReportsMissingLocalInventory(t *testing.T) {
	fixture := startOfflineClientFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), brokerTestWait)
	defer cancel()

	terminal := newTerminalCompositionTerminal()
	notices := make(chan client.LifecycleNotice, 16)
	failures := make(chan error, 16)
	var lifecycleID domain.SessionLifecycleID
	lifecycleID[0], lifecycleID[1] = 3, 0x5a
	navigation := client.InitialNavigation{
		Kind:        client.InitialNavigationAttachExact,
		Epoch:       terminalCompositionEpoch,
		Destination: ports.BrokerEndpointFence{Local: true},
		Target:      protocol.ExactSessionTarget{LifecycleID: lifecycleID, SessionName: "work"},
	}
	require.NoError(t, navigation.Validate())

	done := make(chan error, 1)
	go func() {
		done <- runBrokerClient(ctx, brokerClientConfig{
			Connector:          brokeripc.NewConnector(fixture.socket, brokeripc.Config{}),
			Terminal:           terminal,
			InitialNavigation:  navigation,
			SessionEnvironment: client.SessionEnvironment{Provenance: client.SessionEnvironmentLocalCLI},
			OnFailure: func(err error) {
				select {
				case failures <- err:
				default:
				}
			},
			OnLifecycle: func(notice client.LifecycleNotice) {
				select {
				case notices <- notice:
				default:
				}
			},
		})
	}()

	select {
	case notice := <-notices:
		require.Equal(t, client.LifecycleNoticeSelectionUnavailable, notice.Kind)
	case <-time.After(brokerTestWait):
		t.Fatal("the exact attach reported no lifecycle notice")
	}
	select {
	case failure := <-failures:
		require.Error(t, failure)
	case <-time.After(brokerTestWait):
		t.Fatal("the exact attach reported no failure")
	}
	// The refused navigation never writes a session frame and never attaches:
	// it presents Connecting while pending and ends on the picker.
	publishes := terminal.publishes()
	for _, status := range publishes {
		require.NotEqual(t, ports.UIStatusAttached, status, "a refused navigation never attaches")
	}
	require.NotEmpty(t, publishes)
	require.Equal(t, ports.UIStatusPicker, publishes[len(publishes)-1], "a refused navigation ends on the picker")
	cancel()
	require.NoError(t, ignoreContextCancellation(<-done))
}

// awaitTerminalCompositionEpoch waits for one connection's first committed
// publication and returns its epoch.
func awaitTerminalCompositionEpoch(t *testing.T, service ports.BrokerService) ports.BrokerEpoch {
	t.Helper()
	sub, err := service.Subscribe()
	require.NoError(t, err)
	defer sub.Close()
	deadline := time.NewTimer(brokerTestWait)
	defer deadline.Stop()
	for {
		if snapshot := service.Snapshot(); snapshot.Epoch != 0 {
			return snapshot.Epoch
		}
		select {
		case <-sub.Changed():
		case <-service.Done():
			t.Fatalf("broker connection ended before publishing a snapshot: %v", service.Err())
		case <-deadline.C:
			t.Fatal("broker never published a snapshot")
		}
	}
}
