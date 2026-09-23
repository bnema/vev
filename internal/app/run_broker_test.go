package app

// List/kill/stop broker-only caller contract (Plan 001 P7.4f).
//
// runList, runKill, and requestDaemonStop reach the sole per-user broker through
// the private connectBroker composition seam. These tests substitute a scripted
// broker connection there, so every case drives the real caller code without a
// production broker process, a daemon dialer, or a real signal. They pin the
// explicit outcome table: success is printed only for an explicit KillSucceeded,
// a definite failure keeps its bounded error, and a lost or uncorrelated reply
// is an exit-3 unknown outcome that is never read as success.

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/protocol/catalogue"
)

const seamBrokerTestEpoch = ports.BrokerEpoch(0x3c)

// seamBrokerTestPolicy is the exact local policy the scripted snapshot carries.
func seamBrokerTestPolicy() ports.BrokerPolicy {
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

// seamBrokerTestSnapshot publishes one reachable local daemon at the scripted
// epoch, so a route built from it validates against the served snapshot.
func seamBrokerTestSnapshot() ports.BrokerSnapshot {
	return ports.BrokerSnapshot{
		Epoch:    seamBrokerTestEpoch,
		Revision: 1,
		Daemons: []ports.BrokerDaemonObservation{{
			Local:           true,
			DisplayOrigin:   "local",
			Policy:          seamBrokerTestPolicy(),
			Identity:        ports.BrokerDaemonIdentity("local-daemon"),
			Incarnation:     ports.BrokerDaemonIncarnation{9},
			ProtocolVersion: protocol.Version,
			Availability:    domain.RemoteAvailabilityReachable,
		}},
	}
}

// seamBrokerStream is one scripted broker logical stream. It records every
// typed client message and serves a fixed reply queue: an exhausted queue is an
// EOF, which is exactly the lost-reply condition the caller must classify as
// unknown rather than success.
type seamBrokerStream struct {
	mu      sync.Mutex
	sent    []protocol.ClientMessage
	replies []protocol.ServerMessage
	closed  bool
	done    chan struct{}
}

func newSeamBrokerStream(replies ...protocol.ServerMessage) *seamBrokerStream {
	return &seamBrokerStream{replies: replies, done: make(chan struct{})}
}

func (s *seamBrokerStream) SendClient(message protocol.ClientMessage) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sent = append(s.sent, message)
	return nil
}

func (s *seamBrokerStream) ReceiveServer() (protocol.ServerMessage, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.replies) == 0 {
		return nil, io.EOF
	}
	reply := s.replies[0]
	s.replies = s.replies[1:]
	return reply, nil
}

func (s *seamBrokerStream) Capabilities() protocol.ConnectionCapabilities {
	return protocol.ConnectionCapabilities{}
}

func (s *seamBrokerStream) LinkState() ports.LinkState         { return ports.LinkStateConnected }
func (s *seamBrokerStream) LinkEvents() <-chan ports.LinkEvent { return nil }
func (s *seamBrokerStream) Done() <-chan struct{}              { return s.done }
func (s *seamBrokerStream) Err() error                         { return nil }

func (s *seamBrokerStream) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.closed {
		s.closed = true
		close(s.done)
	}
	return nil
}

// seamBrokerService is a scripted ports.BrokerService. Every open returns the
// same scripted stream and records the exact open request, so a test can assert
// the start mode each caller authorized.
type seamBrokerService struct {
	snapshot ports.BrokerSnapshot
	stream   *seamBrokerStream

	mu     sync.Mutex
	next   ports.BrokerStreamID
	opens  []ports.BrokerOpenStreamRequest
	closed int
}

func newSeamBrokerService(stream *seamBrokerStream) *seamBrokerService {
	return &seamBrokerService{snapshot: seamBrokerTestSnapshot(), stream: stream}
}

func (s *seamBrokerService) ConnectionID() ports.BrokerConnectionID {
	return ports.BrokerConnectionID{4}
}

func (s *seamBrokerService) NextStreamID() (ports.BrokerStreamID, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.next++
	return s.next, nil
}

func (s *seamBrokerService) Done() <-chan struct{} { return make(chan struct{}) }
func (s *seamBrokerService) Err() error            { return nil }

func (s *seamBrokerService) Snapshot() ports.BrokerSnapshot { return s.snapshot.Clone() }

func (s *seamBrokerService) Subscribe() (ports.BrokerSubscription, error) {
	return nil, errors.New("the seam broker service does not support subscriptions")
}

func (s *seamBrokerService) SubscribePreview(ports.BrokerPreviewRequest) (ports.BrokerPreviewSubscription, error) {
	return nil, errors.New("the seam broker service does not support preview")
}

func (s *seamBrokerService) OpenStream(_ context.Context, request ports.BrokerOpenStreamRequest) (ports.BrokerLogicalConnection, error) {
	s.mu.Lock()
	s.opens = append(s.opens, request)
	s.mu.Unlock()
	return s.stream, nil
}

func (s *seamBrokerService) CloseStream(ports.BrokerConnectionID, ports.BrokerStreamID) error {
	return nil
}

func (s *seamBrokerService) AddHost(context.Context, string, ports.BrokerPolicy) (domain.RemoteRegistration, error) {
	return domain.RemoteRegistration{}, nil
}

func (s *seamBrokerService) RemoveHost(context.Context, domain.RemoteRegistration) (bool, error) {
	return false, nil
}

func (s *seamBrokerService) UpdateHostPolicy(context.Context, domain.RemoteRegistration, ports.BrokerPolicy) (domain.RemoteRegistration, error) {
	return domain.RemoteRegistration{}, nil
}

func (s *seamBrokerService) RequestReconcile(string) {}

func (s *seamBrokerService) Close() error {
	s.mu.Lock()
	s.closed++
	s.mu.Unlock()
	return nil
}

func (s *seamBrokerService) openedRequests() []ports.BrokerOpenStreamRequest {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]ports.BrokerOpenStreamRequest(nil), s.opens...)
}

func (s *seamBrokerService) closeCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closed
}

var _ ports.BrokerService = (*seamBrokerService)(nil)

// withSeamBroker installs one scripted broker connection on the connectBroker
// seam for the duration of a test, so no production broker is ever dialed.
func withSeamBroker(t *testing.T, service ports.BrokerService) {
	t.Helper()
	previous := connectBroker
	previousStop := connectDaemonStopBroker
	connectBroker = func(context.Context) (ports.BrokerService, error) { return service, nil }
	connectDaemonStopBroker = connectBroker
	t.Cleanup(func() {
		connectBroker = previous
		connectDaemonStopBroker = previousStop
	})
}

func seamKillResult(requestID uint64, outcome protocol.KillOutcome, code uint16, text string) protocol.KillResult {
	return protocol.KillResult{RequestID: requestID, Outcome: outcome, Code: code, Text: text}
}

// TestRunListRendersBrokerSessionsWithoutADaemon pins that runList reads the
// live session list through BrokerOperations over the per-user broker and
// renders it, so a local list never reads durable state or dials a daemon.
func TestRunBrokerSnapshotListReportsNoDaemon(t *testing.T) {
	remote := ports.BrokerDaemonObservation{
		Endpoint: "demo@host.test", DisplayOrigin: "demo@host.test",
		Registration: domain.RemoteRegistration{Endpoint: "demo@host.test", Incarnation: [16]byte{1}, Generation: 1},
		Policy:       seamBrokerTestPolicy(), Availability: domain.RemoteAvailabilityNoDaemon,
		Sessions: []catalogue.RemoteCatalogSession{},
	}
	snapshot := ports.BrokerSnapshot{Epoch: seamBrokerTestEpoch, Revision: 1, Daemons: []ports.BrokerDaemonObservation{remote}}

	var all bytes.Buffer
	require.NoError(t, runBrokerSnapshotList(command{listAll: true}, snapshot, &all))
	require.Contains(t, all.String(), "demo@host.test: no vev daemon")
	require.Contains(t, all.String(), "Enter in the picker to create a session")
	require.NotContains(t, all.String(), "no sessions")

	var host bytes.Buffer
	require.NoError(t, runBrokerSnapshotList(command{listHost: "demo@host.test"}, snapshot, &host))
	require.Contains(t, host.String(), "no vev daemon")
}

func TestRunBrokerSnapshotListAllShowsNoDaemonAlongsideSessions(t *testing.T) {
	remote := ports.BrokerDaemonObservation{
		Endpoint: "demo@host.test", DisplayOrigin: "demo@host.test",
		Registration: domain.RemoteRegistration{Endpoint: "demo@host.test", Incarnation: [16]byte{1}, Generation: 1},
		Policy:       seamBrokerTestPolicy(), Availability: domain.RemoteAvailabilityNoDaemon,
		Sessions: []catalogue.RemoteCatalogSession{},
	}
	local := seamBrokerTestSnapshot().Daemons[0]
	local.InventoryKnown = true
	local.Sessions = []catalogue.RemoteCatalogSession{{LifecycleID: domain.SessionLifecycleID{1}, Name: "work", State: catalogue.RemoteCatalogSessionUp, Tabs: []catalogue.RemoteCatalogTab{}}}
	snapshot := ports.BrokerSnapshot{Epoch: seamBrokerTestEpoch, Revision: 1, Daemons: []ports.BrokerDaemonObservation{local, remote}}
	var output bytes.Buffer
	require.NoError(t, runBrokerSnapshotList(command{listAll: true}, snapshot, &output))
	require.Contains(t, output.String(), "demo@host.test: no vev daemon")
	require.Contains(t, output.String(), "work")
}

func TestRunListRendersBrokerSessionsWithoutADaemon(t *testing.T) {
	stream := newSeamBrokerStream(protocol.Sessions{Sessions: []protocol.SessionInfo{
		{Name: "work", State: protocol.SessionUp, Tabs: 2, Attached: true},
	}})
	service := newSeamBrokerService(stream)
	withSeamBroker(t, service)

	got := captureStdout(t, func() {
		require.NoError(t, runList(context.Background(), command{kind: kindList}))
	})
	for _, want := range []string{"NAME", "work", "up", "2", "yes"} {
		require.Contains(t, got, want)
	}
	require.Equal(t, 1, service.closeCount(), "the borrowed broker service is closed exactly once")
	opens := service.openedRequests()
	require.Len(t, opens, 1)
	require.Equal(t, ports.BrokerStreamControl, opens[0].Purpose)
	require.True(t, opens[0].Local)
	require.Equal(t, ports.BrokerDaemonStartIfNeeded, opens[0].StartMode,
		"a local listing may start a stopped daemon to read its records")
}

// TestRunKillExplicitOutcomesTable pins the runKill outcome contract: an
// explicit success prints, a definite failure keeps its bounded error at exit
// one, and a lost or uncorrelated reply is an exit-three unknown that is never
// printed as success.
func TestRunKillExplicitOutcomesTable(t *testing.T) {
	tests := []struct {
		name        string
		kill        []string
		replies     []protocol.ServerMessage
		wantCode    int
		wantUnknown bool
		wantStdout  string
		wantErr     string
	}{
		{
			name:       "named success prints only on explicit success",
			kill:       []string{"work", "false", "false"},
			replies:    []protocol.ServerMessage{seamKillResult(1, protocol.KillSucceeded, 0, "")},
			wantCode:   0,
			wantStdout: "killed work",
		},
		{
			name:       "kill-all success",
			kill:       []string{"", "true", "false"},
			replies:    []protocol.ServerMessage{seamKillResult(1, protocol.KillSucceeded, 0, "")},
			wantCode:   0,
			wantStdout: "killed all sessions",
		},
		{
			name:     "daemon-stop success",
			kill:     []string{"", "false", "true"},
			replies:  []protocol.ServerMessage{seamKillResult(1, protocol.KillSucceeded, 0, "")},
			wantCode: 0,
		},
		{
			name:     "definite failure is a bounded exit-one failure",
			kill:     []string{"work", "false", "false"},
			replies:  []protocol.ServerMessage{seamKillResult(1, protocol.KillFailed, protocol.ErrInternal, "no such session: work")},
			wantCode: 1,
			wantErr:  "no such session: work",
		},
		{
			name:        "explicit unknown outcome is exit three",
			kill:        []string{"work", "false", "false"},
			replies:     []protocol.ServerMessage{seamKillResult(1, protocol.KillOutcomeUnknown, protocol.ErrServerShutdown, "daemon is shutting down")},
			wantCode:    3,
			wantUnknown: true,
		},
		{
			name:        "lost reply is exit three and never success",
			kill:        []string{"work", "false", "false"},
			wantCode:    3,
			wantUnknown: true,
		},
		{
			name:        "uncorrelated success reply is exit three",
			kill:        []string{"work", "false", "false"},
			replies:     []protocol.ServerMessage{seamKillResult(999, protocol.KillSucceeded, 0, "")},
			wantCode:    3,
			wantUnknown: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stream := newSeamBrokerStream(tt.replies...)
			service := newSeamBrokerService(stream)
			withSeamBroker(t, service)

			var err error
			got := captureStdout(t, func() {
				err = runKill(context.Background(), tt.kill[0], tt.kill[1] == "true", tt.kill[2] == "true")
			})
			require.Equal(t, tt.wantCode, ExitCode(err), "exit code for %v", tt.kill)
			if tt.wantUnknown {
				require.ErrorIs(t, err, errKillOutcomeUnknown, "a lost or uncorrelated reply is unknown")
				require.NotContains(t, got, "killed", "an unknown outcome is never printed as success")
			} else if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
			}
			if tt.wantStdout != "" {
				require.Contains(t, got, tt.wantStdout)
			}
			opens := service.openedRequests()
			require.Len(t, opens, 1, "exactly one control stream per kill")
			require.Equal(t, ports.BrokerStreamControl, opens[0].Purpose)
			require.Equal(t, 1, stream.sentCount(), "the kill is never replayed")
		})
	}
}

// TestRunKillUnreachableBrokerIsExitThree pins that a broker that cannot be
// reached is reported as a distinct exit-three unreachable failure before any
// stream is opened.
func TestRunKillUnreachableBrokerIsExitThree(t *testing.T) {
	previous := connectBroker
	connectBroker = func(context.Context) (ports.BrokerService, error) {
		return nil, errors.New("broker not reachable")
	}
	t.Cleanup(func() { connectBroker = previous })

	err := runKill(context.Background(), "work", false, false)
	require.Equal(t, 3, ExitCode(err))
	require.ErrorIs(t, err, errDaemonUnreachable)
}

// TestRequestDaemonStopUsesExistingOnlyAndExplicitOutcomes pins the daemon-stop
// contract: the stop authorizes ExistingOnly so it can never start the daemon it
// is stopping, success is exactly an explicit KillSucceeded, a definite failure
// is a plain error, and a lost reply is an explicit unknown outcome.
func TestRequestDaemonStopUsesExistingOnlyAndExplicitOutcomes(t *testing.T) {
	tests := []struct {
		name        string
		replies     []protocol.ServerMessage
		wantUnknown bool
		wantErr     string
	}{
		{
			name:    "explicit success",
			replies: []protocol.ServerMessage{seamKillResult(1, protocol.KillSucceeded, 0, "")},
		},
		{
			name:    "definite failure keeps its text",
			replies: []protocol.ServerMessage{seamKillResult(1, protocol.KillFailed, protocol.ErrInternal, "daemon refused stop")},
			wantErr: "daemon refused stop",
		},
		{
			name:        "lost reply is unknown",
			wantUnknown: true,
		},
		{
			name:        "explicit unknown stays unknown",
			replies:     []protocol.ServerMessage{seamKillResult(1, protocol.KillOutcomeUnknown, protocol.ErrServerShutdown, "daemon is shutting down")},
			wantUnknown: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stream := newSeamBrokerStream(tt.replies...)
			service := newSeamBrokerService(stream)
			withSeamBroker(t, service)

			err := requestDaemonStop(context.Background())
			opens := service.openedRequests()
			require.Len(t, opens, 1)
			require.Equal(t, ports.BrokerStreamControl, opens[0].Purpose)
			require.Equal(t, ports.BrokerDaemonExistingOnly, opens[0].StartMode,
				"a daemon-stop must never authorize starting the daemon it stops")

			if tt.wantUnknown {
				require.ErrorIs(t, err, errKillOutcomeUnknown)
			} else if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
				require.NotErrorIs(t, err, errKillOutcomeUnknown)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// sentCount reports how many typed messages one scripted stream received.
func (s *seamBrokerStream) sentCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.sent)
}

// TestCreateDetachedLocalSessionUsesBrokerOperations proves nested `vev new`
// creates through one broker control stream and never needs a daemon dialer.
func TestCreateDetachedLocalSessionUsesBrokerOperations(t *testing.T) {
	t.Setenv("VEV", "session=outer,tab=1,pane=1")
	stream := newSeamBrokerStream(protocol.CommandResult{RequestID: 1, Outcome: protocol.CommandSucceeded})
	service := newSeamBrokerService(stream)
	withSeamBroker(t, service)

	require.NoError(t, createDetachedLocalSession(context.Background(), "smoke"))
	require.Equal(t, 1, service.closeCount())
	require.Len(t, service.openedRequests(), 1)
	stream.mu.Lock()
	defer stream.mu.Unlock()
	require.Len(t, stream.sent, 1)
	request, ok := stream.sent[0].(protocol.CommandRequest)
	require.True(t, ok)
	require.Equal(t, "new-session", request.Slug)
	require.Equal(t, []string{"smoke"}, request.Args)
	require.Equal(t, "outer", request.TargetSession)
	require.False(t, request.Attached)
	require.False(t, request.Self)
}

// TestDetachedCreationProductionSourceHasNoDaemonBypass keeps the production
// helper on the broker contract: direct IPC and daemon lifecycle helpers may
// not reappear in its body as a fallback.
func TestDetachedCreationProductionSourceHasNoDaemonBypass(t *testing.T) {
	sources := loadAppSources(t, false)
	body := sources["run.go"]
	start := strings.Index(body, "func createDetachedLocalSession")
	require.NotEqual(t, -1, start)
	end := strings.Index(body[start:], "\nfunc renderDetachedCreationResult")
	require.NotEqual(t, -1, end)
	helper := body[start : start+end]
	require.Contains(t, helper, "client.NewBrokerOperations")
	require.Contains(t, helper, "operations.Command")
	require.NotContains(t, helper, "ipc.DialContext")
	require.NotContains(t, helper, "ensureDaemon")
	require.NotContains(t, helper, "realDial")
}
