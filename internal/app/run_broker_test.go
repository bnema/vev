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
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	portsmocks "github.com/bnema/vev/internal/ports/mocks"
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

// newSeamBroker is a scripted ports.BrokerService whose every open returns one
// scripted stream serving a fixed reply queue and records the exact open
// request, so a test can assert the start mode each caller authorized. An
// exhausted queue is an EOF, which is exactly the lost-reply condition the
// caller must classify as unknown rather than success.
func newSeamBroker(t *testing.T, replies ...protocol.ServerMessage) (*portsmocks.MockBrokerService, *brokerServiceState, *brokerStreamState) {
	t.Helper()
	stream, streamState := newBrokerStreamMock(t, replies)
	service, state := newBrokerServiceMock(t, seamBrokerTestSnapshot(), func(_ *portsmocks.MockBrokerService, state *brokerServiceState) {
		state.subErr = errors.New("the seam broker service does not support subscriptions")
		state.openStream = func(ports.BrokerOpenStreamRequest) (ports.BrokerLogicalConnection, error) { return stream, nil }
	})
	return service, state, streamState
}

// withSeamBroker installs one scripted broker connection on the connectBroker
// seam for the duration of a test, so no production broker is ever dialed.
func withSeamBroker(t *testing.T, service ports.BrokerService) {
	t.Helper()
	previous := connectBroker
	previousList := connectListBroker
	previousStop := connectDaemonStopBroker
	connectBroker = func(context.Context) (ports.BrokerService, error) { return service, nil }
	connectListBroker = connectBroker
	connectDaemonStopBroker = connectBroker
	t.Cleanup(func() {
		connectBroker = previous
		connectListBroker = previousList
		connectDaemonStopBroker = previousStop
	})
}

func seamKillResult(requestID uint64, outcome protocol.KillOutcome, code uint16, text string) protocol.KillResult {
	return protocol.KillResult{RequestID: requestID, Outcome: outcome, Code: code, Text: text}
}

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

// TestRunListRendersBrokerSessionsWithoutADaemon pins that runList reads the
// live session list through BrokerOperations over the per-user broker and
// renders it, so a local list never reads durable state or dials a daemon.
func TestRunListRendersBrokerSessionsWithoutADaemon(t *testing.T) {
	service, state, _ := newSeamBroker(t, protocol.Sessions{Sessions: []protocol.SessionInfo{
		{Name: "work", State: protocol.SessionUp, Tabs: 2, Attached: true},
	}})
	withSeamBroker(t, service)

	got := captureStdout(t, func() {
		require.NoError(t, runList(context.Background(), command{kind: kindList}))
	})
	for _, want := range []string{"NAME", "work", "up", "2", "yes"} {
		require.Contains(t, got, want)
	}
	require.Equal(t, 1, state.closeCount(), "the borrowed broker service is closed exactly once")
	opens := state.allOpened()
	require.Len(t, opens, 1)
	require.Equal(t, ports.BrokerStreamControl, opens[0].Purpose)
	require.True(t, opens[0].Local)
	require.Equal(t, ports.BrokerDaemonExistingOnly, opens[0].StartMode,
		"a listing must not start a stopped daemon")
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
			name:       "kill-sessions success",
			kill:       []string{"", "true", "false"},
			replies:    []protocol.ServerMessage{seamKillResult(1, protocol.KillSucceeded, 0, "")},
			wantCode:   0,
			wantStdout: "killed all sessions",
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
			service, state, stream := newSeamBroker(t, tt.replies...)
			withSeamBroker(t, service)

			var err error
			got := captureStdout(t, func() {
				err = runKill(context.Background(), tt.kill[0], tt.kill[1] == "true")
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
			opens := state.allOpened()
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

	err := runKill(context.Background(), "work", false)
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
			service, state, _ := newSeamBroker(t, tt.replies...)
			withSeamBroker(t, service)

			err := requestDaemonStop(context.Background())
			opens := state.allOpened()
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

// TestCreateDetachedLocalSessionUsesBrokerOperations proves nested `vev new`
// creates through one broker control stream and never needs a daemon dialer.
func TestCreateDetachedLocalSessionUsesBrokerOperations(t *testing.T) {
	t.Setenv("VEV", "session=outer,tab=1,pane=1")
	service, state, stream := newSeamBroker(t, protocol.CommandResult{RequestID: 1, Outcome: protocol.CommandSucceeded})
	withSeamBroker(t, service)

	require.NoError(t, createDetachedLocalSession(context.Background(), "smoke"))
	require.Equal(t, 1, state.closeCount())
	require.Len(t, state.allOpened(), 1)
	sent := stream.sentMessages()
	require.Len(t, sent, 1)
	request, ok := sent[0].(protocol.CommandRequest)
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
	body := sources["sessions.go"]
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
