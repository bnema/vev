//go:build linux

package app

import (
	"context"
	"testing"
	"time"

	"github.com/bnema/vev/internal/adapters/clock"
	"github.com/bnema/vev/internal/adapters/uiterm"
	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/usecase/client"
	"github.com/stretchr/testify/require"
)

// scriptedBrokerConnector adapts one scripted broker service to the connector
// port, so a driver test can run the whole JSONL composition over a deterministic
// committed publication without a broker process.
type scriptedBrokerConnector struct{ service *terminalCompositionService }

func (c scriptedBrokerConnector) Connect(context.Context) (ports.BrokerService, error) {
	return c.service, nil
}

// startScriptedUIDriverRun launches one driver run over a scripted broker
// service, waits for its discovery response, and then waits for the run's single
// logical stream admission.
func startScriptedUIDriverRun(t *testing.T, service *terminalCompositionService, navigation client.InitialNavigation, resolver client.InitialNavigationResolver) *uiDriverFixtureRun {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	terminal, err := uiterm.New(ctx, domain.Geometry{Size: domain.Size{Cols: 80, Rows: 24}}, "")
	require.NoError(t, err)
	ui := client.NewUI(terminal, clock.New())
	stream := newUIDriverTestStream()
	run := &uiDriverFixtureRun{stream: stream, terminal: terminal, ui: ui, cancel: cancel, done: make(chan error, 1)}
	t.Cleanup(func() {
		_ = stream.Close()
		cancel()
		terminal.Close()
		select {
		case <-run.done:
		case <-time.After(brokerTestWait):
			t.Error("the scripted UI driver did not stop")
		}
	})
	go func() {
		run.done <- runUIDriverClient(ctx, brokerClientConfig{
			Connector:                scriptedBrokerConnector{service: service},
			Terminal:                 terminal,
			Clock:                    clock.New(),
			UI:                       ui,
			InitialNavigation:        navigation,
			ResolveInitialNavigation: resolver,
			AttachmentEnvironment:    terminalAttachmentEnvironment(),
			SessionEnvironment:       client.SessionEnvironment{Provenance: client.SessionEnvironmentLocalCLI},
		}, stream)
	}()
	run.Ready(t)
	require.Eventually(t, func() bool { return len(service.openedRequests()) != 0 }, brokerTestWait, 5*time.Millisecond,
		"the run never admitted its logical stream")
	return run
}

func TestUIDriverUsesBrokerSupervisorAndDaemon(t *testing.T) {
	fixture := startUIDriverBrokerFixture(t)
	run := fixture.StartDriver(t)
	ready := run.Ready(t)
	require.NotEmpty(t, ready.Attachment)
	require.True(t, ready.Control)
	require.Zero(t, ready.Generation, "the discovery response never invents an actionable generation")

	awaitLogicalStream(t, fixture.Streams())
	attached := run.awaitAttachedSnapshot(t, ready.Attachment, "", "offline-ready")
	generation := attached.Context.Generation
	require.Contains(t, offlineSnapshotText(attached), "offline-ready")

	// The driver service is usable before the attachment commits: capture works
	// on the same JSONL connection while the presentation is not yet attached.
	require.NotNil(t, run.capture(t, ready.Attachment))

	// The CLI intent became a real broker navigation: the driver created an
	// ephemeral local session through the shared client, and the terminal
	// published its committed initial frame.
	// A typed batch reaches the session through the normal client input owner,
	// and the committed publication boundary is observable.
	require.Equal(t, ports.UIActionProcessed, run.text(t, ready.Attachment, generation, "printf 'DRIVER_%s' OK"))

	run.EOF(t)
}

// TestUIDriverCreationRoutes pins the two decided creation routes against a real
// broker, pool, daemonmux, and daemon: the omitted name creates one
// automatically numbered ephemeral local session, and `--session NAME` creates
// exactly that named one. Each run's own committed publication is the proof,
// so the test never races the broker's own observation cadence.
func TestUIDriverCreationRoutes(t *testing.T) {
	fixture := startUIDriverBrokerFixture(t)

	ephemeral := fixture.StartDriver(t)
	ephemeralReady := ephemeral.Ready(t)
	awaitLogicalStream(t, fixture.Streams())
	ephemeralAttached := ephemeral.awaitAttachedSnapshot(t, ephemeralReady.Attachment, "", "offline-ready")
	ephemeralName := ephemeralAttached.Context.Route.Target.SessionName
	require.NotEmpty(t, ephemeralName, "an ephemeral creation commits an allocated session name")
	require.NotEqual(t, "work", ephemeralName)

	namedRun := fixture.StartNamedDriver(t, "work")
	namedReady := namedRun.Ready(t)
	awaitLogicalStream(t, fixture.Streams())
	namedAttached := namedRun.awaitAttachedSnapshot(t, namedReady.Attachment, "work", "offline-ready")
	require.Equal(t, "work", namedAttached.Context.Route.Target.SessionName)
}

// TestUIDriverExactAttachResolvesTheCommittedLifecycle pins the attach route
// deterministically against the scripted broker: `attach work` resolves the
// committed local lifecycle into exactly one exact admission whose target is
// that published identity, and the run dials no daemon itself.
func TestUIDriverExactAttachResolvesTheCommittedLifecycle(t *testing.T) {
	snapshot := terminalCompositionSnapshot([]string{"work"}, nil)
	navigation, resolver, err := terminalBrokerNavigation(protocol.IntentAttach, "work", "")
	require.NoError(t, err)
	require.Equal(t, client.InitialNavigation{}, navigation, "a name is resolved against the committed publication")
	require.NotNil(t, resolver)

	service := newTerminalCompositionService(snapshot)
	run := startScriptedUIDriverRun(t, service, navigation, resolver)
	require.NotNil(t, run)

	opened := service.openedRequests()
	require.Len(t, opened, 1, "attach work opens exactly one broker stream")
	request := opened[0]
	require.NoError(t, request.Validate())
	require.Equal(t, ports.BrokerAdmissionExact, request.Admission)
	require.True(t, request.Local)
	require.Equal(t, "work", request.Target.SessionName)
	require.Equal(t, terminalCompositionSessions([]string{"work"})[0].LifecycleID, request.Target.LifecycleID)
}

// TestUIDriverRefusedTargetStaysPickerAndKeepsServing pins the local-refusal
// contract: a target carrying a lifecycle the broker has never committed is
// refused, the driver stays usable in the picker, and it never attaches.
func TestUIDriverRefusedTargetStaysPickerAndKeepsServing(t *testing.T) {
	fixture := startUIDriverBrokerFixture(t)
	run := fixture.StartRefusedTargetDriver(t)
	ready := run.Ready(t)
	require.NotEmpty(t, ready.Attachment)
	require.Zero(t, ready.Generation)
	// The refused target is never attached and the driver keeps answering while
	// it stays in the picker.
	require.Never(t, func() bool {
		return run.presentationStatus(t, ready.Attachment) == ports.UIStatusAttached
	}, 500*time.Millisecond, 20*time.Millisecond, "a refused target never attaches")
	require.NotNil(t, run.capture(t, ready.Attachment))
	run.EOF(t)
}

// TestUIDriverDetachReturnsToPickerWithoutRecreating pins that an ended
// attachment returns the driver to the picker in the same run: the process stays
// alive and the one-shot initial navigation is not re-armed into a second
// attachment. A re-armed intent would return the driver to Attached on its own,
// so staying in the picker is the observable proof.
func TestUIDriverDetachReturnsToPickerWithoutRecreating(t *testing.T) {
	fixture := startUIDriverBrokerFixture(t)
	run := fixture.StartDriver(t)
	ready := run.Ready(t)
	awaitLogicalStream(t, fixture.Streams())
	attached := run.awaitAttachedSnapshot(t, ready.Attachment, "", "offline-ready")
	generation := attached.Context.Generation

	// Ctrl+D ends the shell: the daemon settles the session and the driver's
	// attachment ends, so the run returns to its picker presentation. The action
	// itself may be attributed to the committed boundary or reported as an
	// unknown outcome; either way the input is not replayed.
	status := run.keys(t, ready.Attachment, generation, "Ctrl+D")
	require.Contains(t, []ports.UIActionStatus{ports.UIActionProcessed, ports.UIActionOutcomeUnknown}, status)
	require.Eventually(t, func() bool {
		return run.presentationStatus(t, ready.Attachment) != ports.UIStatusAttached
	}, brokerTestWait, 20*time.Millisecond, "an ended attachment returns the driver to the picker")

	require.Never(t, func() bool {
		return run.presentationStatus(t, ready.Attachment) == ports.UIStatusAttached
	}, 750*time.Millisecond, 20*time.Millisecond, "a consumed intent is never re-armed")
	run.EOF(t)
}

// TestUIDriverNavigationReusesTheSharedTerminalTranslation pins that the driver
// intents are exactly the shared closed initial-navigation union: `--picker`
// opens nothing, an omitted name creates an ephemeral local session, a local
// name creates it, and a remote target stays a single-shot resolver over the
// committed publication (never an invented identity). The driver keeps no
// navigation vocabulary of its own.
func TestUIDriverNavigationReusesTheSharedTerminalTranslation(t *testing.T) {
	picker, resolver, err := uiDriverNavigation(uiDriverOptions{picker: true}, time.Time{})
	require.NoError(t, err)
	require.Nil(t, resolver)
	require.Equal(t, client.InitialNavigation{}, picker)
	require.NoError(t, picker.Validate(), "the picker intent is the safe zero value")

	local, resolver, err := uiDriverNavigation(uiDriverOptions{}, time.Time{})
	require.NoError(t, err)
	require.Nil(t, resolver)
	require.Equal(t, client.InitialNavigationCreateEphemeral, local.Kind)
	require.True(t, local.Destination.Local)

	named, resolver, err := uiDriverNavigation(uiDriverOptions{session: "work"}, time.Time{})
	require.NoError(t, err)
	require.Nil(t, resolver)
	require.Equal(t, client.InitialNavigationCreateNamed, named.Kind)
	require.Equal(t, "work", named.Name)
	require.True(t, named.Destination.Local)

	requestedAt := time.Unix(2000, 0)
	unobserved := ports.BrokerSnapshot{
		Epoch:    terminalCompositionEpoch,
		Revision: 1,
		Daemons: []ports.BrokerDaemonObservation{{
			Endpoint:      "user@example.com",
			DisplayOrigin: "user@example.com",
			Registration:  terminalCompositionRegistration("user@example.com"),
		}},
	}
	observedAt := func(at time.Time, sessions ...string) ports.BrokerSnapshot {
		return ports.BrokerSnapshot{
			Epoch:    terminalCompositionEpoch,
			Revision: 1,
			Daemons: []ports.BrokerDaemonObservation{{
				Endpoint:       "user@example.com",
				DisplayOrigin:  "user@example.com",
				Registration:   terminalCompositionRegistration("user@example.com"),
				InventoryKnown: true,
				LastSuccess:    at,
				Sessions:       terminalCompositionSessions(sessions),
			}},
		}
	}
	remoteSnapshot := func(sessions ...string) ports.BrokerSnapshot {
		return observedAt(requestedAt.Add(time.Second), sessions...)
	}
	tests := []struct {
		name     string
		options  uiDriverOptions
		snapshot ports.BrokerSnapshot
		wantKind client.InitialNavigationKind
		wantName string
		wantErr  error
	}{
		{name: "remote named before first inventory waits", options: uiDriverOptions{remote: "user@example.com", session: "work"}, snapshot: unobserved, wantErr: client.ErrInitialNavigationNotObserved},
		{name: "absence in an inventory older than the invocation waits", options: uiDriverOptions{remote: "user@example.com", session: "fresh"}, snapshot: observedAt(requestedAt.Add(-time.Second), "work"), wantErr: client.ErrInitialNavigationNotObserved},
		{name: "presence in an older inventory attaches", options: uiDriverOptions{remote: "user@example.com", session: "work"}, snapshot: observedAt(requestedAt.Add(-time.Second), "work"), wantKind: client.InitialNavigationAttachExact, wantName: "work"},
		{name: "remote ephemeral", options: uiDriverOptions{remote: "user@example.com"}, snapshot: remoteSnapshot("work"), wantKind: client.InitialNavigationCreateEphemeral},
		{name: "remote named existing attaches exact lifecycle", options: uiDriverOptions{remote: "user@example.com", session: "work"}, snapshot: remoteSnapshot("other", "work"), wantKind: client.InitialNavigationAttachExact, wantName: "work"},
		{name: "remote named missing creates", options: uiDriverOptions{remote: "user@example.com", session: "fresh"}, snapshot: remoteSnapshot("work"), wantKind: client.InitialNavigationCreateNamed, wantName: "fresh"},
		{name: "remote named on empty inventory creates", options: uiDriverOptions{remote: "user@example.com", session: "fresh"}, snapshot: remoteSnapshot(), wantKind: client.InitialNavigationCreateNamed, wantName: "fresh"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			navigation, resolver, err := uiDriverNavigation(tt.options, requestedAt)
			require.NoError(t, err)
			require.Equal(t, client.InitialNavigation{}, navigation, "a remote target is never decided before the connection")
			require.NotNil(t, resolver, "a remote target resolves against the committed publication")

			resolved, resolveErr := resolver(tt.snapshot)
			if tt.wantErr != nil {
				require.ErrorIs(t, resolveErr, tt.wantErr)
				var pending client.InitialNavigationNotObserved
				require.ErrorAs(t, resolveErr, &pending)
				require.Equal(t, "user@example.com", pending.Endpoint, "the wait names the endpoint to reconcile")
				require.Equal(t, client.InitialNavigationCreateNamed, pending.Fallback.Kind, "an unobservable host still falls back to creation")
				require.Equal(t, tt.options.session, pending.Fallback.Name)
				require.NoError(t, pending.Fallback.Validate())
				return
			}
			require.NoError(t, resolveErr)
			require.NoError(t, resolved.Validate())
			require.False(t, resolved.Destination.Local)
			require.Equal(t, tt.snapshot.Daemons[0].Registration, resolved.Destination.Registration)
			require.Equal(t, tt.wantKind, resolved.Kind)
			switch tt.wantKind {
			case client.InitialNavigationAttachExact:
				want, ok := brokerObservationExactTarget(tt.snapshot.Daemons[0], tt.wantName)
				require.True(t, ok)
				require.Equal(t, want, resolved.Target, "an existing name attaches its exact lifecycle, never a same-name creation")
			case client.InitialNavigationCreateNamed:
				require.Equal(t, tt.wantName, resolved.Name)
			}
		})
	}

	// A target the committed publication does not carry is refused, never
	// created implicitly.
	_, resolver, err = uiDriverNavigation(uiDriverOptions{remote: "user@elsewhere.test"}, requestedAt)
	require.NoError(t, err)
	_, err = resolver(ports.BrokerSnapshot{Epoch: terminalCompositionEpoch, Revision: 1})
	require.Error(t, err)
}
