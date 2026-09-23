//go:build linux

package app

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/bnema/vev/internal/adapters/brokeripc"
	"github.com/bnema/vev/internal/adapters/clock"
	"github.com/bnema/vev/internal/adapters/uidriver"
	"github.com/bnema/vev/internal/adapters/uiterm"
	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/usecase/client"
	"github.com/stretchr/testify/require"
)

// UI-driver broker fixture (Plan 001 P7 UI-driver slice 2).
//
// The fixture owns every resource a UI-driver test family must provision and tear
// down: one private offline root, one real broker sandbox process, the daemon it
// observes, and every driver run started over the fixture's connector. Nothing
// here reaches the production runtime or state directories; the production roots
// are redirected into private temporary directories and asserted untouched.
//
// Ownership is explicit and one-directional: the fixture stops the runs *it*
// started before the broker and daemon it created are torn down. A run's EOF is a
// detach — it closes that run's logical stream and service only — so the fixture
// never interprets a driver's exit as proof the shared daemon went away, and the
// daemon stays alive for the next run.

// uiDriverBrokerFixture is the shared private broker, daemon, and run registry of
// one UI-driver test family.
type uiDriverBrokerFixture struct {
	broker offlineClientFixture
	// prodRuntime and prodState are the redirected production roots the fixture
	// asserts untouched.
	prodRuntime string
	prodState   string

	mu     sync.Mutex
	runs   []*uiDriverFixtureRun
	closed bool
}

// uiDriverFixtureRun is one UI-driver run the fixture owns: its JSONL stream, its
// virtual terminal, its UI service, its cancellation, and the channel reporting
// its result.
type uiDriverFixtureRun struct {
	stream   *uidriverTestStream
	terminal *uiterm.Terminal
	ui       *client.UI
	cancel   context.CancelFunc
	done     chan error
}

// startUIDriverBrokerFixture provisions one private broker sandbox with its own
// daemon and registers the teardown immediately, so a failure part-way through
// provisioning still cleans up exactly what was created.
func startUIDriverBrokerFixture(t *testing.T) *uiDriverBrokerFixture {
	t.Helper()
	broker := startOfflineClientFixture(t)
	fixture := &uiDriverBrokerFixture{broker: broker, prodRuntime: broker.prodRuntime, prodState: broker.prodState}
	t.Cleanup(fixture.stopRuns)
	return fixture
}

// Connector returns the broker IPC connector of this fixture's private offline
// broker. It is the only transport authority a sandbox harness is given: it
// starts no production broker and never falls back to the production XDG layout.
func (f *uiDriverBrokerFixture) Connector() ports.BrokerConnector {
	return brokeripc.NewConnector(f.broker.socket, brokeripc.Config{})
}

// Streams reports one event per logical stream the fixture broker admitted.
func (f *uiDriverBrokerFixture) Streams() <-chan struct{} { return f.broker.streams }

// StartDriver launches the no-argument driver: one ephemeral local creation, the
// product default of every frontend.
func (f *uiDriverBrokerFixture) StartDriver(t *testing.T) *uiDriverFixtureRun {
	t.Helper()
	return f.startDriver(t, localEphemeralNavigation(), nil)
}

// StartPickerDriver launches the `--picker` driver: no initial navigation at
// all, so it reaches ready without opening a logical stream.
func (f *uiDriverBrokerFixture) StartPickerDriver(t *testing.T) *uiDriverFixtureRun {
	t.Helper()
	return f.startDriver(t, client.InitialNavigation{}, nil)
}

// StartNamedDriver launches the `--session NAME` driver: one named local
// creation.
func (f *uiDriverBrokerFixture) StartNamedDriver(t *testing.T, name string) *uiDriverFixtureRun {
	t.Helper()
	navigation, resolver, err := terminalBrokerNavigation(protocol.IntentNew, name, "")
	require.NoError(t, err)
	return f.startDriver(t, navigation, resolver)
}

// StartRefusedTargetDriver launches a driver whose resolved target carries a
// lifecycle the broker has never committed, so the run refuses it locally and
// stays in the picker.
func (f *uiDriverBrokerFixture) StartRefusedTargetDriver(t *testing.T) *uiDriverFixtureRun {
	t.Helper()
	var unknown domain.SessionLifecycleID
	unknown[0], unknown[1] = 0xEE, 0x11
	target := protocol.ExactSessionTarget{LifecycleID: unknown, SessionName: "absent"}
	require.NoError(t, target.Validate())
	navigation := client.InitialNavigation{
		Kind:        client.InitialNavigationAttachExact,
		Epoch:       f.epoch(t),
		Destination: ports.BrokerEndpointFence{Local: true},
		Target:      target,
	}
	require.NoError(t, navigation.Validate())
	return f.startDriver(t, navigation, nil)
}

// startDriver composes and registers one harness run over the fixture's private
// broker.
func (f *uiDriverBrokerFixture) startDriver(t *testing.T, navigation client.InitialNavigation, resolver client.InitialNavigationResolver) *uiDriverFixtureRun {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	terminal, err := uiterm.New(ctx, domain.Geometry{Size: domain.Size{Cols: 80, Rows: 24}}, "")
	require.NoError(t, err)
	ui := client.NewUI(terminal, clock.New())
	run := &uiDriverFixtureRun{
		stream: newUIDriverTestStream(), terminal: terminal, ui: ui,
		cancel: cancel, done: make(chan error, 1),
	}
	f.mu.Lock()
	f.runs = append(f.runs, run)
	f.mu.Unlock()
	go func() {
		run.done <- runOfflineUIDriverClient(ctx, offlineUIDriverHarness{
			socket:     f.broker.socket,
			terminal:   terminal,
			ui:         ui,
			log:        discardLog(),
			navigation: navigation,
			resolver:   resolver,
			stream:     run.stream,
		})
	}()
	return run
}

// localSessionNames returns the local session names the fixture broker currently
// publishes. It is the session-scoped proof of what a driver did: unlike the
// logical-stream signal, it is unaffected by the broker's own observation dials.
func (f *uiDriverBrokerFixture) localSessionNames(t *testing.T) []string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), brokerTestWait)
	defer cancel()
	service, err := brokeripc.NewConnector(f.broker.socket, brokeripc.Config{}).Connect(ctx)
	require.NoError(t, err)
	defer service.Close()
	f.waitEpoch(t, service)
	observation, ok := localBrokerObservation(service.Snapshot())
	if !ok {
		return nil
	}
	names := make([]string, 0, len(observation.Sessions))
	for _, session := range observation.Sessions {
		names = append(names, session.Name)
	}
	return names
}

// epoch returns the fixture broker's current committed epoch.
func (f *uiDriverBrokerFixture) epoch(t *testing.T) ports.BrokerEpoch {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), brokerTestWait)
	defer cancel()
	service, err := brokeripc.NewConnector(f.broker.socket, brokeripc.Config{}).Connect(ctx)
	require.NoError(t, err)
	defer service.Close()
	return f.waitEpoch(t, service)
}

// waitEpoch waits for one broker connection's first committed publication and
// returns its epoch.
func (f *uiDriverBrokerFixture) waitEpoch(t *testing.T, service ports.BrokerService) ports.BrokerEpoch {
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
			t.Fatal("the fixture broker never published a snapshot")
		}
	}
}

// Ready reads this run's single discovery response.
func (r *uiDriverFixtureRun) Ready(t *testing.T) uidriver.Ready {
	t.Helper()
	return r.stream.awaitReady(t)
}

// Terminal returns the run's virtual terminal.
func (r *uiDriverFixtureRun) Terminal() *uiterm.Terminal { return r.terminal }

// EOF closes this run's JSONL stream and joins it. Closing twice is safe: the
// stream owns its work exactly once, so a duplicate EOF can never end another
// run.
func (r *uiDriverFixtureRun) EOF(t *testing.T) {
	t.Helper()
	require.NoError(t, r.stream.Close())
	select {
	case err := <-r.done:
		require.NoError(t, ignoreContextCancellation(err))
	case <-time.After(brokerTestWait):
		t.Fatal("the UI driver did not stop after its stream closed")
	}
}

// capture performs one capture over the run's own JSONL stream.
func (r *uiDriverFixtureRun) capture(t *testing.T, attachment string) map[string]any {
	t.Helper()
	return r.stream.awaitCapture(t, attachment)
}

// awaitAttachedSnapshot waits on the UI owner's broadcast until one coherent
// attached publication carries the expected target, a usable generation, and
// the expected rendered content. Assertions must use the returned snapshot.
func (r *uiDriverFixtureRun) awaitAttachedSnapshot(t *testing.T, attachment, session, text string) ports.UISnapshot {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), brokerTestWait)
	defer cancel()
	status := ports.UIStatusAttached
	// The public UI-driver session predicate is an exact lifecycle fence: a
	// session name by itself is intentionally invalid. Creation does not know
	// that lifecycle until the attached publication, so wait on the other
	// committed predicates and assert the requested creation name afterward.
	expect := ports.UIExpect{Status: &status, TextContains: &text}
	_, err := r.ui.Wait(ctx, ports.UIWaitRequest{Attachment: attachment, Expect: expect, Timeout: brokerTestWait})
	require.NoError(t, err, "the UI driver never published the expected attached snapshot")
	snapshot, err := r.ui.Capture(attachment)
	require.NoError(t, err)
	require.Equal(t, ports.UIStatusAttached, snapshot.Context.Status)
	require.NotZero(t, snapshot.Context.Generation)
	require.NotEqual(t, domain.SessionLifecycleID{}, snapshot.Context.Route.Target.LifecycleID)
	if session != "" {
		require.Equal(t, session, snapshot.Context.Route.Target.SessionName)
	}
	require.Contains(t, offlineSnapshotText(snapshot), text)
	return snapshot
}

// keys sends one key batch at the given generation and returns its action status.
// An accepted batch whose boundary was lost is reported as outcome_unknown, which
// is an honest terminal status and not a refusal.
func (r *uiDriverFixtureRun) keys(t *testing.T, attachment string, generation uint64, values ...string) ports.UIActionStatus {
	t.Helper()
	id := r.stream.nextID()
	require.NoError(t, r.stream.writeRequest(map[string]any{
		"version": 1, "id": id, "op": "keys", "attachment": attachment,
		"generation": generation, "keys": values,
	}))
	envelope := r.stream.awaitEnvelope(t, id)
	if envelope.Error != nil {
		require.True(t, envelope.Error.Accepted, "the driver refused a key batch: %+v", envelope.Error)
		require.Equal(t, ports.UIErrOutcomeUnknown, envelope.Error.Code, "the key batch failed for another reason: %+v", envelope.Error)
		return ports.UIActionOutcomeUnknown
	}
	var result struct {
		Status ports.UIActionStatus `json:"status"`
	}
	require.NoError(t, remarshal(envelope.Result, &result))
	return result.Status
}

// presentationStatus returns the run's currently published presentation status.
func (r *uiDriverFixtureRun) presentationStatus(t *testing.T, attachment string) ports.UIPresentationStatus {
	t.Helper()
	snapshot, err := r.ui.Capture(attachment)
	require.NoError(t, err)
	return snapshot.Context.Status
}

// text sends one typed batch at the given generation and returns its action
// status.
func (r *uiDriverFixtureRun) text(t *testing.T, attachment string, generation uint64, value string) ports.UIActionStatus {
	t.Helper()
	id := r.stream.nextID()
	require.NoError(t, r.stream.writeRequest(map[string]any{
		"version": 1, "id": id, "op": "text", "attachment": attachment,
		"generation": generation, "text": value,
	}))
	envelope := r.stream.awaitEnvelope(t, id)
	require.Nil(t, envelope.Error, "the driver refused a typed batch: %+v", envelope.Error)
	var result struct {
		Status ports.UIActionStatus `json:"status"`
	}
	require.NoError(t, remarshal(envelope.Result, &result))
	return result.Status
}

// stopRuns ends every run the fixture started, before the broker and daemon it
// created are torn down.
func (f *uiDriverBrokerFixture) stopRuns() {
	if f == nil {
		return
	}
	f.mu.Lock()
	if f.closed {
		f.mu.Unlock()
		return
	}
	f.closed = true
	runs := append([]*uiDriverFixtureRun(nil), f.runs...)
	f.mu.Unlock()
	for _, run := range runs {
		_ = run.stream.Close()
		run.cancel()
		run.terminal.Close()
		select {
		case <-run.done:
		case <-time.After(brokerTestWait):
		}
	}
}
