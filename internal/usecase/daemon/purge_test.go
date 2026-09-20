package daemon

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	portsmocks "github.com/bnema/vev/internal/ports/mocks"
	"github.com/bnema/vev/internal/protocol"
	recoveryusecase "github.com/bnema/vev/internal/usecase/recovery"
	snapcodec "github.com/bnema/vev/internal/usecase/snapshot"
)

// selectivePurgeRepository fails incarnation deletion only for the configured
// lifecycles, so a purge can succeed for some records and fail for others. It
// can also run a one-shot onDelete hook to interleave a state change (such as
// an explicit shutdown) after the first destructive purge unit.
type selectivePurgeRepository struct {
	noOpSnapshotRepository

	mu       sync.Mutex
	fail     map[domain.IncarnationID]error
	calls    []domain.IncarnationID
	onDelete func()
	once     sync.Once
}

func (r *selectivePurgeRepository) DeleteIncarnation(_ context.Context, id domain.IncarnationID) error {
	r.mu.Lock()
	r.calls = append(r.calls, id)
	err := r.fail[id]
	hook := r.onDelete
	r.mu.Unlock()
	if hook != nil {
		r.once.Do(hook)
	}
	return err
}

func newSelectivePurgeRepository(fail map[domain.IncarnationID]error) *selectivePurgeRepository {
	return &selectivePurgeRepository{fail: fail}
}

// TestPurgeAllRemovesLiveAndInactiveAndKeepsDaemonActive is the KillAll
// survival contract: every live, stopped, and broken record is removed, the
// daemon-wide shutdown facts are untouched, and the same daemon creates and
// drives new named and ephemeral sessions.
func TestPurgeAllRemovesLiveAndInactiveAndKeepsDaemonActive(t *testing.T) {
	sz := domain.Size{Cols: 80, Rows: 24}
	seedPTY, releaseSeed := newBlockingPTY(t)
	writes := make(chan []byte, 8)
	afterPTY, releaseAfter := newBlockingPTYWithWrites(t, writes)
	tabPTY, releaseTab := newBlockingPTY(t)
	d := newTestDaemon(t, newFactorySeq(t, seedPTY, afterPTY, tabPTY), stubClock{})

	seed, err := createSessionForTest(d, "0", true, "/tmp", sz, terminalEnv{}, nil)
	require.NoError(t, err)
	require.NotNil(t, seed)
	d.mu.Lock()
	d.inactive["stopped"] = inactiveSession{name: "stopped", state: protocol.SessionDown, incarnation: domain.IncarnationID{7}}
	d.inactive["broken"] = inactiveSession{name: "broken", state: protocol.SessionBroken, incarnation: domain.IncarnationID{8}}
	d.mu.Unlock()

	result := d.purgeAllSessions(protocol.ReasonSessionKilled)
	require.False(t, result.Failed(), "purge failures: %v", result.summary())
	require.Equal(t, 0, sessionCount(d))
	requirePurgeAdmissionBalanced(t, d)
	d.mu.Lock()
	require.Empty(t, d.inactive, "purge must remove stopped and broken records")
	closing := d.closing
	d.mu.Unlock()
	require.False(t, closing, "KillAll must not mark the daemon closing")
	d.moveLifecycleMu.Lock()
	moveClosing := d.moveLifecycleClosing
	d.moveLifecycleMu.Unlock()
	require.False(t, moveClosing, "KillAll must not close move admission")
	select {
	case <-d.done:
		t.Fatal("KillAll must not close done")
	default:
	}
	select {
	case <-d.paneProcessCtx.Done():
		t.Fatal("KillAll must not cancel the daemon-wide pane process context")
	default:
	}

	// The same daemon admits a new named session, forwards PTY input, and opens
	// a tab through the ordinary key-driven command API.
	tr, sends, releaseConn := newConn(t,
		mustHello(protocol.IntentNew, "after", sz),
		frameInput([]byte("ping\n")),
		frameInput([]byte("\x1b ")),
		frameInput([]byte("CNT\r")),
	)
	var hg sync.WaitGroup
	hg.Go(func() { d.handleConn(tr) })
	awaitFrame(t, sends, "Welcome")
	awaitFrame(t, sends, "Output")

	require.Eventually(t, func() bool {
		select {
		case written := <-writes:
			return bytes.Contains(written, []byte("ping"))
		default:
			return false
		}
	}, 2*time.Second, time.Millisecond, "PTY input was not forwarded after the purge")
	require.Eventually(t, func() bool {
		sessions := listSessions(t, d)
		return len(sessions.Sessions) == 1 && sessions.Sessions[0].Tabs == 2
	}, 2*time.Second, time.Millisecond, "tab was not created after the purge")

	releaseConn()
	releaseSeed()
	releaseAfter()
	releaseTab()
	hg.Wait()
	d.sessWg.Wait()
	d.waitNotifies()
}

// TestPurgeAllReportsStructuredPartialFailures proves failed stopped and
// broken records are reported with their class and name without cancelling the
// purge of the healthy live sibling, and without stopping the daemon.
func TestPurgeAllReportsStructuredPartialFailures(t *testing.T) {
	repository := newSelectivePurgeRepository(map[domain.IncarnationID]error{
		{2}: errors.New("stopped delete failed"),
		{3}: errors.New("broken delete failed"),
	})
	d := newTestDaemon(t, portsmocks.NewMockPTYFactory(t), stubClock{})
	WithSnapshotRepository(repository)(d)
	store, _ := newMockStore(t)
	WithStore(t, store)(d)

	live := newSnapshotTestSession(t, "live", false, "/live")
	liveRecord := live.persistRecordLocked(1)
	require.NoError(t, d.catalogue.Create(liveRecord))
	livePTY, ok := live.tabs[0].panes["pane-1"].pty.(*portsmocks.MockPTY)
	require.True(t, ok)
	livePTY.EXPECT().Close().Return(nil).Once()
	d.sessions = map[domain.SessionID]*session{live.id: live}

	stoppedRecord := domain.CatalogueRecord{Name: "stopped", IncarnationID: domain.IncarnationID{2}, CreatedAt: 7}
	require.NoError(t, d.catalogue.Create(stoppedRecord))
	d.inactive["stopped"] = inactiveSessionFromRecord(stoppedRecord, protocol.SessionDown, nil)
	brokenRecord := domain.CatalogueRecord{Name: "broken", IncarnationID: domain.IncarnationID{3}, CreatedAt: 8}
	require.NoError(t, d.catalogue.Create(brokenRecord))
	// A broken registry state with a still-present durable record: the purge
	// must classify the failure as broken, not stopped.
	d.inactive["broken"] = inactiveSessionFromRecord(brokenRecord, protocol.SessionBroken, nil)

	result := d.purgeAllSessions(protocol.ReasonSessionKilled)
	require.True(t, result.Failed())
	require.Len(t, result.Failures, 2)
	require.Equal(t, purgeRecordStopped, result.Failures[0].Class)
	require.Equal(t, "stopped", result.Failures[0].Name)
	require.Equal(t, purgeRecordBroken, result.Failures[1].Class)
	require.Equal(t, "broken", result.Failures[1].Name)
	requirePurgeAdmissionBalanced(t, d)

	require.Equal(t, 0, sessionCount(d), "the healthy live session must still be purged")
	_, ok, err := d.catalogue.Record("live")
	require.NoError(t, err)
	require.False(t, ok)
	d.mu.Lock()
	_, stoppedRemains := d.inactive["stopped"]
	_, brokenRemains := d.inactive["broken"]
	closing := d.closing
	d.mu.Unlock()
	require.True(t, stoppedRemains, "a failed durable purge must leave its hidden record for retry")
	require.True(t, brokenRemains, "a failed broken purge must leave its hidden record for retry")
	require.False(t, closing, "a partial purge must not stop the daemon")
}

// TestPurgeAllReportsDurableOrphanFailure proves a durable catalogue record
// with no live/stopped/broken registry entry is purged and, when its deletion
// fails, reported as a durable-class failure.
func TestPurgeAllReportsDurableOrphanFailure(t *testing.T) {
	repository := newSelectivePurgeRepository(map[domain.IncarnationID]error{{9}: errors.New("orphan delete failed")})
	d := newTestDaemon(t, portsmocks.NewMockPTYFactory(t), stubClock{})
	WithSnapshotRepository(repository)(d)
	store, _ := newMockStore(t)
	WithStore(t, store)(d)

	orphan := domain.CatalogueRecord{Name: "orphan", IncarnationID: domain.IncarnationID{9}, CreatedAt: 3}
	require.NoError(t, d.catalogue.Create(orphan))

	result := d.purgeAllSessions(protocol.ReasonSessionKilled)
	require.True(t, result.Failed())
	require.Len(t, result.Failures, 1)
	require.Equal(t, purgeRecordDurable, result.Failures[0].Class)
	require.Equal(t, "orphan", result.Failures[0].Name)
}

// TestPurgeAdmissionGatesAndReopens proves the transient gate: while a purge
// holds admission a route waits and a move is rejected, and after the gate is
// reopened both proceed.
func TestPurgeAdmissionGatesAndReopens(t *testing.T) {
	routePTY, releaseRoute := newBlockingPTY(t)
	defer releaseRoute()
	d := newTestDaemon(t, newFactorySeq(t, routePTY), stubClock{})
	addControlSession(d, "work", "t_work", "p_work")
	addNamedMoveDestination(d, "dest", "t_dest", "p_dest")
	require.Equal(t, 2, sessionCount(d))

	beginTestPurgeAdmission(t, d)

	created := make(chan error, 1)
	go func() {
		_, _, err := d.route(helloResumeCapable(protocol.IntentEphemeral, "", 0), &closeTrackingTransport{})
		created <- err
	}()
	select {
	case err := <-created:
		t.Fatalf("route bypassed the closed purge admission gate: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	require.Equal(t, 2, sessionCount(d), "gated creation must not insert a session")

	rejected := sendCommand(t, d, protocol.CommandRequest{
		Slug: "move-pane", Args: []string{"dest", "t_dest"}, Self: true,
		TargetSession: "work", TargetTab: "t_work", TargetPane: "p_work",
	})
	require.False(t, rejected.Outcome == protocol.CommandSucceeded, "a move during the purge must be rejected")

	d.endPurgeAdmission()
	require.NoError(t, awaitTestValue(t, created, "route did not resume after purge admission reopened"))
	require.Equal(t, 3, sessionCount(d), "admission must reopen for creation")

	accepted := sendCommand(t, d, protocol.CommandRequest{
		Slug: "move-pane", Args: []string{"dest", "t_dest"}, Self: true,
		TargetSession: "work", TargetTab: "t_work", TargetPane: "p_work",
	})
	require.True(t, accepted.Outcome == protocol.CommandSucceeded, accepted.Text)
}

// TestPurgeAllLinearizesConcurrentCreate races KillAll against a route create.
// The gate drains the create before its exact snapshot or admits it after, so
// the create never interleaves with the purge and the daemon stays active.
func TestPurgeAllLinearizesConcurrentCreate(t *testing.T) {
	sz := domain.Size{Cols: 80, Rows: 24}
	for attempt := 0; attempt < 24; attempt++ {
		seedPTY, releaseSeed := newBlockingPTY(t)
		routePTY, releaseRoute := newBlockingPTY(t)
		d := newTestDaemon(t, newFactorySeq(t, seedPTY, routePTY), stubClock{})

		seed, err := createSessionForTest(d, "0", true, "/tmp", sz, terminalEnv{}, nil)
		require.NoError(t, err)
		require.NotNil(t, seed)

		start := make(chan struct{})
		var wg sync.WaitGroup
		wg.Add(2)
		var createErr error
		go func() {
			defer wg.Done()
			<-start
			_, _, createErr = d.route(helloResumeCapable(protocol.IntentEphemeral, "", 0), &closeTrackingTransport{})
		}()
		go func() {
			defer wg.Done()
			<-start
			d.purgeAllSessions(protocol.ReasonSessionKilled)
		}()
		close(start)
		wg.Wait()

		require.NoError(t, createErr, "attempt %d: creation racing KillAll must be admitted, not rejected", attempt)
		count := sessionCount(d)
		require.LessOrEqual(t, count, 1, "attempt %d: purge must not leak sessions", attempt)
		d.mu.Lock()
		closing := d.closing
		d.mu.Unlock()
		require.False(t, closing, "attempt %d: KillAll must not stop the daemon", attempt)
		select {
		case <-d.done:
			t.Fatalf("attempt %d: KillAll closed done", attempt)
		default:
		}

		killAllSessions(d)
		releaseSeed()
		releaseRoute()
		d.sessWg.Wait()
		d.waitNotifies()
	}
}

// TestPurgeAllLinearizesConcurrentRestore races KillAll against a resume of the
// only stopped record. The restore is either purged with the set or ordered
// after it, and the daemon stays active either way.
func TestPurgeAllLinearizesConcurrentRestore(t *testing.T) {
	sz := domain.Size{Cols: 80, Rows: 24}
	for attempt := 0; attempt < 16; attempt++ {
		seedPTY, releaseSeed := newBlockingPTY(t)
		restorePTY, releaseRestore := newBlockingPTY(t)
		d := newTestDaemon(t, newFactorySeq(t, seedPTY, restorePTY), stubClock{})

		seed, err := createSessionForTest(d, "work", false, "/tmp", sz, terminalEnv{}, nil)
		require.NoError(t, err)
		require.NoError(t, d.killSession(seed, protocol.ReasonServerShutdown, false))
		require.Equal(t, 0, sessionCount(d))
		d.mu.Lock()
		_, stopped := d.inactive["work"]
		d.mu.Unlock()
		require.True(t, stopped)

		start := make(chan struct{})
		var wg sync.WaitGroup
		wg.Add(2)
		var restoreErr error
		go func() {
			defer wg.Done()
			<-start
			_, _, restoreErr = d.route(helloResumeCapable(protocol.IntentAttach, "work", 0), &closeTrackingTransport{})
		}()
		go func() {
			defer wg.Done()
			<-start
			d.purgeAllSessions(protocol.ReasonSessionKilled)
		}()
		close(start)
		wg.Wait()

		if restoreErr != nil {
			var protoFailure *protoErr
			require.ErrorAs(t, restoreErr, &protoFailure)
			require.Equal(t, protocol.ErrNoSuchSession, protoFailure.code)
		}
		require.LessOrEqual(t, sessionCount(d), 1, "attempt %d: purge must not leak sessions", attempt)
		d.mu.Lock()
		closing := d.closing
		d.mu.Unlock()
		require.False(t, closing, "attempt %d: KillAll must not stop the daemon", attempt)

		killAllSessions(d)
		releaseSeed()
		releaseRestore()
		d.sessWg.Wait()
		d.waitNotifies()
	}
}

// TestShutdownAllStillStopsDaemonWithSessionsClientsAndWorkers proves the
// explicit daemon-stop operation is unchanged: it removes the session, signals
// Serve, marks closing, closes move admission, and cancels the daemon-wide pane
// process context.
func TestShutdownAllStillStopsDaemonWithSessionsClientsAndWorkers(t *testing.T) {
	sz := domain.Size{Cols: 80, Rows: 24}
	pty, releasePTY := newBlockingPTY(t)
	d := newTestDaemon(t, newFactorySeq(t, pty), stubClock{})

	tr, sends, releaseConn := newConn(t, mustHello(protocol.IntentNew, "work", sz))
	var hg sync.WaitGroup
	hg.Go(func() { d.handleConn(tr) })
	awaitFrame(t, sends, "Welcome")
	awaitFrame(t, sends, "Output")
	require.Equal(t, 1, sessionCount(d))

	require.False(t, d.shutdownAll(protocol.ReasonServerShutdown))
	require.Equal(t, 0, sessionCount(d))
	select {
	case <-d.done:
	default:
		t.Fatal("explicit daemon stop must signal Serve")
	}
	select {
	case <-d.paneProcessCtx.Done():
	default:
		t.Fatal("explicit daemon stop must cancel the daemon-wide pane process context")
	}
	d.mu.Lock()
	closing := d.closing
	d.mu.Unlock()
	require.True(t, closing)
	d.moveLifecycleMu.Lock()
	moveClosing := d.moveLifecycleClosing
	d.moveLifecycleMu.Unlock()
	require.True(t, moveClosing)

	releaseConn()
	releasePTY()
	hg.Wait()
	d.sessWg.Wait()
	d.waitNotifies()
}

// beginTestPurgeAdmission closes the KillAll admission gate exactly as a purge
// does once its drain succeeds. It fails the test if the gate could not be
// granted (an admitted transition still holding it, shutdown, or cancellation).
func beginTestPurgeAdmission(t *testing.T, d *Daemon) {
	t.Helper()
	if status := d.beginPurgeAdmission(context.Background(), nil); status != purgeAdmissionGranted {
		t.Fatalf("test purge admission not granted: status %d", status)
	}
}

// killAllSessions removes every remaining live session without asserting on the
// result; it exists to release test PTYs deterministically.
func killAllSessions(d *Daemon) {
	d.mu.Lock()
	sessions := d.sessionsSnapshotLocked()
	d.mu.Unlock()
	for _, sess := range sessions {
		_ = d.killSession(sess, protocol.ReasonSessionKilled, true)
	}
}

// gatedCheckpointRepository blocks LoadCheckpoint until released, so a test can
// hold a restoration worker inside its admitted lifespan deterministically and
// observe KillAll admission against it.
type gatedCheckpointRepository struct {
	noOpSnapshotRepository
	generation ports.SnapshotGeneration
	entered    chan struct{}
	release    chan struct{}
	once       sync.Once
}

func newGatedCheckpointRepository(generation ports.SnapshotGeneration) *gatedCheckpointRepository {
	return &gatedCheckpointRepository{generation: generation, entered: make(chan struct{}), release: make(chan struct{})}
}

func (r *gatedCheckpointRepository) LoadCheckpoint(ctx context.Context, _ domain.IncarnationID, _ string, _ ports.CheckpointRef) (ports.SnapshotGeneration, error) {
	r.once.Do(func() { close(r.entered) })
	select {
	case <-r.release:
	case <-ctx.Done():
		return ports.SnapshotGeneration{}, ctx.Err()
	}
	return r.generation, nil
}

// syncedLogBuffer collects log output for assertions without racing the
// daemon's asynchronous sends.
type syncedLogBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncedLogBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncedLogBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// TestPurgeAllWaitsForAdmittedRestoreThenPurges is the deterministic gated
// checkpoint contract: KillAll blocks while a startup restoration worker holds
// purge admission inside its load, then purges the entire exact set once the
// restore settles. No phantom live session, stopped authority, or taken name
// may survive.
func TestPurgeAllWaitsForAdmittedRestoreThenPurges(t *testing.T) {
	snapshot := restoreAcceptanceSession(t, "work")
	generation := acceptanceGeneration(t, snapshot, 9)
	repository := newGatedCheckpointRepository(generation)
	pty, releasePTY := newBlockingPTY(t)
	defer releasePTY()

	d := newTestDaemon(t, newFactorySeq(t, pty), stubClock{})
	d.baseEnv = []string{"TERM=xterm-256color"}
	checkpoint := domain.CheckpointRef{Generation: generation.Generation, ManifestDigest: snapcodec.ManifestDigest(generation.Manifest)}
	record := domain.CatalogueRecord{Name: snapshot.Name, IncarnationID: generation.IncarnationID, Cwd: "/snapshot/cwd", CreatedAt: int64(snapshot.CreatedAt), Committed: &checkpoint}
	catalogue := newDurableRecoveryCatalogue([]domain.CatalogueRecord{record})
	WithCatalogue(catalogue, []domain.CatalogueRecord{record})(d)
	WithSnapshotRepository(repository)(d)
	WithRecoveryCoordinator(recoveryusecase.NewCoordinator(catalogue, repository, nil))(d)

	restoreFinished := make(chan struct{})
	go func() {
		defer close(restoreFinished)
		d.restoreIncrementalSnapshots(context.Background())
	}()
	<-repository.entered

	purged := make(chan purgeResult, 1)
	go func() { purged <- d.purgeAllSessions(protocol.ReasonSessionKilled) }()
	select {
	case result := <-purged:
		t.Fatalf("KillAll returned while an admitted restore was still loading: %v", result.summary())
	case <-time.After(100 * time.Millisecond):
	}

	close(repository.release)
	<-restoreFinished
	result := awaitTestValue(t, purged, "KillAll did not complete after the admitted restore settled")
	require.False(t, result.Superseded())
	require.False(t, result.Failed(), result.summary())

	require.Equal(t, 0, sessionCount(d), "no live authority may survive the purge")
	d.mu.Lock()
	_, stopped := d.inactive["work"]
	closing := d.closing
	d.mu.Unlock()
	require.False(t, stopped, "no phantom stopped authority may survive the purge")
	require.False(t, closing, "KillAll must leave the daemon serving")
	select {
	case <-d.done:
		t.Fatal("KillAll must not signal Serve")
	default:
	}
	_, ok, err := d.catalogue.Record("work")
	require.NoError(t, err)
	require.False(t, ok, "durable authority must be purged")

	// The purged name is reusable by the same daemon.
	d.mu.Lock()
	taken := d.nameLiveOrStoppedLocked("work")
	d.mu.Unlock()
	require.False(t, taken, "the purged name must not remain taken")
	d.sessWg.Wait()
	d.waitNotifies()
}

// TestRestoreCatalogueAbandonsRecordsSupersededByPurge proves the epoch
// revalidation: a restoration that captured records before a completed KillAll
// must not resurrect them from the pre-purge record set.
func TestRestoreCatalogueAbandonsRecordsSupersededByPurge(t *testing.T) {
	record := durableRecoveryRecord(1)
	record.Name = "work"
	catalogue := newDurableRecoveryCatalogue([]domain.CatalogueRecord{record})
	d := newTestDaemon(t, portsmocks.NewMockPTYFactory(t), stubClock{})
	WithCatalogue(catalogue, []domain.CatalogueRecord{record})(d)
	WithRecoveryCoordinator(recoveryusecase.NewCoordinator(catalogue, noOpSnapshotRepository{}, nil))(d)

	epoch := d.purgeEpochSnapshot()
	result := d.purgeAllSessions(protocol.ReasonSessionKilled)
	require.False(t, result.Failed(), result.summary())
	_, ok, err := catalogue.Record("work")
	require.NoError(t, err)
	require.False(t, ok, "the purge must remove the durable record")

	d.restoreCatalogueEpoch(context.Background(), []domain.CatalogueRecord{record}, epoch)
	require.Equal(t, 0, sessionCount(d))
	d.mu.Lock()
	_, stopped := d.inactive["work"]
	d.mu.Unlock()
	require.False(t, stopped, "a superseded record must not be resurrected as stopped authority")
	d.sessWg.Wait()
	d.waitNotifies()
}

// TestEnsureCatalogueRegistryEntryRejectsSupersededRestore proves both the
// in-progress gate and the completed-purge epoch are revalidated before a
// restore publishes any registry entry.
func TestEnsureCatalogueRegistryEntryRejectsSupersededRestore(t *testing.T) {
	d := newTestDaemon(t, portsmocks.NewMockPTYFactory(t), stubClock{})
	record := durableRecoveryRecord(1)
	record.Name = "work"

	prePurgeEpoch := d.purgeEpochSnapshot()
	beginTestPurgeAdmission(t, d)
	done, ok := d.ensureCatalogueRegistryEntry(record, prePurgeEpoch)
	require.False(t, ok, "a record must not be admitted while a purge owns the gate")
	require.Nil(t, done)
	d.endPurgeAdmission()

	// A worker that waited out the purge still holds a stale boundary: the
	// record set it captured was superseded, so it must stay abandoned.
	done, ok = d.ensureCatalogueRegistryEntry(record, prePurgeEpoch)
	require.False(t, ok, "a record captured before a completed purge must stay superseded")
	require.Nil(t, done)

	// A fresh boundary (post-purge) admits the record.
	done, ok = d.ensureCatalogueRegistryEntry(record, d.purgeEpochSnapshot())
	require.True(t, ok)
	require.NotNil(t, done)
	d.mu.Lock()
	_, exists := d.inactive["work"]
	d.mu.Unlock()
	require.True(t, exists)
}

// TestRestoreCatalogueCancellationDrainsAdmission proves the shutdown interlock:
// a restoration worker blocked on purge admission observes context
// cancellation, drains its job queue without touching the registry, and lets
// the producer and the worker join.
func TestRestoreCatalogueCancellationDrainsAdmission(t *testing.T) {
	d := newTestDaemon(t, portsmocks.NewMockPTYFactory(t), stubClock{})
	record := durableRecoveryRecord(1)
	record.Name = "work"

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	beginTestPurgeAdmission(t, d)
	go func() {
		defer close(done)
		d.restoreCatalogueEpoch(ctx, []domain.CatalogueRecord{record}, d.purgeEpochSnapshot())
	}()
	// The worker is parked on admission; cancellation must release it even
	// though the purge gate is still closed.
	cancel()
	awaitTestValue(t, done, "restoration did not drain after cancellation")
	d.endPurgeAdmission()

	d.mu.Lock()
	_, exists := d.inactive["work"]
	d.mu.Unlock()
	require.False(t, exists, "a cancelled restoration must not register a stopped entry")
}

// TestPurgeAllSerializesConcurrentKillAll proves purgeAllMu admits one KillAll
// owner at a time: a second concurrent purge cannot enter the critical section
// until the first releases it.
func TestPurgeAllSerializesConcurrentKillAll(t *testing.T) {
	d := newTestDaemon(t, portsmocks.NewMockPTYFactory(t), stubClock{})
	entered := make(chan struct{})
	releaseFirst := make(chan struct{})
	var calls atomic.Int32
	d.afterPurgeAdmission = func() {
		if calls.Add(1) == 1 {
			close(entered)
			<-releaseFirst
		}
	}

	first := make(chan purgeResult, 1)
	go func() { first <- d.purgeAllSessions(protocol.ReasonSessionKilled) }()
	awaitTestValue(t, entered, "first purge never entered its critical section")

	second := make(chan purgeResult, 1)
	go func() { second <- d.purgeAllSessions(protocol.ReasonSessionKilled) }()
	select {
	case <-second:
		t.Fatal("a second KillAll entered the critical section while the first still owned it")
	case <-time.After(100 * time.Millisecond):
	}
	close(releaseFirst)
	require.False(t, awaitTestValue(t, first, "first purge did not finish").Failed())
	require.False(t, awaitTestValue(t, second, "second purge did not run after the first released").Failed())
	require.Equal(t, int32(2), calls.Load())
}

// TestCommandPathPurgeAdmissionMapsToRetryableError proves a command-path
// create rejected by the purge gate surfaces a deterministic, retryable result
// rather than silently succeeding or reusing a name verdict.
func TestCommandPathPurgeAdmissionMapsToRetryableError(t *testing.T) {
	routePTY, releaseRoute := newBlockingPTY(t)
	defer releaseRoute()
	d := newTestDaemon(t, newFactorySeq(t, routePTY), stubClock{})
	addControlSession(d, "work", "t_work", "p_work")

	beginTestPurgeAdmission(t, d)
	result := sendCommand(t, d, protocol.CommandRequest{
		Slug: "new-session", Args: []string{"fresh"}, Self: true,
		TargetSession: "work", TargetTab: "t_work", TargetPane: "p_work",
	})
	require.False(t, result.Outcome == protocol.CommandSucceeded)
	require.Equal(t, protocol.ErrInternal, result.Code)
	require.Equal(t, errPurgeAdmissionClosed.Error(), result.Text)
	d.endPurgeAdmission()
}

// TestRemoteTargetRouteWaitsForPurgeAdmission proves an exact remote-target
// handoff is admitted like a local route: it blocks on the closed purge gate,
// then proceeds after the gate reopens.
func TestRemoteTargetRouteWaitsForPurgeAdmission(t *testing.T) {
	sz := domain.Size{Cols: 80, Rows: 24}
	pty, releasePTY := newBlockingPTY(t)
	defer releasePTY()
	d := newTestDaemon(t, newFactorySeq(t, pty), stubClock{})
	sess, err := createSessionForTest(d, "work", false, "/tmp", sz, terminalEnv{}, nil)
	require.NoError(t, err)
	sess.mu.Lock()
	incarnation := sess.incarnation
	tabID := sess.tabs[0].stableID
	sess.mu.Unlock()
	target := domain.RemoteSessionTarget{Endpoint: "arch", DisplayOrigin: "arch", LifecycleID: incarnation, SessionName: "work", LiveTabID: domain.TabStableID(tabID)}
	tr, _ := newCapturingTransport(t)

	beginTestPurgeAdmission(t, d)
	result := make(chan error, 1)
	go func() {
		_, _, routeErr := d.routeWithContext(context.Background(), protocol.Hello{
			Version: protocol.Version, Intent: protocol.IntentAttach, Name: "work", Size: sz,
			RemoteTarget: &target, EnvironmentPolicy: protocol.EnvironmentPolicyDaemonOwned,
		}, tr)
		result <- routeErr
	}()
	select {
	case routeErr := <-result:
		t.Fatalf("remote target route bypassed the closed purge gate: %v", routeErr)
	case <-time.After(50 * time.Millisecond):
	}
	d.endPurgeAdmission()
	require.NoError(t, awaitTestValue(t, result, "remote target route did not resume after the gate reopened"))

	_ = d.killSession(sess, protocol.ReasonSessionKilled, true)
	releasePTY()
	d.sessWg.Wait()
	d.waitNotifies()
}

// TestConcurrentCreateAndExplicitShutdownInterlock restores a real concurrent
// create-versus-explicit-shutdown test: whichever side wins the registry lock,
// no session leaks, the daemon is irreversibly closing and signaled, and later
// Hellos are rejected.
func TestConcurrentCreateAndExplicitShutdownInterlock(t *testing.T) {
	for attempt := 0; attempt < 24; attempt++ {
		pty, releasePTY := newBlockingPTY(t)
		d := newTestDaemon(t, newFactorySeq(t, pty), stubClock{})

		start := make(chan struct{})
		var wg sync.WaitGroup
		var createErr error
		wg.Add(2)
		go func() {
			defer wg.Done()
			<-start
			_, _, createErr = d.route(helloResumeCapable(protocol.IntentEphemeral, "", 0), &closeTrackingTransport{})
		}()
		go func() {
			defer wg.Done()
			<-start
			d.shutdownAll(protocol.ReasonServerShutdown)
		}()
		close(start)
		wg.Wait()

		if createErr != nil {
			var protoFailure *protoErr
			require.ErrorAs(t, createErr, &protoFailure, "attempt %d", attempt)
			require.Equal(t, protocol.ErrServerShutdown, protoFailure.code, "attempt %d", attempt)
		}
		require.Equal(t, 0, sessionCount(d), "attempt %d: explicit shutdown must not leak a session", attempt)
		d.mu.Lock()
		closing := d.closing
		d.mu.Unlock()
		require.True(t, closing, "attempt %d: explicit shutdown must mark closing", attempt)
		select {
		case <-d.done:
		default:
			t.Fatalf("attempt %d: explicit shutdown must signal Serve", attempt)
		}
		_, _, afterErr := d.route(helloResumeCapable(protocol.IntentEphemeral, "", 0), &closeTrackingTransport{})
		require.Error(t, afterErr, "attempt %d: a Hello after shutdown must be rejected", attempt)

		releasePTY()
		d.sessWg.Wait()
		d.waitNotifies()
	}
}

// TestPurgeAllSupersededByExplicitShutdownPreservesStopAuthority proves an
// explicit daemon stop wins over a KillAll that has not started removing
// anything: the stopped authority the stop preserved is left intact.
func TestPurgeAllSupersededByExplicitShutdownPreservesStopAuthority(t *testing.T) {
	sz := domain.Size{Cols: 80, Rows: 24}
	pty, releasePTY := newBlockingPTY(t)
	defer releasePTY()
	d := newTestDaemon(t, newFactorySeq(t, pty), stubClock{})
	catalogue := newDurableRecoveryCatalogue(nil)
	WithCatalogue(catalogue, nil)(d)
	WithRecoveryCoordinator(recoveryusecase.NewCoordinator(catalogue, noOpSnapshotRepository{}, bytes.NewReader(bytes.Repeat([]byte{0x5a}, 32))))(d)

	_, err := createSessionForTest(d, "work", false, "/tmp", sz, terminalEnv{}, nil)
	require.NoError(t, err)
	require.False(t, d.shutdownAll(protocol.ReasonServerShutdown))
	require.Equal(t, 0, sessionCount(d))

	result := d.purgeAllSessions(protocol.ReasonSessionKilled)
	require.True(t, result.Superseded())
	d.mu.Lock()
	_, stopped := d.inactive["work"]
	d.mu.Unlock()
	require.True(t, stopped, "explicit stop must preserve stopped authority")
	_, ok, err := catalogue.Record("work")
	require.NoError(t, err)
	require.True(t, ok, "explicit stop must preserve durable authority")

	releasePTY()
	d.sessWg.Wait()
	d.waitNotifies()
}

// TestPurgeAllLogsFailedControlSend proves a purge failure result that cannot be
// delivered is logged, so a client that never observes the explicit correlated
// KillResult cannot silently infer success from a close.
func TestPurgeAllLogsFailedControlSend(t *testing.T) {
	repository := newSelectivePurgeRepository(map[domain.IncarnationID]error{{2}: errors.New("delete failed")})
	d := newTestDaemon(t, portsmocks.NewMockPTYFactory(t), stubClock{})
	var logged syncedLogBuffer
	d.log = slog.New(slog.NewTextHandler(&logged, nil))
	WithSnapshotRepository(repository)(d)
	store, _ := newMockStore(t)
	WithStore(t, store)(d)

	record := domain.CatalogueRecord{Name: "stopped", IncarnationID: domain.IncarnationID{2}, CreatedAt: 7}
	require.NoError(t, d.catalogue.Create(record))
	d.inactive["stopped"] = inactiveSessionFromRecord(record, protocol.SessionDown, nil)

	tr := newMockServerConnection(t)
	tr.EXPECT().Send(mock.Anything).Return(errors.New("client gone")).Maybe()
	tr.EXPECT().Close().Return(nil).Maybe()
	d.handleKill(tr, protocol.Kill{RequestID: 1, Scope: protocol.KillAll})

	require.Contains(t, logged.String(), "kill result")
	require.Contains(t, logged.String(), "client gone")
	require.Contains(t, logged.String(), "outcome="+strconv.Itoa(int(protocol.KillFailed)), "the send-failure log must name the unobserved outcome")
	require.Contains(t, logged.String(), "code="+strconv.Itoa(int(protocol.ErrInternal)), "the send-failure log must name the unobserved code")
}

// TestPurgeResultSummaryBoundsWireDetail proves the partial-failure summary is
// wire-safe: it reports the total count, only the first N failures, and how
// many were omitted.
func TestPurgeResultSummaryBoundsWireDetail(t *testing.T) {
	result := purgeResult{}
	for i := 0; i < purgeSummaryFailureLimit+3; i++ {
		result.Failures = append(result.Failures, purgeFailure{Class: purgeRecordLive, Name: fmt.Sprintf("s%02d", i), Err: errors.New("boom")})
	}
	summary := result.summary()
	require.Contains(t, summary, fmt.Sprintf("%d record(s) could not be purged", purgeSummaryFailureLimit+3))
	require.Contains(t, summary, "; 3 more omitted")
	require.Contains(t, summary, "s00")
	require.NotContains(t, summary, "s07", "failures beyond the bound must be omitted from the wire detail")
}

// TestPurgeAllPurgesSettledRestore proves the other half of the policy: when the
// admitted restore settles as a live session before KillAll begins, the purge's
// exact snapshot includes it and removes it like any other live session.
func TestPurgeAllPurgesSettledRestore(t *testing.T) {
	snapshot := restoreAcceptanceSession(t, "work")
	generation := acceptanceGeneration(t, snapshot, 4)
	repository := newGatedCheckpointRepository(generation)
	pty, releasePTY := newBlockingPTY(t)
	defer releasePTY()

	d := newTestDaemon(t, newFactorySeq(t, pty), stubClock{})
	d.baseEnv = []string{"TERM=xterm-256color"}
	checkpoint := domain.CheckpointRef{Generation: generation.Generation, ManifestDigest: snapcodec.ManifestDigest(generation.Manifest)}
	record := domain.CatalogueRecord{Name: snapshot.Name, IncarnationID: generation.IncarnationID, Cwd: "/snapshot/cwd", CreatedAt: int64(snapshot.CreatedAt), Committed: &checkpoint}
	catalogue := newDurableRecoveryCatalogue([]domain.CatalogueRecord{record})
	WithCatalogue(catalogue, []domain.CatalogueRecord{record})(d)
	WithSnapshotRepository(repository)(d)
	WithRecoveryCoordinator(recoveryusecase.NewCoordinator(catalogue, repository, nil))(d)

	restoreFinished := make(chan struct{})
	go func() {
		defer close(restoreFinished)
		d.restoreIncrementalSnapshots(context.Background())
	}()
	<-repository.entered
	close(repository.release)
	awaitTestValue(t, restoreFinished, "restore did not settle")
	require.Equal(t, 1, sessionCount(d), "the settled restore must be live before the purge")

	result := d.purgeAllSessions(protocol.ReasonSessionKilled)
	require.False(t, result.Superseded())
	require.False(t, result.Failed(), result.summary())
	require.Equal(t, 0, sessionCount(d), "the settled restore must be purged")
	_, ok, err := d.catalogue.Record("work")
	require.NoError(t, err)
	require.False(t, ok)
	d.sessWg.Wait()
	d.waitNotifies()
}

// TestPurgeAllConcurrentWithExplicitShutdown proves the KillAll-versus-KillDaemon
// interlock terminates safely: whichever side wins, the daemon irreversibly
// stops, no live session leaks, and Serve is signaled.
func TestPurgeAllConcurrentWithExplicitShutdown(t *testing.T) {
	sz := domain.Size{Cols: 80, Rows: 24}
	for attempt := 0; attempt < 16; attempt++ {
		pty, releasePTY := newBlockingPTY(t)
		d := newTestDaemon(t, newFactorySeq(t, pty), stubClock{})
		_, err := createSessionForTest(d, "work", false, "/tmp", sz, terminalEnv{}, nil)
		require.NoError(t, err)

		start := make(chan struct{})
		var wg sync.WaitGroup
		var result purgeResult
		wg.Add(2)
		go func() {
			defer wg.Done()
			<-start
			result = d.purgeAllSessions(protocol.ReasonSessionKilled)
		}()
		go func() {
			defer wg.Done()
			<-start
			d.shutdownAll(protocol.ReasonServerShutdown)
		}()
		close(start)
		wg.Wait()

		require.Equal(t, 0, sessionCount(d), "attempt %d: no live session may survive the race", attempt)
		d.mu.Lock()
		closing := d.closing
		d.mu.Unlock()
		require.True(t, closing, "attempt %d: the explicit stop must win", attempt)
		select {
		case <-d.done:
		default:
			t.Fatalf("attempt %d: the explicit stop must signal Serve", attempt)
		}
		if !result.Superseded() {
			require.False(t, result.Failed(), "attempt %d: %s", attempt, result.summary())
		}
		releasePTY()
		d.sessWg.Wait()
		d.waitNotifies()
	}
}

// cancellationIgnoringRepository blocks LoadCheckpoint until released and
// deliberately ignores context cancellation, modelling a repository call that
// never returns. It proves the purge admission drain and daemon shutdown stay
// bounded instead of waiting on an uncooperative transition.
type cancellationIgnoringRepository struct {
	noOpSnapshotRepository

	generation ports.SnapshotGeneration
	entered    chan struct{}
	release    chan struct{}
	once       sync.Once
}

func newCancellationIgnoringRepository(generation ports.SnapshotGeneration) *cancellationIgnoringRepository {
	return &cancellationIgnoringRepository{
		generation: generation,
		entered:    make(chan struct{}),
		release:    make(chan struct{}),
	}
}

func (r *cancellationIgnoringRepository) LoadCheckpoint(context.Context, domain.IncarnationID, string, ports.CheckpointRef) (ports.SnapshotGeneration, error) {
	r.once.Do(func() { close(r.entered) })
	<-r.release
	return r.generation, nil
}

// awaitPurgeGateClosed waits until a purge has closed the transient admission
// gate, so a test can interleave another transition deterministically without
// polling on a real timer.
func awaitPurgeGateClosed(t *testing.T, d *Daemon) {
	t.Helper()
	deadline := time.NewTimer(testWaitTimeout)
	defer deadline.Stop()
	for {
		d.mu.Lock()
		closed := d.purgeAdmissionClosing
		changed := d.purgeAdmissionChangeLocked()
		d.mu.Unlock()
		if closed {
			return
		}
		select {
		case <-changed:
		case <-deadline.C:
			t.Fatal("purge never closed admission")
		}
	}
}

// requirePurgeAdmissionBalanced asserts a completed KillAll left no outstanding
// create/restore reservation and re-opened the transient admission gate, so the
// daemon keeps admitting transitions after the purge.
func requirePurgeAdmissionBalanced(t *testing.T, d *Daemon) {
	t.Helper()
	d.mu.Lock()
	active := d.purgeAdmissionActive
	closing := d.purgeAdmissionClosing
	d.mu.Unlock()
	require.Zero(t, active, "a completed purge must leave no outstanding admission reservation")
	require.False(t, closing, "a completed purge must re-open purge admission")
}

// TestRestoreCatalogueCaptureOrdersAfterPurgeNotResurrect reproduces the
// capture-during-purge race deterministically: while a purge owns the admission
// gate (closed but not yet snapshotted), a catalogue capture must block on the
// gate. It can then only observe the post-purge catalogue, so it cannot
// resurrect the record the purge removed with a boundary that looks current.
func TestRestoreCatalogueCaptureOrdersAfterPurgeNotResurrect(t *testing.T) {
	record := durableRecoveryRecord(1)
	record.Name = "work"
	catalogue := newDurableRecoveryCatalogue([]domain.CatalogueRecord{record})
	d := newTestDaemon(t, portsmocks.NewMockPTYFactory(t), stubClock{})
	WithCatalogue(catalogue, []domain.CatalogueRecord{record})(d)
	WithRecoveryCoordinator(recoveryusecase.NewCoordinator(catalogue, noOpSnapshotRepository{}, nil))(d)

	// Hold one admitted transition so the purge closes the gate and parks in its
	// drain: the exact interleaving point where a capture previously slipped in.
	require.NoError(t, d.acquirePurgeAdmission(context.Background()))
	purged := make(chan purgeResult, 1)
	go func() { purged <- d.purgeAllSessions(protocol.ReasonSessionKilled) }()
	awaitPurgeGateClosed(t, d)

	captured := make(chan struct{})
	go func() {
		defer close(captured)
		d.restoreIncrementalSnapshots(context.Background())
	}()
	select {
	case <-captured:
		t.Fatal("catalogue capture bypassed the closed purge gate")
	case <-time.After(50 * time.Millisecond):
	}
	d.mu.Lock()
	_, published := d.inactive["work"]
	d.mu.Unlock()
	require.False(t, published, "no registry entry may be published while the purge owns the gate")

	d.releasePurgeAdmission()
	require.False(t, awaitTestValue(t, purged, "purge did not complete after admission drained").Failed())
	awaitTestValue(t, captured, "capture did not finish after the purge reopened admission")

	require.Equal(t, 0, sessionCount(d))
	d.mu.Lock()
	_, stopped := d.inactive["work"]
	d.mu.Unlock()
	require.False(t, stopped, "a capture ordered after the purge must not resurrect the record")
	_, ok, err := catalogue.Record("work")
	require.NoError(t, err)
	require.False(t, ok, "the purge must remove the durable record")
	d.sessWg.Wait()
	d.waitNotifies()
}

// TestPurgeAllAdmissionDrainBoundedWithUncooperativeRepository proves a KillAll
// whose drain is blocked by a repository call that ignores cancellation returns
// a structured failure at its bounded budget, leaves the daemon serving, and
// does not prevent a later explicit shutdown from completing.
func TestPurgeAllAdmissionDrainBoundedWithUncooperativeRepository(t *testing.T) {
	snapshot := restoreAcceptanceSession(t, "work")
	generation := acceptanceGeneration(t, snapshot, 5)
	repository := newCancellationIgnoringRepository(generation)
	clock := &signalClock{timers: make(chan *signalTimer, 8)}
	pty, releasePTY := newBlockingPTY(t)
	defer releasePTY()

	d := newTestDaemon(t, newFactorySeq(t, pty), clock)
	d.baseEnv = []string{"TERM=xterm-256color"}
	checkpoint := domain.CheckpointRef{Generation: generation.Generation, ManifestDigest: snapcodec.ManifestDigest(generation.Manifest)}
	record := domain.CatalogueRecord{Name: snapshot.Name, IncarnationID: generation.IncarnationID, Cwd: "/snapshot/cwd", CreatedAt: int64(snapshot.CreatedAt), Committed: &checkpoint}
	catalogue := newDurableRecoveryCatalogue([]domain.CatalogueRecord{record})
	WithCatalogue(catalogue, []domain.CatalogueRecord{record})(d)
	WithSnapshotRepository(repository)(d)
	WithRecoveryCoordinator(recoveryusecase.NewCoordinator(catalogue, repository, nil))(d)

	restoreFinished := make(chan struct{})
	go func() {
		defer close(restoreFinished)
		d.restoreIncrementalSnapshots(context.Background())
	}()
	<-repository.entered

	purged := make(chan purgeResult, 1)
	go func() { purged <- d.purgeAllSessions(protocol.ReasonSessionKilled) }()

	// The purge is parked in its admission drain; fire the bounded deadline.
	timer := awaitTestValue(t, clock.timers, "purge did not arm its bounded admission deadline")
	timer.ch <- time.Now()

	result := awaitTestValue(t, purged, "KillAll did not return after its bounded drain")
	require.True(t, result.Failed(), "a bounded drain timeout must be a structured failure")
	require.False(t, result.Superseded())
	require.Contains(t, result.summary(), "did not drain")

	d.mu.Lock()
	_, stopped := d.inactive["work"]
	closing := d.closing
	d.mu.Unlock()
	require.True(t, stopped, "the in-flight record must stay as stopped authority")
	require.False(t, closing, "a bounded purge abort must leave the daemon serving")
	_, ok, err := catalogue.Record("work")
	require.NoError(t, err)
	require.True(t, ok, "an aborted purge must not remove the durable record")

	// An explicit stop owns teardown and completes even though the repository
	// still refuses to observe cancellation.
	shutdownDone := make(chan struct{})
	go func() {
		defer close(shutdownDone)
		d.shutdownAll(protocol.ReasonServerShutdown)
	}()
	awaitTestValue(t, shutdownDone, "explicit shutdown did not complete with a stuck repository")
	select {
	case <-d.done:
	default:
		t.Fatal("explicit shutdown must signal Serve")
	}

	close(repository.release)
	awaitTestValue(t, restoreFinished, "restore did not settle after the repository released")
	d.sessWg.Wait()
	d.waitNotifies()
}

// TestPurgeAllAdmissionDrainDefersToExplicitShutdown proves a KillAll parked in
// its admission drain is woken by an explicit stop and defers immediately,
// rather than waiting out its bounded timeout. This is what keeps Serve from
// wedging behind a purge when an admitted transition refuses to drain.
func TestPurgeAllAdmissionDrainDefersToExplicitShutdown(t *testing.T) {
	d := newTestDaemon(t, portsmocks.NewMockPTYFactory(t), stubClock{})
	require.NoError(t, d.acquirePurgeAdmission(context.Background()))

	purged := make(chan purgeResult, 1)
	go func() { purged <- d.purgeAllSessions(protocol.ReasonSessionKilled) }()
	awaitPurgeGateClosed(t, d)

	d.shutdownAll(protocol.ReasonServerShutdown)

	result := awaitTestValue(t, purged, "a parked purge was not woken by the explicit stop")
	require.True(t, result.Superseded(), "a parked purge must defer to the explicit stop")
	require.False(t, result.Failed())
	select {
	case <-d.done:
	default:
		t.Fatal("the explicit stop must signal Serve")
	}
	d.releasePurgeAdmission()
}

// TestPurgeAllDefersToExplicitShutdownPerUnit proves explicit stop wins per
// unit: once a shutdown begins mid-purge, no further unit is removed, so the
// stop can preserve the not-yet-owned lifecycle. A unit whose teardown the
// purge already owns keeps purge authority and completes as a purge (its
// stopped authority is removed), while the not-yet-owned unit stays live for
// the stop to preserve.
func TestPurgeAllDefersToExplicitShutdownPerUnit(t *testing.T) {
	repository := newSelectivePurgeRepository(nil)
	var d *Daemon
	repository.onDelete = func() {
		d.mu.Lock()
		d.closing = true
		d.signalPurgeAdmissionChangedLocked()
		d.mu.Unlock()
	}
	d = newTestDaemon(t, portsmocks.NewMockPTYFactory(t), stubClock{})
	catalogue := newDurableRecoveryCatalogue(nil)
	WithCatalogue(catalogue, nil)(d)
	WithRecoveryCoordinator(recoveryusecase.NewCoordinator(catalogue, repository, nil))(d)

	for _, name := range []string{"a", "b"} {
		live := newSnapshotTestSession(t, name, false, "/"+name)
		require.NoError(t, catalogue.Create(live.persistRecordLocked(live.createdAt)))
		livePTY, ok := live.tabs[0].panes["pane-1"].pty.(*portsmocks.MockPTY)
		require.True(t, ok)
		livePTY.EXPECT().Close().Return(nil).Maybe()
		d.sessions[live.id] = live
	}

	result := d.purgeAllSessions(protocol.ReasonSessionKilled)
	require.True(t, result.Superseded(), "an explicit stop that begins mid-purge must win")
	require.Len(t, repository.calls, 1, "only the purge-owned unit may be removed before the stop wins")

	d.mu.Lock()
	remaining := len(d.sessions)
	var survivor string
	for _, sess := range d.sessions {
		survivor = sess.name
	}
	_, aStopped := d.inactive["a"]
	_, bStopped := d.inactive["b"]
	d.mu.Unlock()
	require.Equal(t, 1, remaining, "exactly the not-yet-owned unit must survive the purge")
	require.NotEmpty(t, survivor)
	require.False(t, aStopped || bStopped, "the purge-owned unit must not retain stopped authority")

	// The explicit stop now owns the surviving session and preserves it as
	// stopped authority instead of purging it.
	d.shutdownAll(protocol.ReasonServerShutdown)
	require.Equal(t, 0, sessionCount(d))
	d.mu.Lock()
	_, survivorStopped := d.inactive[survivor]
	d.mu.Unlock()
	require.True(t, survivorStopped, "the explicit stop must preserve the surviving session's authority")
	d.sessWg.Wait()
	d.waitNotifies()
}

// occupySessionTeardown marks sess as owned by another lifecycle path without
// releasing it, so a bounded kill cannot acquire teardown ownership. The owning
// test releases it with finishTeardown. It models a slow unit parked exactly at
// the destructive boundary: the gate is frozen and teardown ownership is the
// only thing the deadline must outlast.
func occupySessionTeardown(sess *session) {
	sess.teardownMu.Lock()
	sess.teardownActive = true
	sess.teardownDone = make(chan struct{})
	sess.teardownMu.Unlock()
}

// seedPurgeableLiveSession inserts a named live session with durable authority
// and a closed-safe PTY, then parks its teardown ownership so a purge must reach
// the destructive boundary before it can remove the unit.
func seedPurgeableLiveSession(t *testing.T, d *Daemon, catalogue *durableRecoveryCatalogue, name string) *session {
	t.Helper()
	live := newSnapshotTestSession(t, name, false, "/"+name)
	require.NoError(t, catalogue.Create(live.persistRecordLocked(live.createdAt)))
	livePTY, ok := live.tabs[0].panes["pane-1"].pty.(*portsmocks.MockPTY)
	require.True(t, ok)
	livePTY.EXPECT().Close().Return(nil).Maybe()
	d.sessions[live.id] = live
	occupySessionTeardown(live)
	return live
}

// TestPurgeAllLiveDeadlineSkipIsTypedAndRetryable proves the destructive
// boundary is observable: a live unit whose teardown ownership the shared live
// budget outlasts is reported as a typed live failure with its exact name, is
// left completely live with its durable authority intact, and is removed by a
// later purge once teardown ownership is available. The admission budget and the
// live budget are separate deadlines, so a fresh live budget is armed when the
// live phase begins.
func TestPurgeAllLiveDeadlineSkipIsTypedAndRetryable(t *testing.T) {
	clock := &signalClock{timers: make(chan *signalTimer, 64)}
	d := newTestDaemon(t, portsmocks.NewMockPTYFactory(t), clock)
	catalogue := newDurableRecoveryCatalogue(nil)
	WithCatalogue(catalogue, nil)(d)
	WithRecoveryCoordinator(recoveryusecase.NewCoordinator(catalogue, noOpSnapshotRepository{}, nil))(d)

	live := seedPurgeableLiveSession(t, d, catalogue, "work")

	purged := make(chan purgeResult, 1)
	go func() { purged <- d.purgeAllSessions(protocol.ReasonSessionKilled) }()

	admission := awaitTestValue(t, clock.timers, "purge did not arm its admission deadline")
	liveBudget := awaitTestValue(t, clock.timers, "purge did not arm a separate live-teardown budget")
	require.NotSame(t, admission, liveBudget, "live teardown must spend its own budget, not the admission drain's")
	liveBudget.ch <- time.Now()

	result := awaitTestValue(t, purged, "purge did not return after the live budget expired")
	require.True(t, result.Failed(), result.summary())
	require.False(t, result.Superseded())
	require.Len(t, result.Failures, 1)
	require.Equal(t, purgeRecordLive, result.Failures[0].Class)
	require.Equal(t, "work", result.Failures[0].Name, "a skipped unit must keep its exact name")
	require.ErrorIs(t, result.Failures[0].Err, errSessionKillDeadline)

	require.Equal(t, 1, sessionCount(d), "a skipped live unit must stay live")
	d.mu.Lock()
	taken := d.nameLiveOrStoppedLocked("work")
	_, stopped := d.inactive["work"]
	d.mu.Unlock()
	require.True(t, taken, "the skipped name must remain taken and accurately reported")
	require.False(t, stopped, "a skipped live unit must not leave phantom stopped authority")
	requirePurgeAdmissionBalanced(t, d)
	_, ok, err := catalogue.Record("work")
	require.NoError(t, err)
	require.True(t, ok, "a skipped live unit must keep its durable authority for the retry")

	// The retry has full destructive ownership and must purge the unit exactly.
	live.finishTeardown()
	retry := d.purgeAllSessions(protocol.ReasonSessionKilled)
	require.False(t, retry.Failed(), retry.summary())
	require.False(t, retry.Superseded())
	require.Equal(t, 0, sessionCount(d), "the retried purge must remove the unit")
	d.mu.Lock()
	taken = d.nameLiveOrStoppedLocked("work")
	d.mu.Unlock()
	require.False(t, taken, "the retried purge must free the purged name")
	_, ok, err = catalogue.Record("work")
	require.NoError(t, err)
	require.False(t, ok, "the retried purge must remove durable authority")
	d.sessWg.Wait()
	d.waitNotifies()
}

// TestPurgeAllLiveBudgetSkipsEveryRemainingSlowUnit defines the N-unit behavior:
// one live budget is shared by every live unit. A single expiry outlasts all N
// parked units, so each remaining unit is skipped, reported as its own typed
// live failure with its exact name, and left live; no unit is silently reported
// as purged.
func TestPurgeAllLiveBudgetSkipsEveryRemainingSlowUnit(t *testing.T) {
	const units = 3
	clock := &signalClock{timers: make(chan *signalTimer, 64)}
	d := newTestDaemon(t, portsmocks.NewMockPTYFactory(t), clock)
	catalogue := newDurableRecoveryCatalogue(nil)
	WithCatalogue(catalogue, nil)(d)
	WithRecoveryCoordinator(recoveryusecase.NewCoordinator(catalogue, noOpSnapshotRepository{}, nil))(d)

	names := make([]string, 0, units)
	for i := range units {
		name := fmt.Sprintf("slow-%d", i)
		names = append(names, name)
		seedPurgeableLiveSession(t, d, catalogue, name)
	}
	require.Equal(t, units, sessionCount(d))

	purged := make(chan purgeResult, 1)
	go func() { purged <- d.purgeAllSessions(protocol.ReasonSessionKilled) }()

	_ = awaitTestValue(t, clock.timers, "purge did not arm its admission deadline")
	liveBudget := awaitTestValue(t, clock.timers, "purge did not arm a separate live-teardown budget")
	liveBudget.ch <- time.Now()

	result := awaitTestValue(t, purged, "purge did not return after the shared live budget expired")
	require.True(t, result.Failed(), result.summary())
	require.False(t, result.Superseded())
	require.Len(t, result.Failures, units, "every remaining unit must be reported")
	for i, failure := range result.Failures {
		require.Equal(t, purgeRecordLive, failure.Class)
		require.Equal(t, names[i], failure.Name)
		require.ErrorIs(t, failure.Err, errSessionKillDeadline)
	}
	require.Equal(t, units, sessionCount(d), "no unit may be removed after the shared live budget expired")
	for _, name := range names {
		_, ok, err := catalogue.Record(name)
		require.NoError(t, err)
		require.True(t, ok, "a skipped unit must keep its durable authority")
	}

	d.mu.Lock()
	sessions := d.sessionsSnapshotLocked()
	d.mu.Unlock()
	for _, sess := range sessions {
		sess.finishTeardown()
	}
	for _, sess := range sessions {
		require.NoError(t, d.killSession(sess, protocol.ReasonSessionKilled, true))
	}
	require.Equal(t, 0, sessionCount(d))
	d.sessWg.Wait()
	d.waitNotifies()
}

// TestTerminateAllForShutdownReportsDeadlineSkippedSession proves the shutdown
// verdict: a session that a bounded shutdown deadline skips before teardown
// ownership is reported as an incomplete checkpoint instead of silently
// counting as a completed stop. A later deadline-free pass completes the stop
// once the competing owner releases teardown.
func TestTerminateAllForShutdownReportsDeadlineSkippedSession(t *testing.T) {
	clock := &signalClock{timers: make(chan *signalTimer, 64)}
	d := newTestDaemon(t, portsmocks.NewMockPTYFactory(t), clock)

	live := newSnapshotTestSession(t, "0", true, "/tmp")
	pty, ok := live.tabs[0].panes["pane-1"].pty.(*portsmocks.MockPTY)
	require.True(t, ok)
	pty.EXPECT().Close().Return(nil).Maybe()
	d.sessions[live.id] = live
	occupySessionTeardown(live)

	deadline := newSnapshotShutdownDeadline(clock)
	defer deadline.stop()
	timer := awaitTestValue(t, clock.timers, "shutdown deadline did not arm")
	timer.ch <- time.Now()

	require.True(t, d.terminateAllForShutdown(protocol.ReasonServerShutdown, deadline),
		"a session skipped at the teardown deadline must be reported as an incomplete checkpoint")
	require.Equal(t, 1, sessionCount(d), "the skipped session must remain live")

	live.finishTeardown()
	require.False(t, d.terminateAllForShutdown(protocol.ReasonServerShutdown, nil))
	require.Equal(t, 0, sessionCount(d), "a deadline-free pass must complete the stop")
	select {
	case <-d.done:
	default:
		t.Fatal("the explicit stop must signal Serve")
	}
	d.sessWg.Wait()
	d.waitNotifies()
}

// TestPurgeAdmissionDeadlineDefersToConcurrentExplicitStop proves the drain
// deadline select rechecks closing: when an explicit stop begins as the drain
// budget expires, the purge reports supersession instead of a drain timeout and
// removes nothing, so the stop keeps ownership of every lifecycle.
func TestPurgeAdmissionDeadlineDefersToConcurrentExplicitStop(t *testing.T) {
	clock := &signalClock{timers: make(chan *signalTimer, 64)}
	d := newTestDaemon(t, portsmocks.NewMockPTYFactory(t), clock)
	catalogue := newDurableRecoveryCatalogue(nil)
	WithCatalogue(catalogue, nil)(d)
	WithRecoveryCoordinator(recoveryusecase.NewCoordinator(catalogue, noOpSnapshotRepository{}, nil))(d)

	live := seedPurgeableLiveSession(t, d, catalogue, "work")
	// Hold one admitted transition so the purge parks in its bounded drain.
	require.NoError(t, d.acquirePurgeAdmission(context.Background()))

	purged := make(chan purgeResult, 1)
	go func() { purged <- d.purgeAllSessions(protocol.ReasonSessionKilled) }()
	awaitPurgeGateClosed(t, d)

	// The stop begins exactly as the budget expires and is deliberately not
	// signalled through the admission change channel, so the deadline is the only
	// select case that can fire.
	d.mu.Lock()
	d.closing = true
	d.mu.Unlock()
	admission := awaitTestValue(t, clock.timers, "purge did not arm its admission deadline")
	admission.ch <- time.Now()

	result := awaitTestValue(t, purged, "purge did not return after the drain budget expired")
	require.True(t, result.Superseded(), "an explicit stop concurrent with the deadline must win")
	require.False(t, result.Failed())
	require.Equal(t, 1, sessionCount(d), "a superseded purge must remove nothing")

	d.mu.Lock()
	d.closing = false
	d.mu.Unlock()
	d.releasePurgeAdmission()
	live.finishTeardown()
	require.False(t, d.purgeAllSessions(protocol.ReasonSessionKilled).Failed())
	require.Equal(t, 0, sessionCount(d))
	d.sessWg.Wait()
	d.waitNotifies()
}

// TestPurgeLiveDeadlineDefersToConcurrentExplicitStop proves the same recheck at
// the destructive boundary: a live unit whose budget expires as a stop begins is
// classified as superseded rather than a teardown failure, and it stays live for
// the stop to preserve.
func TestPurgeLiveDeadlineDefersToConcurrentExplicitStop(t *testing.T) {
	clock := &signalClock{timers: make(chan *signalTimer, 64)}
	d := newTestDaemon(t, portsmocks.NewMockPTYFactory(t), clock)
	catalogue := newDurableRecoveryCatalogue(nil)
	WithCatalogue(catalogue, nil)(d)
	WithRecoveryCoordinator(recoveryusecase.NewCoordinator(catalogue, noOpSnapshotRepository{}, nil))(d)

	live := seedPurgeableLiveSession(t, d, catalogue, "work")

	purged := make(chan purgeResult, 1)
	go func() { purged <- d.purgeAllSessions(protocol.ReasonSessionKilled) }()
	_ = awaitTestValue(t, clock.timers, "purge did not arm its admission deadline")
	liveBudget := awaitTestValue(t, clock.timers, "purge did not arm a separate live-teardown budget")

	d.mu.Lock()
	d.closing = true
	d.mu.Unlock()
	liveBudget.ch <- time.Now()

	result := awaitTestValue(t, purged, "purge did not return after the live budget expired")
	require.True(t, result.Superseded(), "the concurrent explicit stop must win over the live deadline")
	require.False(t, result.Failed())
	require.Equal(t, 1, sessionCount(d), "a superseded live unit must remain live")

	d.mu.Lock()
	d.closing = false
	d.mu.Unlock()
	live.finishTeardown()
	require.False(t, d.purgeAllSessions(protocol.ReasonSessionKilled).Failed())
	require.Equal(t, 0, sessionCount(d))
	d.sessWg.Wait()
	d.waitNotifies()
}

// blockingPurgeDeleteRepository blocks its first incarnation deletion until it
// is released and ignores context cancellation, so a bounded sweep can only
// finish by stopping its wait on the uncooperative delete.
type blockingPurgeDeleteRepository struct {
	noOpSnapshotRepository

	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func newBlockingPurgeDeleteRepository() *blockingPurgeDeleteRepository {
	return &blockingPurgeDeleteRepository{entered: make(chan struct{}), release: make(chan struct{})}
}

func (r *blockingPurgeDeleteRepository) DeleteIncarnation(context.Context, domain.IncarnationID) error {
	r.once.Do(func() { close(r.entered) })
	<-r.release
	return nil
}

// seedStoppedPurgeRecords publishes one stopped and one broken registry record
// with durable authority, so a stopped/broken sweep has real work to bound.
func seedStoppedPurgeRecords(t *testing.T, d *Daemon, catalogue *durableRecoveryCatalogue) {
	t.Helper()
	stopped := domain.CatalogueRecord{Name: "stopped", IncarnationID: domain.IncarnationID{2}, CreatedAt: 7}
	require.NoError(t, catalogue.Create(stopped))
	d.inactive["stopped"] = inactiveSessionFromRecord(stopped, protocol.SessionDown, nil)
	broken := domain.CatalogueRecord{Name: "broken", IncarnationID: domain.IncarnationID{3}, CreatedAt: 8}
	require.NoError(t, catalogue.Create(broken))
	d.inactive["broken"] = inactiveSessionFromRecord(broken, protocol.SessionBroken, nil)
}

// TestPurgeAllParticipantChangeRetriesOnceAndPurgesExactly proves a participant
// change that races a live purge is never counted as success on its own: the
// first kill aborts with the participants-changed sentinel, the purge retries
// once against a fresh snapshot, and only that retry removes the exact unit.
// Both a detach and a resumed attachment incarnation (a token resume) are
// covered.
func TestPurgeAllParticipantChangeRetriesOnceAndPurgesExactly(t *testing.T) {
	tests := []struct {
		name   string
		change func(*Daemon, *session, *attachedClient)
	}{
		{
			name: "detach",
			change: func(d *Daemon, sess *session, ac *attachedClient) {
				require.True(t, d.detachIfCurrentTransport(sess, ac, ac.transportSnapshot()))
			},
		},
		{
			name: "token resume",
			change: func(d *Daemon, sess *session, ac *attachedClient) {
				fresh := &closeTrackingTransport{}
				ac.replaceTransport(fresh)
				ac.installTestAttachmentCapability(sess.captureAttachmentCapability(ac, fresh))
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			d, sess, ac, _ := newManualSessionWithPTYs(t, newQuietPTY())
			catalogue := newDurableRecoveryCatalogue(nil)
			WithCatalogue(catalogue, nil)(d)
			WithRecoveryCoordinator(recoveryusecase.NewCoordinator(catalogue, noOpSnapshotRepository{}, nil))(d)
			require.NoError(t, catalogue.Create(sess.persistRecordLocked(1)))

			attempts := 0
			d.afterAttachmentEffectParticipantsSnapshotted = func(action string, _ []*attachedClient) {
				if action != "" {
					return
				}
				attempts++
				if attempts == 1 {
					test.change(d, sess, ac)
				}
			}
			t.Cleanup(func() { d.afterAttachmentEffectParticipantsSnapshotted = nil })

			result := d.purgeAllSessions(protocol.ReasonSessionKilled)
			require.False(t, result.Failed(), result.summary())
			require.False(t, result.Superseded())
			require.Equal(t, 2, attempts, "the aborted first attempt must be retried once")
			require.Equal(t, 0, sessionCount(d), "only the fresh-snapshot retry may remove the unit")
			d.mu.Lock()
			taken := d.nameLiveOrStoppedLocked("work")
			_, stopped := d.inactive["work"]
			d.mu.Unlock()
			require.False(t, taken, "the retried purge must free the purged name")
			require.False(t, stopped, "a purged unit must not leave phantom stopped authority")
			_, ok, err := catalogue.Record("work")
			require.NoError(t, err)
			require.False(t, ok, "the retried purge must remove durable authority")
			d.sessWg.Wait()
			d.waitNotifies()
		})
	}
}

// TestPurgeAllParticipantChurnIsTypedNotFalseSuccess proves the no-false-success
// contract: a live unit whose participants keep changing is reported as a typed
// live failure with its exact name, keeps its live identity and durable
// authority, and is removed only when the churn stops. The churn alternates a
// detach with a resumed membership, so every attempt's snapshot is stale.
func TestPurgeAllParticipantChurnIsTypedNotFalseSuccess(t *testing.T) {
	d, sess, ac, _ := newManualSessionWithPTYs(t, newQuietPTY())
	catalogue := newDurableRecoveryCatalogue(nil)
	WithCatalogue(catalogue, nil)(d)
	WithRecoveryCoordinator(recoveryusecase.NewCoordinator(catalogue, noOpSnapshotRepository{}, nil))(d)
	require.NoError(t, catalogue.Create(sess.persistRecordLocked(1)))

	var churn atomic.Bool
	churn.Store(true)
	hookCalls := 0
	d.afterAttachmentEffectParticipantsSnapshotted = func(action string, _ []*attachedClient) {
		if action != "" || !churn.Load() {
			return
		}
		hookCalls++
		if hookCalls%2 == 1 {
			require.True(t, d.detachIfCurrentTransport(sess, ac, ac.transportSnapshot()))
			return
		}
		// Resumed incarnation: re-register the attachment after the first kill
		// snapshotted the detached set.
		d.mu.Lock()
		sess.mu.Lock()
		if sess.attachments == nil {
			sess.attachments = make(map[*attachedClient]struct{})
		}
		sess.attachments[ac] = struct{}{}
		sess.mu.Unlock()
		d.mu.Unlock()
		ac.setSession(sess)
	}
	t.Cleanup(func() { d.afterAttachmentEffectParticipantsSnapshotted = nil })

	result := d.purgeAllSessions(protocol.ReasonSessionKilled)
	require.True(t, result.Failed(), result.summary())
	require.False(t, result.Superseded())
	require.Len(t, result.Failures, 1)
	require.Equal(t, purgeRecordLive, result.Failures[0].Class)
	require.Equal(t, "work", result.Failures[0].Name, "a churned unit must keep its exact name")
	require.ErrorIs(t, result.Failures[0].Err, errSessionKillParticipantsChanged)
	require.Equal(t, 2, hookCalls, "the churn must outlast the single retry")
	require.Equal(t, 1, sessionCount(d), "a churned unit must stay live")
	d.mu.Lock()
	taken := d.nameLiveOrStoppedLocked("work")
	_, stopped := d.inactive["work"]
	d.mu.Unlock()
	require.True(t, taken, "the churned name must remain taken and accurately reported")
	require.False(t, stopped, "a churned unit must not leave phantom stopped authority")
	requirePurgeAdmissionBalanced(t, d)
	_, ok, err := catalogue.Record("work")
	require.NoError(t, err)
	require.True(t, ok, "a churned unit must keep its durable authority for the retry")

	churn.Store(false)
	retry := d.purgeAllSessions(protocol.ReasonSessionKilled)
	require.False(t, retry.Failed(), retry.summary())
	require.Equal(t, 0, sessionCount(d), "the churn-free retry must remove the unit")
	_, ok, err = catalogue.Record("work")
	require.NoError(t, err)
	require.False(t, ok, "the churn-free retry must remove durable authority")
	d.sessWg.Wait()
	d.waitNotifies()
}

// TestPurgeAllAlreadyRemovedParticipantChangeIsSuccess proves the
// already-removed half of the contract: a session a racing lifecycle path
// removed while the purge's participant snapshot went stale owns nothing, so
// the purge reports success rather than a phantom live failure.
func TestPurgeAllAlreadyRemovedParticipantChangeIsSuccess(t *testing.T) {
	d, sess, _, _ := newManualSessionWithPTYs(t, newQuietPTY())
	catalogue := newDurableRecoveryCatalogue(nil)
	WithCatalogue(catalogue, nil)(d)
	WithRecoveryCoordinator(recoveryusecase.NewCoordinator(catalogue, noOpSnapshotRepository{}, nil))(d)
	require.NoError(t, catalogue.Create(sess.persistRecordLocked(1)))

	d.afterAttachmentEffectParticipantsSnapshotted = func(action string, _ []*attachedClient) {
		if action != "" {
			return
		}
		d.mu.Lock()
		d.unregisterSessionLocked(sess)
		d.mu.Unlock()
	}
	t.Cleanup(func() { d.afterAttachmentEffectParticipantsSnapshotted = nil })

	result := d.purgeAllSessions(protocol.ReasonSessionKilled)
	require.False(t, result.Failed(), result.summary())
	require.False(t, result.Superseded())
	require.Equal(t, 0, sessionCount(d), "the racing path already removed the session")
	d.mu.Lock()
	taken := d.nameLiveOrStoppedLocked("work")
	_, stopped := d.inactive["work"]
	d.mu.Unlock()
	require.False(t, taken, "an already-removed unit must not stay taken")
	require.False(t, stopped, "an already-removed unit must not leave phantom stopped authority")
	d.sessWg.Wait()
	d.waitNotifies()
}

// TestPurgeAllStoppedSweepDeadlineIsTypedAndReleasesGate proves the stopped and
// broken sweep spends its own bounded budget: a repository deletion that
// ignores cancellation cannot extend the purge, every record the sweep did not
// finish is reported as its typed stopped/broken failure with its exact name,
// admission is released, the daemon keeps serving, and its next purge removes
// the leftovers.
func TestPurgeAllStoppedSweepDeadlineIsTypedAndReleasesGate(t *testing.T) {
	clock := &signalClock{timers: make(chan *signalTimer, 64)}
	d := newTestDaemon(t, portsmocks.NewMockPTYFactory(t), clock)
	catalogue := newDurableRecoveryCatalogue(nil)
	repository := newBlockingPurgeDeleteRepository()
	WithCatalogue(catalogue, nil)(d)
	WithSnapshotRepository(repository)(d)
	WithRecoveryCoordinator(recoveryusecase.NewCoordinator(catalogue, repository, nil))(d)
	seedStoppedPurgeRecords(t, d, catalogue)

	purged := make(chan purgeResult, 1)
	go func() { purged <- d.purgeAllSessions(protocol.ReasonSessionKilled) }()

	admission := awaitTestValue(t, clock.timers, "purge did not arm its admission deadline")
	stoppedBudget := awaitTestValue(t, clock.timers, "purge did not arm a separate stopped sweep budget")
	require.NotSame(t, admission, stoppedBudget, "the sweep must spend its own budget, not the admission drain's")
	awaitTestValue(t, repository.entered, "the sweep did not start its uncooperative delete")
	stoppedBudget.ch <- time.Now()

	result := awaitTestValue(t, purged, "purge did not return after the stopped sweep budget expired")
	require.True(t, result.Failed(), result.summary())
	require.False(t, result.Superseded())
	require.Len(t, result.Failures, 2, "every unfinished stopped/broken record must be reported")
	require.Equal(t, purgeRecordStopped, result.Failures[0].Class)
	require.Equal(t, "stopped", result.Failures[0].Name)
	require.ErrorIs(t, result.Failures[0].Err, errSessionPurgeSweepDeadline)
	require.Equal(t, purgeRecordBroken, result.Failures[1].Class)
	require.Equal(t, "broken", result.Failures[1].Name)
	require.ErrorIs(t, result.Failures[1].Err, errSessionPurgeSweepDeadline)

	d.mu.Lock()
	admissionStillClosed := d.purgeAdmissionClosing
	daemonClosing := d.closing
	_, stoppedRemains := d.inactive["stopped"]
	_, brokenRemains := d.inactive["broken"]
	d.mu.Unlock()
	require.False(t, admissionStillClosed, "a bounded sweep must reopen purge admission promptly")
	require.False(t, daemonClosing, "a bounded sweep must leave the daemon serving")
	requirePurgeAdmissionBalanced(t, d)
	require.True(t, stoppedRemains, "an unfinished stopped record must keep its authority for the retry")
	require.True(t, brokenRemains, "an unfinished broken record must keep its authority for the retry")

	close(repository.release)
	// The detached sweep finishes the delete it had already started and then
	// stops; the leftover record is purged by the next bounded pass.
	require.Eventually(t, func() bool {
		d.mu.Lock()
		defer d.mu.Unlock()
		_, broken := d.inactive["broken"]
		return !broken
	}, testWaitTimeout, time.Millisecond, "the detached sweep did not finish the delete it had started")
	require.False(t, d.purgeAllSessions(protocol.ReasonSessionKilled).Failed())
	require.Equal(t, 0, sessionCount(d))
	d.mu.Lock()
	require.Empty(t, d.inactive, "the next purge must remove the leftover records")
	d.mu.Unlock()
	d.sessWg.Wait()
	d.waitNotifies()
}

// TestPurgeAllStoppedAndLiveBudgetsAreIndependent proves the stopped/broken
// sweep, the live teardown, and the admission drain each spend a distinct
// budget: the stopped budget expiring leaves a parked live unit untouched and
// arms a fresh live budget, whose expiry then reports that live unit.
func TestPurgeAllStoppedAndLiveBudgetsAreIndependent(t *testing.T) {
	clock := &signalClock{timers: make(chan *signalTimer, 64)}
	d := newTestDaemon(t, portsmocks.NewMockPTYFactory(t), clock)
	catalogue := newDurableRecoveryCatalogue(nil)
	repository := newBlockingPurgeDeleteRepository()
	WithCatalogue(catalogue, nil)(d)
	WithSnapshotRepository(repository)(d)
	WithRecoveryCoordinator(recoveryusecase.NewCoordinator(catalogue, repository, nil))(d)
	seedStoppedPurgeRecords(t, d, catalogue)
	live := seedPurgeableLiveSession(t, d, catalogue, "work")

	purged := make(chan purgeResult, 1)
	go func() { purged <- d.purgeAllSessions(protocol.ReasonSessionKilled) }()

	admission := awaitTestValue(t, clock.timers, "purge did not arm its admission deadline")
	stoppedBudget := awaitTestValue(t, clock.timers, "purge did not arm a stopped sweep budget")
	require.NotSame(t, admission, stoppedBudget)
	awaitTestValue(t, repository.entered, "the sweep did not start its uncooperative delete")
	stoppedBudget.ch <- time.Now()

	liveBudget := awaitTestValue(t, clock.timers, "the expired sweep did not arm a fresh live budget")
	require.NotSame(t, admission, liveBudget)
	require.NotSame(t, stoppedBudget, liveBudget)
	liveBudget.ch <- time.Now()

	result := awaitTestValue(t, purged, "purge did not return after both budgets expired")
	require.True(t, result.Failed(), result.summary())
	require.False(t, result.Superseded())
	require.Len(t, result.Failures, 3)
	require.Equal(t, purgeRecordLive, result.Failures[0].Class)
	require.Equal(t, "work", result.Failures[0].Name)
	require.ErrorIs(t, result.Failures[0].Err, errSessionKillDeadline)
	require.Equal(t, purgeRecordStopped, result.Failures[1].Class)
	require.Equal(t, "stopped", result.Failures[1].Name)
	require.ErrorIs(t, result.Failures[1].Err, errSessionPurgeSweepDeadline)
	require.Equal(t, purgeRecordBroken, result.Failures[2].Class)
	require.Equal(t, "broken", result.Failures[2].Name)
	require.ErrorIs(t, result.Failures[2].Err, errSessionPurgeSweepDeadline)
	require.Equal(t, 1, sessionCount(d), "the swept live budget must leave the live unit untouched")

	close(repository.release)
	live.finishTeardown()
	// The detached sweep finishes the delete it had already started and then
	// stops; the leftover record is purged by the next bounded pass.
	require.Eventually(t, func() bool {
		d.mu.Lock()
		defer d.mu.Unlock()
		_, broken := d.inactive["broken"]
		return !broken
	}, testWaitTimeout, time.Millisecond, "the detached sweep did not finish after release")
	require.False(t, d.purgeAllSessions(protocol.ReasonSessionKilled).Failed())
	require.Equal(t, 0, sessionCount(d))
	d.sessWg.Wait()
	d.waitNotifies()
}

// TestPurgeAllStoppedSweepDeadlineDefersToExplicitShutdown proves explicit stop
// supersedes the sweep classification: when a shutdown begins as the stopped
// budget expires, the purge reports supersession rather than typed
// stopped/broken failures, so the stop keeps authority over every record.
func TestPurgeAllStoppedSweepDeadlineDefersToExplicitShutdown(t *testing.T) {
	clock := &signalClock{timers: make(chan *signalTimer, 64)}
	d := newTestDaemon(t, portsmocks.NewMockPTYFactory(t), clock)
	catalogue := newDurableRecoveryCatalogue(nil)
	repository := newBlockingPurgeDeleteRepository()
	WithCatalogue(catalogue, nil)(d)
	WithSnapshotRepository(repository)(d)
	WithRecoveryCoordinator(recoveryusecase.NewCoordinator(catalogue, repository, nil))(d)
	seedStoppedPurgeRecords(t, d, catalogue)

	purged := make(chan purgeResult, 1)
	go func() { purged <- d.purgeAllSessions(protocol.ReasonSessionKilled) }()

	_ = awaitTestValue(t, clock.timers, "purge did not arm its admission deadline")
	stoppedBudget := awaitTestValue(t, clock.timers, "purge did not arm a stopped sweep budget")
	awaitTestValue(t, repository.entered, "the sweep did not start its uncooperative delete")

	// The stop begins exactly as the sweep budget expires and is deliberately not
	// signalled through the admission change channel, so the deadline is the only
	// select case that can fire.
	d.mu.Lock()
	d.closing = true
	d.mu.Unlock()
	stoppedBudget.ch <- time.Now()

	result := awaitTestValue(t, purged, "purge did not return after the sweep budget expired")
	require.True(t, result.Superseded(), "an explicit stop concurrent with the sweep deadline must win")
	require.False(t, result.Failed())

	d.mu.Lock()
	d.closing = false
	stoppedRemains := len(d.inactive)
	d.mu.Unlock()
	require.Equal(t, 2, stoppedRemains, "a superseded sweep must leave every record for the stop")
	close(repository.release)
	d.sessWg.Wait()
	d.waitNotifies()
}

// TestPurgeAllParticipantChangeDefersToConcurrentExplicitShutdown proves
// explicit stop supersedes the participant-change classification: when a
// shutdown begins as the kill aborts, the purge neither retries against the
// stop nor reports a live failure, so the stop preserves the session.
func TestPurgeAllParticipantChangeDefersToConcurrentExplicitShutdown(t *testing.T) {
	d, sess, ac, _ := newManualSessionWithPTYs(t, newQuietPTY())
	catalogue := newDurableRecoveryCatalogue(nil)
	WithCatalogue(catalogue, nil)(d)
	WithRecoveryCoordinator(recoveryusecase.NewCoordinator(catalogue, noOpSnapshotRepository{}, nil))(d)
	require.NoError(t, catalogue.Create(sess.persistRecordLocked(1)))

	// The racing path changes the participants and begins an explicit stop at
	// once, exactly as the kill's post-freeze revalidation runs.
	hookCalls := 0
	d.afterAttachmentEffectParticipantsSnapshotted = func(action string, _ []*attachedClient) {
		if action != "" {
			return
		}
		hookCalls++
		require.True(t, d.detachIfCurrentTransport(sess, ac, ac.transportSnapshot()))
		d.mu.Lock()
		d.closing = true
		d.mu.Unlock()
	}
	t.Cleanup(func() { d.afterAttachmentEffectParticipantsSnapshotted = nil })

	result := d.purgeAllSessions(protocol.ReasonSessionKilled)
	require.True(t, result.Superseded(), "an explicit stop concurrent with the participant change must win")
	require.False(t, result.Failed())
	require.Equal(t, 1, hookCalls, "a superseded purge must not retry against the stop")
	require.Equal(t, 1, sessionCount(d), "a superseded purge must leave the session for the stop")

	d.mu.Lock()
	d.closing = false
	_, stopped := d.inactive["work"]
	d.mu.Unlock()
	require.False(t, stopped, "a superseded purge must not leave phantom stopped authority")
	d.sessWg.Wait()
	d.waitNotifies()
}

// TestPurgeAllStopBetweenPrecheckAndOwnershipIsSupersededNotSuccess proves the
// no-false-success contract at the last window: when an explicit stop wins a
// live unit after the purge's per-unit precheck but before that unit attempts
// teardown ownership, purgeLiveSession sees an already-removed target and is a
// no-op. The purge must defer to the stop and report supersession instead of
// counting the stop-owned unit as a successful purge.
func TestPurgeAllStopBetweenPrecheckAndOwnershipIsSupersededNotSuccess(t *testing.T) {
	d := newTestDaemon(t, portsmocks.NewMockPTYFactory(t), stubClock{})
	catalogue := newDurableRecoveryCatalogue(nil)
	WithCatalogue(catalogue, nil)(d)
	WithRecoveryCoordinator(recoveryusecase.NewCoordinator(catalogue, noOpSnapshotRepository{}, nil))(d)

	live := newSnapshotTestSession(t, "work", false, "/work")
	record := live.persistRecordLocked(live.createdAt)
	require.NoError(t, catalogue.Create(record))
	livePTY, ok := live.tabs[0].panes["pane-1"].pty.(*portsmocks.MockPTY)
	require.True(t, ok)
	livePTY.EXPECT().Close().Return(nil).Maybe()
	d.sessions[live.id] = live

	d.beforePurgeLiveSessionKill = func(sess *session) {
		d.beforePurgeLiveSessionKill = nil
		// The explicit stop wins the exact unit in this window: it marks the
		// daemon closing, removes the session from the live registry, and
		// preserves its stopped authority, exactly as its own teardown does.
		d.mu.Lock()
		d.closing = true
		d.signalPurgeAdmissionChangedLocked()
		d.unregisterSessionLocked(sess)
		d.inactive["work"] = inactiveSessionFromRecord(record, protocol.SessionDown, nil)
		d.mu.Unlock()
	}
	t.Cleanup(func() { d.beforePurgeLiveSessionKill = nil })

	result := d.purgeAllSessions(protocol.ReasonSessionKilled)
	require.True(t, result.Superseded(), "a stop that wins the unit before ownership must supersede the purge")
	require.False(t, result.Failed(), result.summary())
	require.Equal(t, 0, sessionCount(d), "the stop owns the session from this point")
	requirePurgeAdmissionBalanced(t, d)

	d.mu.Lock()
	_, stopped := d.inactive["work"]
	closing := d.closing
	d.mu.Unlock()
	require.True(t, stopped, "the stop's preserved authority must survive the purge")
	require.True(t, closing, "the stop must stay in effect")
	_, ok, err := catalogue.Record("work")
	require.NoError(t, err)
	require.True(t, ok, "a superseded purge must not delete durable authority")
	d.waitNotifies()
}

// blockingPublicationRepository blocks its first publication until released and
// deliberately ignores context cancellation, modelling an uncooperative
// publisher whose repository call outlives the shared teardown budget.
type blockingPublicationRepository struct {
	noOpSnapshotRepository

	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func newBlockingPublicationRepository() *blockingPublicationRepository {
	return &blockingPublicationRepository{entered: make(chan struct{}), release: make(chan struct{})}
}

func (r *blockingPublicationRepository) Publish(context.Context, ports.SnapshotPublication) error {
	r.once.Do(func() { close(r.entered) })
	<-r.release
	return nil
}

// TestPurgeAllLiveQuarantineJoinBoundedByDeadlineReleasesAdmission proves the
// purge's snapshot-coordinator quarantine join spends the shared live budget:
// an uncooperative publisher that ignores cancellation can never hold purge
// admission open, the live unit is reported as a typed skip instead of being
// counted as purged, and a later purge removes it once the publisher returns.
func TestPurgeAllLiveQuarantineJoinBoundedByDeadlineReleasesAdmission(t *testing.T) {
	clock := &signalClock{timers: make(chan *signalTimer, 64)}
	d := newTestDaemon(t, portsmocks.NewMockPTYFactory(t), clock)
	catalogue := newDurableRecoveryCatalogue(nil)
	repository := newBlockingPublicationRepository()
	WithCatalogue(catalogue, nil)(d)
	WithSnapshotRepository(repository)(d)
	WithRecoveryCoordinator(recoveryusecase.NewCoordinator(catalogue, repository, nil))(d)
	startSnapshotEncodeWorker(t, d)

	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(repository.release) }) }
	// Register after the worker cleanup so the uncooperative publication is
	// always released before cleanup asks the worker to join.
	t.Cleanup(release)

	live := newSnapshotTestSession(t, "work", false, "/work")
	require.NoError(t, catalogue.Create(live.persistRecordLocked(live.createdAt)))
	livePTY, ok := live.tabs[0].panes["pane-1"].pty.(*portsmocks.MockPTY)
	require.True(t, ok)
	livePTY.EXPECT().Close().Return(nil).Maybe()
	d.sessions[live.id] = live

	markSnapshotDirty(live)
	require.True(t, d.scheduleSnapshot(live))
	awaitTestValue(t, repository.entered, "the live publication did not start")

	purged := make(chan purgeResult, 1)
	go func() { purged <- d.purgeAllSessions(protocol.ReasonSessionKilled) }()

	_ = awaitTestValue(t, clock.timers, "purge did not arm its admission deadline")
	liveBudget := awaitTestValue(t, clock.timers, "purge did not arm a separate live-teardown budget")
	liveBudget.ch <- time.Now()

	result := awaitTestValue(t, purged, "purge did not return while the publisher stayed blocked")
	require.True(t, result.Failed(), result.summary())
	require.False(t, result.Superseded())
	require.Len(t, result.Failures, 1)
	require.Equal(t, purgeRecordLive, result.Failures[0].Class)
	require.Equal(t, "work", result.Failures[0].Name)
	require.ErrorIs(t, result.Failures[0].Err, errSessionKillDeadline)
	require.Equal(t, 1, sessionCount(d), "a skipped live unit must stay live")
	requirePurgeAdmissionBalanced(t, d)

	// The retry succeeds once the uncooperative publisher returns: the bound
	// released admission without discarding ownership of the unit.
	release()
	awaitSnapshotIdle(t, live)
	require.False(t, d.purgeAllSessions(protocol.ReasonSessionKilled).Failed())
	require.Equal(t, 0, sessionCount(d), "the retried purge must remove the unit")
	d.mu.Lock()
	_, stopped := d.inactive["work"]
	d.mu.Unlock()
	require.False(t, stopped, "the retried purge must not leave phantom stopped authority")
	d.sessWg.Wait()
	d.waitNotifies()
}

// TestPurgeAllLiveQuarantineAbortRestoresSurvivingSession proves the reversible
// half of the quarantine contract: when the shared live budget expires while a
// purge is joining an uncooperative snapshot publication, the abort rolls the
// coordinator quarantine back instead of leaving a registered live session
// permanently ineligible. The surviving session checkpoints again, an EOF
// teardown completes, and its attached client is never left frozen.
func TestPurgeAllLiveQuarantineAbortRestoresSurvivingSession(t *testing.T) {
	clock := &signalClock{timers: make(chan *signalTimer, 64)}
	d, live, ac, _ := newManualSessionWithPTYsClock(t, clock, newQuietPTY())
	live.createdAt = 42
	live.snapEligible.Store(true)

	catalogue := newDurableRecoveryCatalogue(nil)
	repository := newBlockingPublicationRepository()
	WithCatalogue(catalogue, nil)(d)
	WithSnapshotRepository(repository)(d)
	WithRecoveryCoordinator(recoveryusecase.NewCoordinator(catalogue, repository, nil))(d)
	startSnapshotEncodeWorker(t, d)

	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(repository.release) }) }
	// Register after the worker cleanup so the uncooperative publication is
	// always released before cleanup asks the worker to join.
	t.Cleanup(release)

	require.NoError(t, catalogue.Create(live.persistRecordLocked(live.createdAt)))

	// Park an uncooperative publication so the purge's coordinator join cannot
	// finish inside the shared live budget.
	markSnapshotDirty(live)
	require.True(t, d.scheduleSnapshot(live))
	awaitTestValue(t, repository.entered, "the live publication did not start")

	purged := make(chan purgeResult, 1)
	go func() { purged <- d.purgeAllSessions(protocol.ReasonSessionKilled) }()

	_ = awaitTestValue(t, clock.timers, "purge did not arm its admission deadline")
	liveBudget := awaitTestValue(t, clock.timers, "purge did not arm a separate live-teardown budget")
	liveBudget.ch <- time.Now()

	result := awaitTestValue(t, purged, "purge did not return while the publisher stayed blocked")
	require.True(t, result.Failed(), result.summary())
	require.False(t, result.Superseded())
	require.Len(t, result.Failures, 1)
	require.Equal(t, purgeRecordLive, result.Failures[0].Class)
	require.Equal(t, "work", result.Failures[0].Name)
	require.ErrorIs(t, result.Failures[0].Err, errSessionKillDeadline)
	require.Equal(t, 1, sessionCount(d), "a skipped live unit must stay live")
	requirePurgeAdmissionBalanced(t, d)

	// The abort rolled the quarantine back: the surviving session is eligible
	// and dirty again, and its attached client's gate is stable, not frozen.
	require.True(t, live.snapEligible.Load(), "an aborted purge must restore snapshot eligibility")
	live.snapshotMu.Lock()
	quarantined := live.snapshotQuarantined
	live.snapshotMu.Unlock()
	require.False(t, quarantined, "an aborted purge must roll back the coordinator quarantine")
	require.True(t, live.snapDirty.Load(), "an aborted purge must keep the surviving session dirty")
	ac.lifecycle.mu.Lock()
	phase := ac.lifecycle.phase
	ac.lifecycle.mu.Unlock()
	require.Equal(t, attachmentEffectsStable, phase, "an aborted purge must not leave the attached client frozen")

	// Once the uncooperative publication returns, the surviving session must
	// checkpoint again.
	release()
	awaitSnapshotIdle(t, live)
	// The fake clock's routine pacing interval is zero-now based, so clear it and
	// prove the rolled-back coordinator schedules a real replacement capture.
	live.snapshotMu.Lock()
	live.snapshotNextEligibleAt = time.Time{}
	live.snapshotMu.Unlock()
	require.True(t, d.scheduleSnapshot(live), "the surviving session must schedule a replacement checkpoint")
	awaitSnapshotClean(t, live)
	_, ok, err := catalogue.Record("work")
	require.NoError(t, err)
	require.True(t, ok, "the surviving session must keep its durable authority")

	// An EOF teardown of the surviving session must complete.
	require.NoError(t, d.killSession(live, protocol.ReasonSessionKilled, false))
	require.Equal(t, 0, sessionCount(d), "the EOF teardown must remove the surviving session")
	d.sessWg.Wait()
	d.waitNotifies()
}

// TestSessionKillPostQuarantineParticipantMismatchRestoresSurvivingSession is
// the deterministic post-quarantine half of the reversible teardown contract:
// once a direct session kill or last-tab close has quarantined the snapshot
// coordinator, a participant change caught by the pre-publication revalidation
// aborts without removing anything and rolls the quarantine back. The surviving
// session resumes checkpoint scheduling and a later teardown completes.
func TestSessionKillPostQuarantineParticipantMismatchRestoresSurvivingSession(t *testing.T) {
	tests := []struct {
		name string
		kill func(t *testing.T, d *Daemon, sess *session) error
	}{
		{
			name: "direct kill",
			kill: func(_ *testing.T, d *Daemon, sess *session) error {
				return d.killSession(sess, protocol.ReasonSessionKilled, true)
			},
		},
		{
			name: "close tab",
			kill: func(_ *testing.T, d *Daemon, sess *session) error {
				return d.closeTab(sess, sess.tabs[0], false)
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d, sess, ac, _ := newManualSessionWithPTYs(t, newQuietPTY())
			sess.snapEligible.Store(true)
			catalogue := newDurableRecoveryCatalogue(nil)
			repository := noOpSnapshotRepository{}
			WithCatalogue(catalogue, nil)(d)
			WithSnapshotRepository(repository)(d)
			WithRecoveryCoordinator(recoveryusecase.NewCoordinator(catalogue, repository, nil))(d)
			startSnapshotEncodeWorker(t, d)
			require.NoError(t, catalogue.Create(sess.persistRecordLocked(sess.createdAt)))

			d.afterSessionKillQuarantine = func(*session) {
				d.afterSessionKillQuarantine = nil
				// A token resume replaces the exact transport after the snapshot
				// coordinator was quarantined but before its publication revalidation,
				// so the teardown must abort and roll the quarantine back. The
				// attachment gate is frozen here, so only the transport incarnation
				// changes; the capability is rebound by the next real claim.
				fresh := &closeTrackingTransport{}
				ac.replaceTransport(fresh)
			}
			t.Cleanup(func() { d.afterSessionKillQuarantine = nil })

			require.ErrorIs(t, tt.kill(t, d, sess), errSessionKillParticipantsChanged)
			require.Equal(t, 1, sessionCount(d), "an aborted teardown must leave the session registered")

			require.True(t, sess.snapEligible.Load(), "the abort must restore snapshot eligibility")
			sess.snapshotMu.Lock()
			quarantined := sess.snapshotQuarantined
			sess.snapshotMu.Unlock()
			require.False(t, quarantined, "the abort must roll back the coordinator quarantine")
			require.True(t, sess.snapDirty.Load(), "the abort must keep the surviving session checkpointable")

			// The surviving session checkpoints again.
			require.True(t, d.scheduleSnapshot(sess), "the surviving session must schedule a checkpoint")
			awaitSnapshotClean(t, sess)
			_, ok, err := catalogue.Record("work")
			require.NoError(t, err)
			require.True(t, ok, "the surviving session must keep its durable authority")

			// A later teardown of the surviving session completes.
			require.NoError(t, d.killSession(sess, protocol.ReasonSessionKilled, true))
			require.Equal(t, 0, sessionCount(d), "the later teardown must remove the surviving session")
			d.sessWg.Wait()
			d.waitNotifies()
		})
	}
}

// blockingLiveDeleteRepository blocks its incarnation delete until released,
// ignores context cancellation, and signals when the blocked call returns, so a
// test can observe a detached live exact delete outliving the bounded purge.
type blockingLiveDeleteRepository struct {
	noOpSnapshotRepository

	entered  chan struct{}
	release  chan struct{}
	returned chan struct{}
	// err is returned once release closes, letting a test model a detached
	// delete that fails only after the bounded purge already gave up waiting.
	err  error
	once sync.Once
}

func newBlockingLiveDeleteRepository() *blockingLiveDeleteRepository {
	return &blockingLiveDeleteRepository{
		entered:  make(chan struct{}),
		release:  make(chan struct{}),
		returned: make(chan struct{}),
	}
}

func (r *blockingLiveDeleteRepository) DeleteIncarnation(context.Context, domain.IncarnationID) error {
	r.once.Do(func() { close(r.entered) })
	<-r.release
	select {
	case <-r.returned:
	default:
		close(r.returned)
	}
	return r.err
}

// TestPurgeAllLiveDurableDeleteBoundedDetachesAndReleasesGate proves a live
// unit's uncooperative exact durable delete cannot hold purgeAllMu, admission,
// or control: the purge returns a typed pending failure at the shared live
// budget while the live identity is already removed and its stopped authority
// still reserves the name. A second purge is not queued behind the blocked
// delete, and once the stale delete returns the name is free for a later
// same-name lifecycle.
func TestPurgeAllLiveDurableDeleteBoundedDetachesAndReleasesGate(t *testing.T) {
	clock := &signalClock{timers: make(chan *signalTimer, 64)}
	d := newTestDaemon(t, portsmocks.NewMockPTYFactory(t), clock)
	catalogue := newDurableRecoveryCatalogue(nil)
	repository := newBlockingLiveDeleteRepository()
	WithCatalogue(catalogue, nil)(d)
	WithSnapshotRepository(repository)(d)
	WithRecoveryCoordinator(recoveryusecase.NewCoordinator(catalogue, repository, nil))(d)

	live := newSnapshotTestSession(t, "work", false, "/work")
	require.NoError(t, catalogue.Create(live.persistRecordLocked(live.createdAt)))
	livePTY, ok := live.tabs[0].panes["pane-1"].pty.(*portsmocks.MockPTY)
	require.True(t, ok)
	livePTY.EXPECT().Close().Return(nil).Maybe()
	d.sessions[live.id] = live

	purged := make(chan purgeResult, 1)
	go func() { purged <- d.purgeAllSessions(protocol.ReasonSessionKilled) }()

	_ = awaitTestValue(t, clock.timers, "purge did not arm its admission deadline")
	liveBudget := awaitTestValue(t, clock.timers, "purge did not arm a separate live-teardown budget")
	awaitTestValue(t, repository.entered, "the live purge did not start its uncooperative delete")
	liveBudget.ch <- time.Now()

	result := awaitTestValue(t, purged, "purge did not return while the live durable delete stayed blocked")
	require.True(t, result.Failed(), result.summary())
	require.False(t, result.Superseded())
	require.Len(t, result.Failures, 1)
	require.Equal(t, purgeRecordLive, result.Failures[0].Class)
	require.Equal(t, "work", result.Failures[0].Name, "the pending delete must keep its exact name")
	require.ErrorIs(t, result.Failures[0].Err, errSessionPurgeDeletePending)

	require.Equal(t, 0, sessionCount(d), "the live identity must be removed before its durable delete")
	requirePurgeAdmissionBalanced(t, d)
	d.mu.Lock()
	closing := d.closing
	_, stoppedRemains := d.inactive["work"]
	d.mu.Unlock()
	require.False(t, closing, "a bounded live delete must leave the daemon serving")
	require.True(t, stoppedRemains, "a pending exact delete must reserve the name with stopped authority")

	// A second purge is not queued behind the blocked delete: it takes the purge
	// admission gate and runs while the stale delete still holds the coordinator
	// lock, so it is bounded by its own sweep instead of waiting on purgeAllMu.
	second := make(chan purgeResult, 1)
	go func() { second <- d.purgeAllSessions(protocol.ReasonSessionKilled) }()
	awaitPurgeGateClosed(t, d)

	// Releasing the stale delete lets the detached first finish and the second
	// purge settle. The durable record was already gone, so the second purge
	// clears the stale stopped authority without a new repository delete.
	close(repository.release)
	awaitTestValue(t, repository.returned, "the detached exact delete did not return")
	secondResult := awaitTestValue(t, second, "the second purge did not complete after the stale delete returned")
	require.False(t, secondResult.Failed(), secondResult.summary())
	require.False(t, secondResult.Superseded())
	d.mu.Lock()
	_, stoppedRemains = d.inactive["work"]
	taken := d.nameLiveOrStoppedLocked("work")
	d.mu.Unlock()
	require.False(t, stoppedRemains, "the second purge must clear the stale stopped authority")
	require.False(t, taken, "the purged name must be free for a later same-name lifecycle")

	// A later same-name lifecycle is safe to adopt and keeps its own authority.
	replacement := newSnapshotTestSession(t, "work", false, "/work")
	replacement.incarnation = domain.IncarnationID{2}
	replacement.createdAt = 43
	require.NoError(t, catalogue.Create(replacement.persistRecordLocked(replacement.createdAt)))
	replacementPTY, ok := replacement.tabs[0].panes["pane-1"].pty.(*portsmocks.MockPTY)
	require.True(t, ok)
	replacementPTY.EXPECT().Close().Return(nil).Maybe()
	d.sessions[replacement.id] = replacement
	record, ok, err := catalogue.Record("work")
	require.NoError(t, err)
	require.True(t, ok, "the replacement must own the same-name durable record")
	require.Equal(t, replacement.incarnation, record.IncarnationID)
	require.Equal(t, 1, sessionCount(d))
	d.sessWg.Wait()
	d.waitNotifies()
}

// TestPurgeAllDetachedLiveDeleteFencesSameNameReplacement proves the detached
// live exact delete removes only the stopped authority whose name, incarnation,
// and createdAt it captured: a same-name replacement lifecycle that holds the
// name when the stale delete returns is never removed.
func TestPurgeAllDetachedLiveDeleteFencesSameNameReplacement(t *testing.T) {
	clock := &signalClock{timers: make(chan *signalTimer, 64)}
	d := newTestDaemon(t, portsmocks.NewMockPTYFactory(t), clock)
	catalogue := newDurableRecoveryCatalogue(nil)
	repository := newBlockingLiveDeleteRepository()
	WithCatalogue(catalogue, nil)(d)
	WithSnapshotRepository(repository)(d)
	WithRecoveryCoordinator(recoveryusecase.NewCoordinator(catalogue, repository, nil))(d)

	live := newSnapshotTestSession(t, "work", false, "/work")
	require.NoError(t, catalogue.Create(live.persistRecordLocked(live.createdAt)))
	livePTY, ok := live.tabs[0].panes["pane-1"].pty.(*portsmocks.MockPTY)
	require.True(t, ok)
	livePTY.EXPECT().Close().Return(nil).Maybe()
	d.sessions[live.id] = live

	purged := make(chan purgeResult, 1)
	go func() { purged <- d.purgeAllSessions(protocol.ReasonSessionKilled) }()

	_ = awaitTestValue(t, clock.timers, "purge did not arm its admission deadline")
	liveBudget := awaitTestValue(t, clock.timers, "purge did not arm a separate live-teardown budget")
	awaitTestValue(t, repository.entered, "the live purge did not start its uncooperative delete")
	liveBudget.ch <- time.Now()
	result := awaitTestValue(t, purged, "purge did not return while the live durable delete stayed blocked")
	require.True(t, result.Failed(), result.summary())
	require.Len(t, result.Failures, 1)
	require.ErrorIs(t, result.Failures[0].Err, errSessionPurgeDeletePending)

	// A same-name replacement lifecycle takes the reserved name while the stale
	// delete is still blocked.
	replacement := inactiveSession{name: "work", createdAt: 99, incarnation: domain.IncarnationID{9}, purging: true}
	d.mu.Lock()
	d.inactive["work"] = replacement
	d.mu.Unlock()

	close(repository.release)
	awaitTestValue(t, repository.returned, "the detached exact delete did not return")
	d.mu.Lock()
	current, ok := d.inactive["work"]
	d.mu.Unlock()
	require.True(t, ok, "the detached stale delete must not remove a same-name replacement lifecycle")
	require.Equal(t, replacement.incarnation, current.incarnation)
	d.sessWg.Wait()
	d.waitNotifies()
}

// TestPurgeAllLogsDetachedLiveDeleteLateFailure proves a detached live exact
// delete that fails only after the bounded purge already returned a typed
// pending verdict is still recorded, using bounded non-sensitive fields instead
// of the user-chosen session name.
func TestPurgeAllLogsDetachedLiveDeleteLateFailure(t *testing.T) {
	clock := &signalClock{timers: make(chan *signalTimer, 64)}
	d := newTestDaemon(t, portsmocks.NewMockPTYFactory(t), clock)
	var logged syncedLogBuffer
	d.log = slog.New(slog.NewTextHandler(&logged, nil))
	catalogue := newDurableRecoveryCatalogue(nil)
	repository := newBlockingLiveDeleteRepository()
	repository.err = errors.New("detached delete failed")
	WithCatalogue(catalogue, nil)(d)
	WithSnapshotRepository(repository)(d)
	WithRecoveryCoordinator(recoveryusecase.NewCoordinator(catalogue, repository, nil))(d)

	live := newSnapshotTestSession(t, "work", false, "/work")
	require.NoError(t, catalogue.Create(live.persistRecordLocked(live.createdAt)))
	livePTY, ok := live.tabs[0].panes["pane-1"].pty.(*portsmocks.MockPTY)
	require.True(t, ok)
	livePTY.EXPECT().Close().Return(nil).Maybe()
	d.sessions[live.id] = live

	purged := make(chan purgeResult, 1)
	go func() { purged <- d.purgeAllSessions(protocol.ReasonSessionKilled) }()

	_ = awaitTestValue(t, clock.timers, "purge did not arm its admission deadline")
	liveBudget := awaitTestValue(t, clock.timers, "purge did not arm a separate live-teardown budget")
	awaitTestValue(t, repository.entered, "the live purge did not start its uncooperative delete")
	liveBudget.ch <- time.Now()

	result := awaitTestValue(t, purged, "purge did not return while the live durable delete stayed blocked")
	require.True(t, result.Failed(), result.summary())
	require.Len(t, result.Failures, 1)
	require.ErrorIs(t, result.Failures[0].Err, errSessionPurgeDeletePending)

	// The stale delete fails after the bounded caller already returned, and the
	// detached waiter records the late failure.
	close(repository.release)
	awaitTestValue(t, repository.returned, "the detached exact delete did not return")
	require.Eventually(t, func() bool {
		return strings.Contains(logged.String(), "detached session purge delete failed after deadline")
	}, testWaitTimeout, time.Millisecond, "a late detached delete failure must be logged")

	lateLine := ""
	for _, line := range strings.Split(logged.String(), "\n") {
		if strings.Contains(line, "detached session purge delete failed after deadline") {
			lateLine = line
		}
	}
	require.Contains(t, lateLine, "detached delete failed")
	require.NotContains(t, lateLine, "work", "the late failure must not log the user-chosen session name")
	d.sessWg.Wait()
	d.waitNotifies()
}
