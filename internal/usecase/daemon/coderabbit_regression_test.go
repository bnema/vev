package daemon

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"sync"
	"syscall"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
	portsmocks "github.com/bnema/vev/internal/ports/mocks"
	"github.com/bnema/vev/internal/protocol"
	snapcodec "github.com/bnema/vev/internal/snapshotcodec"
	"github.com/bnema/vev/internal/usecase/picker"
	recoveryusecase "github.com/bnema/vev/internal/usecase/recovery"
)

func TestRestoreAmbiguousBadVersionLoadFailurePreservesCheckpoint(t *testing.T) {
	record := durableRecoveryRecord(0)
	repositoryState := &recoveryRepositoryState{
		errors:  map[string]error{record.Name: errors.Join(snapcodec.ErrBadVersion, syscall.ENOSPC)},
		loads:   make(map[string]int),
		repairs: make(map[string]domain.CheckpointRef),
	}
	repository := newRecoveryRepositoryMock(t, repositoryState)
	d, catalogue := newDurableRecoveryDaemon(t, []domain.CatalogueRecord{record}, repository)

	d.restoreIncrementalSnapshots(context.Background())

	persisted, ok, err := catalogue.Record(record.Name)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, record.IncarnationID, persisted.IncarnationID)
	require.Equal(t, record.Committed, persisted.Committed)
	require.Equal(t, "checkpoint load failed", persisted.DegradedReason)
	require.Empty(t, repositoryState.deleted)
	d.mu.Lock()
	entry := d.inactive[record.Name]
	d.mu.Unlock()
	require.Equal(t, protocol.SessionBroken, entry.state)
	require.Equal(t, persisted, entry.record)
}

func TestRestoreTransientLoadFailureBecomesDiscardable(t *testing.T) {
	record := durableRecoveryRecord(0)
	repositoryState := &recoveryRepositoryState{
		errors:  map[string]error{record.Name: syscall.ENOSPC},
		loads:   make(map[string]int),
		repairs: make(map[string]domain.CheckpointRef),
	}
	repository := newRecoveryRepositoryMock(t, repositoryState)
	d, catalogue := newDurableRecoveryDaemon(t, []domain.CatalogueRecord{record}, repository)

	d.restoreIncrementalSnapshots(context.Background())

	persisted, ok, err := catalogue.Record(record.Name)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, "checkpoint load failed", persisted.DegradedReason)
	d.mu.Lock()
	entry := d.inactive[record.Name]
	d.mu.Unlock()
	require.Equal(t, protocol.SessionBroken, entry.state)
	require.Equal(t, persisted, entry.record)
	require.Equal(t, protocol.SessionBroken, listSessions(t, d).Sessions[0].State)
	select {
	case <-entry.restoreDone:
	default:
		t.Fatal("restore completion was not signaled")
	}
}

func TestRestoreFailureCannotOverwriteDiscardReplacement(t *testing.T) {
	record := durableRecoveryRecord(0)
	record.DegradedReason = "previous restore failure"
	deleteEntered := make(chan struct{})
	releaseDelete := make(chan struct{})
	repository := newRecoveryRepositoryMock(t, nil, func(m *portsmocks.MockSnapshotRepository, _ *recoveryRepositoryState) {
		m.EXPECT().DeleteIncarnation(mock.Anything, mock.Anything).RunAndReturn(func(context.Context, domain.IncarnationID) error {
			close(deleteEntered)
			<-releaseDelete
			return nil
		}).Maybe()
	})
	d, catalogue := newDurableRecoveryDaemon(t, []domain.CatalogueRecord{record}, repository)
	d.recovery = recoveryusecase.NewCoordinator(catalogue, repository, bytes.NewReader(bytes.Repeat([]byte{7}, 16)))

	discarded := make(chan error, 1)
	go func() {
		_, err := (controlExec{ctx: t.Context(), d: d, recoveryName: record.Name}).SessionRecovery("discard")
		discarded <- err
	}()
	<-deleteEntered // The fresh record is now authoritative behind Discard's mutation fence.

	finished := make(chan struct{})
	go func() {
		d.finishRecordRestore(record, errors.New("late restore failure"), make(chan struct{}))
		close(finished)
	}()

	close(releaseDelete)
	require.NoError(t, <-discarded)
	<-finished

	persisted, ok, err := catalogue.Record(record.Name)
	require.NoError(t, err)
	require.True(t, ok)
	require.NotEqual(t, record.IncarnationID, persisted.IncarnationID)
	require.Nil(t, persisted.Committed)
	require.Empty(t, persisted.DegradedReason)
	d.mu.Lock()
	entry := d.inactive[record.Name]
	d.mu.Unlock()
	require.Equal(t, persisted, entry.record)
	require.Equal(t, protocol.SessionDown, entry.state)
}

func TestStaleIncompatibleRestoreCannotOverwriteNewerAuthority(t *testing.T) {
	tests := []struct {
		name        string
		replacement func(domain.CatalogueRecord) domain.CatalogueRecord
		state       protocol.SessionState
	}{
		{
			name: "newer checkpoint",
			replacement: func(record domain.CatalogueRecord) domain.CatalogueRecord {
				ref := *record.Committed
				ref.Generation++
				ref.ManifestDigest[0]++
				record.Committed = &ref
				return record
			},
			state: protocol.SessionDown,
		},
		{
			name: "newer incarnation",
			replacement: func(record domain.CatalogueRecord) domain.CatalogueRecord {
				record.IncarnationID = domain.IncarnationID{9}
				return record
			},
			state: protocol.SessionDown,
		},
		{
			name: "degraded authority",
			replacement: func(record domain.CatalogueRecord) domain.CatalogueRecord {
				record.DegradedReason = "newer restore decision"
				return record
			},
			state: protocol.SessionBroken,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			record := durableRecoveryRecord(0)
			replacement := tt.replacement(record)
			repositoryState := &recoveryRepositoryState{
				errors:  map[string]error{record.Name: snapcodec.ErrBadVersion},
				loads:   make(map[string]int),
				repairs: make(map[string]domain.CheckpointRef),
			}
			repository := newRecoveryRepositoryMock(t, repositoryState)
			d, catalogue := newDurableRecoveryDaemon(t, []domain.CatalogueRecord{record}, repository)
			var logs bytes.Buffer
			d.log = slog.New(slog.NewJSONHandler(&logs, nil))
			repositoryState.onLoad = func() {
				require.NoError(t, catalogue.Replace(record.Name, replacement))
				d.setStoppedRecovery(replacement, tt.state)
			}

			d.restoreIncrementalSnapshots(context.Background())

			persisted, ok, err := catalogue.Record(record.Name)
			require.NoError(t, err)
			require.True(t, ok)
			require.Equal(t, replacement, persisted)
			d.mu.Lock()
			entry := d.inactive[record.Name]
			d.mu.Unlock()
			require.Equal(t, tt.state, entry.state)
			require.Equal(t, replacement, entry.record)
			require.Empty(t, repositoryState.deleted)
			daemonRequireEvent(t, daemonJSONLogs(t, logs.Bytes()), "snapshot_incompatible_reset_superseded")
		})
	}
}

func TestFinishRecordRestoreAlwaysClosesProvidedBarrier(t *testing.T) {
	d := newTestDaemon(t, nil, newStubClock(t))
	done := make(chan struct{})

	d.finishRecordRestore(domain.CatalogueRecord{Name: "removed"}, nil, done)

	select {
	case <-done:
	default:
		t.Fatal("restore completion was not signaled")
	}
}

func TestCreateSessionRechecksShutdownAfterCatalogueRead(t *testing.T) {
	record := durableRecoveryRecord(0)
	catalogue := newBlockingRecordCatalogue(t, []domain.CatalogueRecord{record})
	d := newTestDaemon(t, nil, newStubClock(t))
	d.catalogue = catalogue.mock
	d.persistEnabled = true
	d.inactive[record.Name] = inactiveSessionFromRecord(record, protocol.SessionDown, make(chan struct{}))

	result := make(chan error, 1)
	go func() {
		_, err := createSessionForTest(d, record.Name, false, "/tmp", domain.Size{Cols: 80, Rows: 24}, terminalEnv{}, nil)
		result <- err
	}()
	<-catalogue.entered
	d.mu.Lock()
	d.closing = true
	d.mu.Unlock()
	close(catalogue.release)

	err := <-result
	require.ErrorContains(t, err, "daemon is shutting down")
	d.mu.Lock()
	defer d.mu.Unlock()
	require.Empty(t, d.sessions)
}

// blockingRecordCatalogue is a recovery catalogue mock whose first Record read
// signals entered and then blocks until release is closed.
type blockingRecordCatalogue struct {
	mock    *portsmocks.MockCatalogue
	state   *recoveryCatalogueState
	entered chan struct{}
	release chan struct{}
}

func newBlockingRecordCatalogue(t testing.TB, records []domain.CatalogueRecord) *blockingRecordCatalogue {
	t.Helper()
	blocking := &blockingRecordCatalogue{entered: make(chan struct{}), release: make(chan struct{})}
	var once sync.Once
	blocking.mock, blocking.state = newRecoveryCatalogueMock(t, records,
		func(m *portsmocks.MockCatalogue, state *recoveryCatalogueState) {
			m.EXPECT().Record(mock.Anything).RunAndReturn(func(name string) (domain.CatalogueRecord, bool, error) {
				once.Do(func() { close(blocking.entered) })
				<-blocking.release
				return state.Record(name)
			}).Maybe()
		})
	return blocking
}

func TestResumeStoppedSessionRejectsReplacementDuringCatalogueRead(t *testing.T) {
	sourcePTY, releaseSource := newBlockingPTY(t)
	defer releaseSource()
	targetPTY, releaseTarget := newBlockingPTY(t)
	defer releaseTarget()
	d, from, ac, _ := newManualSessionWithPTYs(t, sourcePTY)
	d.ptys = newFactorySeq(t, targetPTY)

	record := durableRecoveryRecord(1)
	record.Name = "stopped"
	catalogue := newBlockingRecordCatalogue(t, []domain.CatalogueRecord{record})
	d.catalogue = catalogue.mock
	d.persistEnabled = true
	d.recovery = recoveryusecase.NewCoordinator(catalogue.mock, newSnapshotRepositoryMock(t), nil)
	d.inactive[record.Name] = inactiveSessionFromRecord(record, protocol.SessionDown, nil)

	target := picker.Target{
		Name:              record.Name,
		Stopped:           true,
		Incarnation:       record.IncarnationID,
		ExpectedCreatedAt: &record.CreatedAt,
	}
	result := make(chan bool, 1)
	go func() { result <- d.resumeStoppedAndSwitch(from, ac, target) }()

	<-catalogue.entered
	replacement := record
	replacement.IncarnationID = domain.IncarnationID{9}
	replacement.CreatedAt++
	replacement.UpdatedAt = replacement.CreatedAt
	require.NoError(t, catalogue.state.Replace(replacement.Name, replacement))
	d.mu.Lock()
	d.inactive[replacement.Name] = inactiveSessionFromRecord(replacement, protocol.SessionDown, nil)
	d.mu.Unlock()
	close(catalogue.release)

	require.False(t, <-result)
	require.Same(t, from, ac.currentSession())
	require.Empty(t, catalogue.state.MetadataUpdates())
	d.mu.Lock()
	defer d.mu.Unlock()
	require.Nil(t, d.findByNameLocked(record.Name))
	require.Equal(t, replacement.IncarnationID, d.inactive[record.Name].incarnation)
}

func TestSessionAttachTargetRejectsInactiveLifecycleReplacementDuringCatalogueRead(t *testing.T) {
	d := newTestDaemon(t, newFactory(t, newQuietPTY(t)), newStubClock(t))
	record := durableRecoveryRecord(1)
	record.Name = "stopped"
	record.Committed = nil
	record.TabNames = []string{"main"}
	record.TabRecords = []domain.CatalogueTabRecord{{StableID: "tab-1", Name: "main"}}
	catalogue := newBlockingRecordCatalogue(t, []domain.CatalogueRecord{record})
	d.catalogue = catalogue.mock
	d.persistEnabled = true
	d.recovery = recoveryusecase.NewCoordinator(catalogue.mock, newSnapshotRepositoryMock(t), nil)
	d.inactive[record.Name] = inactiveSessionFromRecord(record, protocol.SessionDown, nil)

	target := protocol.SessionAttachTarget{
		LifecycleID: record.IncarnationID, SessionName: record.Name,
		TabID: "tab-1", TabIndex: protocol.NoTabIndex, Stopped: true,
	}
	transport, _ := newCapturingTransport(t)
	result := make(chan error, 1)
	go func() {
		_, _, err := d.routeWithContext(context.Background(), protocol.Hello{
			Version: protocol.Version, Intent: protocol.IntentAttach, Name: record.Name,
			Size: domain.Size{Cols: 80, Rows: 24}, SessionTarget: ptrSessionAttachTarget(target),
			EnvironmentPolicy: protocol.EnvironmentPolicyDaemonOwned,
		}, transport)
		result <- err
	}()

	<-catalogue.entered
	replacement := record
	replacement.IncarnationID = domain.IncarnationID{9}
	replacement.CreatedAt++
	replacement.UpdatedAt = replacement.CreatedAt
	require.NoError(t, catalogue.state.Replace(replacement.Name, replacement))
	d.mu.Lock()
	d.inactive[replacement.Name] = inactiveSessionFromRecord(replacement, protocol.SessionDown, nil)
	d.mu.Unlock()
	close(catalogue.release)

	var protocolErr *protoErr
	require.ErrorAs(t, <-result, &protocolErr)
	require.Equal(t, protocol.ErrNoSuchTarget, protocolErr.code)
	require.Empty(t, catalogue.state.MetadataUpdates())
	d.mu.Lock()
	defer d.mu.Unlock()
	require.Nil(t, d.findByNameLocked(record.Name))
	require.Equal(t, replacement.IncarnationID, d.inactive[record.Name].incarnation)
}

func TestSessionAttachTargetRejectsTabAuthorityChangeDuringCatalogueRead(t *testing.T) {
	for _, test := range []struct {
		name      string
		record    domain.CatalogueRecord
		target    domain.RemoteSessionTarget
		mutate    func(*domain.CatalogueRecord)
		updateMap bool
	}{
		{
			name:   "stable selector replacement",
			record: domain.CatalogueRecord{TabNames: []string{"main"}, TabRecords: []domain.CatalogueTabRecord{{StableID: "tab-1", Name: "main"}}},
			target: domain.RemoteSessionTarget{Stopped: true, StoppedTab: domain.NewStableTabSelector("tab-1")},
			mutate: func(record *domain.CatalogueRecord) {
				record.TabRecords[0].StableID = "tab-2"
			},
			updateMap: true,
		},
		{
			name:   "ordinal selector replacement",
			record: domain.CatalogueRecord{TabNames: []string{"main"}, TabRecords: []domain.CatalogueTabRecord{{Name: "main"}}},
			target: domain.RemoteSessionTarget{Stopped: true, StoppedTab: domain.NewOrdinalTabSelector(0, "main", 1)},
			mutate: func(record *domain.CatalogueRecord) {
				record.TabNames[0] = "other"
				record.TabRecords[0].Name = "other"
			},
			updateMap: true,
		},
		{
			name:   "catalogue-only metadata change",
			record: domain.CatalogueRecord{TabNames: []string{"main"}, TabRecords: []domain.CatalogueTabRecord{{StableID: "tab-1", Name: "main"}}},
			target: domain.RemoteSessionTarget{Stopped: true, StoppedTab: domain.NewStableTabSelector("tab-1")},
			mutate: func(record *domain.CatalogueRecord) {
				record.TabRecords[0].StableID = "tab-2"
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			record := durableRecoveryRecord(1)
			record.Name = "stopped"
			record.Committed = nil
			record.TabNames = append([]string(nil), test.record.TabNames...)
			record.TabRecords = append([]domain.CatalogueTabRecord(nil), test.record.TabRecords...)
			catalogue := newBlockingRecordCatalogue(t, []domain.CatalogueRecord{record})
			d := newTestDaemon(t, portsmocks.NewMockPTYFactory(t), newStubClock(t))
			d.catalogue = catalogue.mock
			d.persistEnabled = true
			d.recovery = recoveryusecase.NewCoordinator(catalogue.mock, newSnapshotRepositoryMock(t), nil)
			d.inactive[record.Name] = inactiveSessionFromRecord(record, protocol.SessionDown, nil)

			target := test.target
			target.Endpoint = "remote"
			target.DisplayOrigin = "remote"
			target.LifecycleID = record.IncarnationID
			target.SessionName = record.Name
			transport, _ := newCapturingTransport(t)
			result := make(chan error, 1)
			go func() {
				_, _, err := d.routeWithContext(context.Background(), protocol.Hello{
					Version: protocol.Version, Intent: protocol.IntentAttach, Name: record.Name,
					Size: domain.Size{Cols: 80, Rows: 24}, SessionTarget: ptrSessionAttachTarget(protocol.SessionAttachTargetFromRemote(target)),
					EnvironmentPolicy: protocol.EnvironmentPolicyDaemonOwned,
				}, transport)
				result <- err
			}()

			<-catalogue.entered
			replacement := record
			replacement.TabNames = append([]string(nil), record.TabNames...)
			replacement.TabRecords = append([]domain.CatalogueTabRecord(nil), record.TabRecords...)
			test.mutate(&replacement)
			replacement.UpdatedAt++
			require.NoError(t, catalogue.state.Replace(replacement.Name, replacement))
			if test.updateMap {
				d.mu.Lock()
				d.inactive[replacement.Name] = inactiveSessionFromRecord(replacement, protocol.SessionDown, nil)
				d.mu.Unlock()
			}
			close(catalogue.release)

			var protocolErr *protoErr
			require.ErrorAs(t, <-result, &protocolErr)
			require.Equal(t, protocol.ErrNoSuchTarget, protocolErr.code)
			require.Empty(t, catalogue.state.MetadataUpdates())
			d.mu.Lock()
			defer d.mu.Unlock()
			require.Nil(t, d.findByNameLocked(record.Name))
		})
	}
}

func TestSnapshotStopContextCancelIsIdempotent(t *testing.T) {
	deadline := &snapshotShutdownDeadline{done: make(chan struct{})}
	_, cancel := snapshotStopContext(deadline)

	var wg sync.WaitGroup
	for range 8 {
		wg.Go(cancel)
	}
	wg.Wait()
}
