package daemon

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"sync"
	"syscall"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/usecase/picker"
	recoveryusecase "github.com/bnema/vev/internal/usecase/recovery"
	snapcodec "github.com/bnema/vev/internal/usecase/snapshot"
)

func TestRestoreAmbiguousBadVersionLoadFailurePreservesCheckpoint(t *testing.T) {
	record := durableRecoveryRecord(0)
	repository := &durableRecoveryRepository{
		errors:  map[string]error{record.Name: errors.Join(snapcodec.ErrBadVersion, syscall.ENOSPC)},
		loads:   make(map[string]int),
		repairs: make(map[string]domain.CheckpointRef),
	}
	d, catalogue := newDurableRecoveryDaemon(t, []domain.CatalogueRecord{record}, repository)

	d.restoreCatalogue(context.Background(), mustDurableRecords(t, catalogue))

	persisted, ok, err := catalogue.Record(record.Name)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, record.IncarnationID, persisted.IncarnationID)
	require.Equal(t, record.Committed, persisted.Committed)
	require.Equal(t, "checkpoint load failed", persisted.DegradedReason)
	require.Empty(t, repository.deleted)
	d.mu.Lock()
	entry := d.stopped[record.Name]
	d.mu.Unlock()
	require.Equal(t, ports.SessionBroken, entry.state)
	require.Equal(t, persisted, entry.record)
}

func TestRestoreTransientLoadFailureBecomesDiscardable(t *testing.T) {
	record := durableRecoveryRecord(0)
	repository := &durableRecoveryRepository{
		errors:  map[string]error{record.Name: syscall.ENOSPC},
		loads:   make(map[string]int),
		repairs: make(map[string]domain.CheckpointRef),
	}
	d, catalogue := newDurableRecoveryDaemon(t, []domain.CatalogueRecord{record}, repository)

	d.restoreCatalogue(context.Background(), mustDurableRecords(t, catalogue))

	persisted, ok, err := catalogue.Record(record.Name)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, "checkpoint load failed", persisted.DegradedReason)
	d.mu.Lock()
	entry := d.stopped[record.Name]
	d.mu.Unlock()
	require.Equal(t, ports.SessionBroken, entry.state)
	require.Equal(t, persisted, entry.record)
	require.Equal(t, ports.SessionBroken, listSessions(t, d).Sessions[0].State)
	select {
	case <-entry.restoreDone:
	default:
		t.Fatal("restore completion was not signaled")
	}
}

type blockingDiscardRecoveryRepository struct {
	*durableRecoveryRepository
	deleteEntered chan struct{}
	releaseDelete chan struct{}
}

func (r *blockingDiscardRecoveryRepository) DeleteIncarnation(context.Context, domain.IncarnationID) error {
	close(r.deleteEntered)
	<-r.releaseDelete
	return nil
}

func TestRestoreFailureCannotOverwriteDiscardReplacement(t *testing.T) {
	record := durableRecoveryRecord(0)
	record.DegradedReason = "previous restore failure"
	repository := &blockingDiscardRecoveryRepository{
		durableRecoveryRepository: &durableRecoveryRepository{},
		deleteEntered:             make(chan struct{}),
		releaseDelete:             make(chan struct{}),
	}
	d, catalogue := newDurableRecoveryDaemon(t, []domain.CatalogueRecord{record}, repository)
	d.recovery = recoveryusecase.NewCoordinator(catalogue, repository, bytes.NewReader(bytes.Repeat([]byte{7}, 16)))

	discarded := make(chan error, 1)
	go func() {
		_, err := (controlExec{ctx: t.Context(), d: d, recoveryName: record.Name}).SessionRecovery("discard")
		discarded <- err
	}()
	<-repository.deleteEntered // The fresh record is now authoritative behind Discard's mutation fence.

	finished := make(chan struct{})
	go func() {
		d.finishRecordRestore(record, errors.New("late restore failure"), make(chan struct{}))
		close(finished)
	}()

	close(repository.releaseDelete)
	require.NoError(t, <-discarded)
	<-finished

	persisted, ok, err := catalogue.Record(record.Name)
	require.NoError(t, err)
	require.True(t, ok)
	require.NotEqual(t, record.IncarnationID, persisted.IncarnationID)
	require.Nil(t, persisted.Committed)
	require.Empty(t, persisted.DegradedReason)
	d.mu.Lock()
	entry := d.stopped[record.Name]
	d.mu.Unlock()
	require.Equal(t, persisted, entry.record)
	require.Equal(t, ports.SessionDown, entry.state)
}

func TestStaleIncompatibleRestoreCannotOverwriteNewerAuthority(t *testing.T) {
	tests := []struct {
		name        string
		replacement func(domain.CatalogueRecord) domain.CatalogueRecord
		state       ports.SessionState
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
			state: ports.SessionDown,
		},
		{
			name: "newer incarnation",
			replacement: func(record domain.CatalogueRecord) domain.CatalogueRecord {
				record.IncarnationID = domain.IncarnationID{9}
				return record
			},
			state: ports.SessionDown,
		},
		{
			name: "degraded authority",
			replacement: func(record domain.CatalogueRecord) domain.CatalogueRecord {
				record.DegradedReason = "newer restore decision"
				return record
			},
			state: ports.SessionBroken,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			record := durableRecoveryRecord(0)
			replacement := tt.replacement(record)
			repository := &durableRecoveryRepository{
				errors:  map[string]error{record.Name: snapcodec.ErrBadVersion},
				loads:   make(map[string]int),
				repairs: make(map[string]domain.CheckpointRef),
			}
			d, catalogue := newDurableRecoveryDaemon(t, []domain.CatalogueRecord{record}, repository)
			var logs bytes.Buffer
			d.log = slog.New(slog.NewJSONHandler(&logs, nil))
			repository.onLoad = func() {
				require.NoError(t, catalogue.Replace(record.Name, replacement))
				d.setStoppedRecovery(replacement, tt.state)
			}

			d.restoreCatalogue(context.Background(), mustDurableRecords(t, catalogue))

			persisted, ok, err := catalogue.Record(record.Name)
			require.NoError(t, err)
			require.True(t, ok)
			require.Equal(t, replacement, persisted)
			d.mu.Lock()
			entry := d.stopped[record.Name]
			d.mu.Unlock()
			require.Equal(t, tt.state, entry.state)
			require.Equal(t, replacement, entry.record)
			require.Empty(t, repository.deleted)
			daemonRequireEvent(t, daemonJSONLogs(t, logs.Bytes()), "snapshot_incompatible_reset_superseded")
		})
	}
}

func TestFinishRecordRestoreAlwaysClosesProvidedBarrier(t *testing.T) {
	d := newTestDaemon(t, nil, stubClock{})
	done := make(chan struct{})

	d.finishRecordRestore(domain.CatalogueRecord{Name: "removed"}, nil, done)

	select {
	case <-done:
	default:
		t.Fatal("restore completion was not signaled")
	}
}

type blockingRecordCatalogue struct {
	ports.Catalogue
	record  domain.CatalogueRecord
	entered chan struct{}
	release chan struct{}
}

func (c *blockingRecordCatalogue) Record(string) (domain.CatalogueRecord, bool, error) {
	close(c.entered)
	<-c.release
	return c.record, true, nil
}

func TestCreateSessionRechecksShutdownAfterCatalogueRead(t *testing.T) {
	record := durableRecoveryRecord(0)
	catalogue := &blockingRecordCatalogue{record: record, entered: make(chan struct{}), release: make(chan struct{})}
	d := newTestDaemon(t, nil, stubClock{})
	d.catalogue = catalogue
	d.persistEnabled = true
	d.stopped[record.Name] = stoppedSessionFromRecord(record, ports.SessionDown, make(chan struct{}))

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

type blockingResumeCatalogue struct {
	*durableRecoveryCatalogue
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (c *blockingResumeCatalogue) Record(name string) (domain.CatalogueRecord, bool, error) {
	c.once.Do(func() { close(c.entered) })
	<-c.release
	return c.durableRecoveryCatalogue.Record(name)
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
	catalogue := &blockingResumeCatalogue{
		durableRecoveryCatalogue: newDurableRecoveryCatalogue([]domain.CatalogueRecord{record}),
		entered:                  make(chan struct{}),
		release:                  make(chan struct{}),
	}
	d.catalogue = catalogue
	d.persistEnabled = true
	d.recovery = recoveryusecase.NewCoordinator(catalogue, noOpSnapshotRepository{}, nil)
	d.stopped[record.Name] = stoppedSessionFromRecord(record, ports.SessionDown, nil)

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
	require.NoError(t, catalogue.Replace(replacement.Name, replacement))
	d.mu.Lock()
	d.stopped[replacement.Name] = stoppedSessionFromRecord(replacement, ports.SessionDown, nil)
	d.mu.Unlock()
	close(catalogue.release)

	require.False(t, <-result)
	require.Same(t, from, ac.currentSession())
	require.Empty(t, catalogue.MetadataUpdates())
	d.mu.Lock()
	defer d.mu.Unlock()
	require.Nil(t, d.findByNameLocked(record.Name))
	require.Equal(t, replacement.IncarnationID, d.stopped[record.Name].incarnation)
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
