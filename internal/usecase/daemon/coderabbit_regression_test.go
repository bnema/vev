package daemon

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"syscall"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	recoveryusecase "github.com/bnema/vev/internal/usecase/recovery"
)

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
	require.Equal(t, ports.SessionStopped, entry.state)
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

func TestSnapshotStopContextCancelIsIdempotent(t *testing.T) {
	deadline := &snapshotShutdownDeadline{done: make(chan struct{})}
	_, cancel := snapshotStopContext(deadline)

	var wg sync.WaitGroup
	for range 8 {
		wg.Go(cancel)
	}
	wg.Wait()
}
