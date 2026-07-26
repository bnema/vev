package daemon

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
)

func TestRestoreTransientLoadFailurePreservesHealthyAuthority(t *testing.T) {
	record := durableRecoveryRecord(0)
	repository := &durableRecoveryRepository{
		errors:  map[string]error{record.Name: ports.ErrBudgetExhausted},
		loads:   make(map[string]int),
		repairs: make(map[string]domain.CheckpointRef),
	}
	d, catalogue := newDurableRecoveryDaemon(t, []domain.CatalogueRecord{record}, repository)

	d.restoreCatalogue(context.Background(), mustDurableRecords(t, catalogue))

	persisted, ok, err := catalogue.Record(record.Name)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, domain.RecoveryHealthy, persisted.RecoveryState)
	d.mu.Lock()
	entry := d.stopped[record.Name]
	d.mu.Unlock()
	require.Equal(t, runtimeRestoring, entry.state)
	require.NotEqual(t, runtimeDegraded, entry.state)
	select {
	case <-entry.restoreDone:
	default:
		t.Fatal("restore completion was not signaled")
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

func TestSnapshotStopContextCancelIsIdempotent(t *testing.T) {
	deadline := &snapshotShutdownDeadline{done: make(chan struct{})}
	_, cancel := snapshotStopContext(deadline)

	var wg sync.WaitGroup
	for range 8 {
		wg.Go(cancel)
	}
	wg.Wait()
}
