package daemon

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"

	"github.com/bnema/vev/internal/domain"
	portsmocks "github.com/bnema/vev/internal/ports/mocks"
	recoveryusecase "github.com/bnema/vev/internal/usecase/recovery"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestSnapshotGarbageCollectionRunsAfterRestorationThenPeriodically(t *testing.T) {
	committed := domain.CheckpointRef{Generation: 3, ManifestDigest: [32]byte{3}}
	records := []domain.CatalogueRecord{{Name: "work", IncarnationID: domain.IncarnationID{1}, Committed: &committed}}
	id := records[0].IncarnationID

	tests := []struct {
		name        string
		recordsErr  error
		wantCollect bool
	}{
		{name: "collects after restoration and on every tick", wantCollect: true},
		{name: "unreadable catalogue never collects", recordsErr: errors.New("catalogue read failed")},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				const passes = 2
				catalogue := portsmocks.NewMockCatalogue(t)
				repository := portsmocks.NewMockSnapshotRepository(t)
				catalogue.EXPECT().Records().Return(records, tt.recordsErr).Times(passes)
				repository.EXPECT().SnapshotIncarnations(mock.Anything).Return([]domain.IncarnationID{id}, nil).Times(passes)
				collected := make(chan struct{}, passes)
				if tt.wantCollect {
					repository.EXPECT().CollectIncarnationGarbage(mock.Anything, id, &committed).
						Run(func(context.Context, domain.IncarnationID, *domain.CheckpointRef) { collected <- struct{}{} }).
						Return(nil).Times(passes)
				}

				clk := &signalClock{timers: make(chan *signalTimer, 1)}
				d := newTestDaemon(t, portsmocks.NewMockPTYFactory(t), clk)
				WithRecoveryCoordinator(recoveryusecase.NewCoordinator(catalogue, repository, nil))(d)
				WithSnapshotGarbageCollection()(d)
				d.startDurableMaintenance()

				synctest.Wait()
				require.Empty(t, clk.timers, "GC must wait for restoration to finish")
				d.closeRestoreDone()

				for pass := range passes {
					synctest.Wait()
					timer := <-clk.timers
					require.Equal(t, snapshotGarbageCollectionInterval, timer.duration)
					if tt.wantCollect {
						require.Len(t, collected, 1)
						<-collected
					}
					if pass < passes-1 {
						timer.ch <- time.Time{}
					}
				}

				d.serveCancel()
				d.WaitDurableWriters()
			})
		})
	}
}

func TestSnapshotGarbageCollectionRequiresExplicitOption(t *testing.T) {
	d := newTestDaemon(t, portsmocks.NewMockPTYFactory(t), stubClock{})
	coordinator := recoveryusecase.NewCoordinator(portsmocks.NewMockCatalogue(t), portsmocks.NewMockSnapshotRepository(t), nil)
	WithRecoveryCoordinator(coordinator)(d)
	d.startDurableMaintenance()

	d.snapshotWorkerMu.Lock()
	defer d.snapshotWorkerMu.Unlock()
	require.Nil(t, d.maintenanceWorkerDone)
}
