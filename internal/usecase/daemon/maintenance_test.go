package daemon

import (
	"context"
	"testing"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	portsmocks "github.com/bnema/vev/internal/ports/mocks"
	"github.com/stretchr/testify/mock"
)

type cursorMaintenanceReconciler struct {
	seen []ports.ReconcileCursor
}

func (r *cursorMaintenanceReconciler) Step(_ context.Context, cursor ports.ReconcileCursor) (ports.ReconcileCursor, []ports.ReconcileDecision, error) {
	r.seen = append(r.seen, cursor)
	if cursor.DirectoryCookie == 0 {
		return ports.ReconcileCursor{DirectoryCookie: 7}, nil, nil
	}
	return ports.ReconcileCursor{}, nil, nil
}

func TestProductionMaintenanceUsesCatalogueAndResumesCursor(t *testing.T) {
	record := domain.CatalogueRecord{Name: "work", IncarnationID: domain.IncarnationID{1}, RecoveryState: domain.RecoveryFresh}
	catalogue := portsmocks.NewMockCatalogue(t)
	catalogue.EXPECT().Records().Return([]domain.CatalogueRecord{record}).Once()
	repository := portsmocks.NewMockSnapshotRepository(t)
	repository.EXPECT().MaintainSession(mock.Anything, mock.MatchedBy(func(plan ports.RetentionPlan) bool {
		return plan.IncarnationID == record.IncarnationID && !plan.PinAll && len(plan.Keep) == 0
	}), ports.MaintenanceBudget{Entries: maintenanceEntries, Bytes: maintenanceBytes}).Return(true, nil).Once()
	reconciler := &cursorMaintenanceReconciler{}
	d := newTestDaemon(t, portsmocks.NewMockPTYFactory(t), stubClock{})
	WithDurableMaintenance(catalogue, repository, reconciler, nil)(d)

	d.runDurableMaintenanceTick(context.Background())
	d.runDurableMaintenanceTick(context.Background())
	if len(reconciler.seen) != 2 || reconciler.seen[0].DirectoryCookie != 0 || reconciler.seen[1].DirectoryCookie != 7 {
		t.Fatalf("reconciliation cursors = %v, want [0 7]", reconciler.seen)
	}
}

func TestRetentionPlanPinsEveryUnresolvedRuntimeState(t *testing.T) {
	committed := domain.CheckpointRef{Generation: 3, ManifestDigest: [32]byte{3}}
	fallback1 := domain.CheckpointRef{Generation: 2, ManifestDigest: [32]byte{2}}
	fallback2 := domain.CheckpointRef{Generation: 1, ManifestDigest: [32]byte{1}}
	healthy := domain.CatalogueRecord{
		Name: "work", IncarnationID: domain.IncarnationID{1}, RecoveryState: domain.RecoveryHealthy,
		Committed: &committed, Fallbacks: [2]*domain.CheckpointRef{&fallback1, &fallback2},
	}

	for _, tc := range []struct {
		name       string
		record     domain.CatalogueRecord
		unresolved bool
		restoring  bool
		wantPin    bool
	}{
		{name: "healthy", record: healthy},
		{name: "pending journal tombstone or repair", record: healthy, unresolved: true, wantPin: true},
		{name: "restoring", record: healthy, restoring: true, wantPin: true},
		{name: "degraded", record: func() domain.CatalogueRecord { r := healthy; r.RecoveryState = domain.RecoveryDegraded; return r }(), wantPin: true},
		{name: "deleting", record: func() domain.CatalogueRecord { r := healthy; r.RecoveryState = domain.RecoveryDeleting; return r }(), wantPin: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			plan := retentionPlan(tc.record, tc.unresolved, tc.restoring)
			if plan.PinAll != tc.wantPin {
				t.Fatalf("PinAll = %v, want %v", plan.PinAll, tc.wantPin)
			}
			if !tc.wantPin && len(plan.Keep) != 3 {
				t.Fatalf("healthy keep set = %v, want committed plus two fallbacks", plan.Keep)
			}
		})
	}
}
