package daemon

import (
	"context"
	"errors"
	"testing"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	portsmocks "github.com/bnema/vev/internal/ports/mocks"
	recoveryusecase "github.com/bnema/vev/internal/usecase/recovery"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
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

type stalledMaintenanceReconciler struct{}

func (stalledMaintenanceReconciler) Step(_ context.Context, cursor ports.ReconcileCursor) (ports.ReconcileCursor, []ports.ReconcileDecision, error) {
	if cursor.DirectoryCookie == 0 {
		return ports.ReconcileCursor{DirectoryCookie: 7}, nil, nil
	}
	return cursor, nil, nil
}

func TestMaintenanceDoesNotImmediatelyRetryStalledCursor(t *testing.T) {
	d := newTestDaemon(t, nil, stubClock{})
	d.maintenance = newMaintenanceDependencies(portsmocks.NewMockCatalogue(t), portsmocks.NewMockSnapshotRepository(t), stalledMaintenanceReconciler{}, nil)
	require.True(t, d.runDurableMaintenanceTick(context.Background()))
	require.False(t, d.runDurableMaintenanceTick(context.Background()))
}

func TestProductionMaintenanceUsesCatalogueAndResumesCursor(t *testing.T) {
	record := domain.CatalogueRecord{Name: "work", IncarnationID: domain.IncarnationID{1}, RecoveryState: domain.RecoveryFresh}
	catalogue := portsmocks.NewMockCatalogue(t)
	catalogue.EXPECT().Records().Return([]domain.CatalogueRecord{record}, nil).Once()
	repository := portsmocks.NewMockSnapshotRepository(t)
	repository.EXPECT().MaintainSession(mock.Anything, mock.MatchedBy(func(plan ports.RetentionPlan) bool {
		return plan.IncarnationID == record.IncarnationID && !plan.PinAll && len(plan.Keep) == 0
	}), ports.MaintenanceBudget{Entries: maintenanceEntries, Bytes: maintenanceBytes}).Return(true, nil).Once()
	reconciler := &cursorMaintenanceReconciler{}
	d := newTestDaemon(t, portsmocks.NewMockPTYFactory(t), stubClock{})
	WithDurableMaintenance(catalogue, repository, reconciler, nil)(d)

	require.True(t, d.runDurableMaintenanceTick(context.Background()), "partial reconciliation must continue immediately")
	require.False(t, d.runDurableMaintenanceTick(context.Background()))
	require.True(t, d.runDurableMaintenanceTick(context.Background()), "the next partial pass must also continue immediately")
	if len(reconciler.seen) != 3 || reconciler.seen[0].DirectoryCookie != 0 || reconciler.seen[1].DirectoryCookie != 7 || reconciler.seen[2].DirectoryCookie != 0 {
		t.Fatalf("reconciliation cursors = %v, want [0 7 0]", reconciler.seen)
	}
}

type reconciliationMaintenanceRepository struct {
	ports.SnapshotRepository
	findings []ports.ReconcileFinding
	err      error
	plans    []ports.RetentionPlan
}

func (r *reconciliationMaintenanceRepository) Reconcile(context.Context, []domain.CatalogueRecord, ports.ReconcileCursor, ports.MaintenanceBudget) (ports.ReconcileCursor, []ports.ReconcileFinding, error) {
	return ports.ReconcileCursor{}, r.findings, r.err
}

func (r *reconciliationMaintenanceRepository) MaintainSession(_ context.Context, plan ports.RetentionPlan, _ ports.MaintenanceBudget) (bool, error) {
	r.plans = append(r.plans, plan)
	return true, nil
}

func TestMaintenanceRetentionUsesReconciliationOutcome(t *testing.T) {
	committed := domain.CheckpointRef{Generation: 3, ManifestDigest: [32]byte{3}}
	fallback1 := domain.CheckpointRef{Generation: 2, ManifestDigest: [32]byte{2}}
	fallback2 := domain.CheckpointRef{Generation: 1, ManifestDigest: [32]byte{1}}
	record := domain.CatalogueRecord{
		Name: "work", IncarnationID: domain.IncarnationID{1}, RecoveryState: domain.RecoveryHealthy,
		Committed: &committed,
	}
	wrongParent := domain.CheckpointRef{Generation: 3, ManifestDigest: [32]byte{9}}

	for _, tc := range []struct {
		name         string
		finding      ports.ReconcileFinding
		reconcileErr error
		wantPlan     *ports.RetentionPlan
	}{
		{
			name: "current HEAD is conclusively not forward",
			finding: ports.ReconcileFinding{Kind: ports.ReconcileForwardOrphan, Status: ports.ReconcileValidated, Candidate: ports.ReconcileCandidate{
				Name: record.Name, IncarnationID: record.IncarnationID, Ref: committed, Parent: &fallback1,
			}},
			wantPlan: &ports.RetentionPlan{IncarnationID: record.IncarnationID, Keep: []ports.CheckpointRef{committed}},
		},
		{
			name: "known predecessor is conclusively not forward",
			finding: ports.ReconcileFinding{Kind: ports.ReconcileForwardOrphan, Status: ports.ReconcileValidated, Candidate: ports.ReconcileCandidate{
				Name: record.Name, IncarnationID: record.IncarnationID, Ref: fallback1, Parent: &fallback2,
			}},
			wantPlan: &ports.RetentionPlan{IncarnationID: record.IncarnationID, Keep: []ports.CheckpointRef{committed}},
		},
		{
			name: "non-forward candidate kind remains pinned",
			finding: ports.ReconcileFinding{Kind: ports.ReconcileInvalidCandidate, Status: ports.ReconcileValidated, Candidate: ports.ReconcileCandidate{
				Name: record.Name, IncarnationID: record.IncarnationID, Ref: domain.CheckpointRef{Generation: 4, ManifestDigest: [32]byte{4}},
			}},
			wantPlan: &ports.RetentionPlan{IncarnationID: record.IncarnationID, PinAll: true},
		},
		{
			name: "unreadable candidate remains pinned",
			finding: ports.ReconcileFinding{Kind: ports.ReconcileInvalidCandidate, Status: ports.ReconcileQuarantined, Candidate: ports.ReconcileCandidate{
				Name: record.Name, IncarnationID: record.IncarnationID,
			}},
			wantPlan: &ports.RetentionPlan{IncarnationID: record.IncarnationID, PinAll: true},
		},
		{
			name: "quarantined forward candidate remains pinned",
			finding: ports.ReconcileFinding{Kind: ports.ReconcileForwardOrphan, Status: ports.ReconcileQuarantined, Candidate: ports.ReconcileCandidate{
				Name: record.Name, IncarnationID: record.IncarnationID, Ref: domain.CheckpointRef{Generation: 4, ManifestDigest: [32]byte{4}}, Parent: &wrongParent,
			}},
			wantPlan: &ports.RetentionPlan{IncarnationID: record.IncarnationID, PinAll: true},
		},
		{
			name: "incarnation mismatch remains pinned",
			finding: ports.ReconcileFinding{Kind: ports.ReconcileForwardOrphan, Status: ports.ReconcileValidated, Candidate: ports.ReconcileCandidate{
				Name: record.Name, IncarnationID: domain.IncarnationID{9}, Ref: domain.CheckpointRef{Generation: 4, ManifestDigest: [32]byte{4}}, Parent: &committed,
			}},
			wantPlan: &ports.RetentionPlan{IncarnationID: record.IncarnationID, PinAll: true},
		},
		{
			name: "incomplete decision remains pinned",
			finding: ports.ReconcileFinding{Status: ports.ReconcileBudgetExhausted, Candidate: ports.ReconcileCandidate{
				Name: record.Name, IncarnationID: record.IncarnationID, Ref: domain.CheckpointRef{Generation: 4, ManifestDigest: [32]byte{4}},
			}},
			wantPlan: &ports.RetentionPlan{IncarnationID: record.IncarnationID, PinAll: true},
		},
		{
			name:         "reconciliation error withholds maintenance",
			reconcileErr: errors.New("scan failed"),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			catalogue := newDurableRecoveryCatalogue([]domain.CatalogueRecord{record})
			repository := &reconciliationMaintenanceRepository{err: tc.reconcileErr}
			if tc.finding.Candidate.Name != "" {
				repository.findings = []ports.ReconcileFinding{tc.finding}
			}
			coordinator := recoveryusecase.NewCoordinator(catalogue, repository, nil, nil)
			reconciler := recoveryusecase.NewReconciler(coordinator, catalogue, repository, ports.MaintenanceBudget{Entries: 8, Bytes: 1024})
			d := newTestDaemon(t, portsmocks.NewMockPTYFactory(t), stubClock{})
			WithDurableMaintenance(catalogue, repository, reconciler, nil)(d)

			d.runDurableMaintenanceTick(context.Background())

			if tc.wantPlan == nil {
				require.Empty(t, repository.plans)
				return
			}
			require.Equal(t, []ports.RetentionPlan{*tc.wantPlan}, repository.plans)
		})
	}
}

func TestRetentionPlanPinsEveryUnresolvedRuntimeState(t *testing.T) {
	committed := domain.CheckpointRef{Generation: 3, ManifestDigest: [32]byte{3}}
	healthy := domain.CatalogueRecord{
		Name: "work", IncarnationID: domain.IncarnationID{1}, RecoveryState: domain.RecoveryHealthy,
		Committed: &committed,
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
			if !tc.wantPin && len(plan.Keep) != 1 {
				t.Fatalf("healthy keep set = %v, want committed only", plan.Keep)
			}
		})
	}
}
