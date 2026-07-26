package daemon

import (
	"context"
	"testing"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	portsmocks "github.com/bnema/vev/internal/ports/mocks"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestProductionMaintenanceUsesCatalogue(t *testing.T) {
	record := domain.CatalogueRecord{Name: "work", IncarnationID: domain.IncarnationID{1}, RecoveryState: domain.RecoveryFresh}
	catalogue := portsmocks.NewMockCatalogue(t)
	catalogue.EXPECT().Records().Return([]domain.CatalogueRecord{record}, nil).Once()
	repository := portsmocks.NewMockSnapshotRepository(t)
	repository.EXPECT().MaintainSession(mock.Anything, mock.MatchedBy(func(plan ports.RetentionPlan) bool {
		return plan.IncarnationID == record.IncarnationID && !plan.PinAll && len(plan.Keep) == 0
	}), ports.MaintenanceBudget{Entries: maintenanceEntries, Bytes: maintenanceBytes}).Return(true, nil).Once()
	d := newTestDaemon(t, portsmocks.NewMockPTYFactory(t), stubClock{})
	WithDurableMaintenance(catalogue, repository, nil)(d)

	require.False(t, d.runDurableMaintenanceTick(context.Background()))
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
		{name: "pending deletion", record: healthy, unresolved: true, wantPin: true},
		{name: "restoring", record: healthy, restoring: true, wantPin: true},
		{name: "degraded", record: func() domain.CatalogueRecord { r := healthy; r.RecoveryState = domain.RecoveryDegraded; return r }(), wantPin: true},
		{name: "deleting", record: func() domain.CatalogueRecord { r := healthy; r.RecoveryState = domain.RecoveryDeleting; return r }(), wantPin: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			plan := retentionPlan(tc.record, tc.unresolved, tc.restoring)
			require.Equal(t, tc.wantPin, plan.PinAll)
			if !tc.wantPin {
				require.Equal(t, []ports.CheckpointRef{committed}, plan.Keep)
			}
		})
	}
}
