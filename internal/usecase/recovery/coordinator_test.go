package recovery

import (
	"context"
	"errors"
	"testing"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	portsmocks "github.com/bnema/vev/internal/ports/mocks"
	"github.com/stretchr/testify/require"
)

type journalStub struct {
	intents []domain.DiscardIntent
	err     error
}

func (j journalStub) SaveDiscard(context.Context, domain.DiscardIntent) error { return nil }
func (j journalStub) ListDiscards(context.Context) ([]domain.DiscardIntent, error) {
	return j.intents, j.err
}
func (j journalStub) DeleteDiscard(context.Context, domain.IncarnationID) error { return nil }

func TestCoordinatorRecoverRollsPendingDiscardForward(t *testing.T) {
	ctx := context.Background()
	old := domain.CatalogueRecord{
		Name: "broken", IncarnationID: domain.IncarnationID{1}, RecoveryState: domain.RecoveryDegraded,
		Committed: &domain.CheckpointRef{Generation: 1, ManifestDigest: [32]byte{1}}, DegradedReason: "corrupt",
	}
	intent := domain.DiscardIntent{
		OldRecord: old, OldIncarnation: old.IncarnationID, NewIncarnation: domain.IncarnationID{2},
		SessionName: old.Name, Reason: "discard",
	}
	repo := portsmocks.NewMockSnapshotRepository(t)
	repo.EXPECT().SaveQuarantineDescriptor(ctx, quarantineDescriptor(intent)).Return(nil)
	repo.EXPECT().QuarantineIncarnation(ctx, old.IncarnationID).Return(nil)
	repo.EXPECT().ListDeletionTombstones(ctx, ports.DeletionTombstoneCursor{}, recoveryListingBudget).
		Return(ports.DeletionTombstonePage{Done: true}, nil)
	catalogue := newTransactionCatalogue(old)
	c := NewCoordinator(catalogue, repo, journalStub{intents: []domain.DiscardIntent{intent}}, nil)
	require.NoError(t, c.Recover(ctx))
	require.Equal(t, intent.NewIncarnation, catalogue.records[old.Name].IncarnationID)
}

func TestCoordinatorRecoverPagesTombstonesAndPropagatesErrors(t *testing.T) {
	cause := errors.New("malformed tombstone")
	repo := portsmocks.NewMockSnapshotRepository(t)
	repo.EXPECT().ListDeletionTombstones(context.Background(), ports.DeletionTombstoneCursor{}, recoveryListingBudget).
		Return(ports.DeletionTombstonePage{}, cause)
	c := NewCoordinator(newTransactionCatalogue(), repo, journalStub{}, nil)
	require.ErrorIs(t, c.Recover(context.Background()), cause)
}
