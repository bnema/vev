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

func TestCoordinatorRecoverFailsClosedForPendingWork(t *testing.T) {
	repo := portsmocks.NewMockSnapshotRepository(t)
	repo.EXPECT().ListDeletionTombstones(context.Background(), ports.DeletionTombstoneCursor{}, recoveryListingBudget).
		Return(ports.DeletionTombstonePage{Done: true}, nil)
	c := NewCoordinator(newTransactionCatalogue(), repo, journalStub{intents: []domain.DiscardIntent{{}}}, nil)
	require.ErrorIs(t, c.Recover(context.Background()), ErrPendingRecoveryUnsupported)
}

func TestCoordinatorRecoverPagesTombstonesAndPropagatesErrors(t *testing.T) {
	cause := errors.New("malformed tombstone")
	repo := portsmocks.NewMockSnapshotRepository(t)
	repo.EXPECT().ListDeletionTombstones(context.Background(), ports.DeletionTombstoneCursor{}, recoveryListingBudget).
		Return(ports.DeletionTombstonePage{}, cause)
	c := NewCoordinator(newTransactionCatalogue(), repo, journalStub{}, nil)
	require.ErrorIs(t, c.Recover(context.Background()), cause)
}
