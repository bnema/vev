package recovery

import (
	"context"
	"errors"
	"testing"

	"github.com/bnema/vev/internal/ports"
	portsmocks "github.com/bnema/vev/internal/ports/mocks"
	"github.com/stretchr/testify/require"
)

func TestDeleteChecksCancellationBeforeCatalogueAccess(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	catalogue := portsmocks.NewMockCatalogue(t)
	repository := portsmocks.NewMockSnapshotRepository(t)
	coordinator := NewCoordinator(catalogue, repository, nil)

	require.ErrorIs(t, coordinator.Delete(ctx, "work"), context.Canceled)
}

func TestCoordinatorRecoverPagesTombstonesAndPropagatesErrors(t *testing.T) {
	cause := errors.New("malformed tombstone")
	repo := portsmocks.NewMockSnapshotRepository(t)
	repo.EXPECT().ListDeletionTombstones(context.Background(), ports.DeletionTombstoneCursor{}, recoveryListingBudget).
		Return(ports.DeletionTombstonePage{}, cause)
	c := NewCoordinator(newTransactionCatalogue(), repo, nil)
	require.ErrorIs(t, c.Recover(context.Background()), cause)
}
