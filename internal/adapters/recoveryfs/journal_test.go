package recoveryfs

import (
	"context"
	"testing"

	"github.com/bnema/vev/internal/domain"
	"github.com/stretchr/testify/require"
)

func TestRecoveryJournalRoundTripAndIdempotentDelete(t *testing.T) {
	j := New(t.TempDir())
	old := domain.IncarnationID{1}
	intent := domain.DiscardIntent{
		OldRecord:      domain.CatalogueRecord{Name: "work", IncarnationID: old, RecoveryState: domain.RecoveryDegraded, Committed: &domain.CheckpointRef{Generation: 1, ManifestDigest: [32]byte{1}}, DegradedReason: "invalid"},
		OldIncarnation: old, NewIncarnation: domain.IncarnationID{2}, SessionName: "work", Reason: "discard",
	}
	require.NoError(t, j.SaveDiscard(context.Background(), intent))
	got, err := j.ListDiscards(context.Background())
	require.NoError(t, err)
	require.Equal(t, []domain.DiscardIntent{intent}, got)
	require.NoError(t, j.DeleteDiscard(context.Background(), old))
	require.NoError(t, j.DeleteDiscard(context.Background(), old))
	got, err = j.ListDiscards(context.Background())
	require.NoError(t, err)
	require.Empty(t, got)
}

func TestRecoveryJournalRejectsMalformedIntent(t *testing.T) {
	j := New(t.TempDir())
	require.Error(t, j.SaveDiscard(context.Background(), domain.DiscardIntent{}))
}
