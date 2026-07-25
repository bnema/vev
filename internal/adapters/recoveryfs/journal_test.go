package recoveryfs

import (
	"context"
	"errors"
	"os"
	"path/filepath"
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

func TestRecoveryJournalBoundsEnumerationAndIntentReads(t *testing.T) {
	state := t.TempDir()
	j := New(state)
	require.NoError(t, os.MkdirAll(j.dir, 0o700))
	for i := 0; i <= maxDiscardIntents; i++ {
		require.NoError(t, os.WriteFile(filepath.Join(j.dir, "discard-extra-"+string(rune(0x1000+i))), nil, 0o600))
	}
	_, err := j.ListDiscards(context.Background())
	require.ErrorContains(t, err, "entry limit exceeded")

	require.NoError(t, os.RemoveAll(j.dir))
	require.NoError(t, os.MkdirAll(j.dir, 0o700))
	require.NoError(t, os.WriteFile(filepath.Join(j.dir, "discard-oversized"), make([]byte, maxDiscardIntentSize+1), 0o600))
	_, err = j.ListDiscards(context.Background())
	require.ErrorContains(t, err, "size limit exceeded")
}

func TestRecoveryJournalFailsClosedAtDurabilityBoundaries(t *testing.T) {
	for _, boundary := range []string{"file sync", "rename", "directory sync"} {
		t.Run(boundary, func(t *testing.T) {
			j := New(t.TempDir())
			old := domain.IncarnationID{1}
			intent := domain.DiscardIntent{
				OldRecord:      domain.CatalogueRecord{Name: "work", IncarnationID: old, RecoveryState: domain.RecoveryDegraded, Committed: &domain.CheckpointRef{Generation: 1, ManifestDigest: [32]byte{1}}, DegradedReason: "invalid"},
				OldIncarnation: old, NewIncarnation: domain.IncarnationID{2}, SessionName: "work", Reason: "discard",
			}
			injected := errors.New("injected durability failure")
			switch boundary {
			case "file sync":
				j.hooks.syncFile = func(string) error { return injected }
			case "rename":
				j.hooks.rename = func(string, string) error { return injected }
			case "directory sync":
				j.hooks.syncDirectory = func(string) error { return injected }
			}
			require.ErrorIs(t, j.SaveDiscard(context.Background(), intent), injected)
			j.hooks = journalHooks{}
			require.NoError(t, j.SaveDiscard(context.Background(), intent))
			got, err := j.ListDiscards(context.Background())
			require.NoError(t, err)
			require.Equal(t, []domain.DiscardIntent{intent}, got)
		})
	}
}
