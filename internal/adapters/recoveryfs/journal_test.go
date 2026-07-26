package recoveryfs

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/bnema/vev/internal/domain"
	"github.com/stretchr/testify/require"
)

func TestDiscardCrashMatrix(t *testing.T) {
	ctx := context.Background()
	state := t.TempDir()
	j := New(state)
	old := domain.IncarnationID{1}
	intent := domain.DiscardIntent{
		OldRecord: domain.CatalogueRecord{
			Name: "work", IncarnationID: old, RecoveryState: domain.RecoveryDegraded,
			Committed: &domain.CheckpointRef{Generation: 1, ManifestDigest: [32]byte{1}}, DegradedReason: "invalid",
		},
		OldIncarnation: old, NewIncarnation: domain.IncarnationID{2}, SessionName: "work", Reason: "discard",
	}
	require.NoError(t, j.SaveDiscard(ctx, intent))

	t.Run("existing intent cannot be overwritten", func(t *testing.T) {
		changed := intent
		changed.Reason = "different"
		require.Error(t, j.SaveDiscard(ctx, changed))
		got, err := j.ListDiscards(ctx)
		require.NoError(t, err)
		require.Equal(t, []domain.DiscardIntent{intent}, got)
	})

	t.Run("malformed intent remains visible", func(t *testing.T) {
		path := j.path(old)
		encoded, err := os.ReadFile(path)
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, os.WriteFile(path, encoded, 0o600)) })
		require.NoError(t, os.WriteFile(path, append(encoded, 0), 0o600))
		_, err = j.ListDiscards(ctx)
		require.Error(t, err)
		_, statErr := os.Stat(path)
		require.NoError(t, statErr)
		require.Error(t, j.SaveDiscard(ctx, intent))
	})

	t.Run("delete sync failure rolls forward", func(t *testing.T) {
		injected := errors.New("injected delete directory sync failure")
		j.hooks.syncDirectory = func(string) error { return injected }
		require.ErrorIs(t, j.DeleteDiscard(ctx, old), injected)
		j.hooks = journalHooks{}
		require.NoError(t, j.DeleteDiscard(ctx, old))
		got, err := j.ListDiscards(ctx)
		require.NoError(t, err)
		require.Empty(t, got)
	})
}

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

func TestRecoveryJournalIgnoresAtomicLeftoversWhenCountingDiscards(t *testing.T) {
	j := New(t.TempDir())
	require.NoError(t, os.MkdirAll(j.dir, 0o700))
	for i := range maxDiscardIntents + 1 {
		require.NoError(t, os.WriteFile(filepath.Join(j.dir, fmt.Sprintf(".intent-leftover-%04d", i)), nil, 0o600))
	}

	got, err := j.ListDiscards(context.Background())
	require.NoError(t, err)
	require.Empty(t, got)
}

func TestRecoveryJournalRejectsMalformedIntent(t *testing.T) {
	j := New(t.TempDir())
	require.Error(t, j.SaveDiscard(context.Background(), domain.DiscardIntent{}))

	_, err := decodeDiscard([]byte(`{"version":1,"intent":{}} !`))
	require.ErrorContains(t, err, "invalid character")
}

func TestRecoveryJournalBoundsEnumerationAndIntentReads(t *testing.T) {
	state := t.TempDir()
	j := New(state)
	require.NoError(t, os.MkdirAll(j.dir, 0o700))
	for i := range maxDiscardIntents + 1 {
		require.NoError(t, os.WriteFile(filepath.Join(j.dir, fmt.Sprintf("discard-extra-%04d", i)), nil, 0o600))
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
