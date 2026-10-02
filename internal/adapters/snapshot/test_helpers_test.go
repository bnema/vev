package snapshot

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/bnema/vev/internal/domain"

	vt "github.com/bnema/vev-vt"
	renderer "github.com/bnema/vev-vt/ansi"
)

// collectAll runs one GC pass the way the recovery coordinator does: every
// listed incarnation absent from keep is an orphan.
func collectAll(ctx context.Context, r *Repository, keep map[domain.IncarnationID]domain.CheckpointRef) error {
	ids, err := r.SnapshotIncarnations(ctx)
	if err != nil {
		return err
	}
	var collected error
	for _, id := range ids {
		var ref *domain.CheckpointRef
		if committed, ok := keep[id]; ok {
			ref = &committed
		}
		collected = errors.Join(collected, r.CollectIncarnationGarbage(ctx, id, ref))
	}
	return collected
}

func privateDir(t *testing.T) string {
	t.Helper()
	return filepath.Join(t.TempDir(), "vev")
}

func canonicalHistoryBlob(t testing.TB, text string) []byte {
	t.Helper()
	history := vt.NewHistory(vt.HistoryConfig{MaxRows: 1, ChunkRows: 1})
	if text != "" {
		row := make([]renderer.Cell, 0, len(text))
		for _, r := range text {
			row = append(row, renderer.Cell{Rune: r})
		}
		if err := history.Append(row, vt.LineBound{End: len(row)}); err != nil {
			t.Fatal(err)
		}
	}
	blob, err := vt.MarshalHistory(history.SealAndView())
	if err != nil {
		t.Fatal(err)
	}
	return blob
}
