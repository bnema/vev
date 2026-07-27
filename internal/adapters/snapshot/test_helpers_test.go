package snapshot

import (
	"path/filepath"
	"testing"

	"github.com/bnema/vev/pkg/renderer"
	"github.com/bnema/vev/pkg/vt"
)

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
