package copy

import (
	"testing"

	vt "bnema.dev/vev-vt"
	"github.com/stretchr/testify/require"
)

func TestSnapshotCarriesFrozenGraphics(t *testing.T) {
	tests := []struct {
		name       string
		snapshot   func(*vt.Screen) Snapshot
		wantFrozen bool
	}{
		{name: "from screen", snapshot: func(s *vt.Screen) Snapshot { return NewSnapshotFromScreen(s.History(), s) }, wantFrozen: true},
		{name: "without screen", snapshot: func(s *vt.Screen) Snapshot { return NewSnapshot(s.History(), s, nil, nil) }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			screen := vt.NewScreenWithHistory(4, 4, vt.HistoryConfig{MaxRows: 8, MaxBytes: 1 << 20, ChunkRows: 4})
			screen.SetGeometry(vt.Geometry{Cols: 4, Rows: 4, PixelWidth: 40, PixelHeight: 80})
			screen.Write([]byte("\x1b_Ga=T,f=24,s=1,v=1,c=1,r=1,C=1;AAAA\x1b\\"))
			doc := NewDocument(test.snapshot(screen), "")
			if !test.wantFrozen {
				require.Nil(t, doc.Graphics())
				return
			}
			require.NotNil(t, doc.Graphics())
			require.Equal(t, uint64(1), doc.Graphics().Usage().Placements)
			screen.Write([]byte("\x1b_Ga=T,f=24,s=1,v=1,c=1,r=1,C=1;AAAA\x1b\\"))
			require.Equal(t, uint64(1), doc.Graphics().Usage().Placements, "later writes must not change the frozen scene")
		})
	}
}
