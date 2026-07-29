package vt

import (
	"testing"

	"github.com/bnema/vev/pkg/renderer"
	"github.com/stretchr/testify/require"
)

func TestScreenSnapshotGeometryAndMetadata(t *testing.T) {
	tests := []struct {
		name        string
		screen      func() *Screen
		wantColumns int
		wantRows    int
		wantTitle   string
		wantCursor  CursorSnapshot
		wantModes   ModeSnapshot
		checkRows   func(t *testing.T, snapshot ScreenSnapshot)
	}{
		{
			name:       "nil receiver is the zero snapshot",
			screen:     func() *Screen { return nil },
			wantCursor: CursorSnapshot{},
			wantModes:  ModeSnapshot{},
			checkRows: func(t *testing.T, snapshot ScreenSnapshot) {
				require.Nil(t, snapshot.Row(0))
				require.Nil(t, snapshot.BorrowedRow(0))
				require.Equal(t, LineBound{}, snapshot.Bound(0))
			},
		},
		{
			name: "normal viewport captures cursor modes and title",
			screen: func() *Screen {
				screen := NewScreen(6, 3)
				screen.Write([]byte("\x1b]2;editor\x07\x1b[2;3H\x1b[?25l\x1b[2 q\x1b[?2004h\x1b[?2026h\x1b[?2031h\x1b[?1002h\x1b[?1006h"))
				return screen
			},
			wantColumns: 6,
			wantRows:    3,
			wantTitle:   "editor",
			wantCursor:  CursorSnapshot{Row: 1, Col: 2, Visible: false, Style: 2, StyleSet: true},
			wantModes: ModeSnapshot{
				BracketedPaste:     true,
				SynchronizedUpdate: true,
				ColorSchemeMode:    true,
				MouseTracking:      1002,
				MouseSGR:           true,
			},
			checkRows: func(t *testing.T, snapshot ScreenSnapshot) {
				require.Len(t, snapshot.BorrowedRow(0), 6)
				require.Equal(t, LineBound{}, snapshot.Bound(0))
			},
		},
		{
			name: "zero by zero preserves screen metadata",
			screen: func() *Screen {
				screen := NewScreen(2, 2)
				screen.Write([]byte("\x1b]2;collapsed\x07\x1b[?2004h"))
				screen.Resize(0, 0)
				return screen
			},
			wantTitle:  "collapsed",
			wantCursor: CursorSnapshot{Visible: true},
			wantModes:  ModeSnapshot{BracketedPaste: true},
			checkRows: func(t *testing.T, snapshot ScreenSnapshot) {
				require.Nil(t, snapshot.Row(0))
				require.Equal(t, LineBound{}, snapshot.Bound(0))
			},
		},
		{
			name: "zero columns retains empty physical rows and bounds",
			screen: func() *Screen {
				screen := NewScreen(2, 2)
				screen.Write([]byte("\x1b]2;zero-columns\x07"))
				screen.Resize(0, 2)
				return screen
			},
			wantRows:   2,
			wantTitle:  "zero-columns",
			wantCursor: CursorSnapshot{Visible: true},
			checkRows: func(t *testing.T, snapshot ScreenSnapshot) {
				require.NotNil(t, snapshot.BorrowedRow(0))
				require.Empty(t, snapshot.BorrowedRow(0))
				require.Nil(t, snapshot.Row(0))
				require.Empty(t, snapshot.Row(1))
				require.Equal(t, LineBound{}, snapshot.Bound(1))
				require.Nil(t, snapshot.Row(2))
			},
		},
		{
			name: "zero rows retains columns without rows",
			screen: func() *Screen {
				screen := NewScreen(2, 2)
				screen.Write([]byte("\x1b]2;zero-rows\x07"))
				screen.Resize(2, 0)
				return screen
			},
			wantColumns: 2,
			wantTitle:   "zero-rows",
			wantCursor:  CursorSnapshot{Visible: true},
			checkRows: func(t *testing.T, snapshot ScreenSnapshot) {
				require.Nil(t, snapshot.BorrowedRow(0))
				require.Equal(t, LineBound{}, snapshot.Bound(0))
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			snapshot := test.screen().Snapshot()
			require.Equal(t, test.wantColumns, snapshot.Columns())
			require.Equal(t, test.wantRows, snapshot.Rows())
			require.Equal(t, test.wantTitle, snapshot.Title())
			require.Equal(t, test.wantCursor, snapshot.Cursor())
			require.Equal(t, test.wantModes, snapshot.Modes())
			test.checkRows(t, snapshot)
		})
	}
}

func TestScreenSnapshotColorSchemeModeExcludesHostPreference(t *testing.T) {
	tests := []struct {
		name string
		run  func(*Screen)
		want bool
	}{
		{
			name: "host preference alone is not DEC 2031 mode",
			run:  func(screen *Screen) { screen.SetColorScheme(true) },
			want: false,
		},
		{
			name: "DEC 2031 mode is captured independently",
			run: func(screen *Screen) {
				screen.SetColorScheme(false)
				screen.Write([]byte("\x1b[?2031h"))
			},
			want: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			screen := NewScreen(4, 2)
			test.run(screen)
			require.Equal(t, test.want, screen.Snapshot().Modes().ColorSchemeMode)
		})
	}
}

func TestScreenSnapshotTitle(t *testing.T) {
	tests := []struct {
		name string
		feed string
		want string
	}{
		{name: "empty title", want: ""},
		{name: "latest title replaces the previous value", feed: "\x1b]2;first\x07\x1b]0;second\x07", want: "second"},
		{name: "UTF-8 title is preserved", feed: "\x1b]2;éditeur 界\x07", want: "éditeur 界"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			screen := NewScreen(8, 2)
			screen.Write([]byte(test.feed))
			require.Equal(t, test.want, screen.Snapshot().Title())
		})
	}
}

func TestScreenSnapshotFidelity(t *testing.T) {
	tests := []struct {
		name      string
		make      func() (*Screen, []renderer.Cell, []LineBound)
		wantModes ModeSnapshot
	}{
		{
			name: "styles wide continuation and bounds are exact",
			make: func() (*Screen, []renderer.Cell, []LineBound) {
				screen := NewScreen(4, 2)
				style := renderer.Style{
					Bold: true, Italic: true, Inverse: true,
					Attrs:            renderer.AttrDim | renderer.AttrUnderline | renderer.AttrBlink | renderer.AttrStrikethrough,
					HasForegroundRGB: true, ForegroundRGB: renderer.RGB{R: 1, G: 2, B: 3},
					Background:           17,
					UnderlineStyle:       renderer.UnderlineCurly,
					HasUnderlineColorRGB: true, UnderlineColorRGB: renderer.RGB{R: 4, G: 5, B: 6},
				}
				cells := []renderer.Cell{
					{Rune: '界', Style: style},
					{Continuation: true, Style: style},
					{Rune: 'x', Style: renderer.DefaultStyle()},
					renderer.BlankCell(),
				}
				copy(screen.Frame.Row(0), cells)
				screen.buffer.boundaries[0] = LineBound{End: 3, Soft: true}
				return screen, cells, []LineBound{{End: 3, Soft: true}, {}}
			},
		},
		{
			name: "active alternate screen is captured",
			make: func() (*Screen, []renderer.Cell, []LineBound) {
				screen := NewScreen(3, 1)
				screen.Write([]byte("primary\x1b[?1049halt"))
				cells := append([]renderer.Cell(nil), screen.Frame.Row(0)...)
				return screen, cells, screen.LineBounds()
			},
			wantModes: ModeSnapshot{AlternateScreen: true},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			screen, wantCells, wantBounds := test.make()
			snapshot := screen.Snapshot()
			require.Equal(t, screen.Frame.Width, snapshot.Columns())
			require.Equal(t, screen.Frame.Height, snapshot.Rows())
			require.Equal(t, wantCells, snapshot.Row(0))
			require.Equal(t, test.wantModes, snapshot.Modes())
			for row, want := range wantBounds {
				require.Equal(t, want, snapshot.Bound(row))
			}
		})
	}
}

func TestScreenSnapshotPreservesLogicalRowsAfterScroll(t *testing.T) {
	screen := NewScreen(3, 2)
	screen.Write([]byte("ABC"))
	screen.Write([]byte("DEF"))
	screen.Write([]byte("G"))

	snapshot := screen.Snapshot()
	for y, want := range []string{"DEF", "G  "} {
		require.Equal(t, want, rowText(snapshot.Row(y)))
		require.Equal(t, screen.Frame.Row(y), snapshot.Row(y))
		require.Equal(t, screen.LineBounds()[y], snapshot.Bound(y))
	}
}

func TestScreenSnapshotOwnership(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*Screen)
	}{
		{name: "later write", mutate: func(screen *Screen) { screen.Write([]byte("later")) }},
		{name: "later scroll", mutate: func(screen *Screen) { screen.Write([]byte("1111222233334444")) }},
		{name: "later resize", mutate: func(screen *Screen) { screen.Resize(2, 4) }},
		{name: "later alternate entry", mutate: func(screen *Screen) { screen.Write([]byte("\x1b[?1049halt")) }},
		{name: "later alternate entry and exit", mutate: func(screen *Screen) { screen.Write([]byte("\x1b[?1049halt\x1b[?1049l")) }},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			screen := NewScreen(4, 2)
			screen.Write([]byte("abcdEF"))
			screen.buffer.boundaries[0] = LineBound{End: 4, Soft: true}
			snapshot := screen.Snapshot()
			wantRows := [][]renderer.Cell{snapshot.Row(0), snapshot.Row(1)}
			wantBounds := []LineBound{snapshot.Bound(0), snapshot.Bound(1)}

			test.mutate(screen)

			require.Equal(t, wantRows[0], snapshot.Row(0))
			require.Equal(t, wantRows[1], snapshot.Row(1))
			require.Equal(t, wantBounds[0], snapshot.Bound(0))
			require.Equal(t, wantBounds[1], snapshot.Bound(1))

			liveBeforeCallerMutation := append([]renderer.Cell(nil), screen.Frame.Row(0)...)
			secondSnapshot := screen.Snapshot()
			secondSnapshotBefore := secondSnapshot.Row(0)

			callerCopy := snapshot.Row(0)
			callerCopy[0] = renderer.Cell{Rune: 'z'}

			require.Equal(t, wantRows[0], snapshot.BorrowedRow(0))
			require.Equal(t, liveBeforeCallerMutation, screen.Frame.Row(0), "mutating Row copy must not change live Screen")
			require.Equal(t, secondSnapshotBefore, secondSnapshot.Row(0), "mutating Row copy must not change another snapshot")
		})
	}
}

func TestScreenSnapshotBoundsAreIndependent(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*Screen)
	}{
		{
			name: "mutating a single boundary entry after the snapshot",
			mutate: func(screen *Screen) {
				screen.buffer.boundaries[0] = LineBound{End: 1, Soft: false}
			},
		},
		{
			name: "replacing the whole boundaries slice after the snapshot",
			mutate: func(screen *Screen) {
				screen.buffer.boundaries = []LineBound{{End: 1, Soft: false}, {End: 1, Soft: false}}
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			screen := NewScreen(4, 2)
			screen.Write([]byte("abcdEF"))
			screen.buffer.boundaries[0] = LineBound{End: 4, Soft: true}
			snapshot := screen.Snapshot()
			want := snapshot.Bound(0)

			test.mutate(screen)

			require.Equal(t, want, snapshot.Bound(0), "snapshot bounds must not alias Screen.buffer.boundaries")
		})
	}
}

func TestScreenSnapshotDoesNotMutateHistoryOrDamage(t *testing.T) {
	tests := []struct {
		name string
		make func() *Screen
	}{
		{
			name: "screen without history",
			make: func() *Screen {
				screen := NewScreen(4, 2)
				screen.ClearDamage()
				screen.Write([]byte("abcdef"))
				return screen
			},
		},
		{
			name: "screen with sealed chunks and mutable tail",
			make: func() *Screen {
				screen := NewScreenWithHistory(4, 2, HistoryConfig{MaxRows: 16, MaxCells: 64, ChunkRows: 2})
				screen.Write([]byte("111122223333444455"))
				return screen
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			screen := test.make()
			beforeDamage := screen.CaptureDamage()
			var beforeHistory HistorySnapshotView
			var beforeRows [][]renderer.Cell
			var beforeBounds []LineBound
			beforeCap, beforeCellCap := 0, 0
			if screen.History() != nil {
				beforeHistory = screen.History().SnapshotView()
				beforeView := screen.History().View()
				beforeRows = make([][]renderer.Cell, beforeView.Len())
				beforeBounds = make([]LineBound, beforeView.Len())
				for i := range beforeRows {
					beforeRows[i] = beforeView.Row(i)
					beforeBounds[i] = beforeView.Bound(i)
				}
				beforeCap = screen.History().Cap()
				beforeCellCap = screen.History().CellCap()
			}

			_ = screen.Snapshot()

			afterDamage := screen.CaptureDamage()
			require.Equal(t, beforeDamage, afterDamage)
			if screen.History() != nil {
				afterHistory := screen.History().SnapshotView()
				afterView := screen.History().View()
				afterRows := make([][]renderer.Cell, afterView.Len())
				afterBounds := make([]LineBound, afterView.Len())
				for i := range afterRows {
					afterRows[i] = afterView.Row(i)
					afterBounds[i] = afterView.Bound(i)
				}
				require.Equal(t, beforeHistory.ChunkCount(), afterHistory.ChunkCount())
				require.Equal(t, beforeHistory.Len(), afterHistory.Len())
				require.Equal(t, beforeHistory.Cells(), afterHistory.Cells())
				require.Equal(t, beforeHistory.Tail().Len(), afterHistory.Tail().Len())
				require.Equal(t, beforeRows, afterRows)
				require.Equal(t, beforeBounds, afterBounds)
				require.Equal(t, beforeCap, screen.History().Cap())
				require.Equal(t, beforeCellCap, screen.History().CellCap())
			}
		})
	}
}

func TestScreenSnapshotPreservesStaleDamageAcknowledge(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*Screen)
	}{
		{name: "write after capture", mutate: func(screen *Screen) { screen.Write([]byte("b")) }},
		{name: "resize after capture", mutate: func(screen *Screen) { screen.Resize(6, 2) }},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			screen := NewScreen(4, 2)
			screen.ClearDamage()
			screen.Write([]byte("a"))
			captured := screen.CaptureDamage()

			_ = screen.Snapshot()
			test.mutate(screen)

			require.False(t, screen.AcknowledgeDamage(captured.Generation))
			require.Equal(t, []renderer.Damage{renderer.FullRedraw()}, screen.Damage())

			screen.record(renderer.Damage{Kind: renderer.DamageText, X: 0, Y: 0, Width: 1, Height: 1, Count: 1})
			require.Equal(t, []renderer.Damage{renderer.FullRedraw()}, screen.Damage(), "full redraw stays sticky until exact acknowledgement")
		})
	}
}

func TestScreenSnapshotPreservesExactDamageAcknowledge(t *testing.T) {
	tests := []struct {
		name string
	}{
		{name: "snapshot does not stale an exact damage capture"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			screen := NewScreen(4, 2)
			screen.ClearDamage()
			screen.Write([]byte("a"))
			captured := screen.CaptureDamage()

			_ = screen.Snapshot()

			require.True(t, screen.AcknowledgeDamage(captured.Generation))
			require.Empty(t, screen.Damage())
		})
	}
}
