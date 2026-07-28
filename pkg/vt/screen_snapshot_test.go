package vt

import (
	"testing"

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
