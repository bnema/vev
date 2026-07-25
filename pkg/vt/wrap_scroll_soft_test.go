package vt

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// A deferred wrap out of the last row of a scroll region scrolls that region,
// and buffer.scrollUp deliberately severs the link on the last moved row. That
// sever is right for a linefeed and wrong when the wrap itself caused the
// scroll, so these cases pin both sides of the distinction.
func TestSoftLinkSurvivesTheScrollItCaused(t *testing.T) {
	const width = 8
	cases := []struct {
		name   string
		height int
		input  string
		// wantBounds is indexed like the live grid.
		wantBounds map[int]LineBound
		// wantRows pins the grid text so a bound is never read off a row that
		// holds something other than what the case describes.
		wantRows map[int]string
	}{
		{
			// The bug: the cursor sits on the last row, as it does in any full
			// shell session, so the wrap scrolls and the link must survive.
			name:   "narrow wrap out of the bottom row keeps the link",
			height: 4,
			input:  "1\r\n2\r\n3\r\n4\r\n5\r\nabcdefghij",
			wantBounds: map[int]LineBound{
				2: {End: 8, Soft: true},
				3: {End: 2},
			},
			wantRows: map[int]string{2: "abcdefgh", 3: "ij      "},
		},
		{
			// Same shape at the wide-rune wrap site: the wide rune does not fit
			// in the last column, so the writer abandons that cell and wraps.
			name:   "wide-rune wrap out of the bottom row keeps the link",
			height: 4,
			input:  "1\r\n2\r\n3\r\nabcdefg漢",
			wantBounds: map[int]LineBound{
				2: {End: 7, Soft: true},
				3: {End: 2},
			},
			wantRows: map[int]string{2: "abcdefg "},
		},
		{
			// The sever that must survive: a full-width row ending in an explicit
			// newline is a hard line that merely happens to fill the grid.
			name:   "explicit newline after a full-width bottom row severs",
			height: 4,
			input:  "1\r\n2\r\n3\r\nabcdefgh\r\n",
			wantBounds: map[int]LineBound{
				2: {End: 8, Soft: false},
			},
			wantRows: map[int]string{2: "abcdefgh"},
		},
		{
			// A DECSTBM region scroll is a local mutation, not a wrap: it must
			// still sever at the row above the region (top-1), whose
			// continuation the scroll just destroyed, and at the last moved row.
			name:   "DECSTBM region scroll severs at both edges",
			height: 4,
			input:  "abcdefghij" + "\x1b[2;3r" + "\x1b[3;1H" + "\n",
			wantBounds: map[int]LineBound{
				0: {End: 8, Soft: false},
				1: {End: 0, Soft: false},
			},
			wantRows: map[int]string{0: "abcdefgh"},
		},
		{
			// A region exactly one row tall blanks the wrapped row in place
			// instead of moving it up, so no surviving grid row may be linked.
			// setScrollRegion rejects top >= bottom, so the only reachable
			// one-row region is a one-row screen.
			name:   "wrap on a one-row screen links nothing on the grid",
			height: 1,
			input:  "abcdefghij",
			wantBounds: map[int]LineBound{
				0: {End: 2, Soft: false},
			},
			wantRows: map[int]string{0: "ij      "},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := NewScreenWithHistory(width, tc.height, HistoryConfig{MaxRows: 64, MaxCells: 4096})
			s.Write([]byte(tc.input))

			bounds := s.LineBounds()
			for y, want := range tc.wantBounds {
				require.Equal(t, want, bounds[y], "LineBounds()[%d]", y)
			}
			for y, want := range tc.wantRows {
				require.Equal(t, want, lineText(s, y), "row %d text", y)
			}
		})
	}
}

// The soft link also drives reflow, so restoring it fixes resize too: a line
// wrapped out of the bottom row used to be two unrelated physical rows, and
// widening the pane left it split instead of rejoining it.
func TestResizeRejoinsALineWrappedOutOfTheBottomRow(t *testing.T) {
	s := NewScreenWithHistory(8, 4, HistoryConfig{MaxRows: 64, MaxCells: 4096})
	s.Write([]byte("1\r\n2\r\n3\r\n4\r\n5\r\nabcdefghij"))

	s.Resize(16, 4)

	require.Equal(t, "abcdefghij      ", lineText(s, 2), "the wrapped line must rejoin on one row")
	require.Equal(t, LineBound{End: 10}, s.LineBounds()[2])
	require.Equal(t, "                ", lineText(s, 3), "its continuation row must be consumed by the rejoin")
}

// A wrap that scrolls the primary screen must hand history the same soft link
// it leaves on the grid, so a line wrapped at the bottom of a full pane still
// rejoins after it scrolls away.
func TestSoftLinkFromABottomRowWrapReachesHistory(t *testing.T) {
	const (
		width  = 8
		height = 4
	)
	s := NewScreenWithHistory(width, height, HistoryConfig{MaxRows: 64, MaxCells: 4096})
	// Wrap out of the bottom row, then push both physical rows into history.
	s.Write([]byte("1\r\n2\r\n3\r\n4\r\n5\r\nabcdefghij\r\nx\r\ny\r\nz\r\n"))

	view := s.History().SealAndView()
	require.Equal(t, 7, view.Len(), "history row count")
	require.Equal(t, "abcdefgh", rowString(view.BorrowedRow(5)))
	require.Equal(t, LineBound{End: 8, Soft: true}, view.Bound(5), "the wrapped row must reach history soft")
	require.Equal(t, "ij      ", rowString(view.BorrowedRow(6)))
	require.Equal(t, LineBound{End: 2, Soft: false}, view.Bound(6), "its continuation is a hard line end")
}
