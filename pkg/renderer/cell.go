package renderer

// Cell is a single terminal grid cell.
//
// A double-width rune (CJK, emoji) occupies TWO adjacent cells: the left cell
// holds the rune ({Rune: r, Style: s}) and the right cell is a continuation
// marker ({Continuation: true, Rune: 0}). Continuation cells are never emitted
// by the renderer — the terminal advances two columns for the wide rune itself.
type Cell struct {
	Rune         rune
	Style        Style
	Continuation bool
}

func BlankCell() Cell { return Cell{Rune: ' ', Style: DefaultStyle()} }

func (c Cell) Equal(other Cell) bool {
	return c.Rune == other.Rune && c.Continuation == other.Continuation && c.Style.Equal(other.Style)
}
