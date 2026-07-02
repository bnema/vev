package renderer

type Cell struct {
	Rune  rune
	Style Style
}

func BlankCell() Cell { return Cell{Rune: ' ', Style: DefaultStyle()} }

func (c Cell) Equal(other Cell) bool {
	return c.Rune == other.Rune && c.Style.Equal(other.Style)
}
