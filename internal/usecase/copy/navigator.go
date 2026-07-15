package copy

// Navigator owns cursor movement policy over a Document. Its positions are
// always normalized glyph heads after a successful movement.
type Navigator struct {
	Pos          Pos
	PreferredCol int
}

// NewNavigator creates a navigator at at. Set can normalize a position when a
// document is available.
func NewNavigator(at Pos) Navigator {
	return Navigator{Pos: at, PreferredCol: at.Col}
}

// Left moves to the previous glyph on the current physical row.
func (n *Navigator) Left(doc *Document) bool {
	if doc == nil {
		return false
	}
	pos, ok := doc.PrevGlyph(n.Pos)
	if !ok {
		return false
	}
	return n.set(pos, true)
}

// Right moves to the next glyph on the current physical row.
func (n *Navigator) Right(doc *Document) bool {
	if doc == nil {
		return false
	}
	pos, ok := doc.NextGlyph(n.Pos)
	if !ok {
		return false
	}
	return n.set(pos, true)
}

// Up moves to the preceding physical row while retaining PreferredCol.
func (n *Navigator) Up(doc *Document) bool {
	if doc == nil {
		return false
	}
	return n.moveVertical(doc, n.Pos.Row-1)
}

// Down moves to the following physical row while retaining PreferredCol.
func (n *Navigator) Down(doc *Document) bool {
	if doc == nil {
		return false
	}
	return n.moveVertical(doc, n.Pos.Row+1)
}

// WordNext moves to the start of the next word, possibly on another row.
func (n *Navigator) WordNext(doc *Document) bool {
	if doc == nil {
		return false
	}
	pos, ok := doc.NextWordStart(n.Pos)
	if !ok {
		return false
	}
	return n.set(pos, true)
}

// WordBackward moves to the start of the current or preceding word.
func (n *Navigator) WordBackward(doc *Document) bool {
	if doc == nil {
		return false
	}
	pos, ok := doc.PreviousWordStart(n.Pos)
	if !ok {
		return false
	}
	return n.set(pos, true)
}

// WordEnd moves to the end of the current or next word.
func (n *Navigator) WordEnd(doc *Document) bool {
	if doc == nil {
		return false
	}
	pos, ok := doc.NextWordEnd(n.Pos)
	if !ok {
		return false
	}
	return n.set(pos, true)
}

// Top moves to the first physical row at the current column and resets the
// preferred column to the normalized destination.
func (n *Navigator) Top(doc *Document) bool {
	if doc == nil || doc.Len() == 0 {
		return false
	}
	return n.Set(doc, Pos{Row: 0, Col: n.Pos.Col})
}

// Bottom moves to the last physical row at the current column and resets the
// preferred column to the normalized destination.
func (n *Navigator) Bottom(doc *Document) bool {
	if doc == nil || doc.Len() == 0 {
		return false
	}
	return n.Set(doc, Pos{Row: doc.Len() - 1, Col: n.Pos.Col})
}

// Page moves rows physical rows while retaining PreferredCol. Its destination
// clamps to the first or last physical row.
func (n *Navigator) Page(doc *Document, rows int) bool {
	if doc == nil || doc.Len() == 0 {
		return false
	}
	return n.moveVertical(doc, min(max(n.Pos.Row+rows, 0), doc.Len()-1))
}

// Set moves to pos after normalizing it through doc and updates PreferredCol.
func (n *Navigator) Set(doc *Document, pos Pos) bool {
	if doc == nil {
		return false
	}
	pos, ok := doc.Normalize(pos)
	if !ok {
		return false
	}
	return n.set(pos, true)
}

func (n *Navigator) moveVertical(doc *Document, row int) bool {
	if row < 0 || row >= doc.Len() {
		return false
	}
	pos, ok := navigatorPosOnRow(doc, row, n.PreferredCol)
	if !ok {
		return false
	}
	return n.set(pos, false)
}

func navigatorPosOnRow(doc *Document, row, col int) (Pos, bool) {
	cells := doc.Row(row)
	if len(cells) == 0 {
		return doc.Normalize(Pos{Row: row, Col: 0})
	}
	col = min(max(col, 0), len(cells)-1)
	return doc.Normalize(Pos{Row: row, Col: col})
}

func (n *Navigator) set(pos Pos, setPreferred bool) bool {
	moved := n.Pos != pos
	n.Pos = pos
	if setPreferred {
		n.PreferredCol = pos.Col
	}
	return moved
}
