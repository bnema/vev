package vt

// RowID identifies one physical terminal row for the lifetime of a Screen.
// Row IDs are never zero and are never reused by their owning Screen.
type RowID uint64

func (s *Screen) nextRowIDValue() RowID {
	if s.nextRowID == ^RowID(0) {
		panic("vt: row ID space exhausted")
	}
	s.nextRowID++
	return s.nextRowID
}

func (s *Screen) newBuffer(width, height int) *buffer {
	b := newBuffer(width, height)
	for i := range b.rowIDs {
		b.rowIDs[i] = s.nextRowIDValue()
	}
	return b
}

func (s *Screen) fillMissingRowIDs(b *buffer) {
	if b == nil {
		return
	}
	for i := range b.rowIDs {
		if b.rowIDs[i] == 0 {
			b.rowIDs[i] = s.nextRowIDValue()
		}
	}
}

// RowIDs returns an owned copy of the active live grid's row identities.
func (s *Screen) RowIDs() []RowID {
	if s == nil || s.buffer == nil {
		return nil
	}
	return append([]RowID(nil), s.buffer.rowIDs...)
}

// RowID returns the identity of active live row y, or zero when y is out of
// range.
func (s *Screen) RowID(y int) RowID {
	if s == nil || s.buffer == nil || y < 0 || y >= len(s.buffer.rowIDs) {
		return 0
	}
	return s.buffer.rowIDs[y]
}
