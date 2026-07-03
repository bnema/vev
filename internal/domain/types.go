package domain

// Size is a terminal dimension in columns and rows.
type Size struct {
	Cols, Rows int
}

// Valid reports whether sz has strictly positive columns and rows.
func (sz Size) Valid() bool {
	return sz.Cols > 0 && sz.Rows > 0
}

// Rect is an axis-aligned rectangle in cell coordinates.
type Rect struct {
	X, Y, Width, Height int
}

// SessionID uniquely identifies a session.
type SessionID string

// TabID uniquely identifies a tab within a session.
type TabID string
