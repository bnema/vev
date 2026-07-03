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

// Tab is a single tab within a session.
type Tab struct {
	ID     TabID
	Index  int // 0-based; drives Alt+1..9 and status-bar order
	Name   string
	Active bool
}

// Session is a persistent multiplexer session containing one or more tabs.
type Session struct {
	ID        SessionID
	Name      string // display name; numbered "0","1"... when ephemeral
	Ephemeral bool
	Tabs      []Tab
	ActiveTab TabID
}
