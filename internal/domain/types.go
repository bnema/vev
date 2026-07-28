package domain

import "strings"

// Anchor specifies a position in a 3x3 grid.
type Anchor uint8

const (
	AnchorCenter Anchor = iota
	AnchorTopLeft
	AnchorTop
	AnchorTopRight
	AnchorLeft
	AnchorRight
	AnchorBottomLeft
	AnchorBottom
	AnchorBottomRight
)

// Valid reports whether a is a defined anchor.
func (a Anchor) Valid() bool {
	return a <= AnchorBottomRight
}

// String returns the canonical text representation of a.
func (a Anchor) String() string {
	switch a {
	case AnchorCenter:
		return "center"
	case AnchorTopLeft:
		return "top-left"
	case AnchorTop:
		return "top"
	case AnchorTopRight:
		return "top-right"
	case AnchorLeft:
		return "left"
	case AnchorRight:
		return "right"
	case AnchorBottomLeft:
		return "bottom-left"
	case AnchorBottom:
		return "bottom"
	case AnchorBottomRight:
		return "bottom-right"
	default:
		return "unknown"
	}
}

// ParseAnchor parses a case-insensitive anchor name.
func ParseAnchor(s string) (Anchor, bool) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "center":
		return AnchorCenter, true
	case "top-left":
		return AnchorTopLeft, true
	case "top":
		return AnchorTop, true
	case "top-right":
		return AnchorTopRight, true
	case "left":
		return AnchorLeft, true
	case "right":
		return AnchorRight, true
	case "bottom-left":
		return AnchorBottomLeft, true
	case "bottom":
		return AnchorBottom, true
	case "bottom-right":
		return AnchorBottomRight, true
	default:
		return AnchorCenter, false
	}
}

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

// TabID identifies a tab by its session-local, user-facing ID.
type TabID string

// TabStableID identifies a tab independently of its mutable session position.
type TabStableID string

// PaneStableID identifies a pane independently of its layout-local PaneID.
type PaneStableID string
