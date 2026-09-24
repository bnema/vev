package domain

// TerminalFocus is whether the user's terminal window shows this client.
//
// A client learns it from terminal focus reporting (DEC mode 1004). Until the
// terminal reports, or when it never does, focus is unknown, and callers must
// treat an unknown client as possibly seen, exactly as before focus existed.
type TerminalFocus uint8

const (
	TerminalFocusUnknown TerminalFocus = iota
	TerminalFocusFocused
	TerminalFocusUnfocused
)

// Valid reports whether f is one of the closed focus states.
func (f TerminalFocus) Valid() bool {
	return f <= TerminalFocusUnfocused
}

// MaySee reports whether a user may be looking at this client. Only a
// reported loss of focus rules that out.
func (f TerminalFocus) MaySee() bool {
	return f != TerminalFocusUnfocused
}

func (f TerminalFocus) String() string {
	switch f {
	case TerminalFocusFocused:
		return "focused"
	case TerminalFocusUnfocused:
		return "unfocused"
	default:
		return "unknown"
	}
}
