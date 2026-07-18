package domain

// ThemeAccentMode selects whether vev infers a terminal accent or uses one
// explicit ANSI slot.
type ThemeAccentMode uint8

const (
	// ThemeAccentAuto infers an accent from the terminal palette.
	ThemeAccentAuto ThemeAccentMode = iota
	// ThemeAccentSlot selects one explicit ANSI palette slot.
	ThemeAccentSlot
)

// ThemeAccent is the parsed terminal accent policy. Slot is meaningful only
// when Mode is ThemeAccentSlot.
type ThemeAccent struct {
	Mode ThemeAccentMode
	Slot uint8
}
