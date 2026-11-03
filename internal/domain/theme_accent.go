package domain

// ThemeAccentMode selects automatic palette inference, an explicit ANSI slot,
// or an explicit RGB color.
type ThemeAccentMode uint8

const (
	// ThemeAccentAuto infers an accent from the terminal palette.
	ThemeAccentAuto ThemeAccentMode = iota
	// ThemeAccentSlot selects one explicit ANSI palette slot.
	ThemeAccentSlot
	// ThemeAccentRGB selects an explicit sRGB color.
	ThemeAccentRGB
)

// ThemeAccent is the parsed terminal accent policy. Slot is meaningful only
// when Mode is ThemeAccentSlot.
type ThemeAccent struct {
	Mode ThemeAccentMode
	Slot uint8
	// RGB holds red, green, blue when Mode is ThemeAccentRGB.
	RGB [3]uint8
}
