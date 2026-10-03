package theme

import (
	renderer "github.com/bnema/vev-vt"
	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/domain/terminalcap"
)

// reducedColor reports whether chrome for c must be built from attributes and
// terminal palette slots instead of RGB surfaces: 16-color and monochrome
// attachments.
func reducedColor(c terminalcap.ColorCapabilities) bool {
	return c.Mode == terminalcap.ANSI16 || c.Mode == terminalcap.Monochrome
}

// ResolveForColor is Resolve for one attachment's color capability. TrueColor
// and ANSI256 return exactly what Resolve returns. ANSI16 and Monochrome
// replace the chrome styles with the reduced-color policy (see reducedStyles)
// and mark the returned Theme so dimming uses the faint attribute instead of
// RGB blends. Color is per attachment, so callers resolve once per attachment.
func ResolveForColor(t Theme, policy domain.ThemeAccent, color terminalcap.ColorCapabilities) ResolvedTheme {
	if !reducedColor(color) {
		return Resolve(t, policy)
	}
	t.DimByAttribute = true
	resolved := Resolve(t, policy) // resolved.Theme carries DimByAttribute
	accent := resolved.Accent
	slot := -1
	if (accent.Known || accent.IndexedOnly) && accent.Slot < 16 {
		slot = int(accent.Slot)
	}
	resolved.Styles = reducedStyles(slot, color.Mode == terminalcap.Monochrome)
	return resolved
}

// reducedStyles builds the chrome for terminals that cannot show tinted RGB
// surfaces. Surfaces keep the terminal's default background; hierarchy comes
// from attributes, and the accent (when known) is only ever a palette slot
// 0-15 so the terminal's own palette renders it:
//
//   - active tab, session name, selected rows, hint keys, search selection:
//     bold reverse, so contrast is always the terminal's own fg/bg pair;
//   - copy status, copy selection and the focused pane title: plain reverse
//     (a selection keeps the text's own colors);
//   - inactive tabs and bars: plain; secondary text, muted borders and
//     unfocused titles: faint;
//   - focused borders: bold, in the accent palette slot when there is one;
//     warn borders: bold yellow (slot 3); errors share the focused border.
//
// Monochrome uses the same attributes with no color at all. The accent never
// fills a background: a palette slot is not guaranteed to contrast with the
// terminal background that reverse video would put on top of it.
func reducedStyles(accentSlot int, mono bool) Styles {
	plain := renderer.DefaultStyle()
	withFG := func(s renderer.Style, slot int) renderer.Style {
		if !mono && slot >= 0 {
			s.Foreground = slot
		}
		return s
	}
	with := func(base renderer.Style, bold, italic, inverse, faint bool) renderer.Style {
		base.Bold, base.Italic, base.Inverse = bold, italic, inverse
		if faint {
			base.Attrs |= renderer.AttrDim
		}
		return base
	}
	active := with(plain, true, false, true, false)
	activeTitle := with(plain, false, false, true, false)
	selectionMuted := with(plain, false, true, true, false)
	faint := with(plain, false, false, false, true)
	description := with(plain, false, true, false, true)
	focusedBorder := with(withFG(plain, accentSlot), true, false, false, false)
	warn := with(plain, true, true, false, false)
	if !mono {
		warn = with(withFG(plain, 3), true, false, false, false)
	}
	copySelection := with(plain, false, false, true, false)

	s := Styles{
		Reduced: true,

		SurfaceBar:      plain,
		SurfaceInactive: plain,
		SurfaceRecent:   plain,
		SurfaceActive:   active,
		BorderMuted:     faint,
		BorderActive:    focusedBorder,
		BorderWarn:      warn,
		NeutralBorder:   faint,

		TabInactive:      plain,
		TabInactiveTitle: faint,
		TabActive:        active,
		TabActiveTitle:   activeTitle,
		MRURecent:        plain,

		PickerBase:        plain,
		PickerSelection:   active,
		HintKey:           active,
		PickerDescription: description,
		PickerSeparator:   faint,
		PromptBase:        plain,
		CopyStatus:        with(plain, false, false, true, false),
		SearchSelection:   active,
	}
	// Aliases consumed by render paths that have not migrated yet. Selection
	// is the copy-mode selection role and StatusBar the focused pane title
	// bar; both are plain reverse.
	s.StatusBar = copySelection
	s.Accent = s.SurfaceActive
	s.Border = s.BorderMuted
	s.Selection = copySelection
	s.PaletteDesc = description
	s.TabName = s.TabInactive
	s.TabNameActive = s.TabActive
	s.TabTitle = s.TabInactiveTitle
	s.TabTitleActive = s.TabActiveTitle
	s.PickerName = plain
	s.PickerSelectionName = active
	s.PickerSelectionMuted = selectionMuted
	for count := range s.mruStyles {
		for index := 0; index <= count; index++ {
			s.mruStyles[count][index] = plain
		}
	}
	return s
}
