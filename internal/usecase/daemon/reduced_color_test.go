package daemon

import (
	"regexp"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	renderer "github.com/bnema/vev-vt"
	ansirenderer "github.com/bnema/vev-vt/ansi"
	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/domain/terminalcap"
	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/usecase/colorprofile"
	"github.com/bnema/vev/internal/usecase/layout"
	themeui "github.com/bnema/vev/internal/usecase/theme"
	"github.com/bnema/vev/internal/usecase/ui"
)

var sgrSequence = regexp.MustCompile("\x1b\\[([0-9;:]*)m")

// colorParams returns every color-bearing SGR parameter group in out, e.g.
// "31", "48;2;1;2;3", "58;5;9". Attribute parameters (0, 1, 2, 3, 4, 7...) are
// not reported.
func colorParams(out []byte) []string {
	var found []string
	for _, m := range sgrSequence.FindAllStringSubmatch(string(out), -1) {
		params := strings.Split(m[1], ";")
		for i := 0; i < len(params); i++ {
			n, err := strconv.Atoi(params[i])
			if err != nil {
				continue
			}
			switch {
			case n == 38 || n == 48 || n == 58:
				end := len(params)
				if i+1 < len(params) && params[i+1] == "2" {
					end = min(i+5, len(params))
				} else if i+1 < len(params) {
					end = min(i+3, len(params))
				}
				found = append(found, strings.Join(params[i:end], ";"))
				i = end - 1
			case n >= 30 && n <= 37, n >= 40 && n <= 47, n >= 90 && n <= 97, n >= 100 && n <= 107:
				found = append(found, params[i])
			}
		}
	}
	return found
}

func reducedTestTheme() themeui.Theme {
	palette := [16]renderer.RGB{}
	palette[4] = renderer.RGB{R: 60, G: 120, B: 220}
	palette[12] = palette[4]
	return themeui.Theme{
		Foreground: renderer.RGB{R: 230, G: 230, B: 230}, Background: renderer.RGB{R: 8, G: 9, B: 10},
		HasFG: true, HasBG: true, Known: true, UsePalette: true, SchemeKnown: true,
		Palette: palette, PaletteKnown: 1<<4 | 1<<12,
	}
}

// reducedChromeFrame composes a representative frame: two tabs (second
// active) with an attention bell, a split with the left pane focused, a
// bottom bar with MRU entries, a toast, and a focused palette modal with a
// selected row. rawTheme is what the attachment reported.
func reducedChromeFrame(t *testing.T, color terminalcap.ColorCapabilities) (composedRenderFrame, themeui.ResolvedTheme) {
	t.Helper()
	resolved := themeui.ResolveForColor(reducedTestTheme(), domain.ThemeAccent{Mode: domain.ThemeAccentAuto}, color)
	state := cachedSplitState("reduced", "left", layout.Horizontal, resolved.Theme)
	state.styles = resolved.Styles
	state.bars = barState{
		status:         statusSnapshot{session: "work", tabs: []statusTab{{name: "one", paneTitle: "sh"}, {name: "two", paneTitle: "vim", active: true, attention: true}}},
		attentionFrame: pulseFrameCount / 2,
		mru:            []recentRouteDisplay{{name: "alp"}, {name: "beta"}},
		theme:          resolved.Theme,
	}
	state.panes[0].title, state.panes[1].title = "left", "right"
	state.overlays.notices = []domain.Notification{{Code: domain.NoticeUser, Severity: domain.NoticeWarn, Message: "careful", Count: 1}}
	inner := renderer.NewFrame(18, 3)
	for x, r := range "> sel" {
		inner.Set(x, 0, renderer.Cell{Rune: r, Style: resolved.Styles.PickerSelection})
	}
	state.overlays.paletteActive = true
	state.overlays.palette = capturedModal{
		active: true, title: "Palette", focused: true, inner: inner,
		presentation: ui.Presentation{Bounds: domain.Rect{X: 10, Y: 2, Width: 20, Height: 5}, Inner: domain.Rect{X: 11, Y: 3, Width: 18, Height: 3}, Borders: ui.BorderAll},
	}
	return composeFrame(state, composeCacheInput{}), resolved
}

// rowIndex returns the x of the first cell of text on row y.
func rowIndex(frame renderer.Frame, y int, text string) int {
	var row strings.Builder
	for x := range frame.Width {
		r := frame.At(x, y).Rune
		if r == 0 {
			r = ' '
		}
		row.WriteRune(r)
	}
	return strings.Index(row.String(), text)
}

func drawProfile(t *testing.T, frame renderer.Frame, color terminalcap.ColorCapabilities) []byte {
	t.Helper()
	out, err := ansirenderer.NewWithColorProfile(ansirenderer.Capabilities{}, colorprofile.Profile(color)).Draw(frame, []renderer.Damage{renderer.FullRedraw()})
	require.NoError(t, err)
	return out
}

func TestReducedColorChromeIsLegible(t *testing.T) {
	ansi16 := terminalcap.ColorCapabilities{Mode: terminalcap.ANSI16, Source: terminalcap.SourceDeclared}
	mono := terminalcap.ColorCapabilities{Mode: terminalcap.Monochrome, Source: terminalcap.SourceDeclared}

	t.Run("ansi16 has no RGB, indexed or underline colors outside slots 0-15", func(t *testing.T) {
		composed, resolved := reducedChromeFrame(t, ansi16)
		out := drawProfile(t, composed.frame, ansi16)
		for _, p := range colorParams(out) {
			require.NotRegexp(t, `^(38|48|58)`, p, "ansi16 chrome emitted extended color %q", p)
		}
		// The accent is a palette slot, never RGB: the border and bell use it.
		require.True(t, resolved.Accent.Known)
		require.Contains(t, colorParams(out), "34", "accent slot 4 renders as the terminal's own blue")
		// Chrome surfaces are the terminal background: no RGB backgrounds.
		for x := range composed.frame.Width {
			for _, y := range []int{0, composed.frame.Height - 1} {
				require.False(t, composed.frame.At(x, y).Style.HasBackgroundRGB)
			}
		}
	})

	t.Run("monochrome has no color sequences at all", func(t *testing.T) {
		composed, _ := reducedChromeFrame(t, mono)
		out := drawProfile(t, composed.frame, mono)
		require.Empty(t, colorParams(out), "%q", out)
		require.NotRegexp(t, `\x1b\[[0-9;:]*(38|48|58)`, string(out))
	})

	for name, color := range map[string]terminalcap.ColorCapabilities{"ansi16": ansi16, "monochrome": mono} {
		t.Run(name+" distinguishes active, selected and focused elements", func(t *testing.T) {
			composed, resolved := reducedChromeFrame(t, color)
			frame := composed.frame
			// Top bar: " one" (inactive) vs " two" (active).
			inactive, active := frame.At(rowIndex(frame, 0, "one"), 0).Style, frame.At(rowIndex(frame, 0, "two"), 0).Style
			require.True(t, active.Inverse && active.Bold, "active tab must be bold reverse")
			require.False(t, inactive.Inverse)
			require.NotEqual(t, inactive.Canonical(), active.Canonical())
			// The attention bell is bold on its visible beat.
			bell := frame.At(rowIndex(frame, 0, string(ui.AttentionGlyph)), 0).Style
			require.True(t, bell.Bold && bell.Inverse)
			// Bottom bar: the session name is the active surface.
			require.True(t, frame.At(rowIndex(frame, frame.Height-1, "work"), frame.Height-1).Style.Inverse)
			require.False(t, frame.At(rowIndex(frame, frame.Height-1, "alp"), frame.Height-1).Style.Inverse)
			// Focused pane title bar is plain reverse (no stack title bars in
			// this fixture, so assert the role the title bar draws with).
			require.True(t, resolved.Styles.StatusBar.Inverse)
			// Palette selection row and focused modal border.
			require.True(t, frame.At(11, 3).Style.Inverse && frame.At(11, 3).Style.Bold, "selected row must be bold reverse")
			border := frame.At(10, 2).Style
			require.True(t, border.Bold, "focused modal border must be bold")
			require.Zero(t, border.Attrs&ansirenderer.AttrDim)
			// The same border, unfocused, is faint: focus is attribute-visible.
			require.NotZero(t, resolved.Styles.BorderMuted.Attrs&ansirenderer.AttrDim)
			// Copy selection is plain reverse.
			require.True(t, resolved.Styles.Selection.Inverse)
			require.True(t, resolved.Styles.CopyStatus.Inverse)
			// Toast box: warn border is visible without color.
			require.True(t, resolved.Styles.BorderWarn.Bold)
		})
	}
}

// TestReducedColorLeavesPaneContentAlone guards that panes are never recolored
// or faded: vev-vt quantizes them, vev chrome must not touch them.
func TestReducedColorLeavesPaneContentAlone(t *testing.T) {
	for _, mode := range []terminalcap.ColorMode{terminalcap.ANSI16, terminalcap.Monochrome} {
		color := terminalcap.ColorCapabilities{Mode: mode}
		resolved := themeui.ResolveForColor(reducedTestTheme(), domain.ThemeAccent{Mode: domain.ThemeAccentAuto}, color)
		state := cachedSplitState("pane", "left", layout.Horizontal, resolved.Theme)
		state.styles = resolved.Styles
		pane := state.panes[1].frame // unfocused
		cell := pane.At(0, 0)
		cell.Style.Foreground = 196
		pane.Set(0, 0, cell)
		composed := composeFrame(state, composeCacheInput{})
		x := state.panes[1].placement.Content.X
		y := state.panes[1].placement.Content.Y + 1
		require.Equal(t, cell.Style.Canonical(), composed.frame.At(x, y).Style.Canonical())
	}
}

// TestResolveForColorKeepsRGBModesIdentical proves TrueColor and ANSI256
// attachments resolve exactly as before this policy existed, so their output
// bytes cannot change.
func TestResolveForColorKeepsRGBModesIdentical(t *testing.T) {
	raw := reducedTestTheme()
	policy := domain.ThemeAccent{Mode: domain.ThemeAccentAuto}
	want := themeui.Resolve(raw, policy)
	for _, color := range []terminalcap.ColorCapabilities{{}, {Mode: terminalcap.TrueColor, Source: terminalcap.SourceForced}, {Mode: terminalcap.ANSI256, Source: terminalcap.SourceDeclared}} {
		require.Equal(t, want, themeui.ResolveForColor(raw, policy, color))
	}
	for _, color := range []terminalcap.ColorCapabilities{{Mode: terminalcap.TrueColor}, {Mode: terminalcap.ANSI256}} {
		composed, _ := reducedChromeFrame(t, color)
		state := cachedSplitState("reference", "left", layout.Horizontal, raw)
		_ = state
		out := drawProfile(t, composed.frame, color)
		require.Regexp(t, `(38|48);[25];`, string(out), "RGB-capable modes keep tinted surfaces")
	}
}

// TestAppliedThemeIsPerAttachment confirms two attachments of one session
// resolve independently.
func TestAppliedThemeIsPerAttachment(t *testing.T) {
	p, release := newBlockingPTY(t)
	defer release()
	d, sess, ac16, _ := newManualSessionWithPTYs(t, p)
	ac16.terminalCapabilities = terminalcap.Capabilities{Color: terminalcap.ColorCapabilities{Mode: terminalcap.ANSI16}}
	tr, _ := newCapturingTransport(t)
	acTrue := &attachedClient{tr: tr, output: newOutputStateStream(), size: domain.Size{Cols: 80, Rows: 24}}
	acTrue.output.attachment = acTrue
	acTrue.initOverlays()
	sess.mu.Lock()
	sess.attachments[acTrue] = struct{}{}
	sess.mu.Unlock()
	acTrue.setSession(sess)

	raw := reducedTestTheme()
	msg := protocol.Theme{Foreground: raw.Foreground, Background: raw.Background, HasForeground: true, HasBackground: true, Palette: raw.Palette, PaletteKnown: raw.PaletteKnown}
	d.applyTheme(sess, ac16, msg)
	d.applyTheme(sess, acTrue, msg)
	d.reapplyThemeSession(sess) // a config reload must keep each mode

	reduced, rgb := ac16.getAppliedTheme(), acTrue.getAppliedTheme()
	require.True(t, reduced.Raw.DimByAttribute)
	require.False(t, rgb.Raw.DimByAttribute)
	require.False(t, reduced.Resolved.Styles.SurfaceActive.HasBackgroundRGB)
	require.True(t, rgb.Resolved.Styles.SurfaceActive.HasBackgroundRGB)
	require.Equal(t, themeui.Resolve(rgb.Raw, domain.ThemeAccent{Mode: domain.ThemeAccentAuto}).Styles, rgb.Resolved.Styles)
}
