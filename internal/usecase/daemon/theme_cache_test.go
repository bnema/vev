package daemon

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/usecase/layout"
	themeui "github.com/bnema/vev/internal/usecase/theme"
	"github.com/bnema/vev/pkg/renderer"
)

func cacheTheme() themeui.Theme {
	palette := [16]renderer.RGB{}
	palette[2] = renderer.RGB{R: 30, G: 190, B: 150}
	palette[10] = palette[2]
	return themeui.Theme{
		Foreground: renderer.RGB{R: 230, G: 230, B: 230}, Background: renderer.RGB{R: 8, G: 9, B: 10},
		HasFG: true, HasBG: true, Known: true, TrueColor: true, UsePalette: true,
		Palette: palette, PaletteKnown: 1<<2 | 1<<10,
	}
}

func TestThemeAccentHotReloadRebuildsAppliedSnapshot(t *testing.T) {
	d, sess, ac, _ := newManualSessionWithPTYs(t)
	raw := cacheTheme()
	d.applyTheme(sess, ac, ports.Theme{Foreground: raw.Foreground, Background: raw.Background, HasForeground: true, HasBackground: true, TrueColor: true, Palette: raw.Palette, PaletteKnown: raw.PaletteKnown})
	before := ac.getAppliedTheme()

	cfg := domain.Defaults()
	cfg.ThemeAccent = domain.ThemeAccent{Mode: domain.ThemeAccentSlot, Slot: 2}
	d.ApplyConfig(cfg)
	after := ac.getAppliedTheme()

	require.Equal(t, before.Raw, after.Raw, "an accent-only reload retains the definitive client palette")
	require.Greater(t, after.Generation, before.Generation)
	require.Equal(t, themeui.Resolve(after.Raw, cfg.ThemeAccent), after.Resolved)
	require.Equal(t, uint8(2), after.Resolved.Accent.Slot)
}

func TestComposeCacheInvalidatesOnStyleGenerationWithoutPaneRecoloring(t *testing.T) {
	raw := cacheTheme()
	first := themeui.Resolve(raw, domain.ThemeAccent{Mode: domain.ThemeAccentAuto})
	second := themeui.Resolve(raw, domain.ThemeAccent{Mode: domain.ThemeAccentSlot, Slot: 2})
	pane := renderer.NewFrame(4, 1)
	pane.Set(0, 0, renderer.Cell{Rune: 'P', Style: renderer.Style{Foreground: 196}})
	state := capturedRenderState{
		reset: false, theme: raw, styles: first.Styles, styleGeneration: 1,
		layout: capturedTabLayout{area: domain.Rect{Width: 4, Height: 1}, focus: "p", valid: true, fingerprint: "same", placements: []layout.Placement{{ID: "p", Content: domain.Rect{Width: 4, Height: 1}}}},
		panes:  []capturedPaneRenderState{{id: "p", frame: pane, focused: true, placement: layout.Placement{ID: "p", Content: domain.Rect{Width: 4, Height: 1}}, damage: []renderer.Damage{renderer.FullRedraw()}}},
	}
	committed := composeFrame(state, composeCacheInput{})
	state.reset, state.styles, state.styleGeneration, state.panes[0].damage = false, second.Styles, 2, nil
	out := composeFrame(state, committed.cache, composeCacheInput{})

	require.Equal(t, []renderer.Damage{renderer.FullRedraw()}, out.damage)
	require.Equal(t, pane.At(0, 0), out.frame.At(0, 1), "captured application cells must not receive chrome styles")
}

func TestAppliedThemeKeepsDimmerOutsideResolvedChrome(t *testing.T) {
	raw := cacheTheme()
	resolved := themeui.Resolve(raw, domain.ThemeAccent{Mode: domain.ThemeAccentSlot, Slot: 2})
	cell := renderer.Cell{Rune: 'X', Style: renderer.Style{Foreground: 196}}
	focused, inactive := renderer.NewFrame(2, 1), renderer.NewFrame(2, 1)
	focused.Set(0, 0, cell)
	inactive.Set(0, 0, cell)
	placements := []layout.Placement{{ID: "focused", Content: domain.Rect{Width: 2, Height: 1}}, {ID: "inactive", Content: domain.Rect{X: 2, Width: 2, Height: 1}}}
	state := capturedRenderState{
		reset: true, theme: raw, styles: resolved.Styles, styleGeneration: 1,
		layout: capturedTabLayout{area: domain.Rect{Width: 4, Height: 1}, focus: "focused", valid: true, placements: placements},
		panes:  []capturedPaneRenderState{{id: "focused", frame: focused, focused: true, placement: placements[0], damage: []renderer.Damage{renderer.FullRedraw()}}, {id: "inactive", frame: inactive, placement: placements[1], damage: []renderer.Damage{renderer.FullRedraw()}}},
	}
	out := composeFrame(state, composeCacheInput{})
	require.Equal(t, cell, out.frame.At(0, 1))
	require.Equal(t, themeui.NewDimmer(raw, themeui.WithForegroundDimming(inactivePaneForegroundDimming)).Dim(cell.Style), out.frame.At(2, 1).Style)
}

func TestChromeDimmersUseNeutralInputsOutsideAccentRamp(t *testing.T) {
	raw := cacheTheme()
	resolved := themeui.Resolve(raw, domain.ThemeAccent{Mode: domain.ThemeAccentSlot, Slot: 2})
	neutral := renderer.DefaultStyle()
	neutral.HasForegroundRGB = true
	neutral.ForegroundRGB = themeui.Blend(raw.Foreground, raw.Background, 0.40)
	want := themeui.NewDimmer(raw).Dim(neutral)
	accentDerived := themeui.NewDimmer(raw).Dim(resolved.Styles.BorderMuted)

	split := cachedSplitState("neutral-divider", "left", layout.Horizontal, raw)
	split.styles = resolved.Styles
	splitOut := composeFrame(split, composeCacheInput{})
	require.Equal(t, 'L', splitOut.frame.At(0, 1).Rune, "split geometry and pane content must remain unchanged")
	require.Equal(t, 'R', splitOut.frame.At(21, 1).Rune, "split geometry and pane content must remain unchanged")
	require.Equal(t, want, splitOut.frame.At(20, 1).Style, "divider must dim a neutral input")
	require.NotEqual(t, accentDerived, splitOut.frame.At(20, 1).Style, "divider must exclude accent-ramp styles from Dimmer")

	titles := cachedStackTitleState("inactive", 1, true)
	titles.theme, titles.styles = raw, resolved.Styles
	titleOut := composeFrame(titles, composeCacheInput{})
	require.Equal(t, 'i', titleOut.frame.At(0, 1).Rune, "inactive title geometry must remain unchanged")
	require.Equal(t, want, titleOut.frame.At(0, 1).Style, "inactive title must dim a neutral input")
	require.NotEqual(t, accentDerived, titleOut.frame.At(0, 1).Style, "inactive title must exclude accent-ramp styles from Dimmer")
	require.Equal(t, 'E', titleOut.frame.At(0, 3).Rune, "pane content must remain unchanged")
}
