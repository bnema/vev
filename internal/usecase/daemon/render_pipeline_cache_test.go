package daemon

import (
	"errors"
	"io"
	"maps"
	"testing"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/usecase/layout"
	themeui "github.com/bnema/vev/internal/usecase/theme"
	"github.com/bnema/vev/internal/usecase/ui"
	"github.com/bnema/vev/pkg/renderer"
	"github.com/stretchr/testify/require"
)

type cacheFailTransport struct{}

func (cacheFailTransport) Send(ports.Frame) error     { return errors.New("send failed") }
func (cacheFailTransport) Recv() (ports.Frame, error) { return ports.Frame{}, io.EOF }
func (cacheFailTransport) Close() error               { return nil }

func TestPipelineCachePublishesOnlyAfterEmission(t *testing.T) {
	for _, failure := range []struct {
		name  string
		apply func(*composedRenderFrame, *attachedClient)
	}{
		{
			name: "prepare",
			apply: func(composed *composedRenderFrame, _ *attachedClient) {
				// Frame validation fails in output.prepare after composition.
				composed.frame = renderer.Frame{Width: 1}
			},
		},
		{
			name:  "send",
			apply: func(_ *composedRenderFrame, ac *attachedClient) { ac.replaceTransport(cacheFailTransport{}) },
		},
	} {
		t.Run(failure.name, func(t *testing.T) {
			d, sess, ac, sends := newManualSessionWithPTYs(t)
			healthy := ac.transport()
			committed := composeFrame(cacheState("committed", 1), composeCacheInput{})
			ac.pipelineCache = committed.cache
			before := cloneComposeCache(ac.pipelineCache)
			pending := composeFrame(cacheState("next", 2), ac.pipelineCache, ac.pipelineScratch)
			failure.apply(&pending, ac)

			state := cacheState("next", 2)
			state.attachment = ac
			ac.sendMu.Lock()
			require.True(t, d.emitFrame(sess, ac, &state, pending))
			require.Equal(t, before, cloneComposeCache(ac.pipelineCache), "failed emission must not publish any composed cache backing storage")

			// A send error detaches the failed link. Re-own this test attachment with
			// its original healthy transport, then retry the pending state.
			if failure.name == "send" {
				sess.mu.Lock()
				sess.client = ac
				sess.mu.Unlock()
				ac.setSession(sess)
				ac.replaceTransport(healthy)
			}
			pending = composeFrame(cacheState("next", 2), ac.pipelineCache, ac.pipelineScratch)
			state = cacheState("next", 2)
			state.attachment = ac
			ac.sendMu.Lock()
			require.True(t, d.emitFrame(sess, ac, &state, pending))
			require.Equal(t, pending.cache, ac.pipelineCache)
			frame := <-sends
			output, err := ports.UnmarshalOutput(frame.Payload)
			require.NoError(t, err)
			require.Contains(t, string(output.Data), "next", "the retry must emit state retained after the failed emission")
		})
	}
}

func TestComposeFrameClearsFloatingFrameWhenItCloses(t *testing.T) {
	visible := cacheState("base", 1)
	committed := composeFrame(visible, composeCacheInput{})

	hidden := visible
	hidden.floating = capturedFloatingRenderState{}
	hidden.panes[0].damage = nil
	result := composeFrame(hidden, committed.cache, committed.cache)

	require.Equal(t, 's', result.frame.Row(1)[2].Rune)
	require.Equal(t, []renderer.Damage{renderer.FullRedraw()}, result.damage)
}

func TestComposeFrameCacheSkipsUndamagedBlitsAndInvalidatesFocusAndLayout(t *testing.T) {
	theme := themeui.Theme{Known: true, TrueColor: true, HasFG: true, HasBG: true, Foreground: renderer.RGB{R: 200, G: 200, B: 200}, Background: renderer.RGB{R: 10, G: 10, B: 10}}
	initial := cachedSplitState("horizontal-left", "left", layout.Horizontal, theme)
	committed := composeFrame(initial, composeCacheInput{})
	require.Equal(t, '│', committed.frame.At(20, 1).Rune)
	focusedStyle := committed.frame.At(0, 1).Style
	dimmedStyle := committed.frame.At(21, 1).Style
	require.Equal(t, renderer.RGB{R: 96, G: 96, B: 96}, dimmedStyle.ForegroundRGB, "inactive panes should use stronger foreground dimming")

	undamaged := initial
	undamaged.reset = false
	undamaged.panes[0].frame.Set(0, 0, renderer.Cell{Rune: 'Z', Style: renderer.DefaultStyle()})
	undamaged.panes[0].damage = nil
	undamaged.panes[1].frame.Set(0, 0, renderer.Cell{Rune: 'x', Style: renderer.DefaultStyle()})
	undamaged.panes[1].damage = []renderer.Damage{{Kind: renderer.DamageText, X: 21, Width: 1, Height: 1}}
	out := composeFrame(undamaged, committed.cache, composeCacheInput{})
	require.Equal(t, 'L', out.frame.At(0, 1).Rune, "an undamaged pane must retain the committed blit")
	require.Equal(t, 'x', out.frame.At(21, 1).Rune)
	require.Contains(t, out.damage, renderer.Damage{Kind: renderer.DamageText, X: 21, Y: 1, Width: 1, Height: 1})

	focusChanged := cachedSplitState("horizontal-right", "right", layout.Horizontal, theme)
	focusChanged.reset = false
	focusChanged.panes[0].damage, focusChanged.panes[1].damage = nil, nil
	out = composeFrame(focusChanged, out.cache, committed.cache)
	require.Equal(t, dimmedStyle, out.frame.At(0, 1).Style, "the old focus must be re-blitted dimmed")
	require.Equal(t, focusedStyle, out.frame.At(21, 1).Style, "the new focus must be re-blitted undimmed")
	require.Equal(t, []renderer.Damage{renderer.FullRedraw()}, out.damage)

	vertical := cachedSplitState("vertical-right", "right", layout.Vertical, theme)
	vertical.reset = false
	vertical.panes[0].damage, vertical.panes[1].damage = nil, nil
	out = composeFrame(vertical, out.cache, committed.cache)
	require.NotEqual(t, '│', out.frame.At(20, 1).Rune, "layout invalidation must clear the old vertical divider")
	require.Equal(t, '─', out.frame.At(20, 3).Rune)
	require.Equal(t, []renderer.Damage{renderer.FullRedraw()}, out.damage)
}

func TestLayoutFingerprintWeightChangeInvalidatesGeometryCache(t *testing.T) {
	root := &layout.Node{Kind: layout.Split, Dir: layout.Horizontal, Children: []*layout.Node{
		{Kind: layout.Leaf, Leaf: "left", Weight: 1},
		{Kind: layout.Leaf, Leaf: "right", Weight: 1},
	}}
	stateFor := func(reset bool, damage []renderer.Damage) capturedRenderState {
		placements, ok := layout.Solve(root, domain.Rect{Width: 61, Height: 5})
		require.True(t, ok)
		panes := make([]capturedPaneRenderState, 0, len(placements))
		for _, placement := range placements {
			frame := renderer.NewFrame(placement.Content.Width, placement.Content.Height)
			for y := 0; y < frame.Height; y++ {
				for x := 0; x < frame.Width; x++ {
					frame.Set(x, y, renderer.Cell{Rune: rune(placement.ID[0]), Style: renderer.DefaultStyle()})
				}
			}
			panes = append(panes, capturedPaneRenderState{
				id: placement.ID, frame: frame, placement: placement, focused: placement.ID == "left", damage: damage,
			})
		}
		return capturedRenderState{
			reset:  reset,
			layout: capturedTabLayout{area: domain.Rect{Width: 61, Height: 5}, focus: "left", placements: placements, fingerprint: layoutFingerprint(root), valid: true},
			panes:  panes, styles: resolveStyles(nil),
		}
	}

	initial := stateFor(true, []renderer.Damage{renderer.FullRedraw()})
	committed := composeFrame(initial, composeCacheInput{})
	root.Children[0].Weight = 2
	next := stateFor(false, nil)
	require.NotEqual(t, initial.layout.fingerprint, next.layout.fingerprint)
	require.Equal(t, 41, next.layout.placements[1].Content.X)
	out := composeFrame(next, committed.cache, composeCacheInput{})
	require.Equal(t, []renderer.Damage{renderer.FullRedraw()}, out.damage)
	require.Equal(t, 'l', out.frame.At(31, 1).Rune, "the enlarged pane must replace its old divider/cache footprint")
	require.Equal(t, 'r', out.frame.At(41, 1).Rune, "weight-derived pane geometry must be reblitted at its new position")
}

func TestComposeFrameUsesCachedNeutralStructuralBorder(t *testing.T) {
	theme := themeui.Theme{Known: true, TrueColor: true, HasFG: true, HasBG: true, Foreground: renderer.RGB{R: 220, G: 210, B: 200}, Background: renderer.RGB{R: 20, G: 30, B: 40}}
	neutralBorder := renderer.Style{HasForegroundRGB: true, ForegroundRGB: renderer.RGB{R: 170, G: 80, B: 30}}
	expected := themeui.NewDimmer(theme).Dim(neutralBorder)

	t.Run("divider", func(t *testing.T) {
		state := cachedSplitState("horizontal-left", "left", layout.Horizontal, theme)
		state.styles.NeutralBorder = neutralBorder

		out := composeFrame(state, composeCacheInput{})
		require.Equal(t, expected, out.frame.At(20, 1).Style)
	})

	t.Run("unfocused title", func(t *testing.T) {
		state := cachedStackTitleState("collapsed", 1, true)
		state.theme = theme
		state.styles = themeui.Resolve(theme, domain.ThemeAccent{Mode: domain.ThemeAccentAuto}).Styles
		state.styles.NeutralBorder = neutralBorder

		out := composeFrame(state, composeCacheInput{})
		require.Equal(t, expected, out.frame.At(0, 1).Style)
	})
}

func TestComposeFrameStackDrawsTitleBarsAndDimsCollapsed(t *testing.T) {
	state := cachedStackTitleState("collapsed", 1, true)
	state.theme = themeui.Theme{
		Known:      true,
		TrueColor:  true,
		HasFG:      true,
		HasBG:      true,
		Foreground: renderer.RGB{R: 220, G: 210, B: 200},
		Background: renderer.RGB{R: 20, G: 30, B: 40},
	}
	state.styles = themeui.Resolve(state.theme, domain.ThemeAccent{Mode: domain.ThemeAccentAuto}).Styles
	before := cloneCapturedStackState(state)

	out := composeFrame(state, composeCacheInput{})

	collapsedTitle := out.frame.At(0, 1)
	require.Equal(t, 'c', collapsedTitle.Rune)
	require.True(t, collapsedTitle.Style.HasForegroundRGB, "collapsed title bar must use dimmed chrome")
	require.Equal(t, 'E', out.frame.At(0, 2).Rune, "expanded pane must draw no title row; its content starts where the title bar used to be")
	require.Equal(t, before, state, "composition must not mutate the captured source frame")
}

func TestComposeFrameCacheRefreshesStackTitlesAndBars(t *testing.T) {
	initial := cachedStackTitleState("one", 1, true)
	committed := composeFrame(initial, composeCacheInput{})

	renamed := initial
	renamed.reset = false
	renamed.panes[0].title, renamed.panes[0].titleGeneration = "renamed", 2
	out := composeFrame(renamed, committed.cache, composeCacheInput{})
	require.Equal(t, "renamed", rowText(out.frame.Row(1))[:7])
	require.Contains(t, out.damage, renderer.Damage{Kind: renderer.DamageText, X: 0, Y: 1, Width: 20, Height: 1})

	unchanged := composeFrame(renamed, out.cache, committed.cache)
	require.NotContains(t, unchanged.damage, renderer.Damage{Kind: renderer.DamageText, X: 0, Y: 1, Width: 20, Height: 1})

	bars := cacheBarState("one", true)
	committed = composeFrame(bars, composeCacheInput{})
	bars.reset = false
	bars.panes[0].damage = []renderer.Damage{{Kind: renderer.DamageText, Width: 1, Height: 1}}
	out = composeFrame(bars, committed.cache, composeCacheInput{})
	for _, damage := range out.damage {
		require.NotEqual(t, 0, damage.Y, "unchanged top bar must remain cached")
		require.NotEqual(t, 2, damage.Y, "unchanged bottom bar must remain cached")
	}
	bars.panes[0].damage = nil
	bars.bars.bottomRight = "two"
	out = composeFrame(bars, out.cache, committed.cache)
	require.Contains(t, out.damage, renderer.Damage{Kind: renderer.DamageText, X: 0, Y: 2, Width: 6, Height: 1})
	require.NotContains(t, out.damage, renderer.Damage{Kind: renderer.DamageText, X: 0, Y: 0, Width: 6, Height: 1})
}

func TestComposeFrameCachedTitleBarsDoNotAllocate(t *testing.T) {
	state := cachedStackTitleState("title", 1, true)
	committed, scratch := composeCacheInput{}, composeCacheInput{}
	compose := func() {
		out := composeFrame(state, committed, scratch)
		scratch, committed = committed, out.cache
		state.reset = false
	}
	compose()
	compose()
	compose()

	allocs := testing.AllocsPerRun(100, compose)
	require.Zero(t, allocs, "warm production title composition must reuse the committed and scratch cache backing storage")
}

func cachedSplitState(fingerprint string, focus layout.PaneID, direction layout.SplitDir, theme themeui.Theme) capturedRenderState {
	left, right := layout.PaneID("left"), layout.PaneID("right")
	area := domain.Rect{Width: 41, Height: 5}
	root := &layout.Node{Kind: layout.Split, Dir: direction, Children: []*layout.Node{layout.NewLeaf(left), layout.NewLeaf(right)}}
	placements, dividers, ok := layout.SolveWithDividers(root, area)
	if !ok {
		panic("cached split fixture has invalid geometry")
	}
	leftFrame := cachePaneFrame(placements[0].Content.Width, placements[0].Content.Height, 'L')
	rightFrame := cachePaneFrame(placements[1].Content.Width, placements[1].Content.Height, 'R')
	return capturedRenderState{
		reset:  true,
		layout: capturedTabLayout{area: area, focus: focus, placements: placements, dividers: dividers, fingerprint: fingerprint, valid: true},
		panes:  []capturedPaneRenderState{{id: left, frame: leftFrame, placement: placements[0], focused: focus == left, damage: []renderer.Damage{renderer.FullRedraw()}}, {id: right, frame: rightFrame, placement: placements[1], focused: focus == right, damage: []renderer.Damage{renderer.FullRedraw()}}},
		theme:  theme,
		styles: themeui.Resolve(theme, domain.ThemeAccent{Mode: domain.ThemeAccentAuto}).Styles,
	}
}

func cachedStackTitleState(title string, generation uint64, reset bool) capturedRenderState {
	first, second := layout.PaneID("first"), layout.PaneID("second")
	placements := []layout.Placement{
		{ID: first, TitleBar: domain.Rect{Width: 20, Height: 1}, Collapsed: true, InStack: true},
		{ID: second, Content: domain.Rect{Y: 1, Width: 20, Height: 4}, InStack: true},
	}
	return capturedRenderState{
		reset:  reset,
		layout: capturedTabLayout{area: domain.Rect{Width: 20, Height: 5}, focus: second, placements: placements, fingerprint: "stack", valid: true},
		panes:  []capturedPaneRenderState{{id: first, title: title, titleGeneration: generation, placement: placements[0]}, {id: second, frame: cachePaneFrame(20, 3, 'E'), title: "second", titleGeneration: 1, placement: placements[1], focused: true, damage: []renderer.Damage{renderer.FullRedraw()}}},
		styles: resolveStyles(nil),
	}
}

func cloneCapturedStackState(in capturedRenderState) capturedRenderState {
	out := in
	out.layout.placements = append([]layout.Placement(nil), in.layout.placements...)
	out.panes = append([]capturedPaneRenderState(nil), in.panes...)
	for i := range out.panes {
		if in.panes[i].frame.Cells != nil {
			out.panes[i].frame = in.panes[i].frame.Clone()
		}
		out.panes[i].rawDamage = append([]renderer.Damage(nil), in.panes[i].rawDamage...)
		out.panes[i].damage = append([]renderer.Damage(nil), in.panes[i].damage...)
	}
	return out
}

func cacheBarState(bottom string, reset bool) capturedRenderState {
	pane := cachePaneFrame(6, 1, 'P')
	placement := layout.Placement{ID: "pane", Content: domain.Rect{Width: 6, Height: 1}}
	return capturedRenderState{reset: reset, layout: capturedTabLayout{area: domain.Rect{Width: 6, Height: 1}, focus: "pane", placements: []layout.Placement{placement}, fingerprint: "bar", valid: true}, panes: []capturedPaneRenderState{{id: "pane", frame: pane, placement: placement, focused: true, damage: []renderer.Damage{renderer.FullRedraw()}}}, bars: barState{bottomRight: bottom}, styles: resolveStyles(nil)}
}

func cachePaneFrame(width, height int, r rune) renderer.Frame {
	frame := renderer.NewFrame(width, height)
	frame.Set(0, 0, renderer.Cell{Rune: r, Style: renderer.DefaultStyle()})
	return frame
}

func cacheState(title string, generation uint64) capturedRenderState {
	pane := renderer.NewFrame(6, 1)
	for x, r := range title[:min(len(title), 6)] {
		pane.Set(x, 0, renderer.Cell{Rune: r, Style: renderer.DefaultStyle()})
	}
	floating := renderer.NewFrame(2, 1)
	floating.Set(0, 0, renderer.Cell{Rune: 'F', Style: renderer.DefaultStyle()})
	return capturedRenderState{
		reset:    false,
		layout:   capturedTabLayout{area: domain.Rect{Width: 6, Height: 3}, valid: true, fingerprint: "layout"},
		panes:    []capturedPaneRenderState{{id: "pane", frame: pane, placement: layout.Placement{ID: "pane", Content: domain.Rect{Width: 6, Height: 1}, TitleBar: domain.Rect{Width: 6, Height: 1}}, title: title, titleGeneration: generation, focused: true, damage: []renderer.Damage{renderer.FullRedraw()}}},
		floating: capturedFloatingRenderState{visible: true, pane: capturedPaneRenderState{frame: floating, damage: []renderer.Damage{renderer.FullRedraw()}}, geometry: floatingGeometry{Mode: ui.PresentationFloating, Bounds: domain.Rect{X: 2, Y: 1, Width: 2, Height: 1}, Inner: domain.Rect{X: 2, Y: 1, Width: 2, Height: 1}}, generation: generation, titleGeneration: generation},
		bars:     barState{topRight: title, bottomRight: title},
		styles:   resolveStyles(nil),
	}
}

func cloneComposeCache(in composeCacheInput) composeCacheInput {
	out := in
	out.frame = in.frame.Clone()
	out.titleGenerations = make(map[layout.PaneID]uint64, len(in.titleGenerations))
	maps.Copy(out.titleGenerations, in.titleGenerations)
	out.damage = append([]renderer.Damage(nil), in.damage...)
	out.toastFootprints = append([]domain.Rect(nil), in.toastFootprints...)
	out.bars.top = append([]renderer.Cell(nil), in.bars.top...)
	out.bars.bottom = append([]renderer.Cell(nil), in.bars.bottom...)
	return out
}
