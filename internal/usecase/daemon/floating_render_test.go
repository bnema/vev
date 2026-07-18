package daemon

import (
	"errors"
	"io"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/usecase/layout"
	themeui "github.com/bnema/vev/internal/usecase/theme"
	"github.com/bnema/vev/pkg/renderer"
)

// applyFloatingResizePlanForTest exercises the transactional apply primitive.
func applyFloatingResizePlanForTest(d *Daemon, p *pane, geometry floatingGeometry) bool {
	if p == nil || !geometry.valid() {
		return false
	}
	plan := resizePlan{members: []resizeMember{{pane: p, rect: geometry.Inner, floating: geometry, isFloating: true}}}
	d.applyResize(&plan)
	member := plan.members[0]
	if !member.ok {
		return false
	}
	p.mu.Lock()
	p.rect = geometry.Inner
	p.popupGeometry = geometry
	p.screen.Resize(geometry.Inner.Width, geometry.Inner.Height)
	p.mu.Unlock()
	return true
}

func TestFloatingGeometryTranslate(t *testing.T) {
	geometry := floatingGeometry{
		Bounds: domain.Rect{X: 3, Y: 5, Width: 10, Height: 8},
		Inner:  domain.Rect{X: 4, Y: 6, Width: 8, Height: 6},
	}

	translated := geometry.translate(11, 13)
	require.Equal(t, floatingGeometry{
		Bounds: domain.Rect{X: 14, Y: 18, Width: 10, Height: 8},
		Inner:  domain.Rect{X: 15, Y: 19, Width: 8, Height: 6},
	}, translated)
	require.Equal(t, floatingGeometry{
		Bounds: domain.Rect{X: 3, Y: 5, Width: 10, Height: 8},
		Inner:  domain.Rect{X: 4, Y: 6, Width: 8, Height: 6},
	}, geometry)
}

func TestFloatingFrameGeometryPreservesBorderRules(t *testing.T) {
	const frameX, frameY = 7, 3
	tests := []struct {
		name                  string
		content               domain.Size
		cfg                   domain.FloatingConfig
		tinyWidth, tinyHeight bool
	}{
		{name: "normal", content: domain.Size{Cols: 80, Rows: 24}, cfg: domain.FloatingConfig{Width: 80, Height: 75}},
		{name: "tiny width omits horizontal borders", content: domain.Size{Cols: 2, Rows: 20}, cfg: domain.FloatingConfig{Width: 100, Height: 100}, tinyWidth: true},
		{name: "tiny height omits vertical borders", content: domain.Size{Cols: 20, Rows: 2}, cfg: domain.FloatingConfig{Width: 100, Height: 100}, tinyHeight: true},
		{name: "full size", content: domain.Size{Cols: 80, Rows: 24}, cfg: domain.FloatingConfig{Width: 100, Height: 100}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			contentGeometry := calculateContentFloatingGeometry(tt.content, tt.cfg)
			frameGeometry := contentGeometry.translate(frameX, frameY)

			require.Equal(t, contentGeometry.Bounds.X+frameX, frameGeometry.Bounds.X)
			require.Equal(t, contentGeometry.Bounds.Y+frameY, frameGeometry.Bounds.Y)
			require.Equal(t, contentGeometry.Inner.X+frameX, frameGeometry.Inner.X)
			require.Equal(t, contentGeometry.Inner.Y+frameY, frameGeometry.Inner.Y)
			require.Equal(t, contentGeometry.Bounds.Width, frameGeometry.Bounds.Width)
			require.Equal(t, contentGeometry.Bounds.Height, frameGeometry.Bounds.Height)
			require.Equal(t, contentGeometry.Inner.Width, frameGeometry.Inner.Width)
			require.Equal(t, contentGeometry.Inner.Height, frameGeometry.Inner.Height)

			if tt.tinyWidth {
				require.Equal(t, frameGeometry.Bounds.X, frameGeometry.Inner.X)
				require.Equal(t, frameGeometry.Bounds.Width, frameGeometry.Inner.Width)
			}
			if tt.tinyHeight {
				require.Equal(t, frameGeometry.Bounds.Y, frameGeometry.Inner.Y)
				require.Equal(t, frameGeometry.Bounds.Height, frameGeometry.Inner.Height)
			}
		})
	}
}

func TestCalculateFloatingGeometry(t *testing.T) {
	tests := []struct {
		name    string
		content domain.Size
		cfg     domain.FloatingConfig
		want    floatingGeometry
	}{
		{"eighty percent centered", domain.Size{Cols: 101, Rows: 51}, domain.FloatingConfig{Width: 80, Height: 80}, floatingGeometry{Bounds: domain.Rect{X: 10, Y: 5, Width: 80, Height: 40}, Inner: domain.Rect{X: 11, Y: 6, Width: 78, Height: 38}}},
		{"one percent clamps to one", domain.Size{Cols: 100, Rows: 20}, domain.FloatingConfig{Width: 1, Height: 1}, floatingGeometry{Bounds: domain.Rect{X: 49, Y: 9, Width: 1, Height: 1}, Inner: domain.Rect{X: 49, Y: 9, Width: 1, Height: 1}}},
		{"full size", domain.Size{Cols: 100, Rows: 20}, domain.FloatingConfig{Width: 100, Height: 100}, floatingGeometry{Bounds: domain.Rect{Width: 100, Height: 20}, Inner: domain.Rect{X: 1, Y: 1, Width: 98, Height: 18}}},
		{"tiny axes omit borders", domain.Size{Cols: 2, Rows: 1}, domain.FloatingConfig{Width: 100, Height: 100}, floatingGeometry{Bounds: domain.Rect{Width: 2, Height: 1}, Inner: domain.Rect{Width: 2, Height: 1}}},
		{"percent clamps", domain.Size{Cols: 10, Rows: 10}, domain.FloatingConfig{Width: 101, Height: -1}, floatingGeometry{Bounds: domain.Rect{Width: 10, Height: 1, Y: 4}, Inner: domain.Rect{X: 1, Width: 8, Y: 4, Height: 1}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, calculateContentFloatingGeometry(tt.content, tt.cfg))
		})
	}
}

func TestFloatingAxisGeometryEndpointsAndTinyBorders(t *testing.T) {
	tests := []struct {
		name               string
		available, percent int
		want               floatingAxisGeometry
	}{
		{name: "one percent", available: 100, percent: 1, want: floatingAxisGeometry{BoundsSize: 1, InnerSize: 1}},
		{name: "full size reserves borders", available: 100, percent: 100, want: floatingAxisGeometry{BoundsSize: 100, InnerSize: 98, BorderOffset: 1}},
		{name: "two cells omits borders", available: 2, percent: 100, want: floatingAxisGeometry{BoundsSize: 2, InnerSize: 2}},
		{name: "three cells leaves one inner cell", available: 3, percent: 100, want: floatingAxisGeometry{BoundsSize: 3, InnerSize: 1, BorderOffset: 1}},
		{name: "percentage clamps", available: 10, percent: 101, want: floatingAxisGeometry{BoundsSize: 10, InnerSize: 8, BorderOffset: 1}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, calculateFloatingAxisGeometry(tt.available, tt.percent))
		})
	}
}

func TestComposeCapturedFloatingFrameDoesNotMutateSourceAndDamagesTitle(t *testing.T) {
	base := renderer.NewFrame(40, 12)
	base.Set(2, 2, renderer.Cell{Rune: 'B'})
	content := domain.Rect{Y: 1, Width: 40, Height: 10}
	geometry := calculateContentFloatingGeometry(domain.Size{Cols: content.Width, Rows: content.Height}, domain.FloatingConfig{Width: 80, Height: 80})
	paneFrame := renderer.NewFrame(6, 3)
	paneFrame.Set(0, 0, renderer.Cell{Rune: 'F'})
	floating := capturedFloatingRenderState{
		visible:         true,
		pane:            capturedPaneRenderState{frame: paneFrame, title: "nvim: project/main.go", titleGeneration: 1},
		geometry:        geometry,
		title:           "nvim: project/main.go",
		generation:      1,
		titleGeneration: 1,
	}
	frame, _ := composeCapturedFloatingFrame(floatingComposeInput{
		baseFrame: base,
		floating:  floating,
		content:   content,
		theme:     themeui.Theme{},
	})
	require.Equal(t, 'B', base.At(2, 2).Rune, "backdrop must only touch the composed destination")
	frameGeometry := geometry.translate(content.X, content.Y)
	require.Equal(t, '┌', frame.At(frameGeometry.Bounds.X, frameGeometry.Bounds.Y).Rune)
	require.Equal(t, 'F', frame.At(frameGeometry.Inner.X, frameGeometry.Inner.Y).Rune)
	var gotTitle strings.Builder
	for x := frameGeometry.Bounds.X + 2; x < frameGeometry.Bounds.X+frameGeometry.Bounds.Width-2; x++ {
		gotTitle.WriteRune(frame.At(x, frameGeometry.Bounds.Y).Rune)
	}
	require.Equal(t, "nvim: project/main.go", strings.TrimRight(gotTitle.String(), "─"))

	floating.titleGeneration++
	_, damage := composeCapturedFloatingFrame(floatingComposeInput{
		baseFrame: base,
		floating:  floating,
		content:   content,
		theme:     themeui.Theme{},
		cache: composeCacheInput{
			valid:                   true,
			floatingGeneration:      1,
			floatingGeometry:        frameGeometry,
			floatingTitleGeneration: 1,
		},
	})
	require.Len(t, damage, 1)
	require.Equal(t, frameGeometry.Bounds.Y, damage[0].Y)
	require.Equal(t, 1, damage[0].Height)
}

func TestComposeCapturedFloatingFrameUsesSemanticBorderWithoutTintingPaneCells(t *testing.T) {
	base := renderer.NewFrame(20, 9)
	content := domain.Rect{Y: 1, Width: 20, Height: 7}
	geometry := floatingGeometry{
		Bounds: domain.Rect{X: 3, Y: 1, Width: 8, Height: 5},
		Inner:  domain.Rect{X: 4, Y: 2, Width: 6, Height: 3},
	}
	active := renderer.Style{Foreground: 2, Background: 3}
	muted := renderer.Style{Foreground: 4, Background: 5}
	pane := renderer.NewFrame(6, 3)
	for y := range pane.Height {
		for x := range pane.Width {
			pane.Set(x, y, renderer.Cell{Rune: rune('a' + y*pane.Width + x), Style: renderer.Style{Foreground: x + y*10, Background: x + y*10 + 1}})
		}
	}

	frame, _ := composeCapturedFloatingFrame(floatingComposeInput{
		baseFrame: base,
		floating: capturedFloatingRenderState{
			visible: true, focused: true, pane: capturedPaneRenderState{frame: pane}, geometry: geometry, title: "float", generation: 1,
		},
		content: content,
		styles:  themeui.Styles{BorderActive: active, BorderMuted: muted},
		full:    true,
	})
	bounds := geometry.translate(content.X, content.Y)
	require.Equal(t, active, frame.At(bounds.Bounds.X, bounds.Bounds.Y).Style, "focused floating border must use BorderActive")
	require.NotEqual(t, muted, frame.At(bounds.Bounds.X, bounds.Bounds.Y).Style)
	for y := range bounds.Inner.Height {
		for x := range bounds.Inner.Width {
			require.Equal(t, pane.At(x, y), frame.At(bounds.Inner.X+x, bounds.Inner.Y+y), "pane cell (%d,%d) must remain byte-identical", x, y)
		}
	}
}

func TestComposeCapturedFloatingFrameUsesMutedBorderWhenUnfocused(t *testing.T) {
	base := renderer.NewFrame(12, 7)
	content := domain.Rect{Y: 1, Width: 12, Height: 5}
	geometry := floatingGeometry{Bounds: domain.Rect{X: 2, Y: 1, Width: 6, Height: 3}, Inner: domain.Rect{X: 3, Y: 2, Width: 4, Height: 1}}
	active, muted := renderer.Style{Foreground: 2}, renderer.Style{Foreground: 4}

	frame, _ := composeCapturedFloatingFrame(floatingComposeInput{
		baseFrame: base,
		floating:  capturedFloatingRenderState{visible: true, pane: capturedPaneRenderState{frame: renderer.NewFrame(4, 1)}, geometry: geometry, title: "float", generation: 1},
		content:   content,
		styles:    themeui.Styles{BorderActive: active, BorderMuted: muted},
		full:      true,
	})
	bounds := geometry.translate(content.X, content.Y)
	require.Equal(t, muted, frame.At(bounds.Bounds.X, bounds.Bounds.Y).Style)
}

func TestComposeCapturedFloatingFrameFocusChangeInvalidatesCache(t *testing.T) {
	content := domain.Rect{Y: 1, Width: 12, Height: 5}
	geometry := floatingGeometry{Bounds: domain.Rect{X: 2, Y: 1, Width: 6, Height: 3}, Inner: domain.Rect{X: 3, Y: 2, Width: 4, Height: 1}}
	frame, damage := composeCapturedFloatingFrame(floatingComposeInput{
		baseFrame: renderer.NewFrame(12, 7),
		floating: capturedFloatingRenderState{
			visible: true, focused: true, pane: capturedPaneRenderState{frame: renderer.NewFrame(4, 1)}, geometry: geometry, generation: 1,
		},
		content: content,
		cache: composeCacheInput{
			valid: true, floatingGeneration: 1, floatingGeometry: geometry.translate(content.X, content.Y),
		},
	})
	require.NotNil(t, frame)
	require.Equal(t, []renderer.Damage{renderer.FullRedraw()}, damage, "a focused border role change must redraw the popup")
}

func TestComposeCapturedFloatingFrameGeometryChangesInvalidateCache(t *testing.T) {
	initial := floatingGeometry{
		Bounds: domain.Rect{X: 4, Y: 2, Width: 8, Height: 5},
		Inner:  domain.Rect{X: 5, Y: 3, Width: 6, Height: 3},
	}
	tests := []struct {
		name string
		next floatingGeometry
	}{
		{
			name: "position change",
			next: floatingGeometry{Bounds: domain.Rect{X: 12, Y: 4, Width: 8, Height: 5}, Inner: domain.Rect{X: 13, Y: 5, Width: 6, Height: 3}},
		},
		{
			name: "bounds change with same inner size",
			next: floatingGeometry{Bounds: domain.Rect{X: 10, Y: 2, Width: 10, Height: 7}, Inner: domain.Rect{X: 12, Y: 4, Width: 6, Height: 3}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			base := renderer.NewFrame(30, 12)
			content := domain.Rect{Y: 1, Width: 30, Height: 10}
			pane := capturedPaneRenderState{frame: renderer.NewFrame(6, 3), titleGeneration: 1}
			initialInput := floatingComposeInput{
				baseFrame: base,
				floating:  capturedFloatingRenderState{visible: true, pane: pane, geometry: initial, generation: 1, titleGeneration: 1},
				content:   content,
				theme:     themeui.Theme{},
			}
			_, damage := composeCapturedFloatingFrame(initialInput)
			require.Equal(t, []renderer.Damage{renderer.FullRedraw()}, damage)

			initialInput.floating.geometry = tt.next
			initialInput.cache = composeCacheInput{
				valid:                   true,
				floatingGeneration:      1,
				floatingGeometry:        initial.translate(content.X, content.Y),
				floatingTitleGeneration: 1,
			}
			_, damage = composeCapturedFloatingFrame(initialInput)
			require.Equal(t, []renderer.Damage{renderer.FullRedraw()}, damage)
		})
	}
}

func TestResizeFloatingPaneCommitsSameSizeGeometryWithoutPTYResize(t *testing.T) {
	initial := floatingGeometry{
		Bounds: domain.Rect{X: 1, Y: 1, Width: 8, Height: 5},
		Inner:  domain.Rect{X: 2, Y: 2, Width: 6, Height: 3},
	}
	tests := []struct {
		name string
		next floatingGeometry
	}{
		{
			name: "position change",
			next: floatingGeometry{Bounds: domain.Rect{X: 9, Y: 4, Width: 8, Height: 5}, Inner: domain.Rect{X: 10, Y: 5, Width: 6, Height: 3}},
		},
		{
			name: "bounds change",
			next: floatingGeometry{Bounds: domain.Rect{X: 3, Y: 1, Width: 10, Height: 7}, Inner: domain.Rect{X: 5, Y: 3, Width: 6, Height: 3}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pty := &resizePTY{}
			p := newPane("floating", pty, rectSize(initial.Inner))
			p.rect, p.popupGeometry = initial.Inner, initial
			d := newTestDaemon(t, nil, stubClock{})

			require.True(t, applyFloatingResizePlanForTest(d, p, tt.next))
			require.Empty(t, pty.sizes(), "same-size geometry must not resize the PTY")
			require.Equal(t, tt.next.Inner, p.rect)
			require.Equal(t, tt.next, p.popupGeometry)
			require.Equal(t, 6, p.screen.Frame.Width)
			require.Equal(t, 3, p.screen.Frame.Height)
		})
	}
}

func TestComposeFloatingFrameRendersPaneOwnedCommandFallback(t *testing.T) {
	d := newTestDaemon(t, nil, stubClock{})
	d.shell = "/usr/bin/fish"
	p := newPane("floating", nil, domain.Size{Cols: 6, Rows: 3})
	p.title.displayFallback = floatingCommandFallback("btop --utf", d.shell)
	require.Equal(t, "btop", d.refreshPaneDisplayTitle(nil, p, true))

	base := renderer.NewFrame(40, 12)
	content := domain.Rect{Y: 1, Width: 40, Height: 10}
	cfg := domain.FloatingConfig{Width: 80, Height: 80}
	geometry := calculateContentFloatingGeometry(domain.Size{Cols: content.Width, Rows: content.Height}, cfg)
	captured := capturedPaneRenderState{frame: renderer.NewFrame(geometry.Inner.Width, geometry.Inner.Height), title: "btop", titleGeneration: 1}
	frame, _ := composeCapturedFloatingFrame(floatingComposeInput{
		baseFrame: base,
		floating: capturedFloatingRenderState{
			visible: true, pane: captured, geometry: geometry, title: captured.title, generation: 1, titleGeneration: captured.titleGeneration,
		},
		content: content,
		theme:   themeui.Theme{},
	})
	geometry = geometry.translate(content.X, content.Y)
	var gotTitle strings.Builder
	for x := geometry.Bounds.X + 2; x < geometry.Bounds.X+geometry.Bounds.Width-2; x++ {
		gotTitle.WriteRune(frame.At(x, geometry.Bounds.Y).Rune)
	}
	require.Equal(t, "btop", strings.TrimRight(gotTitle.String(), "─"))
}

func TestCaptureAndComposeFloatingFrameSynchronizesWithPTYReader(t *testing.T) {
	p := newPane("floating", nil, domain.Size{Cols: 80, Rows: 24})
	tb := newTab(nil, domain.Size{Cols: 80, Rows: 24})
	installTestFloating(tb, p, true)
	ac := &attachedClient{}
	ac.initOverlays()
	sess := &session{tabs: []*tab{tb}, client: ac}
	base := barState{}
	cfg := domain.FloatingConfig{Width: 100, Height: 100}

	start := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		<-start
		for range 500 {
			p.mu.Lock()
			p.screen.Write([]byte("\x1b[Hreader"))
			p.mu.Unlock()
		}
	}()
	go func() {
		defer wg.Done()
		<-start
		cache := composeCacheInput{}
		for range 500 {
			ac.sendMu.Lock()
			state, ok := capturePrimaryRenderState(sess, ac, primaryCaptureRequest{bars: base, floatingCfg: cfg})
			ac.sendMu.Unlock()
			require.True(t, ok)
			out := composeFrame(*state, cache)
			cache = out.cache
		}
	}()
	close(start)
	wg.Wait()
}

func TestDrawFloatingBorderOmitsTinyAxes(t *testing.T) {
	frame := renderer.NewFrame(2, 1)
	drawFloatingBorder(frame, domain.Rect{Width: 2, Height: 1}, "title", renderer.Style{})
	for _, cell := range frame.Cells {
		require.Equal(t, renderer.BlankCell().Rune, cell.Rune)
	}
}

func TestComposeCapturedFloatingFrameCachedAllocationsAreOnlyFrameClone(t *testing.T) {
	base := renderer.NewFrame(80, 24)
	content := domain.Rect{Y: 1, Width: 80, Height: 22}
	geometry := calculateContentFloatingGeometry(domain.Size{Cols: content.Width, Rows: content.Height}, domain.FloatingConfig{Width: 80, Height: 80})
	input := floatingComposeInput{
		baseFrame: base,
		floating: capturedFloatingRenderState{
			visible:  true,
			pane:     capturedPaneRenderState{frame: renderer.NewFrame(62, 18), title: "float", titleGeneration: 1},
			geometry: geometry, title: "float", generation: 1, titleGeneration: 1,
		},
		content: content,
		cache: composeCacheInput{
			valid: true, floatingGeneration: 1, floatingGeometry: geometry.translate(content.X, content.Y), floatingTitleGeneration: 1,
		},
	}

	allocs := testing.AllocsPerRun(100, func() { composeCapturedFloatingFrame(input) })
	// The production entry point deliberately clones Frame.Cells and its row
	// offsets to keep the cached base immutable; those are its two unavoidable
	// allocations. A higher count would reintroduce avoidable cache churn.
	require.LessOrEqual(t, allocs, float64(2))
}

var (
	benchmarkComposeSink composedRenderFrame
	benchmarkOutputSink  []byte
)

// benchmarkCapturedRenderState is a stable, post-capture production input: one
// 80x24 pane, both chrome bars, and no pending pane damage. The primed cache
// below makes each benchmark operation exercise the cached compose path.
func benchmarkCapturedRenderState() capturedRenderState {
	pane := renderer.NewFrame(80, 24)
	for y := range pane.Height {
		for x := range pane.Width {
			pane.Set(x, y, renderer.Cell{Rune: rune('a' + (x+y)%26)})
		}
	}
	terminalTheme := themeui.Theme{
		Foreground: renderer.RGB{R: 0xd8, G: 0xdc, B: 0xe8},
		Background: renderer.RGB{R: 0x08, G: 0x09, B: 0x0a},
		Palette: [16]renderer.RGB{
			2:  {R: 0x7d, G: 0xb5, B: 0xb5},
			4:  {R: 0x6c, G: 0x9b, B: 0xd9},
			10: {R: 0x7d, G: 0xb5, B: 0xb5},
			12: {R: 0x6c, G: 0x9b, B: 0xd9},
			14: {R: 0x7d, G: 0xb5, B: 0xb5},
		},
		PaletteKnown: 1<<2 | 1<<4 | 1<<10 | 1<<12 | 1<<14,
		HasFG:        true,
		HasBG:        true,
		TrueColor:    true,
		Known:        true,
		SchemeKnown:  true,
		UsePalette:   true,
	}
	placement := layout.Placement{ID: "benchmark-pane", Content: domain.Rect{Width: 80, Height: 24}}
	return capturedRenderState{
		layout: capturedTabLayout{
			area:        domain.Rect{Width: 80, Height: 24},
			focus:       placement.ID,
			placements:  []layout.Placement{placement},
			fingerprint: "benchmark-one-pane",
			valid:       true,
		},
		panes: []capturedPaneRenderState{{id: placement.ID, frame: pane, placement: placement, focused: true}},
		bars: barState{
			status:      statusSnapshot{session: "main", tabs: []statusTab{{name: "main", paneTitle: "shell", active: true}}},
			topRight:    "vev",
			bottomRight: "main",
			mru:         []recentSession{{name: "work"}, {name: "logs"}},
			theme:       terminalTheme,
		},
		theme: terminalTheme,
	}
}

func benchmarkComposeCacheInput(state capturedRenderState) composeCacheInput {
	priming := state
	priming.reset = true
	priming.panes = append([]capturedPaneRenderState(nil), state.panes...)
	priming.panes[0].damage = []renderer.Damage{renderer.FullRedraw()}
	return composeFrame(priming, composeCacheInput{}).cache
}

func BenchmarkComposeCapturedFrame(b *testing.B) {
	state := benchmarkCapturedRenderState()
	cache := benchmarkComposeCacheInput(state)
	scratch := composeCacheInput{}
	draw := renderer.New(renderer.Capabilities{})
	var totalBytes int64

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		composed := composeFrame(state, cache, scratch)
		scratch = composed.cache
		draw.Reset()
		out, err := draw.Draw(composed.frame, composed.damage)
		if err != nil {
			b.Fatal(err)
		}
		totalBytes += int64(len(out))
		benchmarkComposeSink, benchmarkOutputSink = composed, out
	}
	b.ReportMetric(float64(totalBytes)/float64(b.N), "output-bytes/op")
}

func BenchmarkComposeCapturedFloatingFrameCached(b *testing.B) {
	base := renderer.NewFrame(80, 24)
	content := domain.Rect{Y: 1, Width: 80, Height: 22}
	geometry := calculateContentFloatingGeometry(domain.Size{Cols: content.Width, Rows: content.Height}, domain.FloatingConfig{Width: 80, Height: 80})
	input := floatingComposeInput{
		baseFrame: base,
		floating: capturedFloatingRenderState{
			visible:         true,
			pane:            capturedPaneRenderState{frame: renderer.NewFrame(62, 18), title: "float", titleGeneration: 1},
			geometry:        geometry,
			title:           "float",
			generation:      1,
			titleGeneration: 1,
		},
		content: content,
		theme:   themeui.Theme{},
		cache: composeCacheInput{
			valid:                   true,
			floatingGeneration:      1,
			floatingGeometry:        geometry.translate(content.X, content.Y),
			floatingTitleGeneration: 1,
		},
	}

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		composeCapturedFloatingFrame(input)
	}
}

func TestToggleFloatingResizesHiddenPaneOnShowAndRetriesFailure(t *testing.T) {
	d := newTestDaemon(t, nil, stubClock{})
	d.ApplyConfig(domain.Config{Floating: domain.FloatingConfig{Width: 50, Height: 50}})
	tb := newTab(nil, domain.Size{Cols: 80, Rows: 24})
	sess := &session{tabs: []*tab{tb}}

	initialGeometry := calculateContentFloatingGeometry(domain.Size{Cols: 80, Rows: 24}, d.currentFloatingConfig())
	currentGeometry := calculateContentFloatingGeometry(domain.Size{Cols: 100, Rows: 40}, d.currentFloatingConfig())
	initial := initialGeometry.Inner
	current := currentGeometry.Inner
	pty := &resizePTY{errs: []error{errors.New("first show fails"), nil}}
	floating := newPane("floating", pty, rectSize(initial))
	floating.rect = initial
	floating.popupGeometry = initialGeometry

	// Prewarming installs a hidden pane at the initial geometry.
	tb.mu.Lock()
	generation := tb.beginFloatingWarmLocked(false)
	require.True(t, tb.installFloatingLocked(floating, generation))
	require.Equal(t, floatingHidden, tb.floating.state)
	require.False(t, tb.floating.desiredVisible)
	tb.mu.Unlock()
	// A client resize changes tab geometry but must leave the hidden PTY alone.
	d.resize(sess, nil, domain.Size{Cols: 100, Rows: 42})
	require.Empty(t, pty.sizes())

	// Showing attempts the current size before paint. A failed resize retains
	// the old screen, but commits the new popup rect so capture clips/pads it.
	require.NoError(t, d.toggleFloating(sess, nil))
	require.Equal(t, []domain.Size{rectSize(current)}, pty.sizes())
	require.Equal(t, current, floating.rect)
	require.Equal(t, initial.Width, floating.screen.Frame.Width)
	require.Equal(t, initial.Height, floating.screen.Frame.Height)
	require.Equal(t, currentGeometry, floating.popupGeometry)

	require.NoError(t, d.toggleFloating(sess, nil)) // hide
	require.NoError(t, d.toggleFloating(sess, nil)) // retry show
	require.Equal(t, []domain.Size{rectSize(current), rectSize(current)}, pty.sizes())
	require.Equal(t, current, floating.rect)
	require.Equal(t, current.Width, floating.screen.Frame.Width)
	require.Equal(t, current.Height, floating.screen.Frame.Height)
	require.Equal(t, currentGeometry, floating.popupGeometry)
}

func TestFailedFloatingResizeKeepsCommittedRenderAndInputGeometry(t *testing.T) {
	cfg := domain.FloatingConfig{Width: 50, Height: 50}
	oldContent := domain.Rect{Width: 80, Height: 24}
	newContent := domain.Rect{Y: 1, Width: 100, Height: 40}
	oldGeometry := calculateContentFloatingGeometry(domain.Size{Cols: oldContent.Width, Rows: oldContent.Height}, cfg)
	newGeometry := calculateContentFloatingGeometry(domain.Size{Cols: newContent.Width, Rows: newContent.Height}, cfg)
	pty := &resizePTY{err: errors.New("resize failed")}
	p := newPane("floating", pty, rectSize(oldGeometry.Inner))
	p.rect = oldGeometry.Inner
	p.popupGeometry = oldGeometry
	tb := newTab(nil, domain.Size{Cols: newContent.Width, Rows: newContent.Height})
	installTestFloating(tb, p, true)
	d := newTestDaemon(t, nil, stubClock{})
	d.ApplyConfig(domain.Config{Floating: cfg})

	require.False(t, applyFloatingResizePlanForTest(d, p, newGeometry))
	tb.mu.Lock()
	_, inputGeometry, visible := tb.visibleFloatingSnapshotLocked(cfg)
	tb.mu.Unlock()
	require.True(t, visible)
	require.Equal(t, oldGeometry, inputGeometry)

	p.mu.Lock()
	captured := capturePaneRenderStateLocked(p, oldGeometry.Inner)
	p.mu.Unlock()
	base := renderer.NewFrame(newContent.Width, newContent.Height+2)
	frame, _ := composeCapturedFloatingFrame(floatingComposeInput{
		baseFrame: base,
		floating: capturedFloatingRenderState{
			visible:         true,
			pane:            captured,
			geometry:        oldGeometry,
			title:           captured.title,
			generation:      1,
			titleGeneration: captured.titleGeneration,
		},
		content: newContent,
		theme:   themeui.Theme{},
		full:    true,
	})
	oldFrameGeometry := oldGeometry.translate(newContent.X, newContent.Y)
	newFrameGeometry := newGeometry.translate(newContent.X, newContent.Y)
	require.Equal(t, '┌', frame.At(oldFrameGeometry.Bounds.X, oldFrameGeometry.Bounds.Y).Rune)
	if newFrameGeometry.Bounds.X != oldFrameGeometry.Bounds.X || newFrameGeometry.Bounds.Y != oldFrameGeometry.Bounds.Y {
		require.NotEqual(t, '┌', frame.At(newFrameGeometry.Bounds.X, newFrameGeometry.Bounds.Y).Rune)
	}
}

func TestResizeFloatingPaneFailureAndSerialization(t *testing.T) {
	t.Run("failure preserves state", func(t *testing.T) {
		pty := &resizePTY{err: errors.New("nope")}
		p := newPane("floating", pty, domain.Size{Cols: 5, Rows: 4})
		p.rect = domain.Rect{X: 8, Y: 3, Width: 5, Height: 4}
		d := newTestDaemon(t, nil, stubClock{})
		requested := floatingGeometry{Bounds: domain.Rect{X: 1, Y: 1, Width: 11, Height: 9}, Inner: domain.Rect{X: 2, Y: 2, Width: 9, Height: 7}}
		require.False(t, applyFloatingResizePlanForTest(d, p, requested))
		require.Equal(t, domain.Rect{X: 8, Y: 3, Width: 5, Height: 4}, p.rect)
		require.Equal(t, floatingGeometry{}, p.popupGeometry)
		require.Equal(t, 5, p.screen.Frame.Width)
		require.Equal(t, 4, p.screen.Frame.Height)
	})

	t.Run("competing resizes serialize", func(t *testing.T) {
		pty := &resizePTY{entered: make(chan struct{}), release: make(chan struct{})}
		p := newPane("floating", pty, domain.Size{Cols: 2, Rows: 2})
		d := newTestDaemon(t, nil, stubClock{})
		first := floatingGeometry{Bounds: domain.Rect{Width: 6, Height: 5}, Inner: domain.Rect{X: 1, Y: 1, Width: 4, Height: 3}}
		second := floatingGeometry{Bounds: domain.Rect{Width: 10, Height: 8}, Inner: domain.Rect{X: 1, Y: 1, Width: 8, Height: 6}}
		done1 := make(chan bool, 1)
		done2 := make(chan bool, 1)
		secondStarted := make(chan struct{})
		go func() { done1 <- applyFloatingResizePlanForTest(d, p, first) }()
		<-pty.entered
		go func() {
			close(secondStarted)
			done2 <- applyFloatingResizePlanForTest(d, p, second)
		}()
		<-secondStarted
		close(pty.release)
		require.True(t, <-done1)
		require.True(t, <-done2)
		require.Equal(t, second.Inner, p.rect)
		require.Equal(t, second, p.popupGeometry)
		require.Equal(t, 1, pty.maxConcurrentCalls())
		require.Equal(t, []domain.Size{{Cols: 4, Rows: 3}, {Cols: 8, Rows: 6}}, pty.sizes())
	})
}

type resizePTY struct {
	mu            sync.Mutex
	resizes       []domain.Size
	err           error
	errs          []error
	entered       chan struct{}
	release       chan struct{}
	activeCalls   int
	maxConcurrent int
}

func (p *resizePTY) Resize(sz domain.Size) error {
	p.mu.Lock()
	p.resizes = append(p.resizes, sz)
	p.activeCalls++
	p.maxConcurrent = max(p.maxConcurrent, p.activeCalls)
	n := len(p.resizes)
	err := p.err
	if n <= len(p.errs) {
		err = p.errs[n-1]
	}
	p.mu.Unlock()
	if n == 1 && p.entered != nil {
		close(p.entered)
		<-p.release
	}
	p.mu.Lock()
	p.activeCalls--
	p.mu.Unlock()
	return err
}
func (p *resizePTY) maxConcurrentCalls() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.maxConcurrent
}
func (p *resizePTY) sizes() []domain.Size {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]domain.Size(nil), p.resizes...)
}
func (*resizePTY) Read([]byte) (int, error)     { return 0, io.EOF }
func (*resizePTY) Write(b []byte) (int, error)  { return len(b), nil }
func (*resizePTY) Close() error                 { return nil }
func (*resizePTY) Pid() int                     { return 0 }
func (*resizePTY) ForegroundPgid() (int, error) { return 0, nil }
