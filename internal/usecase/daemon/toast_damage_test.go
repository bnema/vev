package daemon

import (
	"testing"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/usecase/layout"
	"github.com/bnema/vev/pkg/renderer"
	"github.com/bnema/vev/pkg/vt"
	"github.com/stretchr/testify/require"
)

func TestComposeFrameToastDamageIsIncrementalAndRestoresOldFootprints(t *testing.T) {
	baseState := toastDamageState(nil, nil, true)
	base := composeFrame(baseState, composeCacheInput{})
	stream := newOutputStateStream()
	terminal := vt.NewScreen(base.frame.Width, base.frame.Height)
	replayToastFrame(t, stream, terminal, base)

	one := domain.Notification{Code: domain.NoticeClipboard, Severity: domain.NoticeInfo, Message: "copied", Count: 1}
	shownState := toastDamageState([]domain.Notification{one}, nil, false)
	shown := composeFrame(shownState, base.cache)
	requireNoFullRedraw(t, shown.damage)
	requireToastFootprintsDamaged(t, shown.damage, shown.cache.toastFootprints)
	replayToastFrame(t, stream, terminal, shown)

	// A one-cell PTY update while the toast is stable redraws only the pane cell
	// and toast footprint, never the whole terminal.
	updatedState := toastDamageState([]domain.Notification{one}, []renderer.Damage{{Kind: renderer.DamageText, X: 0, Y: 0, Width: 1, Height: 1}}, false)
	updatedState.panes[0].frame.Set(0, 0, renderer.Cell{Rune: 'X', Style: renderer.DefaultStyle()})
	updated := composeFrame(updatedState, shown.cache)
	requireNoFullRedraw(t, updated.damage)
	require.Contains(t, updated.damage, renderer.Damage{Kind: renderer.DamageText, X: 0, Y: 1, Width: 1, Height: 1})
	requireToastFootprintsDamaged(t, updated.damage, shown.cache.toastFootprints)
	requireToastFootprintsDamaged(t, updated.damage, updated.cache.toastFootprints)
	incremental, err := stream.render(updated.frame, updated.damage, updated.reset)
	require.NoError(t, err)
	fullRenderer := renderer.New(renderer.Capabilities{})
	full, err := fullRenderer.Draw(updated.frame, []renderer.Damage{renderer.FullRedraw()})
	require.NoError(t, err)
	require.Less(t, len(incremental)*2, len(full), "one-cell update with a stable toast must stay bounded below full-screen output")
	terminal.Write(incremental)
	require.Equal(t, frameRows(updated.frame), frameRows(terminal.Frame))

	// Stacking and overflow stay visually identical while their complete old and
	// new coverage remains incremental damage.
	two := domain.Notification{Code: domain.NoticePaneSpawn, Severity: domain.NoticeWarn, Message: "second", Count: 1}
	three := domain.Notification{Code: domain.NoticeConfigReload, Severity: domain.NoticeError, Message: "third", Count: 1}
	stackedState := toastDamageState([]domain.Notification{two, one, three}, nil, false)
	stackedState.overlays.noticeOverflow = 2
	stacked := composeFrame(stackedState, updated.cache)
	requireNoFullRedraw(t, stacked.damage)
	requireToastFootprintsDamaged(t, stacked.damage, updated.cache.toastFootprints)
	requireToastFootprintsDamaged(t, stacked.damage, stacked.cache.toastFootprints)
	replayToastFrame(t, stream, terminal, stacked)

	// Coalescing changes the title count in place. Both old and new coverage is
	// damaged even where their geometry happens to be identical.
	coalescedTwo := two
	coalescedTwo.Count = 12
	coalescedState := toastDamageState([]domain.Notification{coalescedTwo, one, three}, nil, false)
	coalescedState.overlays.noticeOverflow = 2
	coalesced := composeFrame(coalescedState, stacked.cache)
	requireNoFullRedraw(t, coalesced.damage)
	requireToastFootprintsDamaged(t, coalesced.damage, stacked.cache.toastFootprints)
	requireToastFootprintsDamaged(t, coalesced.damage, coalesced.cache.toastFootprints)
	replayToastFrame(t, stream, terminal, coalesced)

	// Dismissal and expiry have the same rendering transition: no current toast
	// exists, so the previous footprint alone must restore every exposed cell.
	for _, tt := range []struct {
		name string
		from composedRenderFrame
	}{
		{name: "dismiss", from: coalesced},
		{name: "expiry", from: shown},
	} {
		t.Run(tt.name, func(t *testing.T) {
			cleared := composeFrame(toastDamageState(nil, nil, false), tt.from.cache)
			requireNoFullRedraw(t, cleared.damage)
			requireToastFootprintsDamaged(t, cleared.damage, tt.from.cache.toastFootprints)
			require.Empty(t, cleared.cache.toastFootprints)
		})
	}

	cleared := composeFrame(toastDamageState(nil, nil, false), coalesced.cache)
	replayToastFrame(t, stream, terminal, cleared)
	require.Equal(t, frameRows(coalesced.cache.frame), frameRows(cleared.frame), "dismissal must restore the toast-free base without artifacts")
}

func BenchmarkComposeFrameStableToastOneCellUpdate(b *testing.B) {
	base := composeFrame(toastDamageState(nil, nil, true), composeCacheInput{})
	notice := domain.Notification{Code: domain.NoticeClipboard, Severity: domain.NoticeInfo, Message: "copied", Count: 1}
	shown := composeFrame(toastDamageState([]domain.Notification{notice}, nil, false), base.cache, composeCacheInput{})
	cache, scratch := shown.cache, base.cache
	state := toastDamageState([]domain.Notification{notice}, []renderer.Damage{{Kind: renderer.DamageText, X: 0, Y: 0, Width: 1, Height: 1}}, false)

	b.ReportAllocs()
	for i := 0; b.Loop(); i++ {
		cell := renderer.Cell{Rune: rune('A' + i%26), Style: renderer.DefaultStyle()}
		state.panes[0].frame.Set(0, 0, cell)
		out := composeFrame(state, cache, scratch)
		if hasFullRedraw(out.damage) {
			b.Fatal("stable toast forced a full redraw")
		}
		scratch, cache = cache, out.cache
	}
}

func toastDamageState(notices []domain.Notification, damage []renderer.Damage, reset bool) capturedRenderState {
	paneFrame := renderer.NewFrame(80, 20)
	for y := 0; y < paneFrame.Height; y++ {
		for x := 0; x < paneFrame.Width; x++ {
			paneFrame.Set(x, y, renderer.Cell{Rune: 'a', Style: renderer.DefaultStyle()})
		}
	}
	placement := layout.Placement{ID: "pane", Content: domain.Rect{Width: 80, Height: 20}}
	return capturedRenderState{
		reset:    reset,
		layout:   capturedTabLayout{area: domain.Rect{Width: 80, Height: 20}, focus: "pane", placements: []layout.Placement{placement}, fingerprint: "toast-damage", valid: true},
		panes:    []capturedPaneRenderState{{id: "pane", frame: paneFrame, placement: placement, focused: true, damage: damage}},
		overlays: capturedOverlayRenderState{notices: notices},
		styles:   resolveStyles(nil),
	}
}

func replayToastFrame(t *testing.T, stream *outputStateStream, terminal *vt.Screen, composed composedRenderFrame) {
	t.Helper()
	data, err := stream.render(composed.frame, composed.damage, composed.reset)
	require.NoError(t, err)
	terminal.Write(data)
	require.Equal(t, frameRows(composed.frame), frameRows(terminal.Frame))
}

func requireNoFullRedraw(t *testing.T, damage []renderer.Damage) {
	t.Helper()
	for _, d := range damage {
		require.NotEqual(t, renderer.DamageFullRedraw, d.Kind)
	}
}

func requireToastFootprintsDamaged(t *testing.T, damage []renderer.Damage, footprints []domain.Rect) {
	t.Helper()
	for _, footprint := range footprints {
		require.Contains(t, damage, renderer.Damage{Kind: renderer.DamageText, X: footprint.X, Y: footprint.Y, Width: footprint.Width, Height: footprint.Height})
	}
}

func hasFullRedraw(damage []renderer.Damage) bool {
	for _, d := range damage {
		if d.Kind == renderer.DamageFullRedraw {
			return true
		}
	}
	return false
}
