package daemon

import (
	"testing"

	renderer "github.com/bnema/vev-vt"
	"github.com/bnema/vev/internal/domain"
	"github.com/stretchr/testify/require"
)

// framesShareStorage reports whether writing through a is visible through b.
func framesShareStorage(a, b renderer.Frame) bool {
	if a.Width <= 0 || a.Height <= 0 || b.Width != a.Width || b.Height != a.Height {
		return false
	}
	orig := a.Cell(0, 0)
	sentinel := renderer.Cell{Rune: '§', Style: renderer.DefaultStyle()}
	if orig.Equal(sentinel) {
		sentinel.Rune = '¶'
	}
	a.Set(0, 0, sentinel)
	shared := b.Cell(0, 0).Equal(sentinel)
	a.Set(0, 0, orig)
	return shared
}

func transientToastFloatingState(withToast, withFloating bool, seed rune) capturedRenderState {
	var notices []domain.Notification
	if withToast {
		notices = []domain.Notification{{Code: domain.NoticeClipboard, Severity: domain.NoticeInfo, Message: "copied", Count: 1}}
	}
	state := toastDamageState(notices, []renderer.Damage{{Kind: renderer.DamageText, X: 0, Y: 0, Width: 1, Height: 1}}, false)
	state.panes[0].frame.Set(0, 0, renderer.Cell{Rune: seed, Style: renderer.DefaultStyle()})
	if withFloating {
		content := domain.Rect{Y: 1, Width: 80, Height: 20}
		geometry := calculateContentFloatingGeometry(domain.Size{Cols: content.Width, Rows: content.Height}, domain.FloatingConfig{Width: 60, Height: 60})
		state.floating = capturedFloatingRenderState{
			visible:    true,
			pane:       capturedPaneRenderState{frame: renderer.NewFrame(geometry.Inner.Width, geometry.Inner.Height), title: "float"},
			geometry:   geometry,
			title:      "float",
			generation: 1,
		}
	}
	return state
}

// TestComposeFrameTransientScratchNeverAliasesCaches replays the attachment
// commit/spare rotation across toast, floating, and plain frames. Each result
// must match an independent composition without scratch, the retained cache
// must stay toast-free, and no handed-out frame may share a page with either
// cache slot (which would let a later paint corrupt committed state).
func TestComposeFrameTransientScratchNeverAliasesCaches(t *testing.T) {
	type step struct {
		toast, floating bool
	}
	steps := []step{
		{false, false}, {true, false}, {true, false}, {true, true}, {false, true},
		{true, true}, {true, false}, {false, false}, {true, false}, {false, true}, {true, true},
	}
	var render attachmentRenderState
	for i, st := range steps {
		seed := rune('A' + i)
		state := transientToastFloatingState(st.toast, st.floating, seed)
		state.reset = i == 0
		// A frame without scratch is the reference: it only ever clones.
		want := composeFrame(state, render.cache)
		got := composeFrame(state, render.cache, render.spare)

		require.Equal(t, frameRows(want.frame), frameRows(got.frame), "step %d frame", i)
		require.Equal(t, frameRows(want.cache.frame), frameRows(got.cache.frame), "step %d cache", i)
		require.Equal(t, want.damage, got.damage, "step %d damage", i)

		// The result is built in the spare page, so it may share that one, but it
		// must never touch the committed cache a failed publication falls back to.
		require.False(t, framesShareStorage(got.frame, render.cache.frame), "step %d: composed frame aliases the committed cache", i)
		require.False(t, framesShareStorage(got.cache.frame, render.cache.frame), "step %d: new cache base aliases the committed cache", i)
		if st.toast || st.floating {
			// A decorated frame lives on a transient page: never the cache base
			// (toast-free by contract) nor the committed or spare cache pages.
			require.False(t, framesShareStorage(got.frame, got.cache.frame), "step %d: decorated frame aliases the cache base", i)
			require.False(t, framesShareStorage(got.frame, render.spare.frame), "step %d: decorated frame aliases the spare cache", i)
		}
		if !st.toast {
			require.NotContains(t, frameText(got.cache.frame), "copied")
		}
		require.NotContains(t, frameText(got.cache.frame), "copied", "step %d: cache must stay toast-free", i)

		beforeCommitted := frameRows(render.cache.frame)
		render.commitComposition(got.cache)
		if i > 0 {
			require.Equal(t, beforeCommitted, frameRows(render.spare.frame))
		}
		// The two slots' transient pages must be distinct from each other, from
		// both cache frames, and from every frame the next composition reads.
		pages := []renderer.Frame{render.cache.transient.overlay, render.cache.transient.popup, render.spare.transient.overlay, render.spare.transient.popup}
		for a := range pages {
			for b := a + 1; b < len(pages); b++ {
				require.False(t, framesShareStorage(pages[a], pages[b]), "step %d: transient pages %d and %d alias", i, a, b)
			}
			require.False(t, framesShareStorage(pages[a], render.cache.frame), "step %d: transient page %d aliases cache", i, a)
			require.False(t, framesShareStorage(pages[a], render.spare.frame), "step %d: transient page %d aliases spare", i, a)
		}
	}
}

// TestComposeFrameStableToastReusesScratchPages pins the saving: with a primed
// scratch, a stable toast frame no longer clones the base page.
func TestComposeFrameStableToastReusesScratchPages(t *testing.T) {
	base := composeFrame(toastDamageState(nil, nil, true), composeCacheInput{})
	notice := []domain.Notification{{Code: domain.NoticeClipboard, Severity: domain.NoticeInfo, Message: "copied", Count: 1}}
	shown := composeFrame(toastDamageState(notice, nil, false), base.cache, composeCacheInput{})
	state := toastDamageState(notice, []renderer.Damage{{Kind: renderer.DamageText, X: 0, Y: 0, Width: 1, Height: 1}}, false)

	cache, scratch := shown.cache, base.cache
	// Prime both rotation slots so every page has reached its steady size.
	for range 3 {
		out := composeFrame(state, cache, scratch)
		scratch, cache = cache, out.cache
	}
	reused := testing.AllocsPerRun(50, func() {
		out := composeFrame(state, cache, scratch)
		scratch, cache = cache, out.cache
	})
	cloned := testing.AllocsPerRun(50, func() { benchmarkComposeSink.frame = cache.frame.Clone() })
	require.Less(t, reused, 30.0, "per-frame compose allocations")
	t.Logf("compose allocs=%v, one Clone allocs=%v", reused, cloned)
	out := composeFrame(state, cache, scratch)
	require.False(t, framesShareStorage(out.frame, out.cache.frame))
	require.Contains(t, frameText(out.frame), "copied")
	require.NotContains(t, frameText(out.cache.frame), "copied")
}
