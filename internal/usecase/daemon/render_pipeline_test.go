package daemon

import (
	"testing"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/pkg/renderer"
	"github.com/stretchr/testify/require"
)

func TestCapturePaneRenderStateOwnsVisibleFrameAndConsumesDamage(t *testing.T) {
	p := newPane("p", nil, domain.Size{Cols: 8, Rows: 2})
	p.screen.ClearDamage()
	p.screen.Write([]byte("old"))

	p.mu.Lock()
	captured := capturePaneRenderStateLocked(p, domain.Rect{Width: 3, Height: 1}, damageCaptureConsume)
	p.mu.Unlock()

	require.Equal(t, 3, captured.frame.Width)
	require.Equal(t, 1, captured.frame.Height)
	require.Empty(t, p.screen.Damage())
	p.mu.Lock()
	p.screen.Write([]byte("new"))
	p.mu.Unlock()
	require.Equal(t, 'o', captured.frame.At(0, 0).Rune, "capture must not alias the mutable VT frame")
}

func TestCapturePaneRenderStatePreviewIsNonDestructive(t *testing.T) {
	p := newPane("p", nil, domain.Size{Cols: 8, Rows: 2})
	p.screen.ClearDamage()
	p.screen.Write([]byte("preview"))

	p.mu.Lock()
	_ = capturePaneRenderStateLocked(p, domain.Rect{Width: 8, Height: 2}, damageCapturePreview)
	p.mu.Unlock()

	require.NotEmpty(t, p.screen.Damage())
}

func TestCapturePaneRenderStateMalformedDamageFallsBackToFullRedraw(t *testing.T) {
	p := newPane("p", nil, domain.Size{Cols: 8, Rows: 2})
	p.screen.ClearDamage()
	p.screen.Write([]byte("x"))
	p.screen.Damage()[0] = renderer.Damage{Kind: renderer.DamageText, X: -1, Y: 0, Width: 4, Height: 1}

	p.mu.Lock()
	captured := capturePaneRenderStateLocked(p, domain.Rect{Width: 8, Height: 2}, damageCaptureConsume)
	p.mu.Unlock()

	require.Equal(t, []renderer.Damage{renderer.FullRedraw()}, captured.damage)
}

func TestCapturePaneRenderStateCollapsedConsumesBoundedly(t *testing.T) {
	p := newPane("p", nil, domain.Size{Cols: 8, Rows: 2})
	p.screen.ClearDamage()
	p.screen.Write([]byte("hidden"))

	p.mu.Lock()
	captured := capturePaneRenderStateLocked(p, domain.Rect{}, damageCaptureConsume)
	p.mu.Unlock()

	require.Zero(t, captured.frame.Width)
	require.Zero(t, captured.frame.Height)
	require.Empty(t, p.screen.Damage(), "capture owner cleans pending hidden-pane damage")
}

func TestPaintACKBlockedDoesNotDestructivelyCapture(t *testing.T) {
	pty, release := newBlockingPTY(t)
	t.Cleanup(release)
	d, sess, _, sends := newManualSessionWithPTYs(t, pty)
	ac := sess.client
	ac.sendMu.Lock()
	ac.output.next = ac.output.maxOutstanding
	ac.output.acked = 0
	ac.sendMu.Unlock()
	p := sess.tabs[0].focusedPane()
	p.mu.Lock()
	p.screen.ClearDamage()
	p.screen.Write([]byte("blocked"))
	p.mu.Unlock()

	d.paint(sess, ac, false)

	p.mu.Lock()
	require.NotEmpty(t, p.screen.Damage())
	p.mu.Unlock()
	select {
	case frame := <-sends:
		t.Fatalf("ACK-blocked paint sent %#v", frame)
	default:
	}
}
