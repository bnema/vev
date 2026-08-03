package daemon

import (
	"testing"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/usecase/layout"
	"github.com/bnema/vev/internal/usecase/picker"
	"github.com/bnema/vev/pkg/renderer"
	"github.com/stretchr/testify/require"
)

func TestPrimaryCaptureAloneRecordsDamageReceipts(t *testing.T) {
	_, sess, ac, _ := newManualSessionWithPTYs(t, nil)
	p := sess.tabs[0].focusedPane()
	p.mu.Lock()
	p.screen.ClearDamage()
	p.screen.Write([]byte("preview-safe"))
	p.mu.Unlock()

	preview := snapshotPickerPreview(sess.tabs[0])
	require.NotEmpty(t, preview.Rows)
	require.Empty(t, ac.renderScratch.receipts, "picker preview must not create primary damage receipts")
	p.mu.Lock()
	require.NotEmpty(t, p.screen.Damage(), "picker preview must not acknowledge VT damage")
	p.mu.Unlock()

	ac.sendMu.Lock()
	state, ok := capturePrimaryRenderState(sess, ac, primaryCaptureRequest{
		bars:        barState{},
		overlays:    capturedOverlayRenderState{},
		preview:     pickerPreviewEmpty(),
		floatingCfg: domain.FloatingConfig{},
		reset:       false,
		lease:       nil,
	})
	ac.sendMu.Unlock()
	require.True(t, ok)
	require.Len(t, state.receipts, 1)
	require.Same(t, p, state.receipts[0].pane, "the primary transaction owns the pane receipt")
	p.mu.Lock()
	require.NotEmpty(t, p.screen.Damage(), "primary capture remains non-destructive until emission commits")
	p.mu.Unlock()
}

// These are deliberately end-to-end pipeline tests: the pending damage comes
// from a real pane VT Write, not a hand-built FullRedraw.
func TestCaptureComposeEmitAcknowledgesCollapsedPaneDamageOnlyAfterEmission(t *testing.T) {
	d, sess, ac, sends := newManualSessionWithPTYs(t, nil)
	tb := sess.tabs[0]
	visible := tb.focusedPane()
	collapsed := newPane("collapsed", nil, domain.Size{Cols: 80, Rows: 23})
	tb.panes[collapsed.id] = collapsed
	tb.tree = &layout.Tree{
		Root:  &layout.Node{Kind: layout.Stack, Children: []*layout.Node{layout.NewLeaf(visible.id), layout.NewLeaf(collapsed.id)}, Expanded: visible.id},
		Focus: visible.id,
	}
	visible.screen.ClearDamage()
	collapsed.screen.ClearDamage()
	collapsed.screen.Write([]byte("hidden damage"))

	ac.sendMu.Lock()
	state, ok := capturePrimaryRenderState(sess, ac, primaryCaptureRequest{bars: barState{}})
	require.True(t, ok)
	collapsedReceipt := false
	for _, receipt := range state.receipts {
		collapsedReceipt = collapsedReceipt || receipt.pane == collapsed
	}
	require.True(t, collapsedReceipt, "capture must retain a receipt for the collapsed pane")
	require.NotEmpty(t, collapsed.screen.Damage(), "capture alone must not acknowledge collapsed-pane damage")
	composed := composeFrame(*state, ac.pipelineCache, ac.pipelineScratch)
	require.True(t, d.emitFrame(sess, ac, state, composed))

	<-sends
	require.Empty(t, collapsed.screen.Damage(), "successful emission must acknowledge the collapsed-pane receipt")
}

func TestRenderDamageReceiptRetainsRealVTDamageAcrossFailedEmission(t *testing.T) {
	for _, tt := range []struct {
		name    string
		fail    func(*composedRenderFrame, *attachedClient)
		restore func(*session, *attachedClient, ports.Transport)
	}{
		{
			name: "prepare failure",
			fail: func(c *composedRenderFrame, _ *attachedClient) {
				// An invalid frame makes the real output.prepare reject after capture.
				c.frame = renderer.Frame{Width: 1}
			},
			restore: func(_ *session, _ *attachedClient, _ ports.Transport) {},
		},
		{
			name: "send failure",
			fail: func(_ *composedRenderFrame, ac *attachedClient) { ac.replaceTransport(cacheFailTransport{}) },
			restore: func(sess *session, ac *attachedClient, healthy ports.Transport) {
				sess.mu.Lock()
				sess.registerAttachmentLocked(ac)
				sess.mu.Unlock()
				ac.setSession(sess)
				ac.replaceTransport(healthy)
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			d, sess, ac, sends := newManualSessionWithPTYs(t, nil)
			// Prepare failures now schedule recovery through the coordinator;
			// retain the failed transaction here until this test explicitly retries.
			d.clock = newNoticeClock()
			d.attachCoordinator(sess, nil, ac, true)
			healthy := ac.transport()
			p := sess.tabs[0].focusedPane()

			// Prime both the renderer shadow and the committed capture cache.
			d.paint(sess, ac, true, nil)
			<-sends
			p.mu.Lock()
			p.screen.Write([]byte("\x1b[1;1Hchanged"))
			p.mu.Unlock()

			state, composed := captureComposeForReceiptTest(t, sess, ac)
			tt.fail(&composed, ac)
			require.True(t, d.emitFrame(sess, ac, state, composed))
			p.mu.Lock()
			require.NotEmpty(t, p.screen.Damage(), "failed emission must retain real VT damage")
			p.mu.Unlock()

			tt.restore(sess, ac, healthy)
			state, composed = captureComposeForReceiptTest(t, sess, ac)
			require.True(t, d.emitFrame(sess, ac, state, composed))
			frame := <-sends
			out, err := ports.UnmarshalOutput(frame.Payload)
			require.NoError(t, err)
			require.Contains(t, string(out.Data), "changed", "retry must emit the retained terminal bytes")
			p.mu.Lock()
			require.Empty(t, p.screen.Damage(), "only the successful retry commits its receipt")
			require.Equal(t, 'c', p.screen.Frame.At(0, 0).Rune, "retry preserves the changed VT shadow")
			p.mu.Unlock()
		})
	}
}

func TestPrepareFailureNotifiesAndSchedulesRecovery(t *testing.T) {
	clock := newNoticeClock()
	d, sess, ac, _ := newNoticeFixture(t, clock)
	rc := d.attachCoordinator(sess, nil, ac, true)
	var producers []string
	rc.opts.onInvalidate = func(inv renderInvalidation) {
		// Both the notice repaint and the explicit recovery must re-enter only
		// after emitFrame releases its attachment locks.
		require.True(t, ac.sendMu.TryLock(), "invalidation must not run under sendMu")
		ac.sendMu.Unlock()
		producers = append(producers, inv.producer)
	}
	state, composed := captureComposeForReceiptTest(t, sess, ac)
	// An invalid frame makes output.prepare fail after capture. emitFrame owns
	// sendMu on entry and releases it before reporting and rescheduling.
	composed.frame = renderer.Frame{Width: 1}

	require.True(t, d.emitFrame(sess, ac, state, composed))

	history := d.notices.history()
	require.Len(t, history, 1)
	require.Equal(t, domain.NoticeInternal, history[0].Code)
	require.Equal(t, "display update failed", history[0].Message)
	toasts, _ := visibleToasts(ac)
	require.Len(t, toasts, 1)
	require.Equal(t, "display update failed", toasts[0].Message)

	rc.mu.Lock()
	pending, reset := rc.pending, rc.pendingReset
	rc.mu.Unlock()
	require.True(t, pending, "prepare failure must schedule a recovery render")
	require.True(t, reset, "recovery must invalidate the full render state")
	require.Contains(t, producers, "render_pipeline.go:prepare-failed", "prepare failure must explicitly schedule recovery")
	require.Contains(t, clock.durations(), urgentRenderDeadline, "notice/recovery invalidation must arm the coordinator")
}

func TestPrepareFailureFallbackOnlySuppressesRecursiveNoticePaint(t *testing.T) {
	d, sess, ac, sends := newNoticeFixture(t, newNoticeClock())

	// A failure while reportError is repainting its own toast is intentionally
	// ignored. It must not start another notification/repaint cycle.
	ac.prepareFailureFallback.Store(true)
	state, composed := captureComposeForReceiptTest(t, sess, ac)
	composed.frame = renderer.Frame{Width: 1}
	require.True(t, d.emitFrame(sess, ac, state, composed))
	require.Empty(t, d.notices.history())
	ac.prepareFailureFallback.Store(false)

	// Later outer failures each notify and repaint after sendMu has been
	// released; neither may be hidden by the prior recursive guard.
	for want := 1; want <= 2; want++ {
		state, composed = captureComposeForReceiptTest(t, sess, ac)
		composed.frame = renderer.Frame{Width: 1}
		require.True(t, d.emitFrame(sess, ac, state, composed))
		require.Len(t, d.notices.history(), want)
		frame := <-sends
		require.Equal(t, ports.MsgOutput, frame.Type, "outer failure %d must repaint its notice", want)
		require.False(t, ac.prepareFailureFallback.Load())
	}
}

func TestRenderDamageReceiptStaleGenerationForcesFullRedraw(t *testing.T) {
	d, sess, ac, sends := newManualSessionWithPTYs(t, nil)
	d.paint(sess, ac, true, nil)
	<-sends
	p := sess.tabs[0].focusedPane()
	p.mu.Lock()
	p.screen.Write([]byte("\x1b[1;1Hfirst"))
	p.mu.Unlock()
	state, composed := captureComposeForReceiptTest(t, sess, ac)
	p.mu.Lock()
	p.screen.Write([]byte("\x1b[2;1Hsecond"))
	p.mu.Unlock()
	require.True(t, d.emitFrame(sess, ac, state, composed))
	<-sends
	p.mu.Lock()
	defer p.mu.Unlock()
	require.Equal(t, []renderer.Damage{renderer.FullRedraw()}, p.screen.Damage(), "a stale receipt conservatively preserves intervening VT output")
}

func captureComposeForReceiptTest(t *testing.T, sess *session, ac *attachedClient) (*capturedRenderState, composedRenderFrame) {
	t.Helper()
	ac.sendMu.Lock() // emitFrame releases the transaction lock.
	state, ok := capturePrimaryRenderState(sess, ac, primaryCaptureRequest{
		bars:        barState{},
		overlays:    capturedOverlayRenderState{},
		preview:     pickerPreviewEmpty(),
		floatingCfg: domain.FloatingConfig{},
		reset:       false,
		lease:       nil,
	})
	require.True(t, ok)
	return state, composeFrame(*state, ac.pipelineCache, ac.pipelineScratch)
}

func pickerPreviewEmpty() picker.Preview { return picker.Preview{} }
