package daemon

import (
	"testing"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/usecase/picker"
	"github.com/bnema/vev/pkg/renderer"
	"github.com/stretchr/testify/require"
)

// These are deliberately end-to-end pipeline tests: the pending damage comes
// from a real pane VT Write, not a hand-built FullRedraw.
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
				sess.client = ac
				sess.mu.Unlock()
				ac.setSession(sess)
				ac.replaceTransport(healthy)
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			d, sess, ac, sends := newManualSessionWithPTYs(t, nil)
			healthy := ac.transport()
			p := sess.tabs[0].focusedPane()

			// Prime both the renderer shadow and the committed capture cache.
			d.paint(sess, ac, true)
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

func TestRenderDamageReceiptStaleGenerationForcesFullRedraw(t *testing.T) {
	d, sess, ac, sends := newManualSessionWithPTYs(t, nil)
	d.paint(sess, ac, true)
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
	state, ok := captureRenderState(sess, ac, barState{}, capturedOverlayRenderState{}, pickerPreviewEmpty(), domain.FloatingConfig{}, false, damageCaptureConsume)
	require.True(t, ok)
	return state, composeFrame(*state, ac.pipelineCache, ac.pipelineScratch)
}

func pickerPreviewEmpty() picker.Preview { return picker.Preview{} }
