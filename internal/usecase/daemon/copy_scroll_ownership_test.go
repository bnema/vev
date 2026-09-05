package daemon

import (
	"testing"

	renderer "github.com/bnema/vev-vt"
	"github.com/bnema/vev/internal/protocol/wire"
	scopy "github.com/bnema/vev/internal/usecase/copy"
	"github.com/stretchr/testify/require"
)

func TestCopyCacheFailedPublicationAndRetry(t *testing.T) {
	for _, failure := range []string{"prepare", "send"} {
		t.Run(failure, func(t *testing.T) {
			d, sess, ac, sends := newManualSessionWithPTYs(t)
			healthy := ac.transport()
			state := cacheState("live", 1)
			state.floating = capturedFloatingRenderState{}
			state.overlays = capturedOverlayRenderState{
				copyActive: true, copyPaneID: "pane",
				copyMode: scopy.NewMode(scopy.NewDocument(scopy.NewSnapshotFromRows([][]renderer.Cell{testRow("COPY")}, 6, 1), "")),
			}
			committed := composeFrame(state, composeCacheInput{})
			require.True(t, committed.cache.valid)
			ac.pipelineCache = committed.cache
			before := cloneComposeCache(ac.pipelineCache)
			state.bars.topRight = "retry"
			state.attachment = ac
			pending := composeFrame(state, ac.pipelineCache, ac.pipelineScratch)
			if failure == "prepare" {
				pending.frame = renderer.Frame{Width: 1}
			} else {
				ac.replaceTransport(cacheFailTransport{})
			}
			ac.sendMu.Lock()
			require.True(t, d.emitFrame(sess, ac, &state, pending))
			require.Equal(t, before, cloneComposeCache(ac.pipelineCache))
			require.Zero(t, ac.output.next)
			if failure == "send" {
				sess.mu.Lock()
				sess.registerAttachmentLocked(ac)
				sess.mu.Unlock()
				ac.setSession(sess)
				ac.replaceTransport(healthy)
			}
			pending = composeFrame(state, ac.pipelineCache, ac.pipelineScratch)
			ac.sendMu.Lock()
			require.True(t, d.emitFrame(sess, ac, &state, pending))
			require.Equal(t, pending.cache, ac.pipelineCache)
			require.NotContains(t, frameText(ac.pipelineCache.frame), "COPY")
			output, err := wire.UnmarshalOutput((<-sends).Payload)
			require.NoError(t, err)
			terminal := renderer.NewScreen(pending.frame.Width, pending.frame.Height)
			terminal.Write(output.Data)
			require.Contains(t, frameText(captureTestFrame(terminal)), "COPY", "retry must render copy cells despite interleaved ANSI styling")
			require.Equal(t, uint64(1), ac.output.next)
		})
	}
}

func TestCopyCacheSearchTransition(t *testing.T) {
	d, sess, ac, sends := newManualSessionWithPTYs(t, nil)
	sess.tabs[0].focusedPane().screen.Write([]byte("alpha"))
	ack := func() {
		mustOutputData(t, sends)
		ac.ackOutputState(ac.output.currentEpoch(), ac.output.next)
	}
	d.enterCopyMode(sess, ac)
	ack()
	require.True(t, ac.pipelineCache.valid)
	base := captureTestFrame(ac.pipelineCache.frame)
	d.handleInput(sess, ac, []byte("/"))
	ack()
	require.NotNil(t, ac.overlays.copySearch)
	require.False(t, ac.pipelineCache.valid)
	require.Equal(t, base, captureTestFrame(ac.pipelineCache.frame), "search modal must not contaminate the live base")
	d.handleInput(sess, ac, []byte("\x1b"))
	ack()
	require.Nil(t, ac.overlays.copySearch)
	require.NotNil(t, ac.overlays.copyMode)
	require.True(t, ac.pipelineCache.valid)
	require.Equal(t, base, captureTestFrame(ac.pipelineCache.frame))
}

func TestCopyExitRefreshesAfterOtherAttachmentConsumesDamage(t *testing.T) {
	d, sess, ac, sends := newManualSessionWithPTYs(t, nil)
	p := sess.tabs[0].focusedPane()
	p.screen.Write([]byte("before"))
	d.enterCopyMode(sess, ac)
	mustOutputData(t, sends)
	ac.ackOutputState(ac.output.currentEpoch(), ac.output.next)
	copyBefore := captureTestFrame(ac.pipelineCache.frame)

	tr, peerSends := newCapturingTransport(t)
	peer := &attachedClient{tr: tr, output: newOutputStateStream(), size: ac.size}
	peer.output.attachment = peer
	peer.initOverlays()
	peer.setSession(sess)
	sess.mu.Lock()
	require.True(t, sess.registerAttachmentLocked(peer))
	sess.mu.Unlock()
	p.mu.Lock()
	p.screen.Write([]byte("\x1b[1;1Hafter-peer"))
	p.mu.Unlock()
	// A real peer capture/emission consumes the shared pane damage first.
	d.paint(sess, peer, true, nil)
	mustOutputData(t, peerSends)
	p.mu.Lock()
	require.Empty(t, p.screen.Damage())
	p.mu.Unlock()
	require.Contains(t, frameText(peer.pipelineCache.frame), "after-peer")
	require.Equal(t, copyBefore, captureTestFrame(ac.pipelineCache.frame), "peer publication cannot mutate another attachment's cache")

	// The copy document is still frozen, but leaving it must recapture live
	// cells even though the other attachment already acknowledged the damage.
	d.copyWheel(sess, ac, 1)
	mustOutputData(t, sends)
	require.Nil(t, ac.overlays.copyMode)
	require.Contains(t, frameText(ac.pipelineCache.frame), "after-peer")
	require.NoError(t, ac.pipelineCache.frame.CheckInvariants())
}
