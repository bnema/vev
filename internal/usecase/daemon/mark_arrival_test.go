package daemon

import (
	"fmt"
	"testing"
	"testing/synctest"

	"bnema.dev/vev/internal/usecase/ui"
	"github.com/stretchr/testify/require"
)

func TestMarkArrivalNotice(t *testing.T) {
	for _, tt := range []struct {
		name, note string
		marked     bool
	}{
		{"note", "remember", true}, {"empty", "", true}, {"unmarked", "remember", false},
	} {
		for _, arrival := range []string{"tab", "session", "reattach"} {
			t.Run(tt.name+"/"+arrival, func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					d, sess, ac, _, releases := newManualTabSession(t, 2)
					for _, release := range releases {
						defer release()
					}
					stopOverlayTimersAtCleanup(t, ac)
					peer := addMultiplexTestAttachment(t, sess, ac.geometrySnapshot())
					stopOverlayTimersAtCleanup(t, peer)
					selectTestAttachmentTab(sess, 0)
					d.paint(sess, ac, true, nil)
					target := sess.tabs[1]
					target.marked = tt.marked
					target.markNote = tt.note
					if arrival == "tab" {
						require.True(t, sess.switchAttachmentTab(ac, 1))
					} else {
						// Keep the tab identity stable across membership changes to exercise
						// session arrival and detach/reattach independently of tab publication.
						selectTestAttachmentTab(sess, 1)
						ac.viewMu.Lock()
						ac.markArrivalPending = false
						ac.viewMu.Unlock()
						if arrival == "session" {
							other, _ := addAgentTestSession(t, d, "other")
							ac.setSession(other)
						} else {
							ac.setSession(nil)
						}
						ac.setSession(sess)
					}
					d.paint(sess, ac, true, nil)
					want := 0
					if tt.marked && tt.note != "" {
						want = 1
					}
					toasts, _ := visibleToasts(ac)
					require.Len(t, toasts, want)
					if want == 1 {
						require.Equal(t, string(ui.NerdGlyphs().Mark)+" "+tt.note, toasts[0].Message)
						require.Equal(t, 1, toasts[0].Count)
					}
					d.paint(sess, ac, true, nil)
					toasts, _ = visibleToasts(ac)
					require.Len(t, toasts, want)
					if want == 1 {
						require.Equal(t, 1, toasts[0].Count)
					}
					peerToasts, _ := visibleToasts(peer)
					require.Empty(t, peerToasts)
					require.Empty(t, d.notices.history())
					require.True(t, sess.switchAttachmentTab(ac, 0))
					d.paint(sess, ac, true, nil)
					require.True(t, sess.switchAttachmentTab(ac, 1))
					d.paint(sess, ac, true, nil)
					toasts, _ = visibleToasts(ac)
					if want == 1 {
						require.Equal(t, 2, toasts[0].Count)
					}
				})
			})
		}
	}
}

func TestUnmarkedEditNoticeIsAttachmentOnly(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		d, sess, ac, _, releases := newManualTabSession(t, 1)
		defer releases[0]()
		stopOverlayTimersAtCleanup(t, ac)
		peer := addMultiplexTestAttachment(t, sess, ac.geometrySnapshot())
		stopOverlayTimersAtCleanup(t, peer)
		require.NoError(t, (paletteExec{d: d, sess: sess, ac: ac}).EditMarkNote())
		toasts, _ := visibleToasts(ac)
		require.Len(t, toasts, 1)
		require.Equal(t, "Tab is not marked", toasts[0].Message)
		other, _ := visibleToasts(peer)
		require.Empty(t, other)
		require.Empty(t, d.notices.history(), fmt.Sprint(d.notices.history()))
	})
}

func TestRejectedPaintRetainsMarkArrival(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		d, sess, ac, _, releases := newManualTabSession(t, 1)
		defer releases[0]()
		stopOverlayTimersAtCleanup(t, ac)
		sess.tabs[0].marked = true
		sess.tabs[0].markNote = "retry"
		sess.mu.Lock()
		delete(sess.attachments, ac)
		sess.mu.Unlock()
		require.Equal(t, paintRejected, d.paint(sess, ac, true, nil))
		ac.viewMu.Lock()
		pending := ac.markArrivalPending
		ac.viewMu.Unlock()
		require.True(t, pending)
		require.True(t, sess.registerAttachment(ac))
		require.Equal(t, paintEmitted, d.paint(sess, ac, true, nil))
		toasts, _ := visibleToasts(ac)
		require.Len(t, toasts, 1)
		require.Contains(t, toasts[0].Message, "retry")
	})
}

func TestStaleViewFrameRetainsMarkArrival(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		d, sess, ac, _, releases := newManualTabSession(t, 1)
		defer releases[0]()
		stopOverlayTimersAtCleanup(t, ac)
		sess.tabs[0].marked = true
		sess.tabs[0].markNote = "retry after resize"
		ac.renderStages.compose = func() {
			ac.renderStages.compose = nil
			view := ac.viewSnapshot()
			view.revision++
			ac.publishView(view)
		}
		require.Equal(t, paintEmitted, d.paint(sess, ac, true, nil))
		ac.viewMu.Lock()
		pending := ac.markArrivalPending
		ac.viewMu.Unlock()
		require.True(t, pending)
		toasts, _ := visibleToasts(ac)
		require.Empty(t, toasts)
		require.Equal(t, paintEmitted, d.paint(sess, ac, true, nil))
		toasts, _ = visibleToasts(ac)
		require.Len(t, toasts, 1)
		require.Contains(t, toasts[0].Message, "retry after resize")
	})
}
