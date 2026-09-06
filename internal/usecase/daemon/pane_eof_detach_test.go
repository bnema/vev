package daemon

import (
	"testing"

	"github.com/bnema/vev/internal/domain"
	"github.com/stretchr/testify/require"
)

func TestLastPaneEOFRetriesSessionTeardownAfterConcurrentDetach(t *testing.T) {
	for _, change := range []string{"detach", "transport replacement"} {
		t.Run(change, func(t *testing.T) {
			d, sess, ac, _ := newManualSessionWithPTYs(t, newQuietPTY())
			tb := sess.tabs[0]
			last := tb.focusedPane()
			hiddenPTY := newQuietPTY()
			defer func() { _ = hiddenPTY.Close() }()
			installTestFloating(tb, newPane("floating", hiddenPTY, domain.Size{Cols: 20, Rows: 5}), false)
			defer tb.closeAllPanes()
			changed := false
			d.afterAttachmentEffectParticipantsSnapshotted = func(string, []*attachedClient) {
				if changed {
					return
				}
				changed = true
				if change == "detach" {
					require.True(t, d.detachIfCurrentTransport(sess, ac, ac.transportSnapshot()))
				} else {
					ac.replaceTransport(&closeTrackingTransport{})
				}
			}

			// This is the reader's actual EOF handler. The hook makes its first
			// kill snapshot stale before the attachment gates are frozen.
			d.reapPaneOwner(last)

			require.True(t, changed)
			d.mu.Lock()
			remaining := d.sessions[sess.id]
			d.mu.Unlock()
			require.Nil(t, remaining, "EOF must not abandon an uncommitted session teardown")
			require.Nil(t, last.ownerSnapshot())
			select {
			case <-hiddenPTY.done:
			default:
				t.Fatal("hidden floating PTY survived last-pane EOF")
			}
		})
	}
}
