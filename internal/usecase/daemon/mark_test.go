package daemon

import (
	"fmt"
	"testing"
	"testing/synctest"
	"time"

	"bnema.dev/vev/internal/protocol/wire"
	"bnema.dev/vev/internal/usecase/ui"

	portsmocks "bnema.dev/vev/internal/ports/mocks"
	"bnema.dev/vev/internal/usecase/keys"
	"github.com/stretchr/testify/require"
)

func TestToggleMarkActiveTab(t *testing.T) {
	for _, ephemeral := range []bool{false, true} {
		name := "named"
		if ephemeral {
			name = "ephemeral"
		}
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				d, sess, ac, _, releases := newManualTabSession(t, 2)
				for _, release := range releases {
					defer release()
				}
				sess.mu.Lock()
				sess.ephemeral = ephemeral
				if !ephemeral {
					sess.name = "work"
				}
				sess.mu.Unlock()
				selectTestAttachmentTab(sess, 1)
				now := time.Date(2025, 4, 3, 2, 1, 0, 0, time.UTC)
				clk := portsmocks.NewMockClock(t)
				clk.EXPECT().Now().Return(now)
				d.clock = clk
				h := daemonKeyHandler{d: d, ac: ac}
				h.Action(keys.ActionToggleMark, nil)
				sess.mu.Lock()
				require.True(t, sess.tabs[1].marked)
				require.Equal(t, now, sess.tabs[1].markedAt)
				require.False(t, sess.tabs[0].marked)
				require.Empty(t, sess.tabs[1].markNote)
				sess.tabs[1].markNote = "remember this"
				sess.mu.Unlock()
				h.Action(keys.ActionToggleMark, nil)
				sess.mu.Lock()
				require.False(t, sess.tabs[1].marked)
				require.True(t, sess.tabs[1].markedAt.IsZero())
				require.Empty(t, sess.tabs[1].markNote)
				sess.mu.Unlock()
			})
		})
	}
}

func TestToggleMarkRepaintsEveryAttachment(t *testing.T) {
	for _, marked := range []bool{false, true} {
		t.Run(fmt.Sprint(marked), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				d, sess, first, firstSends, releases := newManualTabSession(t, 1)
				defer releases[0]()
				transport, secondSends := newCapturingTransport(t)
				second := &attachedClient{tr: transport, output: newOutputStateStream()}
				second.output.attachment = second
				second.initOverlays()
				second.setGeometry(first.geometrySnapshot())
				second.setSession(sess)
				require.True(t, sess.registerAttachment(second))
				sess.mu.Lock()
				sess.tabs[0].marked = marked
				sess.mu.Unlock()
				firstRevision := first.viewSnapshot().revision
				secondRevision := second.viewSnapshot().revision
				(daemonKeyHandler{d: d, ac: first}).Action(keys.ActionToggleMark, nil)
				require.Greater(t, first.viewSnapshot().revision, firstRevision)
				require.Greater(t, second.viewSnapshot().revision, secondRevision)
				for _, sends := range []chan wire.Envelope{firstSends, secondSends} {
					data := mustOutputData(t, sends)
					if marked {
						require.NotContains(t, string(data), string(ui.NerdGlyphs().Mark))
					} else {
						require.Contains(t, string(data), string(ui.NerdGlyphs().Mark))
					}
				}
			})
		})
	}
}
