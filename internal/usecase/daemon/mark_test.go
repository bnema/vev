package daemon

import (
	"testing"
	"testing/synctest"
	"time"

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
