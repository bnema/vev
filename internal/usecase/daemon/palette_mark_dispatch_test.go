package daemon

import (
	"testing"
	"testing/synctest"
	"time"

	"bnema.dev/vev/internal/protocol"
	"bnema.dev/vev/internal/usecase/command"
	"github.com/stretchr/testify/require"
)

func TestExecutePaletteMarkCommands(t *testing.T) {
	for _, tt := range []struct {
		name, code    string
		cross, marked bool
	}{
		{"jump local", "JMK", false, true}, {"jump cross session", "JMK", true, true},
		{"mark prompt", "MRK", false, false}, {"unmark", "MRK", false, true}, {"edit prompt", "EMN", false, true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				d, sess, ac, _, releases := newManualTabSession(t, 2)
				for _, release := range releases {
					defer release()
				}
				stopOverlayTimersAtCleanup(t, ac)
				selectTestAttachmentTab(sess, 0)
				targetSession := sess
				target := sess.tabs[0]
				if tt.code == "JMK" {
					target = sess.tabs[1]
				}
				if tt.cross {
					targetSession, _ = addAgentTestSession(t, d, "other")
					targetSession.name = "other"
					target = targetSession.tabs[0]
				}
				target.marked = tt.marked
				target.markedAt = time.Unix(1, 0)
				if tt.code == "EMN" {
					target.markNote = "old"
				}
				d.enterPalette(sess, ac)
				ac.overlays.paletteMu.Lock()
				generation := ac.overlays.paletteGeneration
				query := ac.overlays.palette.Query()
				ac.overlays.paletteMu.Unlock()
				cmd, ok := command.ByCode(tt.code)
				require.True(t, ok)
				d.executePaletteCommand(ac, sess, nil, generation, query, cmd, nil, protocol.RecentRouteSnapshot{})
				require.False(t, ac.overlays.paletteActive())
				if tt.code == "JMK" {
					require.Same(t, targetSession, ac.currentSession())
					require.Same(t, target, targetSession.tabForAttachment(ac))
					return
				}
				if tt.code == "MRK" && tt.marked {
					require.False(t, target.marked)
					require.Nil(t, ac.overlays.prompt)
					return
				}
				require.NotNil(t, ac.overlays.prompt)
				// The submit callback takes runMutation only after command dispatch returned.
				d.handlePromptInput(ac, []byte("note\r"))
				require.True(t, target.marked)
				if tt.code == "EMN" {
					require.Equal(t, "oldnote", target.markNote)
				} else {
					require.Equal(t, "note", target.markNote)
				}
			})
		})
	}
}
