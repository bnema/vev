package daemon

import (
	"testing"
	"testing/synctest"
	"time"

	"bnema.dev/vev/internal/domain"
	"bnema.dev/vev/internal/usecase/command"
	"bnema.dev/vev/internal/usecase/keys"
	"bnema.dev/vev/internal/usecase/ui"
	"github.com/stretchr/testify/require"
)

func TestJumpMarkLocalCycle(t *testing.T) {
	for _, tt := range []struct {
		name    string
		current int
		marks   []int
		want    int
	}{
		{"oldest", 0, []int{2, 1}, 2}, {"next", 2, []int{2, 1}, 1}, {"wrap", 1, []int{2, 1}, 2}, {"single current", 1, []int{1}, 1}, {"none", 0, nil, 0},
	} {
		for _, palette := range []bool{false, true} {
			name := tt.name + map[bool]string{false: "/key", true: "/palette"}[palette]
			t.Run(name, func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					d, sess, ac, _, releases := newManualTabSession(t, 3)
					for _, release := range releases {
						defer release()
					}
					stopOverlayTimersAtCleanup(t, ac)
					selectTestAttachmentTab(sess, tt.current)
					d.paint(sess, ac, true, nil)
					for i, index := range tt.marks {
						sess.tabs[index].marked = true
						sess.tabs[index].markedAt = time.Unix(int64(i+1), 0)
						sess.tabs[index].markNote = "remember"
					}
					// Bells favor the newer mark and must not change mark navigation.
					sess.tabs[1].attention = true
					sess.tabs[1].attentionAt = time.Unix(0, 0)
					if palette {
						cmd, ok := command.ByCode("JMK")
						require.True(t, ok)
						require.NoError(t, cmd.Run(paletteExec{d: d, sess: sess, ac: ac}, nil))
					} else {
						(daemonKeyHandler{d: d, ac: ac}).Action(keys.ActionJumpMark, nil)
					}
					require.Equal(t, tt.want, testAttachmentTabIndex(sess))
					toasts, _ := visibleToasts(ac)
					if len(tt.marks) == 0 {
						require.Len(t, toasts, 1)
						require.Equal(t, "No marked tabs", toasts[0].Message)
					} else if tt.current != tt.want {
						require.Len(t, toasts, 1)
						require.Equal(t, string(ui.NerdGlyphs().Mark)+" remember", toasts[0].Message)
					} else {
						require.Empty(t, toasts)
					}
				})
			})
		}
	}
}

func TestJumpMarkCrossSessionAndTies(t *testing.T) {
	for _, tie := range []bool{false, true} {
		t.Run(map[bool]string{false: "oldest other session", true: "session name and tab order"}[tie], func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				d, source, ac, _, releases := newManualTabSession(t, 2)
				for _, release := range releases {
					defer release()
				}
				stopOverlayTimersAtCleanup(t, ac)
				source.name = "zulu"
				destination, _ := addAgentTestSession(t, d, "alpha")
				destination.name = "alpha"
				source.tabs[1].marked = true
				source.tabs[1].markedAt = time.Unix(2, 0)
				destination.tabs[0].marked = true
				destination.tabs[0].markedAt = time.Unix(1, 0)
				destination.tabs[0].markNote = "cross session"
				if tie {
					source.tabs[1].markedAt = time.Unix(1, 0)
					source.tabs[0].marked = true
					source.tabs[0].markedAt = time.Unix(1, 0)
				}
				targets := d.markedTabTargets()
				require.Same(t, destination, targets[0].session)
				if tie {
					require.Same(t, source.tabs[0], targets[1].tab)
					require.Same(t, source.tabs[1], targets[2].tab)
					source.tabs[0].marked = false
				}
				require.NoError(t, d.jumpMark(source, ac, nil))
				synctest.Wait()
				d.paint(destination, ac, true, nil)
				require.Same(t, destination, ac.currentSession())
				require.Equal(t, 0, testAttachmentTabIndex(destination))
				toasts, _ := visibleToasts(ac)
				require.Len(t, toasts, 1)
				require.Equal(t, string(ui.NerdGlyphs().Mark)+" cross session", toasts[0].Message)
				require.Empty(t, d.notices.history())
			})
		})
	}
}

func TestJumpMarkNoMarksNoticeIsAttachmentOnly(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		d, sess, ac, _, releases := newManualTabSession(t, 1)
		defer releases[0]()
		stopOverlayTimersAtCleanup(t, ac)
		peer := addMultiplexTestAttachment(t, sess, domain.Geometry{Size: domain.Size{Cols: 80, Rows: 25}})
		stopOverlayTimersAtCleanup(t, peer)
		require.NoError(t, d.jumpMark(sess, ac, nil))
		toasts, _ := visibleToasts(peer)
		require.Empty(t, toasts)
	})
}
