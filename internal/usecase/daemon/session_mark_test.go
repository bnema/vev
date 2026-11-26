package daemon

import (
	"fmt"
	"testing"
	"testing/synctest"

	renderer "bnema.dev/vev-vt"
	"bnema.dev/vev/internal/domain"
	"bnema.dev/vev/internal/protocol"
	"bnema.dev/vev/internal/usecase/keys"
	"bnema.dev/vev/internal/usecase/ui"
	"github.com/stretchr/testify/require"
)

func TestResolveLocalSessionMarks(t *testing.T) {
	for _, marked := range []bool{false, true} {
		t.Run(fmt.Sprint(marked), func(t *testing.T) {
			lifecycle := newTestLifecycle(t)
			sess := &session{sessionCore: sessionCore{incarnation: lifecycle}, tabs: []*tab{{}, {marked: marked}}}
			d := &Daemon{sessions: map[domain.SessionID]*session{"work": sess}}
			entries := []recentRouteDisplay{{lifecycle: lifecycle}, {kind: recentRouteRemote, lifecycle: lifecycle}, {lifecycle: newTestLifecycle(t)}}
			d.sessionMarks.Store(lifecycle, marked)
			d.resolveSessionMarks(entries)
			require.Equal(t, marked, entries[0].marked)
			require.False(t, entries[1].marked)
			require.False(t, entries[2].marked)
		})
	}
}

func TestSessionMarkCellsAndWidths(t *testing.T) {
	for _, ranked := range []bool{false, true} {
		for _, activity := range []statusActivity{activityNone, activityWorking, activityDone, activityBell} {
			t.Run(fmt.Sprintf("%t/%d", ranked, activity), func(t *testing.T) {
				glyphs := ui.ASCIIGlyphs()
				styles := resolveStyles(nil)
				cell := sessionCellPaint{frame: pulseFrameCount / 2, spinner: glyphs.Spinner, glyphs: glyphs, done: styles.AgentDone, accent: styles.Accent}
				row := make([]renderer.Cell, 30)
				x := 0
				entry := recentRouteDisplay{name: "work", marked: true, activity: activity}
				wantWidth := recentRouteDisplayWidth(entry)
				if ranked {
					drawRankedStatusSessionEntry(row, &x, rankedRecent{rank: 1, name: "work", marked: true, activity: activity}, styles.SurfaceInactive, cell)
					wantWidth += 2
				} else {
					drawStatusSessionEntry(row, &x, entry, styles.SurfaceInactive, cell)
				}
				require.Equal(t, wantWidth, x)
				require.Contains(t, rowText(row), "#")
				if activity != activityNone {
					require.Equal(t, 10, recentRouteDisplayWidth(entry))
				}
				for _, c := range row {
					if c.Rune == '#' {
						require.True(t, c.Style.Bold)
						require.Equal(t, styles.SurfaceInactive.BackgroundRGB, c.Style.BackgroundRGB)
					}
				}
				require.Empty(t, fitMRU([]recentRouteDisplay{entry}, wantWidth-1, 0, "", ranked))
			})
		}
	}
}

func TestToggleMarkRepaintsRouteViewer(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		d, sess, ac, _, releases := newManualTabSession(t, 1)
		defer releases[0]()
		other, sends := addAgentTestSession(t, d, "other")
		viewer := testAttachment(other)
		viewer.setRouteSnapshot(protocol.RecentRouteSnapshot{Generation: 1, Entries: []protocol.RecentRouteEntry{{Target: protocol.ExactSessionTarget{LifecycleID: sess.incarnation}}}})
		rc := d.attachCoordinator(other, nil, viewer, true)
		rc.opts.clock = nil
		(daemonKeyHandler{d: d, ac: ac}).Action(keys.ActionToggleMark, nil)
		rc.fireCurrent(false)
		require.NotEmpty(t, mustOutputData(t, sends))
	})
}

func TestSessionMarkRemoval(t *testing.T) {
	for _, removeSession := range []bool{false, true} {
		t.Run(fmt.Sprint(removeSession), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				d, sess, _, _, releases := newManualTabSession(t, 2)
				for _, release := range releases {
					defer release()
				}
				sess.mu.Lock()
				sess.tabs[1].marked = true
				d.publishSessionMarkLocked(sess)
				sess.mu.Unlock()
				_, ok := d.sessionMarks.Load(sess.incarnation)
				require.True(t, ok)
				if removeSession {
					require.NoError(t, d.killSession(sess, protocol.ReasonSessionKilled, false))
				} else {
					require.NoError(t, d.closeTab(sess, sess.tabs[1], false))
				}
				_, ok = d.sessionMarks.Load(sess.incarnation)
				require.False(t, ok)
			})
		})
	}
}
