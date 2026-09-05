package daemon

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/protocol"
)

func TestHomePickerStatusKeepsActiveRemoteRoute(t *testing.T) {
	for _, tc := range []struct {
		name        string
		homePicker  bool
		noSnapshot  bool
		staleRef    bool
		ephemeral   bool
		wantSession string
	}{
		{name: "ordinary local picker ignores unrelated route", wantSession: "vev"},
		{name: "remote home picker", homePicker: true, wantSession: "misc@igor"},
		{name: "ephemeral remote route", homePicker: true, ephemeral: true, wantSession: "misc*@igor"},
		{name: "before route publication", homePicker: true, noSnapshot: true},
		{name: "inconsistent active reference", homePicker: true, staleRef: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d, sess, ac, _, releases := newManualTabSession(t, 1)
			defer releases[0]()
			sess.name = "vev"
			sess.incarnation = domain.SessionLifecycleID{1}
			d.barScripts = &barScriptState{outputs: map[domain.SessionID]barScriptOutputs{
				sess.id: {topRight: "local top script", bottomRight: "local bottom script"},
			}}
			if tc.homePicker {
				ac.startupOverlay = protocol.StartupOverlaySessionPicker
				ac.navigationCapabilities = protocol.NavigationCapabilityBack
			}
			ref := protocol.RouteRef{Key: 2, Generation: 1}
			snapshot := protocol.RecentRouteSnapshot{
				Generation: 2, Active: ref,
				ActiveEntry: protocol.RecentRouteEntry{
					Key: ref.Key, Generation: ref.Generation,
					Target: protocol.ExactSessionTarget{LifecycleID: domain.SessionLifecycleID{2}, SessionName: "misc"},
					Name:   "misc", HostLabel: "dev@igor", Kind: protocol.RouteKindRemote, Ephemeral: tc.ephemeral,
				},
			}
			if tc.staleRef {
				snapshot.Active.Generation++
			}
			if !tc.noSnapshot {
				ac.setRouteSnapshot(snapshot)
			}
			d.enterPicker(sess, ac)
			require.True(t, ac.overlays.pickerActive())
			bars := d.barStateForAttachmentPaletteHintsFor(sess, ac, "", nil, protocol.RecentRouteSnapshot{})
			frame := composeFrame(capturedRenderState{window: domain.Size{Cols: 80, Rows: 24}, bars: bars}, composeCacheInput{}).frame
			require.Equal(t, tc.wantSession, bars.status.session)
			require.Contains(t, rowText(frame.Row(frame.Height-1)), tc.wantSession)
			if tc.homePicker {
				require.Empty(t, bars.status.tabs, "local tabs must not masquerade as remote tabs")
				require.Empty(t, bars.topRight)
				require.Empty(t, bars.bottomRight)
				require.NotContains(t, rowText(frame.Row(frame.Height-1)), "vev")
			} else {
				require.Len(t, bars.status.tabs, 1)
				require.Equal(t, "local top script", bars.topRight)
				require.Equal(t, "local bottom script", bars.bottomRight)
			}
			require.Equal(t, "vev", sess.statusSegments(true).session, "picker presentation must not rename its local backing session")
		})
	}
}
