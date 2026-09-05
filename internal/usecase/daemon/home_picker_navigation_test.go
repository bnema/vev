package daemon

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/protocol/wire"
)

func TestHomePickerTabSelectionHandsOffToClient(t *testing.T) {
	for _, tc := range []struct {
		name        string
		sameSession bool
		homePicker  bool
	}{
		{name: "different local session", homePicker: true},
		{name: "backing local session", homePicker: true, sameSession: true},
		{name: "ordinary local session switch"},
		{name: "ordinary local tab switch", sameSession: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d, source, ac, sends, releases := newManualTabSession(t, 2)
			defer releaseAll(releases)
			source.name = "home"
			source.incarnation = domain.SessionLifecycleID{1}
			target := source
			if !tc.sameSession {
				target = &session{
					sessionCore: sessionCore{id: "destination", name: "destination", incarnation: domain.SessionLifecycleID{2}, attachments: make(map[*attachedClient]struct{})},
					ctx:         source.ctx, cancel: func() {},
					tabs: []*tab{newTab(nil, domain.Size{Cols: 80, Rows: 23}), newTab(nil, domain.Size{Cols: 80, Rows: 23})},
				}
				for _, tab := range target.tabs {
					publishTiledPaneOwners(target, tab)
				}
				d.sessions[target.id] = target
			}
			target.tabs[1].name = "chosen-tab"
			if tc.homePicker {
				ac.startupOverlay = protocol.StartupOverlaySessionPicker
				ac.navigationCapabilities = protocol.NavigationCapabilityBack
			}
			ref := protocol.RouteRef{Key: 7, Generation: 1}
			ac.setRouteSnapshot(protocol.RecentRouteSnapshot{
				Generation: 1, Active: ref,
				ActiveEntry: protocol.RecentRouteEntry{Key: ref.Key, Generation: ref.Generation,
					Target: protocol.ExactSessionTarget{LifecycleID: domain.SessionLifecycleID{3}, SessionName: "remote"},
					Name:   "remote", HostLabel: "remote-host", Kind: protocol.RouteKindRemote},
			})
			effect := beginRecentRoutePaletteEffect(t, d, source, ac)
			d.enterPicker(source, ac)
			// Exercise the real picker model and input dispatch, not a fabricated
			// AttachTarget: navigation selects tab rows, including search hits.
			d.handleInputForAttachment(effect, []byte("/chosen-tab"))
			ac.overlays.pickerMu.Lock()
			selected, ok := ac.overlays.picker.Selected()
			ac.overlays.pickerMu.Unlock()
			require.True(t, ok)
			require.Equal(t, domain.TabStableID(target.tabs[1].stableID), selected.TabID)
			d.handleInputForAttachment(effect, []byte("\r"))

			if !tc.homePicker {
				require.Same(t, target, ac.currentAttachmentSession())
				require.Equal(t, selected.TabID, ac.viewSnapshot().tabID)
				return
			}
			require.True(t, source == ac.currentAttachmentSession(), "a temporary picker must not become the destination attachment")
			var handoff *protocol.AttachTarget
			for len(sends) != 0 {
				frame := <-sends
				if frame.Type == wire.MsgAttachTarget {
					decoded, err := wire.UnmarshalAttachTarget(frame.Payload)
					require.NoError(t, err)
					handoff = &decoded
				}
			}
			require.NotNil(t, handoff, "selection must return to the client that owns the parked remote route")
			require.Equal(t, &protocol.ExactSessionTarget{LifecycleID: target.incarnation, SessionName: target.name}, handoff.ExactTarget)
			require.Equal(t, selected.TabID, handoff.PreferredTabID)
			require.False(t, handoff.SamePeer, "the temporary connection must end, not be promoted in place")
		})
	}
}
