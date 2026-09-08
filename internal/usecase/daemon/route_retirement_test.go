package daemon

import (
	"testing"
	"time"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/protocol/catalogue"
	"github.com/bnema/vev/internal/protocol/wire"
	"github.com/bnema/vev/internal/usecase/picker"
	"github.com/stretchr/testify/require"
)

func TestPickerDeleteRetiresSubscribedHistory(t *testing.T) {
	for _, stopped := range []bool{false, true} {
		t.Run(map[bool]string{false: "live", true: "stopped"}[stopped], func(t *testing.T) {
			d, source, ac, sends := newManualSessionWithPTYs(t, nil)
			victim := &session{sessionCore: sessionCore{id: "victim", name: "victim", incarnation: domain.SessionLifecycleID{2}}, ctx: source.ctx, cancel: func() {}}
			d.mu.Lock()
			d.sessions[victim.id] = victim
			d.mu.Unlock()
			target := protocol.ExactSessionTarget{LifecycleID: victim.incarnation, SessionName: victim.name}
			ref := protocol.RouteRef{Key: 2, Generation: 1}
			ac.installTestAttachmentCapability(source.captureAttachmentCapability(ac, ac.transport()))
			ac.setRouteAttentionSubscription(protocol.RouteAttentionSubscription{Targets: []protocol.RouteAttentionTarget{{Ref: ref, Target: target}}}, ac.transportSnapshot(), d.clock.Now())
			if stopped {
				require.NoError(t, d.killSession(victim, protocol.ReasonSessionKilled, false))
			}
			require.NoError(t, d.killPickerTarget(picker.Target{Session: victim.id, Name: victim.name, Incarnation: victim.incarnation, Stopped: stopped}))
			frame := awaitFrame(t, sends, wire.MsgRouteRetired)
			message, err := wire.UnmarshalRouteRetired(frame.Payload)
			require.NoError(t, err)
			require.Equal(t, protocol.RouteRetired{Ref: ref, Target: target}, message)
			require.Equal(t, source, ac.currentAttachmentSession())
		})
	}
}

func TestRemoteDirectoryDeletionRetiresHistoryWithPickerClosed(t *testing.T) {
	d, source, ac, sends := newManualSessionWithPTYs(t, nil)
	ac.installTestAttachmentCapability(source.captureAttachmentCapability(ac, ac.transport()))
	target := protocol.ExactSessionTarget{LifecycleID: domain.SessionLifecycleID{9}, SessionName: "remote-work"}
	ref := protocol.RouteRef{Key: 3, Generation: 2}
	ac.setRouteAttentionSubscription(protocol.RouteAttentionSubscription{Targets: []protocol.RouteAttentionTarget{{Ref: ref, Target: target, SourceKey: protocol.RemoteInventorySourceKey("remote")}}}, ac.transportSnapshot(), d.clock.Now())
	seedRemoteDirectory(t, d, ports.RemoteHostSnapshot{Endpoint: "remote", InventoryKnown: true, Availability: domain.RemoteAvailabilityReachable, LastAttempt: d.clock.Now().Add(time.Second), LastSuccess: d.clock.Now().Add(2 * time.Second)})
	require.False(t, ac.overlays.pickerActive())
	d.refreshRemoteDirectoryViews()
	frame := awaitFrame(t, sends, wire.MsgRouteRetired)
	message, err := wire.UnmarshalRouteRetired(frame.Payload)
	require.NoError(t, err)
	require.Equal(t, protocol.RouteRetired{Ref: ref, Target: target}, message)
}

func TestRetiredRoutesRequiresAuthoritativeAbsence(t *testing.T) {
	target := protocol.ExactSessionTarget{LifecycleID: domain.SessionLifecycleID{1}, SessionName: "work"}
	for _, tc := range []struct {
		name        string
		remote      bool
		known       bool
		unreachable bool
		present     bool
		replacement bool
		want        bool
	}{
		{name: "local deleted", want: true},
		{name: "local present", present: true},
		{name: "local recreated", replacement: true, want: true},
		{name: "remote deleted", remote: true, known: true, want: true},
		{name: "remote present", remote: true, known: true, present: true},
		{name: "remote recreated", remote: true, known: true, replacement: true, want: true},
		{name: "remote unknown", remote: true},
		{name: "remote unreachable", remote: true, known: true, unreachable: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sub := protocol.RouteAttentionSubscription{Targets: []protocol.RouteAttentionTarget{{Ref: protocol.RouteRef{Key: 1, Generation: 1}, Target: target}}}
			inv := sessionInventory{}
			lifecycle := target.LifecycleID
			if tc.replacement {
				lifecycle = domain.SessionLifecycleID{2}
			}
			if tc.remote {
				sub.Targets[0].SourceKey = protocol.RemoteInventorySourceKey("remote")
				host := ports.RemoteHostSnapshot{Endpoint: "remote", InventoryKnown: tc.known, Availability: domain.RemoteAvailabilityReachable, LastAttempt: time.Unix(2, 0), LastSuccess: time.Unix(3, 0)}
				if tc.unreachable {
					host.Availability = domain.RemoteAvailabilityUnreachable
				}
				if tc.present || tc.replacement {
					host.Sessions = []catalogue.RemoteCatalogSession{{Name: "work", LifecycleID: lifecycle}}
				}
				inv.hosts = []ports.RemoteHostSnapshot{host}
			} else if tc.present || tc.replacement {
				inv.live = []inventoriedLiveSession{{view: sessionView{name: "work", incarnation: lifecycle}}}
			}
			require.Equal(t, tc.want, len(retiredRoutes(sub, map[protocol.RouteAttentionTarget]time.Time{sub.Targets[0]: time.Unix(1, 0)}, inv)) == 1)
		})
	}
}
