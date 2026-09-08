package client_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/protocol/wire"
	"github.com/bnema/vev/internal/usecase/client"
)

func TestHomePickerPreservesActiveRouteSnapshot(t *testing.T) {
	for _, tc := range []struct {
		name        string
		udp         bool
		rename      bool
		selectLocal bool
		retireLocal bool
	}{
		{name: "UDP", udp: true},
		{name: "stdio"},
		{name: "UDP local deletion", udp: true, retireLocal: true},
		{name: "stdio local deletion", retireLocal: true},
		{name: "UDP backing session renamed", udp: true, rename: true},
		{name: "stdio backing session renamed", rename: true},
		{name: "stdio select new local destination", selectLocal: true},
		{name: "UDP select new local destination", udp: true, selectLocal: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			term := newRunTerminal()
			defer term.in.unblock()
			localLifecycle := domain.SessionLifecycleID{1}
			remoteLifecycle := domain.SessionLifecycleID{2}
			target := domain.RemoteSessionTarget{
				Endpoint: "igor", DisplayOrigin: "igor", LifecycleID: remoteLifecycle,
				SessionName: "misc", LiveTabID: "remote-tab",
			}
			initial := hybridLocalBootstrap(localLifecycle, target)
			remote := &recordingTransport{recvs: []recvItem{
				{f: hybridWelcomeFrame("misc", remoteLifecycle)},
				{f: navigationDirectiveFrame(protocol.NavigationOpenHomePicker)},
			}}
			remoteReturn := &recordingTransport{recvs: []recvItem{
				{f: hybridWelcomeFrame("misc", remoteLifecycle)},
				{f: frameOf(wire.MsgDetached, wire.MarshalDetached(protocol.Detached{Reason: protocol.ReasonDetach}))},
			}}
			remoteDialer := &sequenceDialer{trs: []wire.Transport{remote, remoteReturn}}
			if tc.udp {
				prepareSent, resumeSent := make(chan struct{}), make(chan struct{})
				var retiredSnapshot chan struct{}
				if tc.retireLocal {
					retiredSnapshot = make(chan struct{})
				}
				remote.onSend = hybridParkedRequestHandler(nil, nil, map[protocol.ParkedRouteAction]chan struct{}{
					protocol.ParkedRoutePrepare: prepareSent,
					protocol.ParkedRouteResume:  resumeSent,
				})
				if tc.retireLocal {
					parkedHandler := remote.onSend
					remote.onSend = func(frame wire.Frame) {
						parkedHandler(frame)
						if frame.Type == wire.MsgRecentRouteSnapshot {
							snapshot, err := wire.UnmarshalRecentRouteSnapshot(frame.Payload)
							if err == nil && snapshot.Generation > 2 && len(snapshot.Entries) == 0 {
								select {
								case <-retiredSnapshot:
								default:
									close(retiredSnapshot)
								}
							}
						}
					}
				}
				remote.recvs = append(remote.recvs,
					recvItem{f: frameOf(wire.MsgParkedRouteResponse, wire.MarshalParkedRouteResponse(protocol.ParkedRouteResponse{RequestID: 1, Status: protocol.ParkedRouteReady})), wait: prepareSent},
					recvItem{f: frameOf(wire.MsgParkedRouteResponse, wire.MarshalParkedRouteResponse(protocol.ParkedRouteResponse{RequestID: 2, Status: protocol.ParkedRouteResumed})), wait: resumeSent},
					recvItem{f: frameOf(wire.MsgDetached, wire.MarshalDetached(protocol.Detached{Reason: protocol.ReasonDetach})), wait: retiredSnapshot},
				)
				remoteDialer.trs = []wire.Transport{markedDatagramTransport{Transport: remote}}
			}
			localPicker := &recordingTransport{recvs: []recvItem{{f: hybridWelcomeFrame("local", localLifecycle)}}}
			if tc.rename {
				localPicker.recvs = append(localPicker.recvs, recvItem{f: frameOf(wire.MsgCommittedRouteIdentity, mustMarshalCommittedIdentity(protocol.CommittedRouteIdentity{
					Target: protocol.ExactSessionTarget{LifecycleID: localLifecycle, SessionName: "renamed-local"},
				}))})
			}
			var localRetiredSnapshot chan struct{}
			if tc.retireLocal {
				localRetiredSnapshot = make(chan struct{})
				localPicker.onSend = func(frame wire.Frame) {
					if frame.Type == wire.MsgRecentRouteSnapshot {
						snapshot, err := wire.UnmarshalRecentRouteSnapshot(frame.Payload)
						if err == nil && snapshot.Generation > 2 && len(snapshot.Entries) == 0 {
							select {
							case <-localRetiredSnapshot:
							default:
								close(localRetiredSnapshot)
							}
						}
					}
				}
				payload, err := wire.MarshalRouteRetired(protocol.RouteRetired{Ref: protocol.RouteRef{Key: 1, Generation: 1}, Target: protocol.ExactSessionTarget{LifecycleID: localLifecycle, SessionName: "local"}})
				require.NoError(t, err)
				localPicker.recvs = append(localPicker.recvs, recvItem{f: frameOf(wire.MsgRouteRetired, payload)})
			}
			localDialer := &sequenceDialer{trs: []wire.Transport{initial, localPicker}}
			var localDestination *recordingTransport
			if tc.selectLocal {
				selected := protocol.ExactSessionTarget{LifecycleID: domain.SessionLifecycleID{3}, SessionName: "destination"}
				localPicker.recvs = append(localPicker.recvs, recvItem{f: frameOf(wire.MsgAttachTarget, wire.MarshalAttachTarget(protocol.AttachTarget{
					Session: selected.SessionName, Intent: protocol.IntentAttach, ExactTarget: &selected,
					EnvironmentPolicy: protocol.EnvironmentPolicyDaemonOwned, PreferredTabID: "chosen-tab",
				}))})
				localDestination = &recordingTransport{recvs: []recvItem{
					{f: hybridWelcomeFrame(selected.SessionName, selected.LifecycleID)},
					{f: frameOf(wire.MsgDetached, wire.MarshalDetached(protocol.Detached{Reason: protocol.ReasonDetach}))},
				}}
				localDialer.trs = append(localDialer.trs, localDestination)
			} else {
				localPicker.recvs = append(localPicker.recvs, recvItem{f: navigationDirectiveFrame(protocol.NavigationBack), wait: localRetiredSnapshot})
			}
			clock := &reconnectTestClock{}
			deps := hybridPickerDependencies(localDialer, term, clock, map[string]ports.ClientDialer{"igor": remoteDialer})
			require.NoError(t, runTestClient(t.Context(), deps, client.AttachRequest{
				Intent: protocol.IntentAttach, SessionName: "local", Origin: protocol.RouteOriginLocal, OriginKey: "local",
			}))

			var source protocol.RecentRouteSnapshot
			for _, sent := range remote.Sends() {
				if sent.Type == wire.MsgRecentRouteSnapshot {
					var err error
					source, err = wire.UnmarshalRecentRouteSnapshot(sent.Payload)
					require.NoError(t, err)
					break
				}
			}
			require.Equal(t, "misc", source.ActiveEntry.Name)
			publications := 0
			for _, sent := range localPicker.Sends() {
				if sent.Type != wire.MsgRecentRouteSnapshot {
					continue
				}
				publications++
				snapshot, err := wire.UnmarshalRecentRouteSnapshot(sent.Payload)
				require.NoError(t, err)
				if tc.retireLocal && publications > 1 {
					require.Empty(t, snapshot.Entries)
					require.Equal(t, source.Active, snapshot.Active)
				} else {
					require.Equal(t, source, snapshot, "rendering the home picker must not commit its backing session as the active route")
				}
			}
			require.Positive(t, publications)
			if tc.retireLocal {
				require.Greater(t, publications, 1)
				returned := remoteReturn
				if tc.udp {
					returned = remote
				}
				var latest protocol.RecentRouteSnapshot
				for _, sent := range returned.Sends() {
					if sent.Type == wire.MsgRecentRouteSnapshot {
						var err error
						latest, err = wire.UnmarshalRecentRouteSnapshot(sent.Payload)
						require.NoError(t, err)
					}
				}
				require.Equal(t, source.ActiveEntry.Target, latest.ActiveEntry.Target)
				require.Empty(t, latest.Entries, "returning to remote must publish the pruned ledger")
			}
			wantDials := int32(2)
			if tc.udp || tc.selectLocal {
				wantDials = 1
			}
			require.Equal(t, wantDials, remoteDialer.calls.Load())
			if localDestination != nil {
				hello := helloFromSend(t, localDestination)
				require.Equal(t, protocol.StartupOverlayNone, hello.StartupOverlay)
				require.Equal(t, domain.TabStableID("chosen-tab"), hello.PreferredTabID)
				require.Zero(t, hello.NavigationCapabilities&protocol.NavigationCapabilityBack)
				var selected protocol.RecentRouteSnapshot
				for _, sent := range localDestination.Sends() {
					if sent.Type == wire.MsgRecentRouteSnapshot {
						var err error
						selected, err = wire.UnmarshalRecentRouteSnapshot(sent.Payload)
						require.NoError(t, err)
					}
				}
				require.Equal(t, "destination", selected.ActiveEntry.Name)
				require.Equal(t, protocol.RouteKindLocal, selected.ActiveEntry.Kind)
				require.Equal(t, source.Active, selected.Previous)
			}
		})
	}
}
