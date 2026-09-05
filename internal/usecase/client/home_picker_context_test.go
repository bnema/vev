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
	}{
		{name: "UDP", udp: true},
		{name: "stdio"},
		{name: "UDP backing session renamed", udp: true, rename: true},
		{name: "stdio backing session renamed", rename: true},
		{name: "stdio select new local destination", selectLocal: true},
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
				remote.onSend = hybridParkedRequestHandler(nil, nil, map[protocol.ParkedRouteAction]chan struct{}{
					protocol.ParkedRoutePrepare: prepareSent,
					protocol.ParkedRouteResume:  resumeSent,
				})
				remote.recvs = append(remote.recvs,
					recvItem{f: frameOf(wire.MsgParkedRouteResponse, wire.MarshalParkedRouteResponse(protocol.ParkedRouteResponse{RequestID: 1, Status: protocol.ParkedRouteReady})), wait: prepareSent},
					recvItem{f: frameOf(wire.MsgParkedRouteResponse, wire.MarshalParkedRouteResponse(protocol.ParkedRouteResponse{RequestID: 2, Status: protocol.ParkedRouteResumed})), wait: resumeSent},
					recvItem{f: frameOf(wire.MsgDetached, wire.MarshalDetached(protocol.Detached{Reason: protocol.ReasonDetach}))},
				)
				remoteDialer.trs = []wire.Transport{markedDatagramTransport{Transport: remote}}
			}
			localPicker := &recordingTransport{recvs: []recvItem{{f: hybridWelcomeFrame("local", localLifecycle)}}}
			if tc.rename {
				localPicker.recvs = append(localPicker.recvs, recvItem{f: frameOf(wire.MsgCommittedRouteIdentity, mustMarshalCommittedIdentity(protocol.CommittedRouteIdentity{
					Target: protocol.ExactSessionTarget{LifecycleID: localLifecycle, SessionName: "renamed-local"},
				}))})
			}
			localDialer := &sequenceDialer{trs: []wire.Transport{initial, localPicker}}
			var localDestination *recordingTransport
			if tc.selectLocal {
				selected := protocol.ExactSessionTarget{LifecycleID: domain.SessionLifecycleID{3}, SessionName: "destination"}
				localPicker.recvs = append(localPicker.recvs, recvItem{f: frameOf(wire.MsgAttachTarget, wire.MarshalAttachTarget(protocol.AttachTarget{
					Session: selected.SessionName, Intent: protocol.IntentAttach, ExactTarget: &selected,
					EnvironmentPolicy: protocol.EnvironmentPolicyDaemonOwned, SamePeer: true,
				}))})
				localDestination = &recordingTransport{recvs: []recvItem{
					{f: hybridWelcomeFrame(selected.SessionName, selected.LifecycleID)},
					{f: frameOf(wire.MsgDetached, wire.MarshalDetached(protocol.Detached{Reason: protocol.ReasonDetach}))},
				}}
				localDialer.trs = append(localDialer.trs, localDestination)
			} else {
				localPicker.recvs = append(localPicker.recvs, recvItem{f: navigationDirectiveFrame(protocol.NavigationBack)})
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
				require.Equal(t, source, snapshot, "rendering the home picker must not commit its backing session as the active route")
			}
			require.Positive(t, publications)
			wantDials := int32(2)
			if tc.udp || tc.selectLocal {
				wantDials = 1
			}
			require.Equal(t, wantDials, remoteDialer.calls.Load())
			if localDestination != nil {
				hello := helloFromSend(t, localDestination)
				require.Equal(t, protocol.StartupOverlayNone, hello.StartupOverlay)
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
