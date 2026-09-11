package client_test

import (
	"context"
	"sync"
	"testing"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	portsmocks "github.com/bnema/vev/internal/ports/mocks"
	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/protocol/wire"
	"github.com/bnema/vev/internal/usecase/client"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestInventoryFailureRestoresLatestRemoteIdentityAfterResumeRejection(t *testing.T) {
	term := newRunTerminal()
	defer term.in.unblock()
	local := protocol.ExactSessionTarget{LifecycleID: domain.SessionLifecycleID{1}, SessionName: "local"}
	first := protocol.ExactSessionTarget{LifecycleID: domain.SessionLifecycleID{2}, SessionName: "remote-first"}
	current := protocol.ExactSessionTarget{LifecycleID: domain.SessionLifecycleID{3}, SessionName: "remote-current"}
	welcome := func(target protocol.ExactSessionTarget) wire.Frame {
		return frameOf(wire.MsgWelcome, wire.MarshalWelcome(protocol.Welcome{SessionID: target.SessionName, SessionName: target.SessionName, ResumeToken: 17, Capabilities: protocol.CapabilityResume, CommittedIdentity: &protocol.CommittedRouteIdentity{Target: target}}))
	}
	rejected := func() wire.Frame {
		return frameOf(wire.MsgError, wire.MarshalErrorMsg(protocol.ErrorMsg{Code: protocol.ErrNoSuchTarget, Text: "unavailable"}))
	}
	initial := &recordingTransport{recvs: []recvItem{{f: welcome(local)}, {f: frameOf(wire.MsgAttachTarget, wire.MarshalAttachTarget(protocol.AttachTarget{Endpoint: "remote", Session: first.SessionName, Intent: protocol.IntentAttach, EnvironmentPolicy: protocol.EnvironmentPolicyDaemonOwned}))}}}
	published := make(chan struct{})
	var once sync.Once
	selection := protocol.NavigationInventorySelection{InteractionGeneration: 1, PublicationGeneration: 1, SourceKey: "local", EntryKey: "entry"}
	remote := &recordingTransport{recvs: []recvItem{
		{f: welcome(first)},
		{f: frameOf(wire.MsgCommittedRouteIdentity, mustMarshalCommittedIdentity(protocol.CommittedRouteIdentity{Target: current}))},
		{f: frameOf(wire.MsgRoutePosition, mustMarshalRoutePosition(protocol.RoutePosition{Target: current, ActiveTabID: "remembered-tab"}))},
		{f: frameOf(wire.MsgNavigationInventoryDemand, wire.MarshalNavigationInventoryDemand(protocol.NavigationInventoryDemand{InteractionGeneration: 1, Open: true}))},
		{f: frameOf(wire.MsgNavigationInventorySelection, wire.MarshalNavigationInventorySelection(selection)), wait: published},
	}, stall: make(chan struct{})}
	release := make(chan struct{})
	remote.stall = release
	remote.onClose = func() {
		select {
		case <-release:
		default:
			close(release)
		}
	}
	remote.onSend = func(f wire.Frame) {
		if f.Type == wire.MsgNavigationInventoryPublication {
			once.Do(func() { close(published) })
		}
	}
	failed := &recordingTransport{recvs: []recvItem{{f: rejected()}}}
	resume := &recordingTransport{recvs: []recvItem{{f: rejected()}}}
	restored := &recordingTransport{recvs: []recvItem{{f: welcome(current)}, {f: frameOf(wire.MsgDetached, wire.MarshalDetached(protocol.Detached{Reason: protocol.ReasonDetach}))}}}
	localDialer := &sequenceDialer{trs: []wire.Transport{initial, failed}}
	remoteDialer := &sequenceDialer{trs: []wire.Transport{remote, resume, restored}}
	control := portsmocks.NewMockClientDialer(t)
	control.EXPECT().Dial(mock.Anything).RunAndReturn(func(context.Context) (ports.ClientConnection, error) {
		conn := portsmocks.NewMockClientConnection(t)
		var request protocol.NavigationInventoryRequest
		conn.EXPECT().SendClient(mock.Anything).RunAndReturn(func(m protocol.ClientMessage) error { request = m.(protocol.NavigationInventoryRequest); return nil }).Once()
		conn.EXPECT().ReceiveServer().RunAndReturn(func() (protocol.ServerMessage, error) {
			response := protocol.NavigationInventoryResponse{RequestID: request.RequestID, Operation: request.Operation, Status: protocol.NavigationInventoryOK}
			if request.Operation == protocol.NavigationInventorySnapshot {
				response.Groups = []protocol.NavigationInventorySourceGroup{{SourceKey: "local", Status: protocol.NavigationInventorySourceOK, Entries: []protocol.NavigationInventoryEntry{{SourceKey: "local", EntryKey: "entry", Name: "local", DisplayOrigin: "local", State: "up"}}}}
			} else {
				response.Resolved = &protocol.AttachTarget{Session: local.SessionName, Intent: protocol.IntentAttach, ExactTarget: &local, EnvironmentPolicy: protocol.EnvironmentPolicyClientOwned}
			}
			return response, nil
		}).Once()
		conn.EXPECT().Close().Return(nil).Maybe()
		return conn, nil
	}).Twice()
	deps := testDependencies(localDialer, term, realClock{}, nil, nil)
	deps.LocalControlDialer = control
	deps.HostRegistry = dialerForEndpoints(map[string]ports.ClientDialer{"remote": remoteDialer})
	require.NoError(t, runTestClient(context.Background(), deps, client.AttachRequest{Intent: protocol.IntentAttach, SessionName: local.SessionName, Origin: protocol.RouteOriginLocal, OriginKey: "local"}))
	for _, tr := range []*recordingTransport{resume, restored} {
		hello := helloFromSend(t, tr)
		require.Equal(t, &current, hello.ExactTarget)
		require.Equal(t, current.SessionName, hello.Name)
		require.Equal(t, domain.TabStableID("remembered-tab"), hello.PreferredTabID)
	}
	require.Equal(t, protocol.IntentAttach, helloFromSend(t, restored).Intent)
}
