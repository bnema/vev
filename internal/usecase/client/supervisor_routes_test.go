package client

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/protocol/catalogue"
)

// Supervisor halves of Plan 003 C4/C5/E3: the route snapshot published to the
// serving daemon while attached, and the daemon's navigation requests settled
// through the broker like a picker commit. They drive the real supervisor,
// host, foreground, and worker with bounded waits and no sleeps.

// routeTestCatalogue is a local daemon serving the attached "alpha" plus one
// remote daemon with "rem", whose bell is remoteBell.
func routeTestCatalogue(revision ports.BrokerRevision, remoteBell bool) ports.BrokerSnapshot {
	snapshot := routeTestSnapshot(
		routeTestLocal(routeTestLive("alpha", 1, 2), routeTestLive("beta", 2, 1)),
		routeTestRemoteHost(routeTestLive("rem", 3, 0, remoteBell)),
	)
	snapshot.Revision = revision
	return snapshot
}

// sentRouteSnapshots returns every route snapshot the attachment published.
func sentRouteSnapshots(stream *sessionTestStream) []protocol.RecentRouteSnapshot {
	var out []protocol.RecentRouteSnapshot
	for _, message := range stream.messages() {
		if snapshot, ok := message.(protocol.RecentRouteSnapshot); ok {
			out = append(out, snapshot)
		}
	}
	return out
}

func awaitRouteSnapshot(t *testing.T, stream *sessionTestStream, what string, match func(protocol.RecentRouteSnapshot) bool) protocol.RecentRouteSnapshot {
	t.Helper()
	var found protocol.RecentRouteSnapshot
	require.Eventually(t, func() bool {
		for _, snapshot := range sentRouteSnapshots(stream) {
			if match(snapshot) {
				found = snapshot
				return true
			}
		}
		return false
	}, 5*time.Second, time.Millisecond, "client never published %s", what)
	return found
}

func routeEntryNamed(snapshot protocol.RecentRouteSnapshot, name string) (protocol.RecentRouteEntry, bool) {
	for _, entry := range snapshot.Entries {
		if entry.Name == name {
			return entry, true
		}
	}
	return protocol.RecentRouteEntry{}, false
}

// TestSupervisorPublishesRoutesWhileAttached ports main's local/remote
// attention publication: the first snapshot follows the committed initial
// publication, and a broker publication while attached republishes it with
// the other daemon's bell.
func TestSupervisorPublishesRoutesWhileAttached(t *testing.T) {
	picker := newAttachTestPicker()
	harness := startAttachHarness(t, picker)
	harness.service.publishSnapshot(routeTestCatalogue(2, false))
	stream := attachLiveSession(t, harness, picker)

	first := awaitRouteSnapshot(t, stream, "the first route snapshot", func(s protocol.RecentRouteSnapshot) bool {
		_, ok := routeEntryNamed(s, "rem")
		return ok
	})
	require.NoError(t, first.Validate())
	require.Equal(t, "alpha", first.ActiveEntry.Name, "the attached lifecycle is metadata only")
	_, listed := routeEntryNamed(first, "alpha")
	require.False(t, listed)
	remote, _ := routeEntryNamed(first, "rem")
	require.False(t, remote.Attention)
	require.Equal(t, protocol.RouteKindRemote, remote.Kind)

	harness.service.publishSnapshot(routeTestCatalogue(3, true))
	rung := awaitRouteSnapshot(t, stream, "the remote bell", func(s protocol.RecentRouteSnapshot) bool {
		entry, ok := routeEntryNamed(s, "rem")
		return ok && entry.Attention
	})
	require.Greater(t, rung.Generation, first.Generation)
	require.Equal(t, first.Active, rung.Active, "the attachment keeps its reference")

	harness.service.publishSnapshot(routeTestCatalogue(4, true))
	require.Never(t, func() bool { return len(sentRouteSnapshots(stream)) > 2 }, 50*time.Millisecond, time.Millisecond,
		"an unchanged catalogue publishes nothing")
	require.Len(t, harness.service.openedRequests(), 1)
}

// TestSupervisorSettlesDaemonNavigation pins C5 and the E3 palette commits:
// the daemon's requests resolve through the broker catalogue and open through
// the broker exactly like a picker commit; a refusal is answered typed.
func TestSupervisorSettlesDaemonNavigation(t *testing.T) {
	beta := protocol.ExactSessionTarget{LifecycleID: pickerTestLifecycle(2), SessionName: "beta"}
	tests := []struct {
		name string
		// request builds the daemon message from the first route snapshot.
		request   func(protocol.RecentRouteSnapshot) protocol.ServerMessage
		detached  bool
		wantOpen  func(*testing.T, ports.BrokerOpenStreamRequest)
		wantReply func(*testing.T, *sessionTestStream)
	}{
		{
			name: "recent route to another daemon swaps",
			request: func(s protocol.RecentRouteSnapshot) protocol.ServerMessage {
				entry, _ := routeEntryNamed(s, "rem")
				return protocol.RouteNavigationAction{SnapshotGeneration: s.Generation, Key: entry.Key, Generation: entry.Generation}
			},
			wantOpen: func(t *testing.T, request ports.BrokerOpenStreamRequest) {
				require.False(t, request.Local)
				require.Equal(t, routeTestRemote, request.Endpoint)
				require.Equal(t, ports.BrokerAdmissionExact, request.Admission)
				require.Equal(t, "rem", request.Target.SessionName)
			},
		},
		{
			name: "a stale reference is refused typed",
			request: func(s protocol.RecentRouteSnapshot) protocol.ServerMessage {
				return protocol.RouteNavigationAction{SnapshotGeneration: s.Generation, Key: 999, Generation: 1}
			},
			wantReply: func(t *testing.T, stream *sessionTestStream) {
				failure := awaitSent(t, stream, "RouteNavigationFailure", isSent[protocol.RouteNavigationFailure]).(protocol.RouteNavigationFailure)
				require.Equal(t, protocol.RouteNavigationFailure{Key: 999, Generation: 1, Code: protocol.RouteFailureStaleSelection}, failure)
			},
		},
		{
			name: "create on another host",
			request: func(s protocol.RecentRouteSnapshot) protocol.ServerMessage {
				host := s.Hosts[0]
				return protocol.RouteCreateSessionAction{RequestID: 4, SnapshotGeneration: s.Generation, Key: host.Key, Generation: host.Generation, SessionName: "fresh"}
			},
			wantOpen: func(t *testing.T, request ports.BrokerOpenStreamRequest) {
				require.Equal(t, routeTestRemote, request.Endpoint)
				require.Equal(t, ports.BrokerAdmissionCreateNamed, request.Admission)
				require.Equal(t, "fresh", request.Name)
			},
		},
		{
			name: "create with a stale host is refused typed",
			request: func(s protocol.RecentRouteSnapshot) protocol.ServerMessage {
				return protocol.RouteCreateSessionAction{RequestID: 4, SnapshotGeneration: s.Generation, Key: 999, Generation: 1, SessionName: "fresh"}
			},
			wantReply: func(t *testing.T, stream *sessionTestStream) {
				failure := awaitSent(t, stream, "SessionCreationFailure", isSent[protocol.SessionCreationFailure]).(protocol.SessionCreationFailure)
				require.Equal(t, protocol.SessionCreationFailure{RequestID: 4, Code: protocol.RouteFailureStaleSelection}, failure)
			},
		},
		{
			name: "a same-peer offer is confirmed in place",
			request: func(protocol.RecentRouteSnapshot) protocol.ServerMessage {
				target := beta
				return protocol.AttachTarget{Session: "beta", Intent: protocol.IntentAttach, ExactTarget: &target, EnvironmentPolicy: protocol.EnvironmentPolicyDaemonOwned, SamePeer: true}
			},
			wantReply: func(t *testing.T, stream *sessionTestStream) {
				request := awaitSent(t, stream, "SamePeerSwitchRequest", isSent[protocol.SamePeerSwitchRequest]).(protocol.SamePeerSwitchRequest)
				require.Equal(t, beta, request.Target)
				require.NotZero(t, request.RequestID)
			},
		},
		{
			name: "a close-and-dial handoff reopens on the serving daemon",
			request: func(protocol.RecentRouteSnapshot) protocol.ServerMessage {
				target := beta
				return protocol.AttachTarget{Session: "beta", Intent: protocol.IntentAttach, ExactTarget: &target, EnvironmentPolicy: protocol.EnvironmentPolicyDaemonOwned, PreferredTabID: "t_x"}
			},
			detached: true,
			wantOpen: func(t *testing.T, request ports.BrokerOpenStreamRequest) {
				require.True(t, request.Local)
				require.Equal(t, ports.BrokerAdmissionExact, request.Admission)
				require.Equal(t, beta, request.Target)
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			picker := newAttachTestPicker()
			harness := startAttachHarness(t, picker)
			harness.service.publishSnapshot(routeTestCatalogue(2, false))
			stream := attachLiveSession(t, harness, picker)
			snapshot := awaitRouteSnapshot(t, stream, "the route snapshot", func(s protocol.RecentRouteSnapshot) bool {
				_, ok := routeEntryNamed(s, "rem")
				return ok && len(s.Hosts) == 1
			})

			var mu sync.Mutex
			var second *sessionTestStream
			harness.service.setOpenStream(func(context.Context, ports.BrokerOpenStreamRequest) (ports.BrokerLogicalConnection, error) {
				mu.Lock()
				defer mu.Unlock()
				second = newSessionTestStream()
				return second, nil
			})
			stream.deliver(tt.request(snapshot))
			if tt.detached {
				// The daemon ends the source right after a close-and-dial
				// handoff; the supervisor still adopts the handoff.
				stream.deliver(protocol.Detached{Reason: protocol.ReasonDetach})
			}

			if tt.wantOpen != nil {
				require.Eventually(t, func() bool { return len(harness.service.openedRequests()) == 2 }, 5*time.Second, time.Millisecond, "the navigation never opened a stream")
				opened := harness.service.openedRequests()
				tt.wantOpen(t, opened[1])
				awaitStreamHello(t, &mu, &second)
				if tt.detached {
					mu.Lock()
					hello := second.messages()[0].(protocol.Hello)
					mu.Unlock()
					require.Equal(t, domain.TabStableID("t_x"), hello.PreferredTabID)
				} else {
					awaitSent(t, stream, "Detach", isSent[protocol.Detach])
				}
				return
			}
			tt.wantReply(t, stream)
			require.Never(t, func() bool { return len(harness.service.openedRequests()) > 1 }, 50*time.Millisecond, time.Millisecond, "a refusal or an in-place switch never reconnects")
			require.Zero(t, countSent[protocol.Detach](stream))
		})
	}
}

// TestSupervisorRouteSnapshotExcludesServingHost pins E3's destinations: a
// remote attachment offers the local daemon as a creation host and never
// itself.
func TestSupervisorRouteSnapshotExcludesServingHost(t *testing.T) {
	picker := newAttachTestPicker()
	harness := startAttachHarness(t, picker)
	remote := routeTestRemoteHost(catalogue.RemoteCatalogSession{LifecycleID: domain.SessionLifecycleID{1}, Name: "alpha", State: catalogue.RemoteCatalogSessionUp})
	remote.Registration = sessionTestRequest(false).Registration
	remote.Endpoint = sessionTestRequest(false).Endpoint
	remote.DisplayOrigin = "example"
	snapshot := routeTestSnapshot(routeTestLocal(routeTestLive("loc", 4, 1)), remote)
	snapshot.Revision = 2
	harness.service.publishSnapshot(snapshot)

	stream := newSessionTestStream()
	harness.service.setOpenStream(func(context.Context, ports.BrokerOpenStreamRequest) (ports.BrokerLogicalConnection, error) {
		return stream, nil
	})
	picker.commit(sessionTestRequest(false))
	awaitStreamHello(t, &sync.Mutex{}, &stream)
	deliverReadyStream(t, stream)
	awaitAttachedState(t, harness.sup)

	published := awaitRouteSnapshot(t, stream, "the route snapshot", func(s protocol.RecentRouteSnapshot) bool { return len(s.Hosts) > 0 })
	require.Equal(t, []protocol.RouteHost{{Key: published.Hosts[0].Key, Generation: 1, Label: "local", Kind: protocol.RouteKindLocal}}, published.Hosts)
	require.Equal(t, protocol.RouteKindRemote, published.ActiveEntry.Kind)
	require.Equal(t, "example", published.ActiveEntry.HostLabel)
	require.True(t, published.Home.IsZero(), "only a local attachment is home")
}

// TestSupervisorSamePeerSwitchSettlesUIAction pins the driver action that
// causes a daemon same-peer switch (palette "Switch to session" on the serving
// daemon): the attachment is kept, so the action must follow the offer and
// settle on the destination's first committed output — never on a source
// publication or a source-boundary receipt — and a refused switch fails it.
func TestSupervisorSamePeerSwitchSettlesUIAction(t *testing.T) {
	beta := protocol.ExactSessionTarget{LifecycleID: pickerTestLifecycle(2), SessionName: "beta"}
	tests := []struct {
		name string
		// settle delivers the daemon's answer to the confirmed switch.
		settle func(*sessionTestStream, protocol.SamePeerSwitchRequest)
		// sourceReceipt delivers a daemon receipt on the source boundary; the
		// daemon may also lose the source fence and never answer it.
		sourceReceipt bool
		wantErr       ports.UIErrorCode
	}{
		{
			name: "destination output completes the action without a daemon receipt",
			settle: func(stream *sessionTestStream, _ protocol.SamePeerSwitchRequest) {
				destination := sessionTestOutput(4, "\x1b[Hbeta")
				destination.Epoch = 2
				destination.Context.Route.Target = beta
				stream.deliver(destination)
			},
		},
		{
			name:          "a source-boundary receipt waits for the destination output",
			sourceReceipt: true,
			settle: func(stream *sessionTestStream, _ protocol.SamePeerSwitchRequest) {
				destination := sessionTestOutput(4, "\x1b[Hbeta")
				destination.Epoch = 2
				destination.Context.Route.Target = beta
				stream.deliver(destination)
			},
		},
		{
			name:          "a refused switch fails the action",
			sourceReceipt: true,
			settle: func(stream *sessionTestStream, request protocol.SamePeerSwitchRequest) {
				stream.deliver(protocol.SamePeerSwitchFailure{RequestID: request.RequestID, Code: protocol.SamePeerSwitchStaleTarget})
			},
			wantErr: ports.UIErrNavigationFailed,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			picker := newAttachTestPicker()
			var ui *UI
			harness := startAttachHarnessConfig(t, picker, func(cfg *SupervisorConfig) {
				ui = NewUI(cfg.Terminal.(ports.UIState), cfg.Clock)
				cfg.UI = ui
			})
			stream := attachLiveSession(t, harness, picker)
			attached := sessionTestOutput(2, "\x1b[Hattached")
			attached.Full, attached.Base, attached.New = false, 1, 2
			stream.deliver(attached)
			var snapshot ports.UISnapshot
			require.Eventually(t, func() bool {
				var err error
				snapshot, err = ui.Capture(ui.Handle())
				return err == nil && snapshot.Context.Status == ports.UIStatusAttached && snapshot.Context.ViewPublication == 2
			}, 5*time.Second, time.Millisecond)

			type actionOutcome struct {
				result ports.UIActionResult
				err    error
			}
			done := make(chan actionOutcome, 1)
			go func() {
				result, err := ui.Action(t.Context(), ports.UIActionRequest{Attachment: ui.Handle(), Generation: snapshot.Context.Generation, Keys: []string{"Enter"}})
				done <- actionOutcome{result: result, err: err}
			}()
			var actionID uint64
			require.Eventually(t, func() bool {
				for _, message := range stream.messages() {
					if fence, ok := message.(protocol.UIFence); ok && fence.ActionID != 0 {
						actionID = fence.ActionID
						return true
					}
				}
				return false
			}, 5*time.Second, time.Millisecond, "the action never reached the daemon")

			target := beta
			stream.deliver(protocol.AttachTarget{Session: "beta", Intent: protocol.IntentAttach, ExactTarget: &target, EnvironmentPolicy: protocol.EnvironmentPolicyDaemonOwned, SamePeer: true, CauseActionID: actionID})
			request := awaitSent(t, stream, "SamePeerSwitchRequest", isSent[protocol.SamePeerSwitchRequest]).(protocol.SamePeerSwitchRequest)

			// A source publication and a receipt on the source boundary arrive
			// before the destination: neither may settle the navigating action.
			source := sessionTestOutput(3, "\x1b[Hsource")
			source.Full, source.Base, source.New = false, 2, 3
			stream.deliver(source)
			if tt.sourceReceipt {
				stream.deliver(protocol.UIReceipt{ActionID: actionID, Epoch: 1, State: 3, ViewPublication: 3, Outcome: protocol.UIReceiptProcessed})
			}
			require.Eventually(t, func() bool {
				ui.mu.Lock()
				defer ui.mu.Unlock()
				return ui.boundary.Context.ViewPublication == 3
			}, 5*time.Second, time.Millisecond, "the source publication was never committed")
			select {
			case outcome := <-done:
				t.Fatalf("the action settled before the destination: %+v %v", outcome.result, outcome.err)
			default:
			}

			tt.settle(stream, request)
			var outcome actionOutcome
			select {
			case outcome = <-done:
			case <-time.After(5 * time.Second):
				t.Fatal("the same-peer switch never settled the UI action")
			}
			if tt.wantErr != "" {
				var uiErr *ports.UIError
				require.ErrorAs(t, outcome.err, &uiErr)
				require.Equal(t, tt.wantErr, uiErr.Code)
				require.Equal(t, actionID, uiErr.ActionID)
				return
			}
			require.NoError(t, outcome.err)
			require.Equal(t, ports.UIActionProcessed, outcome.result.Status)
			require.Equal(t, beta, outcome.result.Context.Route.Target)
			require.Equal(t, snapshot.Context.Generation, outcome.result.Context.Generation, "an in-place switch keeps the actionable generation")
		})
	}
}
