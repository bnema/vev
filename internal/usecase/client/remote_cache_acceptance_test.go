package client_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/protocol/wire"
	"github.com/bnema/vev/internal/usecase/client"
	"github.com/stretchr/testify/require"
)

// Only the cache TTL is manually fired; ordinary protocol budgets retain their
// normal clocks. This avoids sleeping through expiration or firing unrelated
// handshake timers while testing ownership transfer.
type acceptanceCacheClock struct {
	realClock
	mu     sync.Mutex
	expiry chan time.Time
}
type acceptanceCacheTimer struct{ ch chan time.Time }

func (t acceptanceCacheTimer) C() <-chan time.Time    { return t.ch }
func (acceptanceCacheTimer) Stop() bool               { return true }
func (acceptanceCacheTimer) Reset(time.Duration) bool { return false }
func (c *acceptanceCacheClock) NewTimer(d time.Duration) ports.Timer {
	if d != 7*time.Minute {
		return c.realClock.NewTimer(d)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.expiry = make(chan time.Time, 1)
	return acceptanceCacheTimer{c.expiry}
}
func (c *acceptanceCacheClock) expire(t *testing.T) {
	t.Helper()
	c.mu.Lock()
	ch := c.expiry
	c.mu.Unlock()
	require.NotNil(t, ch)
	ch <- time.Now()
}

func TestRunnerRemoteCacheFallbackAndOwnership(t *testing.T) {
	for _, scenario := range []string{"expiry", "dormant death", "stale lifecycle", "activation rejected"} {
		t.Run(scenario, func(t *testing.T) {
			term := newHybridPickerTerminal()
			defer term.reader.unblock()
			local1, local2 := newHybridPickerTransport(), newHybridPickerTransport()
			remote1, remote2 := newHybridPickerTransport(), newHybridPickerTransport()
			localDialer := &sequenceDialer{trs: []wire.Transport{local1, local2}}
			remoteDialer := &sequenceDialer{trs: []wire.Transport{remote1, remote2}}
			clk := &acceptanceCacheClock{}
			deps := testDependencies(localDialer, term, clk, nil, nil)
			deps.AttachmentCacheTTL = 7 * time.Minute
			deps.HostRegistry = stubHostRegistry{resolve: func(string) (ports.RemoteEndpointBinding, error) {
				return ports.RemoteEndpointBinding{Dialer: remoteDialer}, nil
			}}
			target := protocol.ExactSessionTarget{LifecycleID: domain.SessionLifecycleID{2}, SessionName: "source"}
			toRemote := protocol.AttachTarget{Endpoint: "remote", Session: "source", Intent: protocol.IntentAttach, ExactTarget: &target, EnvironmentPolicy: protocol.EnvironmentPolicyDaemonOwned}
			local1.push(hybridPickerWelcome("local", domain.SessionLifecycleID{1}))
			local1.push(frameOfMessage(toRemote))
			remote1.push(hybridPickerWelcome("source", target.LifecycleID))
			hybridPickerPaint(t, remote1, 1, target, "remote foreground")
			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan error, 1)
			go func() {
				done <- runTestClient(ctx, deps, client.AttachRequest{Intent: protocol.IntentAttach, SessionName: "local", Origin: protocol.RouteOriginLocal, OriginKey: "local"})
			}()
			defer cancel()
			term.awaitDisplay(t, "remote foreground")
			remote1.push(frameOfMessage(protocol.RouteNavigationAction{SnapshotGeneration: 2, Key: 1, Generation: 1}))
			suspend := mustAwaitSend(t, remote1, "SuspendAttachment").(protocol.SuspendAttachment)
			remote1.push(frameOfMessage(protocol.AttachmentSuspended{RequestID: suspend.RequestID, Target: target}))
			local2.push(hybridPickerWelcome("local", domain.SessionLifecycleID{1}))
			hybridPickerPaint(t, local2, 1, protocol.ExactSessionTarget{LifecycleID: domain.SessionLifecycleID{1}, SessionName: "local"}, "local foreground")
			term.awaitDisplay(t, "local foreground")
			require.Equal(t, int32(1), remoteDialer.calls.Load(), "no background redial")
			// Late paints and route updates from a suspended generation must not
			// affect the terminal or redirect input/resize away from local.
			hybridPickerPaint(t, remote1, 9, target, "FORBIDDEN dormant paint")
			remote1.push(frameOfMessage(protocol.CommittedRouteIdentity{Target: target}))
			remote1.awaitReceived(t)
			term.reader.pressEnter()
			mustAwaitSend(t, local2, "Input")
			geometry := domain.Geometry{Size: domain.Size{Cols: 100, Rows: 30}}
			select {
			case term.resize <- geometry:
			case <-time.After(time.Second):
				t.Fatal("resize not consumed")
			}
			mustAwaitSend(t, local2, "Resize")
			require.False(t, remote1.sentType(t, "Input"))
			require.False(t, remote1.sentType(t, "Resize"))
			switch scenario {
			case "expiry":
				clk.expire(t)
				require.Eventually(t, func() bool { return remote1.closed.Load() == 1 }, time.Second, time.Millisecond)
			case "dormant death":
				// A remote Detached is terminal server traffic, not a client Close call.
				remote1.push(frameOfMessage(protocol.Detached{Reason: protocol.ReasonDetach}))
				require.Eventually(t, func() bool { return remote1.closed.Load() == 1 }, time.Second, time.Millisecond)
			case "stale lifecycle":
				replacement := target
				replacement.LifecycleID[0]++
				toRemote.ExactTarget = &replacement
			}
			require.Equal(t, int32(1), remoteDialer.calls.Load(), "eviction must not reconnect in the background")
			local2.push(frameOfMessage(toRemote))
			if scenario == "activation rejected" {
				activation := mustAwaitSend(t, remote1, "ActivateAttachment").(protocol.ActivateAttachment)
				require.Equal(t, target, activation.Target)
				remote1.push(frameOfMessage(protocol.ErrorMsg{Text: "stale lifecycle"}))
			}
			hello := mustAwaitSend(t, remote2, "Hello").(protocol.Hello)
			require.Equal(t, toRemote.ExactTarget, hello.ExactTarget, "fallback must preserve authoritative exact lifecycle")
			require.Equal(t, toRemote.PreferredTabID, hello.PreferredTabID, "cold attach must apply the preferred tab or its normal missing-tab fallback")
			require.Equal(t, int32(2), remoteDialer.calls.Load())
			remote2.push(hybridPickerWelcome("source", toRemote.ExactTarget.LifecycleID))
			hybridPickerPaint(t, remote2, 1, *toRemote.ExactTarget, "fallback foreground")
			term.awaitDisplay(t, "fallback foreground")
			if scenario == "stale lifecycle" {
				require.False(t, remote1.sentType(t, "ActivateAttachment"))
			}
			remote2.push(frameOfMessage(protocol.Detached{Reason: protocol.ReasonDetach}))
			select {
			case err := <-done:
				require.NoError(t, err)
			case <-time.After(5 * time.Second):
				t.Fatal("runner did not exit")
			}
			require.NotContains(t, term.screen(), "FORBIDDEN dormant paint")
			for _, transport := range []*hybridPickerTransport{local1, local2, remote1, remote2} {
				require.Equal(t, int32(1), transport.closed.Load(), "each owned connection closes once")
			}
		})
	}
}
