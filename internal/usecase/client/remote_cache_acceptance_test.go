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

// acceptanceTimerClock hands out registered manual timers for every request.
// Ordinary protocol and toast budgets simply never fire, keeping the ownership
// transfer deterministic; the journey fires exactly the cache idle timer it
// configured, so the fake never keys on a hardcoded duration.
type acceptanceTimerClock struct {
	realClock
	mu     sync.Mutex
	timers []*acceptanceManualTimer
}

type acceptanceManualTimer struct {
	ch chan time.Time
	d  time.Duration
}

func (t *acceptanceManualTimer) C() <-chan time.Time    { return t.ch }
func (*acceptanceManualTimer) Stop() bool               { return true }
func (*acceptanceManualTimer) Reset(time.Duration) bool { return false }

func (c *acceptanceTimerClock) NewTimer(d time.Duration) ports.Timer {
	timer := &acceptanceManualTimer{ch: make(chan time.Time, 1), d: d}
	c.mu.Lock()
	c.timers = append(c.timers, timer)
	c.mu.Unlock()
	return timer
}

// fire delivers the timer most recently armed for d, waiting for it to be
// registered first.
func (c *acceptanceTimerClock) fire(t *testing.T, d time.Duration) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		c.mu.Lock()
		var match *acceptanceManualTimer
		for _, timer := range c.timers {
			if timer.d == d {
				match = timer
			}
		}
		c.mu.Unlock()
		if match != nil {
			match.ch <- time.Now()
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("no timer armed for %s", d)
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
			clk := &acceptanceTimerClock{}
			deps := testDependencies(localDialer, term, clk, nil, nil)
			deps.AttachmentCache = domain.AttachmentCacheConfig{Enabled: true, Capacity: domain.DefaultAttachmentCacheCapacity, IdleTimeout: 7 * time.Minute}
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
				clk.fire(t, deps.AttachmentCache.IdleTimeout)
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

// slowCloseTransport unblocks its reader immediately on Close and then stalls,
// modelling an SSH process whose reaping outlives the logical revocation.
type slowCloseTransport struct {
	*hybridPickerTransport
	release <-chan struct{}
}

func (t *slowCloseTransport) Close() error {
	_ = t.hybridPickerTransport.Close()
	<-t.release
	return nil
}

// TestWarmActivationDeadlineColdDialsWhileCloseStalls proves the client starts
// the cold dial at the warm deadline even when the retained transport's
// physical close has not returned.
func TestWarmActivationDeadlineColdDialsWhileCloseStalls(t *testing.T) {
	term := newHybridPickerTerminal()
	defer term.reader.unblock()
	local1, local2 := newHybridPickerTransport(), newHybridPickerTransport()
	owner := newHybridPickerTransport()
	release := make(chan struct{})
	remote2 := newHybridPickerTransport()
	localDialer := &sequenceDialer{trs: []wire.Transport{local1, local2}}
	remoteDialer := &sequenceDialer{trs: []wire.Transport{&slowCloseTransport{hybridPickerTransport: owner, release: release}, remote2}}
	clk := &acceptanceTimerClock{}
	deps := testDependencies(localDialer, term, clk, nil, nil)
	// No idle timeout: the warm deadline is the only client-local budget here.
	deps.AttachmentCache = domain.DefaultAttachmentCacheConfig()
	deps.HostRegistry = stubHostRegistry{resolve: func(string) (ports.RemoteEndpointBinding, error) {
		return ports.RemoteEndpointBinding{Dialer: remoteDialer}, nil
	}}
	target := protocol.ExactSessionTarget{LifecycleID: domain.SessionLifecycleID{2}, SessionName: "source"}
	toRemote := protocol.AttachTarget{Endpoint: "remote", Session: "source", Intent: protocol.IntentAttach, ExactTarget: &target, EnvironmentPolicy: protocol.EnvironmentPolicyDaemonOwned}
	local1.push(hybridPickerWelcome("local", domain.SessionLifecycleID{1}))
	local1.push(frameOfMessage(toRemote))
	owner.push(hybridPickerWelcome("source", target.LifecycleID))
	hybridPickerPaint(t, owner, 1, target, "owner foreground")
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- runTestClient(ctx, deps, client.AttachRequest{Intent: protocol.IntentAttach, SessionName: "local", Origin: protocol.RouteOriginLocal, OriginKey: "local"})
	}()
	term.awaitDisplay(t, "owner foreground")
	owner.push(frameOfMessage(protocol.RouteNavigationAction{SnapshotGeneration: 2, Key: 1, Generation: 1}))
	suspend := mustAwaitSend(t, owner, "SuspendAttachment").(protocol.SuspendAttachment)
	owner.push(frameOfMessage(protocol.AttachmentSuspended{RequestID: suspend.RequestID, Target: target}))
	local2.push(hybridPickerWelcome("local", domain.SessionLifecycleID{1}))
	hybridPickerPaint(t, local2, 1, protocol.ExactSessionTarget{LifecycleID: domain.SessionLifecycleID{1}, SessionName: "local"}, "local foreground")
	term.awaitDisplay(t, "local foreground")
	// Returning reuses the retained transport, which stalls without answering.
	local2.push(frameOfMessage(toRemote))
	mustAwaitSend(t, owner, "ActivateAttachment")
	clk.fire(t, 2*time.Second)
	hello := mustAwaitSend(t, remote2, "Hello").(protocol.Hello)
	require.Equal(t, toRemote.ExactTarget, hello.ExactTarget)
	require.Equal(t, int32(2), remoteDialer.calls.Load(), "a stalled retained transport must not delay the cold dial")
	require.Equal(t, int32(1), owner.closed.Load(), "the stalled transport is closed before its reaping finishes")
	select {
	case <-release:
		t.Fatal("the physical close returned before the journey asserted the cold dial")
	default:
	}
	remote2.push(hybridPickerWelcome("source", toRemote.ExactTarget.LifecycleID))
	hybridPickerPaint(t, remote2, 1, *toRemote.ExactTarget, "cold foreground")
	term.awaitDisplay(t, "cold foreground")
	close(release)
	cancel()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("runner did not exit")
	}
}
