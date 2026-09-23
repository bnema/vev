package client

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol"
)

func TestP54LifecycleActionsAreDistinct(t *testing.T) {
	for _, tc := range []struct {
		name       string
		action     AttachmentLifecycleActionKind
		wantNotice LifecycleNoticeKind
		wantExit   bool
	}{
		{name: "detach to picker", action: AttachmentDetachToPicker, wantNotice: LifecycleNoticeDetachToPicker},
		{name: "detach and exit", action: AttachmentDetachAndExit, wantNotice: LifecycleNoticeDetachAndExit, wantExit: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			actions := make(chan AttachmentLifecycleAction, 1)
			notices := make(chan LifecycleNotice, 4)
			picker := newAttachTestPicker()
			harness := startAttachHarnessConfig(t, picker, func(cfg *SupervisorConfig) {
				cfg.LifecycleActions = actions
				cfg.NotifyLifecycle = func(notice LifecycleNotice) { notices <- notice }
			})
			stream := newSessionTestStream()
			harness.service.setOpenStream(func(context.Context, ports.BrokerOpenStreamRequest) (ports.BrokerLogicalConnection, error) {
				return stream, nil
			})
			picker.commit(sessionTestRequest(true))
			awaitStreamHello(t, &sync.Mutex{}, &stream)
			deliverReadyStream(t, stream)
			awaitAttachedState(t, harness.sup)
			actions <- AttachmentLifecycleAction{Token: authorityToken(&harness.sup.attachments.authority), Kind: tc.action}
			require.Equal(t, tc.wantNotice, (<-notices).Kind)
			if tc.wantExit {
				require.NoError(t, harness.waitRun(t))
			} else {
				awaitPickerState(t, harness.sup)
				require.True(t, picker.owns())
				require.Len(t, harness.service.openedRequests(), 1, "detach must not auto-reattach")
			}
		})
	}
}

func TestP54AttachmentOutcomesReturnToPicker(t *testing.T) {
	for _, tc := range []struct {
		name   string
		settle func(*sessionTestStream)
		notice LifecycleNoticeKind
	}{
		{name: "session termination", settle: func(s *sessionTestStream) { s.deliver(protocol.Detached{Reason: protocol.ReasonDetach}) }, notice: LifecycleNoticeSessionEnded},
		{name: "session deletion", settle: func(s *sessionTestStream) { s.deliver(protocol.Detached{Reason: protocol.ReasonSessionKilled}) }, notice: LifecycleNoticeSessionEnded},
		{name: "destination failure", settle: func(s *sessionTestStream) {
			s.fail(ports.BrokerStreamLost{Connection: ports.BrokerConnectionID{1}, Stream: 1, Epoch: 3, Cause: domain.RemoteFailureTransport, Err: errors.New("private diagnostic")})
		}, notice: LifecycleNoticeDestinationFailed},
	} {
		t.Run(tc.name, func(t *testing.T) {
			picker := newAttachTestPicker()
			notices := make(chan LifecycleNotice, 4)
			harness := startAttachHarness(t, picker)
			harness.sup.cfg.NotifyLifecycle = func(notice LifecycleNotice) { notices <- notice }
			stream := newSessionTestStream()
			harness.service.setOpenStream(func(context.Context, ports.BrokerOpenStreamRequest) (ports.BrokerLogicalConnection, error) {
				return stream, nil
			})
			picker.commit(sessionTestRequest(true))
			require.Eventually(t, func() bool { return len(stream.messages()) > 0 }, 5*time.Second, time.Millisecond)
			deliverReadyStream(t, stream)
			awaitAttachedState(t, harness.sup)
			tc.settle(stream)
			awaitPickerState(t, harness.sup)
			require.Equal(t, tc.notice, (<-notices).Kind)
			require.Len(t, harness.service.openedRequests(), 1)
		})
	}
}

func TestP54LocalRefusalIsNotReportedAsDestinationFailure(t *testing.T) {
	picker := newAttachTestPicker()
	notices := make(chan LifecycleNotice, 4)
	harness := startAttachHarness(t, picker)
	harness.sup.cfg.NotifyLifecycle = func(notice LifecycleNotice) { notices <- notice }

	// The committed selection is refused locally, before any destination exists:
	// reporting that as a destination failure would name the wrong cause.
	picker.mu.Lock()
	picker.resolveErr = pickerCatalogueError{Code: pickerCatalogueGone, Text: "the committed session is gone"}
	picker.mu.Unlock()
	picker.commit(sessionTestRequest(true))

	awaitPickerState(t, harness.sup)
	require.Equal(t, LifecycleNoticeSelectionUnavailable, (<-notices).Kind)
	require.Empty(t, harness.service.openedRequests(), "a refused selection never dials a destination")
}

// TestP54ResolveInitialUsesRealInitialNavigationResolve proves the closed union
// resolves the no-argument ephemeral creation through a real catalogue: the
// resolution uses the same rules as the interactive picker and applies no
// privileged initial semantics.
func TestP54ResolveInitialUsesRealInitialNavigationResolve(t *testing.T) {
	clock := newSupervisorTestClock()
	catalogue, _ := pickerTestCatalogue(t)
	require.True(t, catalogue.Apply(ports.BrokerSnapshot{Epoch: 3, Revision: 1, Daemons: []ports.BrokerDaemonObservation{pickerTestLocalObservation(clock.Now())}}))
	navigation := InitialNavigation{Kind: InitialNavigationCreateEphemeral, Destination: ports.BrokerEndpointFence{Local: true}}
	request, err := navigation.Resolve(catalogue.Snapshot())
	require.NoError(t, err)
	require.Equal(t, ports.BrokerAdmissionCreateEphemeral, request.Admission)
	require.True(t, request.Local)
	require.Empty(t, request.Name)
	require.Equal(t, protocol.ExactSessionTarget{}, request.Target)
}

func TestP54InitialNavigationCreatesEphemeralThroughBroker(t *testing.T) {
	picker := newAttachTestPicker()
	navigation := InitialNavigation{Kind: InitialNavigationCreateEphemeral, Destination: ports.BrokerEndpointFence{Local: true}}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	clock := newSupervisorTestClock()
	reader := newAttachTestReader()
	terminal := &attachTestTerminal{in: reader}
	service := newSupervisorTestService(ports.BrokerConnectionID{1})
	service.publishSnapshot(localDaemonSnapshot(3, 1))
	stream := newSessionTestStream()
	service.setOpenStream(func(context.Context, ports.BrokerOpenStreamRequest) (ports.BrokerLogicalConnection, error) {
		return stream, nil
	})
	sup := mustSupervisor(t, SupervisorConfig{Connector: newSupervisorTestConnector(func(context.Context, int) (ports.BrokerService, error) { return service, nil }), Terminal: terminal, Clock: clock, Jitter: func() float64 { return 0 }, Picker: picker, InitialNavigation: navigation})
	done := make(chan error, 1)
	go func() { done <- sup.Run(ctx) }()
	require.Eventually(t, func() bool { return len(service.openedRequests()) == 1 }, 5*time.Second, time.Millisecond)
	request := service.openedRequests()[0]
	require.Equal(t, ports.BrokerAdmissionCreateEphemeral, request.Admission)
	require.Equal(t, sessionTestRequest(true).Local, request.Local)
	require.Zero(t, picker.resolveCount(), "initial navigation resolves through the union, never through the picker")
	stream.deliver(protocol.ErrorMsg{Code: protocol.ErrInternal, Text: "creation failed"})
	require.Eventually(t, func() bool { return sup.State().Presentation == PresentPicker }, 5*time.Second, time.Millisecond)
	cancel()
	require.ErrorIs(t, <-done, context.Canceled)
}

func TestP54InitialNavigationIsNotReplayedAfterReconnect(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	clock := newSupervisorTestClock()
	reader := newAttachTestReader()
	terminal := &attachTestTerminal{in: reader}
	picker := newAttachTestPicker()
	navigation := InitialNavigation{Kind: InitialNavigationCreateEphemeral, Destination: ports.BrokerEndpointFence{Local: true}}
	first := newSupervisorTestService(ports.BrokerConnectionID{1})
	first.publishSnapshot(localDaemonSnapshot(1, 1))
	second := newSupervisorTestService(ports.BrokerConnectionID{2})
	stream := newSessionTestStream()
	first.setOpenStream(func(context.Context, ports.BrokerOpenStreamRequest) (ports.BrokerLogicalConnection, error) {
		return stream, nil
	})
	connector := newSupervisorTestConnector(func(_ context.Context, call int) (ports.BrokerService, error) {
		if call == 1 {
			return first, nil
		}
		return second, nil
	})
	sup := mustSupervisor(t, SupervisorConfig{Connector: connector, Terminal: terminal, Clock: clock, Jitter: func() float64 { return 0 }, Picker: picker, InitialNavigation: navigation})
	done := make(chan error, 1)
	go func() { done <- sup.Run(ctx) }()
	require.Equal(t, 1, connector.awaitStart(t))
	require.Eventually(t, func() bool { return len(first.openedRequests()) == 1 }, 5*time.Second, time.Millisecond)
	stream.deliver(protocol.ErrorMsg{Code: protocol.ErrInternal, Text: "creation failed"})
	require.Eventually(t, func() bool { return sup.State().Presentation == PresentPicker }, 5*time.Second, time.Millisecond)
	first.lose(ports.BrokerError{Code: ports.BrokerErrorUnavailable})
	require.Eventually(t, func() bool { return sup.State().Connectivity == ConnectivityRetryWait }, 5*time.Second, time.Millisecond)
	for connector.callCount() < 2 {
		clock.mu.Lock()
		var timer *supervisorTestTimer
		if len(clock.timers) != 0 {
			timer = clock.timers[len(clock.timers)-1]
		}
		clock.mu.Unlock()
		if timer != nil {
			timer.fire()
		}
	}
	second.publishSnapshot(localDaemonSnapshot(2, 1))
	require.Eventually(t, func() bool { return sup.State().Connectivity == ConnectivityReady }, 5*time.Second, time.Millisecond)
	require.Empty(t, second.openedRequests(), "one-shot navigation must not replay on the replacement generation")
	cancel()
	require.ErrorIs(t, <-done, context.Canceled)
}

func TestP54SingleReaderDeliversAttachedByteAfterRealSupervisorHandoff(t *testing.T) {
	picker := newAttachTestPicker()
	harness := startAttachHarness(t, picker)
	stream := newSessionTestStream()
	harness.service.setOpenStream(func(context.Context, ports.BrokerOpenStreamRequest) (ports.BrokerLogicalConnection, error) {
		return stream, nil
	})
	picker.commit(sessionTestRequest(true))
	require.Eventually(t, func() bool { return len(stream.messages()) > 0 }, 5*time.Second, time.Millisecond)
	deliverReadyStream(t, stream)
	awaitAttachedState(t, harness.sup)
	harness.reader.push([]byte("x"))
	require.Eventually(t, func() bool {
		for _, message := range stream.messages() {
			if input, ok := message.(protocol.Input); ok && string(input.Data) == "x" {
				return true
			}
		}
		return false
	}, 5*time.Second, time.Millisecond, "one typed byte must survive the picker-to-attachment claim handoff")
}

func TestP54BrokerRestartDoesNotReattachOrReplayUnknownMutation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	clock := newSupervisorTestClock()
	reader := newSupervisorTestReader()
	t.Cleanup(reader.unblock)
	first := newSupervisorTestService(ports.BrokerConnectionID{1})
	first.publish(1, 1)
	second := newSupervisorTestService(ports.BrokerConnectionID{2})
	connector := newSupervisorTestConnector(func(_ context.Context, call int) (ports.BrokerService, error) {
		if call == 1 {
			return first, nil
		}
		return second, nil
	})
	notices := make(chan LifecycleNotice, 4)
	sup := mustSupervisor(t, SupervisorConfig{Connector: connector, Terminal: newSupervisorTestTerminal(reader), Clock: clock, Jitter: func() float64 { return 0 }, NotifyLifecycle: func(n LifecycleNotice) { notices <- n }})
	done := make(chan error, 1)
	go func() { done <- sup.Run(ctx) }()
	require.Equal(t, 1, connector.awaitStart(t))
	require.Eventually(t, func() bool { return sup.State().Connectivity == ConnectivityReady }, 5*time.Second, time.Millisecond)
	first.lose(ports.BrokerError{Code: ports.BrokerErrorOutcomeUnknown, Text: "reply lost after mutation"})
	require.Equal(t, LifecycleNoticeBrokerLost, (<-notices).Kind)
	retry := clock.awaitTimer(t)
	retry.fire()
	require.Equal(t, 2, connector.awaitStart(t))
	second.publish(2, 1)
	require.Eventually(t, func() bool { return sup.State().Connectivity == ConnectivityReady }, 5*time.Second, time.Millisecond)
	require.Equal(t, LifecycleNoticeBrokerReconnected, (<-notices).Kind)
	require.Empty(t, first.openedRequests())
	require.Empty(t, second.openedRequests(), "restart must not auto-reattach or replay a mutation")
	cancel()
	require.ErrorIs(t, <-done, context.Canceled)
}

func TestP54ReadyMapPrunedAcrossClaims(t *testing.T) {
	pump := newTerminalInputPump(nil)
	for i := 0; i < 100; i++ {
		consumer, ok := pump.tryClaim()
		require.True(t, ok)
		pump.revoke(consumer)
	}
	pump.readyMu.Lock()
	defer pump.readyMu.Unlock()
	require.Empty(t, pump.ready, "revoked monotonic claim IDs must not retain readiness channels")
}

// TestP54PickerCommittedReadIsNotReplayedToAttachment proves a picker read the
// user already acted on is committed at the handoff and never re-delivered as
// session input.
func TestP54PickerCommittedReadIsNotReplayedToAttachment(t *testing.T) {
	reader := newAttachTestReader()
	consumer := &recordingPickerConsumer{reads: make(chan []byte, 1)}
	lifetime := startTerminalInputLifetime(reader, consumer)
	defer lifetime.stop()
	reader.push([]byte("1\r"))
	var committed []byte
	require.Eventually(t, func() bool {
		select {
		case committed = <-consumer.reads:
			return true
		default:
			return false
		}
	}, 5*time.Second, time.Millisecond)
	require.Equal(t, []byte("1\r"), committed)
	lifetime.releasePicker()
	attachment, claimed := lifetime.pump.tryClaim()
	require.True(t, claimed)
	_, ok := lifetime.pump.take(context.Background(), attachment)
	require.False(t, ok, "committed picker read must not cross the handoff")
	lifetime.pump.revoke(attachment)
}

func TestP54AttachmentFinalizeDropsUnleasedPendingBeforePicker(t *testing.T) {
	reader := newAttachTestReader()
	consumer := &recordingPickerConsumer{reads: make(chan []byte, 1)}
	lifetime := startTerminalInputLifetime(reader, consumer)
	defer lifetime.stop()
	lifetime.releasePicker()
	host := newAttachmentHost(attachmentHostConfig{Input: lifetime.pump})
	host.ownerBoundary = true
	fg := host.newForeground(AttachmentToken{Generation: 1}, newSessionTestStream())
	require.True(t, host.authority.grant(fg))
	reader.push([]byte("ls\r"))
	require.Eventually(t, func() bool {
		lifetime.pump.mu.Lock()
		defer lifetime.pump.mu.Unlock()
		return lifetime.pump.pending != nil && lifetime.pump.delivering == 0
	}, 5*time.Second, time.Millisecond)
	fg.finish()
	lifetime.acquirePicker()
	assertNoPickerRead(t, consumer.reads)
}

func TestP54AttachmentFinalizeDropsPreservedResidualBeforePicker(t *testing.T) {
	reader := newAttachTestReader()
	consumer := &recordingPickerConsumer{reads: make(chan []byte, 1)}
	lifetime := startTerminalInputLifetime(reader, consumer)
	defer lifetime.stop()
	lifetime.releasePicker()
	host := newAttachmentHost(attachmentHostConfig{Input: lifetime.pump})
	host.ownerBoundary = true
	fg := host.newForeground(AttachmentToken{Generation: 1}, newSessionTestStream())
	require.True(t, host.authority.grant(fg))
	reader.push([]byte("\r"))
	event, ok := fg.Input(context.Background())
	require.True(t, ok)
	fg.PreserveInput(event.Data)
	fg.finish()
	lifetime.acquirePicker()
	assertNoPickerRead(t, consumer.reads)
}

func assertNoPickerRead(t *testing.T, reads <-chan []byte) {
	t.Helper()
	select {
	case got := <-reads:
		t.Fatalf("attachment bytes crossed into picker: %q", got)
	case <-time.After(100 * time.Millisecond):
	}
}

func TestP54UndecidedAttachmentReadIsDroppedBeforePicker(t *testing.T) {
	reader := newAttachTestReader()
	consumer := &recordingPickerConsumer{reads: make(chan []byte, 1)}
	lifetime := startTerminalInputLifetime(reader, consumer)
	defer lifetime.stop()
	lifetime.releasePicker()
	attachment, claimed := lifetime.pump.tryClaim()
	require.True(t, claimed)
	reader.push([]byte("ls\r"))
	var delivery terminalReadResult
	require.Eventually(t, func() bool {
		var ok bool
		delivery, ok = lifetime.pump.take(context.Background(), attachment)
		return ok
	}, 5*time.Second, time.Millisecond)
	require.Equal(t, []byte("ls\r"), delivery.data)
	lifetime.pump.dropOwned(attachment)
	lifetime.pump.revoke(attachment)
	lifetime.acquirePicker()
	select {
	case got := <-consumer.reads:
		t.Fatalf("attachment bytes crossed into picker: %q", got)
	case <-time.After(100 * time.Millisecond):
	}
}

type recordingPickerConsumer struct{ reads chan []byte }

func (c *recordingPickerConsumer) ConsumeTerminalRead(data []byte) bool {
	c.reads <- append([]byte(nil), data...)
	return true
}

func TestP54InputDecisionProtocol(t *testing.T) {
	pump := newTerminalInputPump(nil)
	<-pump.space
	host := newAttachmentHost(attachmentHostConfig{Input: pump})
	fg := host.newForeground(AttachmentToken{Generation: 1}, newSessionTestStream())
	require.True(t, host.authority.grant(fg))
	pump.mu.Lock()
	pump.pending = &terminalReadResult{data: []byte("once")}
	pump.mu.Unlock()
	pump.signalReady(fg.consumer)
	event, ok := fg.Input(context.Background())
	require.True(t, ok)
	require.Equal(t, []byte("once"), event.Data)
	fg.PreserveInput(event.Data)
	fg.AckInput() // refused: already decided
	host.authority.revoke(fg)
	next := host.newForeground(AttachmentToken{Generation: 2}, newSessionTestStream())
	require.True(t, host.authority.grant(next))
	replayed, ok := next.Input(context.Background())
	require.True(t, ok)
	require.Equal(t, []byte("once"), replayed.Data)
	next.AckInput()
	next.PreserveInput([]byte("duplicate")) // refused: no outstanding delivery
	require.Empty(t, pump.residual)
}

func TestP54GeometryIsPerAttachmentAndCarriesAcrossPicker(t *testing.T) {
	resizes := make(chan domain.Geometry, 4)
	host := newAttachmentHost(attachmentHostConfig{})
	host.resizes = resizes
	host.ownerBoundary = true
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	host.startGeometry(ctx)
	defer host.stopGeometry()
	first := domain.Geometry{Size: domain.Size{Cols: 90, Rows: 30}}
	latest := domain.Geometry{Size: domain.Size{Cols: 120, Rows: 40}}
	resizes <- first
	resizes <- latest
	require.Eventually(t, func() bool { geometry, ok := host.latestGeometry(); return ok && geometry.Size == latest.Size }, 5*time.Second, time.Millisecond)
	fg1 := host.newForeground(AttachmentToken{Generation: 1}, newSessionTestStream())
	require.True(t, host.authority.grant(fg1))
	geometry, ok := fg1.Resize(context.Background())
	require.True(t, ok)
	require.Equal(t, latest.Size, geometry.Size)
	host.authority.revoke(fg1)
	fg2 := host.newForeground(AttachmentToken{Generation: 2}, newSessionTestStream())
	require.True(t, host.authority.grant(fg2))
	geometry, ok = fg2.Resize(context.Background())
	require.True(t, ok)
	require.Equal(t, latest.Size, geometry.Size)
	_, stale := fg1.Resize(context.Background())
	require.False(t, stale)
}

func TestP54LifecycleSkipsStaleTokenAndDeliversLiveDetach(t *testing.T) {
	actions := make(chan AttachmentLifecycleAction, 2)
	host := newAttachmentHost(attachmentHostConfig{Actions: actions})
	fg := host.newForeground(AttachmentToken{Generation: 2, Attempt: 3}, newSessionTestStream())
	require.True(t, host.authority.grant(fg))
	actions <- AttachmentLifecycleAction{Token: AttachmentToken{Generation: 1, Attempt: 9}, Kind: AttachmentDetachAndExit}
	actions <- AttachmentLifecycleAction{Token: fg.token, Kind: AttachmentDetachToPicker}
	action, ok := fg.Lifecycle(context.Background())
	require.True(t, ok)
	require.Equal(t, AttachmentDetachToPicker, action)
	fg.finish()
}

func TestP54LifecycleInterleavingsRace(t *testing.T) {
	for i := 0; i < 50; i++ {
		pump := newTerminalInputPump(nil)
		resizes := make(chan domain.Geometry, 1)
		actions := make(chan AttachmentLifecycleAction, 1)
		host := newAttachmentHost(attachmentHostConfig{Input: pump, Actions: actions})
		host.resizes = resizes
		ctx, cancel := context.WithCancel(context.Background())
		host.startGeometry(ctx)
		fg := host.newForeground(AttachmentToken{Generation: 1}, newSessionTestStream())
		require.True(t, host.authority.grant(fg))
		var wg sync.WaitGroup
		wg.Add(3)
		go func() { defer wg.Done(); resizes <- domain.Geometry{Size: domain.Size{Cols: 80, Rows: 24}} }()
		go func() {
			defer wg.Done()
			actions <- AttachmentLifecycleAction{Token: fg.token, Kind: AttachmentDetachToPicker}
		}()
		go func() { defer wg.Done(); fg.finish() }()
		wg.Wait()
		cancel()
		host.stopGeometry()
	}
}
