package daemon

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/ports"
)

type attachmentCommandTestTransport struct {
	sent    chan ports.Frame
	entered chan struct{}
	gate    <-chan struct{}
}

func newAttachmentCommandTestTransport() *attachmentCommandTestTransport {
	return &attachmentCommandTestTransport{sent: make(chan ports.Frame, 8)}
}

func (t *attachmentCommandTestTransport) Send(frame ports.Frame) error {
	if t.entered != nil {
		select {
		case t.entered <- struct{}{}:
		default:
		}
	}
	if t.gate != nil {
		<-t.gate
	}
	t.sent <- frame
	return nil
}

func (t *attachmentCommandTestTransport) Recv() (ports.Frame, error) {
	return ports.Frame{}, nil
}

func (t *attachmentCommandTestTransport) Close() error { return nil }

type attachmentCommandTestTimer struct {
	ch   chan time.Time
	once sync.Once
}

func (t *attachmentCommandTestTimer) C() <-chan time.Time      { return t.ch }
func (t *attachmentCommandTestTimer) Reset(time.Duration) bool { return false }
func (t *attachmentCommandTestTimer) Stop() bool               { return true }
func (t *attachmentCommandTestTimer) fire()                    { t.once.Do(func() { t.ch <- time.Time{} }) }

type attachmentCommandTestClock struct {
	timers chan *attachmentCommandTestTimer
}

func newAttachmentCommandTestClock() *attachmentCommandTestClock {
	return &attachmentCommandTestClock{timers: make(chan *attachmentCommandTestTimer, 8)}
}

func (c *attachmentCommandTestClock) Now() time.Time { return time.Time{} }

func (c *attachmentCommandTestClock) NewTimer(delay time.Duration) ports.Timer {
	if delay != attachedCommandTimeout {
		return stubTimer{}
	}
	timer := &attachmentCommandTestTimer{ch: make(chan time.Time, 1)}
	c.timers <- timer
	return timer
}

func awaitAttachedCommandFrame(t *testing.T, transport *attachmentCommandTestTransport) ports.CommandRequest {
	t.Helper()
	select {
	case frame := <-transport.sent:
		require.Equal(t, ports.MsgCommand, frame.Type)
		request, err := ports.UnmarshalCommandRequest(frame.Payload)
		require.NoError(t, err)
		return request
	case <-time.After(time.Second):
		t.Fatal("attached command was not published")
		return ports.CommandRequest{}
	}
}

func awaitAttachedCommandTimer(t *testing.T, clock *attachmentCommandTestClock) *attachmentCommandTestTimer {
	t.Helper()
	select {
	case timer := <-clock.timers:
		return timer
	case <-time.After(time.Second):
		t.Fatal("attached command timeout was not armed")
		return nil
	}
}

func awaitAttachedCommandResult(t *testing.T, done <-chan attachedCommandOutcome) attachedCommandOutcome {
	t.Helper()
	select {
	case result := <-done:
		return result
	case <-time.After(time.Second):
		t.Fatal("attached command did not finish")
		return attachedCommandOutcome{}
	}
}

func startAttachedCommand(t *testing.T, ac *attachedClient, clock *attachmentCommandTestClock, slug string) (ports.CommandRequest, *attachmentCommandTestTimer, <-chan attachedCommandOutcome) {
	t.Helper()
	done := make(chan attachedCommandOutcome, 1)
	go func() {
		result, err := ac.sendCommand(context.Background(), clock, slug, nil)
		done <- attachedCommandOutcome{result: result, err: err}
	}()
	request := awaitAttachedCommandFrame(t, ac.transport().(*attachmentCommandTestTransport))
	return request, awaitAttachedCommandTimer(t, clock), done
}

func TestAttachedCommandWaitReleasesSenderLockAfterPublication(t *testing.T) {
	transport := newAttachmentCommandTestTransport()
	gate := make(chan struct{})
	transport.gate = gate
	transport.entered = make(chan struct{}, 1)
	ac := &attachedClient{tr: transport}
	ac.connectionGeneration.Store(1)
	clock := newAttachmentCommandTestClock()
	done := make(chan attachedCommandOutcome, 1)
	go func() {
		result, err := ac.sendCommand(context.Background(), clock, "next-tab", nil)
		done <- attachedCommandOutcome{result: result, err: err}
	}()

	select {
	case <-transport.entered:
	case <-time.After(time.Second):
		t.Fatal("command publication did not reach the transport")
	}
	close(gate)
	request := awaitAttachedCommandFrame(t, transport)
	_ = awaitAttachedCommandTimer(t, clock)

	require.True(t, ac.sendMu.TryLock(), "command wait retained sender lock")
	ac.sendMu.Unlock()
	require.NoError(t, ac.sendExpectedTransport(ac.transportSnapshot(), ports.Frame{Type: ports.MsgPing}))
	select {
	case frame := <-transport.sent:
		require.Equal(t, ports.MsgPing, frame.Type)
	case <-time.After(time.Second):
		t.Fatal("ping was blocked by command wait")
	}

	ac.completeCommandResult(1, ports.CommandResult{RequestID: request.RequestID, OK: true})
	result := awaitAttachedCommandResult(t, done)
	require.NoError(t, result.err)
	require.True(t, result.result.OK)
}

func TestAttachedCommandTimeoutRemovesOnlyRequestAndIgnoresLateResult(t *testing.T) {
	transport := newAttachmentCommandTestTransport()
	ac := &attachedClient{tr: transport}
	ac.connectionGeneration.Store(4)
	clock := newAttachmentCommandTestClock()

	first, timer, done := startAttachedCommand(t, ac, clock, "next-tab")
	timer.fire()
	result := awaitAttachedCommandResult(t, done)
	require.ErrorIs(t, result.err, errAttachedCommandTimeout)
	require.Zero(t, ac.pendingAttachedCommandCount())

	ac.completeCommandResult(4, ports.CommandResult{RequestID: first.RequestID, OK: true})
	select {
	case <-done:
		t.Fatal("late result completed a timed-out command")
	default:
	}

	second, secondTimer, secondDone := startAttachedCommand(t, ac, clock, "previous-tab")
	require.Equal(t, first.RequestID+1, second.RequestID)
	ac.completeCommandResult(4, ports.CommandResult{RequestID: first.RequestID, OK: true})
	select {
	case <-secondDone:
		t.Fatal("old request ID completed a newer command")
	default:
	}
	ac.completeCommandResult(4, ports.CommandResult{RequestID: second.RequestID, OK: true})
	result = awaitAttachedCommandResult(t, secondDone)
	require.NoError(t, result.err)
	require.True(t, result.result.OK)
	secondTimer.fire()
}

func TestAttachedCommandIgnoresOldGenerationResults(t *testing.T) {
	transport := newAttachmentCommandTestTransport()
	ac := &attachedClient{tr: transport}
	ac.connectionGeneration.Store(7)
	clock := newAttachmentCommandTestClock()

	request, timer, done := startAttachedCommand(t, ac, clock, "next-tab")
	ac.connectionGeneration.Store(8)
	ac.completeCommandResult(7, ports.CommandResult{RequestID: request.RequestID, OK: true})
	select {
	case <-done:
		t.Fatal("old-generation result completed a command")
	default:
	}
	timer.fire()
	result := awaitAttachedCommandResult(t, done)
	require.ErrorIs(t, result.err, errAttachedCommandTimeout)
}

func TestAttachedCommandTrackingIsIndependentPerAttachment(t *testing.T) {
	firstTransport := newAttachmentCommandTestTransport()
	secondTransport := newAttachmentCommandTestTransport()
	first := &attachedClient{tr: firstTransport}
	second := &attachedClient{tr: secondTransport}
	first.connectionGeneration.Store(1)
	second.connectionGeneration.Store(1)
	clock := newAttachmentCommandTestClock()

	firstDone := make(chan attachedCommandOutcome, 1)
	go func() {
		result, err := first.sendCommand(context.Background(), clock, "next-tab", nil)
		firstDone <- attachedCommandOutcome{result: result, err: err}
	}()
	secondDone := make(chan attachedCommandOutcome, 1)
	go func() {
		result, err := second.sendCommand(context.Background(), clock, "previous-tab", nil)
		secondDone <- attachedCommandOutcome{result: result, err: err}
	}()

	firstRequest := awaitAttachedCommandFrame(t, firstTransport)
	secondRequest := awaitAttachedCommandFrame(t, secondTransport)
	require.Equal(t, uint64(1), firstRequest.RequestID)
	require.Equal(t, uint64(1), secondRequest.RequestID)
	_ = awaitAttachedCommandTimer(t, clock)
	_ = awaitAttachedCommandTimer(t, clock)

	first.completeCommandResult(1, ports.CommandResult{RequestID: firstRequest.RequestID, OK: true})
	second.completeCommandResult(1, ports.CommandResult{RequestID: secondRequest.RequestID, OK: true})
	require.NoError(t, awaitAttachedCommandResult(t, firstDone).err)
	require.NoError(t, awaitAttachedCommandResult(t, secondDone).err)
	require.Equal(t, 0, first.pendingAttachedCommandCount())
	require.Equal(t, 0, second.pendingAttachedCommandCount())
}

func TestAttachedCommandUnavailableAndContextCancellation(t *testing.T) {
	var nilAttachment *attachedClient
	clock := newAttachmentCommandTestClock()
	_, err := nilAttachment.sendCommand(context.Background(), clock, "next-tab", nil)
	require.True(t, errors.Is(err, errAttachedCommandUnavailable))

	transport := newAttachmentCommandTestTransport()
	ac := &attachedClient{tr: transport}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = ac.sendCommand(ctx, clock, "next-tab", nil)
	require.ErrorIs(t, err, context.Canceled)
}
