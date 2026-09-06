package dgram

import (
	"testing"
	"time"

	"github.com/bnema/vev/internal/ports"
	portsmocks "github.com/bnema/vev/internal/ports/mocks"
	"github.com/stretchr/testify/require"
)

func TestACKScheduleCoalescedWakeCompletesEmptyDeadline(t *testing.T) {
	clock := portsmocks.NewMockClock(t)
	timer := portsmocks.NewMockTimer(t)
	deadline := make(chan time.Time, 1)
	completed := make(chan bool, 2)
	transport := &Transport{
		clock: clock, done: make(chan struct{}),
		ackWake: make(chan struct{}, 1), ackSend: make(chan uint64, 1),
		afterACKDispatchAttempt: func(dispatched bool) { completed <- dispatched },
	}
	clock.EXPECT().NewTimer(maxACKDelay).RunAndReturn(func(time.Duration) ports.Timer {
		// A second record's wake and the first deadline become ready together.
		// The deadline consumes the cumulative ACK, leaving a harmless queued
		// wake whose next deadline has nothing left to dispatch.
		transport.queueACK(2)
		deadline <- time.Time{}
		return timer
	}).Once()
	timer.EXPECT().C().Return((<-chan time.Time)(deadline)).Twice()
	timer.EXPECT().Reset(maxACKDelay).Run(func(time.Duration) { deadline <- time.Time{} }).Return(false).Once()
	timer.EXPECT().Stop().Return(false).Once()
	transport.queueACK(1)
	done := make(chan struct{})
	go func() { defer close(done); transport.ackScheduleLoop() }()
	t.Cleanup(func() { close(transport.done); awaitSignal(t, done, "ACK scheduler shutdown") })
	require.True(t, awaitResult(t, completed, "cumulative ACK dispatch"))
	require.Equal(t, uint64(2), awaitResult(t, transport.ackSend, "cumulative ACK"))
	require.False(t, awaitResult(t, completed, "coalesced empty ACK deadline completion"), "a timer deadline need not dispatch a second ACK")
	require.Empty(t, transport.ackSend)
}

func TestACKScheduleCoalescedWakeDoesNotArmAnotherDeadline(t *testing.T) {
	clock := portsmocks.NewMockClock(t)
	timer := portsmocks.NewMockTimer(t)
	deadline := make(chan time.Time, 1)
	armed := make(chan bool, 2)
	completed := make(chan bool, 2)
	transport := &Transport{
		clock: clock, done: make(chan struct{}),
		ackWake: make(chan struct{}, 1), ackSend: make(chan uint64, 1),
		afterACKScheduled: func(created bool) {
			armed <- created
			if !created {
				deadline <- time.Time{}
			}
		},
		afterACKDispatchAttempt: func(dispatched bool) { completed <- dispatched },
	}
	clock.EXPECT().NewTimer(maxACKDelay).RunAndReturn(func(time.Duration) ports.Timer {
		transport.queueACK(2)
		return timer
	}).Once()
	timer.EXPECT().C().Return((<-chan time.Time)(deadline)).Once()
	timer.EXPECT().Stop().Return(false).Once()
	transport.queueACK(1)
	done := make(chan struct{})
	go func() { defer close(done); transport.ackScheduleLoop() }()
	t.Cleanup(func() { close(transport.done); awaitSignal(t, done, "ACK scheduler shutdown") })
	require.True(t, awaitResult(t, armed, "first ACK deadline"))
	require.False(t, awaitResult(t, armed, "coalesced ACK scheduling"))
	require.True(t, awaitResult(t, completed, "one cumulative ACK dispatch"))
	require.Equal(t, uint64(2), awaitResult(t, transport.ackSend, "cumulative ACK"))
	require.Empty(t, completed)
}
