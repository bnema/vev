package client

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	portsmocks "github.com/bnema/vev/internal/ports/mocks"
	"github.com/bnema/vev/internal/protocol"
)

// livenessTimer is one probe timer handed out by the mock clock: tests fire it
// by sending on ch, and delay records the duration it was armed for.
type livenessTimer struct {
	ch    chan time.Time
	delay time.Duration
}

type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

type livenessHarness struct {
	t        *testing.T
	now      atomic.Int64 // wall clock in Unix nanoseconds, no monotonic reading
	created  chan *livenessTimer
	stream   *sessionTestStream
	terminal *workerTestTerminal
	host     *attachmentHost
	logs     *lockedBuffer
	timer    *livenessTimer // the armed probe timer
	events   chan AttachmentEvent
}

const livenessNotice = "Host not responding"

func newLivenessHarness(t *testing.T, local bool) *livenessHarness {
	t.Helper()
	h := &livenessHarness{
		t: t, created: make(chan *livenessTimer, 64), stream: newSessionTestStream(), terminal: newWorkerTestTerminal(),
		logs: &lockedBuffer{}, events: make(chan AttachmentEvent, 1),
	}
	cfg := sessionTestWorkerConfig(sessionTestRequest(local))
	h.now.Store(time.Unix(1000, 0).UnixNano())
	clock := portsmocks.NewMockClock(t)
	clock.EXPECT().Now().RunAndReturn(func() time.Time { return time.Unix(0, h.now.Load()) }).Maybe()
	clock.EXPECT().NewTimer(mock.Anything).RunAndReturn(func(d time.Duration) ports.Timer {
		fired := &livenessTimer{ch: make(chan time.Time, 1), delay: d}
		timer := portsmocks.NewMockTimer(t)
		timer.EXPECT().C().Return(fired.ch).Maybe()
		timer.EXPECT().Stop().Return(true).Maybe()
		h.created <- fired
		return timer
	}).Maybe()
	cfg.Clock = clock
	cfg.Logger = slog.New(slog.NewTextHandler(h.logs, nil))
	attached := newSessionTestAttachments()
	h.host = newWorkerTestHost(h.terminal, nil, attached.record)
	worker, err := newSessionAttachmentWorker(cfg)
	require.NoError(t, err)
	go func() {
		event, _ := h.host.Run(context.Background(), AttachmentToken{Generation: 4, Attempt: 1}, worker, h.stream)
		h.events <- event
	}()
	awaitHello(t, h.stream)
	h.stream.deliver(protocol.Welcome{SessionName: "alpha"})
	h.stream.deliver(sessionTestOutput(1, "frame"))
	attached.token(t)
	t.Cleanup(func() {
		_ = h.stream.Close()
		select {
		case <-h.events:
		case <-time.After(5 * time.Second):
			t.Error("the worker never settled")
		}
	})
	if !local {
		h.timer = h.nextTimer()
		require.Equal(t, linkProbeIdle, h.timer.delay)
	}
	return h
}

// nextTimer waits for the probe to arm its next timer.
func (h *livenessHarness) nextTimer() *livenessTimer {
	h.t.Helper()
	select {
	case timer := <-h.created:
		return timer
	case <-time.After(5 * time.Second):
		h.t.Fatal("the liveness probe never armed a timer")
		return nil
	}
}

func (h *livenessHarness) advance(d time.Duration) { h.now.Add(int64(d)) }

// tick advances the clock and fires the armed timer, then waits for the probe
// to re-arm so the step is fully processed.
func (h *livenessHarness) tick(d time.Duration) {
	h.t.Helper()
	h.advance(d)
	h.timer.ch <- time.Time{}
	h.timer = h.nextTimer()
}

func (h *livenessHarness) count(match func(protocol.ClientMessage) bool) int {
	n := 0
	for _, message := range h.stream.messages() {
		if match(message) {
			n++
		}
	}
	return n
}

func (h *livenessHarness) pings() int {
	return h.count(func(m protocol.ClientMessage) bool { _, ok := m.(protocol.Ping); return ok })
}

func (h *livenessHarness) acks() int {
	return h.count(func(m protocol.ClientMessage) bool { _, ok := m.(protocol.Ack); return ok })
}

// livenessDelta is the incremental publication that follows the initial frame.
func livenessDelta() protocol.Output {
	out := sessionTestOutput(2, "x")
	out.Full, out.Base, out.New = false, 1, 2
	return out
}

func (h *livenessHarness) resets() int {
	return h.count(func(m protocol.ClientMessage) bool { _, ok := m.(protocol.OutputResetRequest); return ok })
}

func (h *livenessHarness) toasts() int {
	return strings.Count(h.terminal.written(), livenessNotice)
}

func (h *livenessHarness) toastDrawn() bool { return h.toasts() > 0 }

// sync delivers the next incremental frame and waits for its Ack. The pump
// handles messages in order, so everything delivered before it is processed.
func (h *livenessHarness) sync() {
	h.stream.deliver(livenessDelta())
	h.eventually(func() bool { return h.acks() == 2 }, "the pump processed the frame")
}

// suspect drives the probe to a painted notice.
func (h *livenessHarness) suspect() {
	h.tick(linkProbeIdle)
	h.tick(linkProbeSuspectAfter)
	h.eventually(h.toastDrawn, "an unanswered probe must show the notice")
}

func (h *livenessHarness) eventually(cond func() bool, msg string) {
	h.t.Helper()
	require.Eventually(h.t, cond, 5*time.Second, time.Millisecond, msg)
}

func TestAttachmentLivenessProbe(t *testing.T) {
	tests := []struct {
		name   string
		script func(t *testing.T, h *livenessHarness)
	}{
		{
			name: "ping after idle",
			script: func(t *testing.T, h *livenessHarness) {
				require.Zero(t, h.pings())
				h.tick(linkProbeIdle)
				h.eventually(func() bool { return h.pings() == 1 }, "an idle link must be probed")
				require.False(t, h.toastDrawn(), "one unanswered ping is not yet suspect")
			},
		},
		{
			name: "toast after suspect timeout then reping",
			script: func(t *testing.T, h *livenessHarness) {
				h.suspect()
				require.Contains(t, h.logs.String(), "client_link_suspect")
				require.Equal(t, 1, h.pings())
				h.tick(linkProbeReping)
				h.eventually(func() bool { return h.pings() == 2 }, "a suspect link is re-probed")
				h.tick(linkProbeReping)
				h.eventually(func() bool { return h.pings() == 3 }, "a suspect link keeps being probed at a fixed cadence")
				require.Zero(t, h.resets())
			},
		},
		{
			name: "any message clears suspect and requests repaint",
			script: func(t *testing.T, h *livenessHarness) {
				h.suspect()
				h.stream.deliver(protocol.Pong{})
				h.eventually(func() bool { return h.resets() == 1 }, "recovery must request a full repaint")
				require.Contains(t, h.logs.String(), "client_link_recovered")
				h.timer = h.nextTimer()
				// Healthy again: the next idle period probes once more and
				// draws no second notice.
				before := len(h.terminal.written())
				h.tick(linkProbeIdle)
				h.eventually(func() bool { return h.pings() == 2 }, "idle again probes again")
				require.Len(t, h.terminal.written(), before, "no second notice")
				require.Equal(t, 1, h.resets(), "a healthy message must not request another repaint")
			},
		},
		{
			name: "no ping while traffic flows",
			script: func(t *testing.T, h *livenessHarness) {
				h.advance(2 * time.Second)
				h.sync()
				h.tick(linkProbeIdle - 2*time.Second)
				require.Zero(t, h.pings(), "traffic 1s ago keeps the link alive")
				require.False(t, h.toastDrawn())
			},
		},
		{
			name: "no toast while an overlay owns the terminal",
			script: func(t *testing.T, h *livenessHarness) {
				fg := h.host.authority.foreground()
				require.True(t, fg.setOverlay(attachmentOverlayNavigation, nil))
				h.tick(linkProbeIdle)
				h.tick(linkProbeSuspectAfter)
				h.eventually(func() bool { return h.pings() == 1 }, "probe")
				require.Contains(t, h.logs.String(), "client_link_suspect")
				require.False(t, h.toastDrawn(), "an overlay owns the terminal")
				// The release asks for a repaint that may never arrive on a
				// suspect link, so the notice is drawn at once.
				fg.clearOverlay(attachmentOverlayNavigation)
				h.eventually(h.toastDrawn, "the notice is painted once the overlay is gone")
			},
		},
		{
			name: "overlay opened after the toast redraws it on release",
			script: func(t *testing.T, h *livenessHarness) {
				h.suspect()
				fg := h.host.authority.foreground()
				require.True(t, fg.setOverlay(attachmentOverlayNavigation, nil))
				fg.clearOverlay(attachmentOverlayNavigation)
				h.eventually(func() bool { return h.toasts() == 2 }, "the picker box replaced the notice, so it is drawn again")
				h.eventually(func() bool { return h.resets() == 1 }, "the release asks for a repaint")
			},
		},
		{
			name: "recovery by a frame delta",
			script: func(t *testing.T, h *livenessHarness) {
				h.suspect()
				h.sync()
				require.Equal(t, 1, h.resets(), "any message recovers, not only Pong")
				require.Contains(t, h.logs.String(), "client_link_recovered")
			},
		},
		{
			name: "recovery with a repaint already outstanding",
			script: func(t *testing.T, h *livenessHarness) {
				h.suspect()
				fg := h.host.authority.foreground()
				require.True(t, fg.setOverlay(attachmentOverlayNavigation, nil))
				fg.clearOverlay(attachmentOverlayNavigation)
				h.eventually(func() bool { return h.resets() == 1 }, "the release asks for a repaint")
				h.stream.deliver(protocol.Pong{})
				h.sync()
				require.Equal(t, 1, h.resets(), "the outstanding repaint is not requested twice")
				require.Contains(t, h.logs.String(), "client_link_recovered")
			},
		},
		{
			name: "recovery without a painted notice sends no repaint",
			script: func(t *testing.T, h *livenessHarness) {
				fg := h.host.authority.foreground()
				require.True(t, fg.setOverlay(attachmentOverlayNavigation, nil))
				h.tick(linkProbeIdle)
				h.tick(linkProbeSuspectAfter)
				h.eventually(func() bool { return strings.Contains(h.logs.String(), "client_link_suspect") }, "suspect")
				h.stream.deliver(protocol.Pong{})
				h.sync()
				require.Zero(t, h.resets(), "nothing was drawn, so nothing needs erasing")
				require.Contains(t, h.logs.String(), "client_link_recovered")
			},
		},
		{
			name: "wall clock jump while already suspect",
			script: func(t *testing.T, h *livenessHarness) {
				h.suspect()
				h.tick(time.Hour)
				h.eventually(func() bool { return h.pings() == 2 }, "a suspend probes at once even when suspect")
				require.Equal(t, linkProbeSuspectAfter, h.timer.delay)
				require.Equal(t, 1, h.toasts(), "the notice is not redrawn")
			},
		},
		{
			name: "wall clock jump probes immediately",
			script: func(t *testing.T, h *livenessHarness) {
				h.tick(time.Hour)
				h.eventually(func() bool { return h.pings() == 1 }, "a suspend must probe at once")
				require.False(t, h.toastDrawn())
				require.Equal(t, linkProbeSuspectAfter, h.timer.delay, "the probe gets a fresh suspect window")
			},
		},
		{
			name: "toast follows the latest size",
			script: func(t *testing.T, h *livenessHarness) {
				h.tick(linkProbeIdle)
				h.tick(linkProbeSuspectAfter)
				h.eventually(h.toastDrawn, "suspect notice")
				first := h.terminal.written()
				h.terminal.resizes <- domain.Geometry{Size: domain.Size{Cols: 40, Rows: 10}}
				h.eventually(func() bool { return len(h.terminal.written()) > len(first) }, "a resize repaints the notice")
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.script(t, newLivenessHarness(t, false))
		})
	}
}

// TestAttachmentLivenessLocalIsInert proves a local attachment never probes.
func TestAttachmentLivenessLocalIsInert(t *testing.T) {
	h := newLivenessHarness(t, true)
	h.sync()
	require.Empty(t, h.created, "a local attachment must not arm a liveness timer")
	require.Zero(t, h.pings())
}
