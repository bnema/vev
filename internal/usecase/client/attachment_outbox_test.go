package client

import (
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/protocol"
)

// gatedSessionStream blocks every write until release is closed, like a
// carriage stalled on a dead link, and can fail writes after release.
type gatedSessionStream struct {
	*sessionTestStream
	release chan struct{}
	fail    error
}

func (s *gatedSessionStream) SendClient(message protocol.ClientMessage) error {
	select {
	case <-s.release:
	case <-s.closed:
		return errSessionTestClosed
	}
	if s.fail != nil {
		return s.fail
	}
	return s.sessionTestStream.SendClient(message)
}

func TestAttachmentOutbox(t *testing.T) {
	writeErr := errors.New("link lost")
	tests := []struct {
		name    string
		fail    error
		wantErr error
	}{
		{name: "stalled link does not block enqueue and preserves order"},
		{name: "write failure is sticky and signalled", fail: writeErr, wantErr: writeErr},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stream := &gatedSessionStream{sessionTestStream: newSessionTestStream(), release: make(chan struct{}), fail: tt.fail}
			outbox := newAttachmentOutbox(stream)
			defer outbox.stop()

			messages := []protocol.ClientMessage{
				protocol.Input{InputSeq: 1, Data: []byte("a")},
				protocol.Input{InputSeq: 2, Data: []byte("b")},
				protocol.Input{InputSeq: 3, Data: []byte("c")},
			}
			enqueued := make(chan struct{})
			go func() {
				for _, m := range messages {
					if err := outbox.SendClient(m); err != nil {
						t.Errorf("SendClient: %v", err)
					}
				}
				close(enqueued)
			}()
			select {
			case <-enqueued:
			case <-time.After(5 * time.Second):
				t.Fatal("SendClient blocked on a stalled link")
			}

			close(stream.release)
			if tt.wantErr != nil {
				<-outbox.Failed()
				require.ErrorIs(t, outbox.failure(), tt.wantErr)
				require.ErrorIs(t, outbox.SendClient(protocol.Detach{}), tt.wantErr)
				return
			}
			<-outbox.Idle()
			stream.mu.Lock()
			defer stream.mu.Unlock()
			require.Equal(t, messages, stream.sent)
		})
	}
}

func TestAttachmentOutboxCloseUnblocksStalledWrite(t *testing.T) {
	stream := &gatedSessionStream{sessionTestStream: newSessionTestStream(), release: make(chan struct{})}
	outbox := newAttachmentOutbox(stream)
	require.NoError(t, outbox.SendClient(protocol.Detach{}))

	require.NoError(t, outbox.Close())
	select {
	case <-outbox.done:
	case <-time.After(5 * time.Second):
		t.Fatal("sender did not exit after Close")
	}
}

// TestResumeWatch proves the resume backoff reads the terminal: Ctrl-C or a
// lone Esc cancels, anything else is replayed to the next owner in order.
func TestResumeWatch(t *testing.T) {
	tests := []struct {
		name         string
		reads        []string
		wantCancel   bool
		wantResidual string
	}{
		{name: "ctrl-c cancels and drops held keys", reads: []string{"ls", "\x03"}, wantCancel: true},
		{name: "lone esc cancels", reads: []string{"\x1b"}, wantCancel: true},
		{name: "kitty esc cancels", reads: []string{"\x1b[27u"}, wantCancel: true},
		{name: "kitty ctrl-c cancels", reads: []string{"\x1b[99;5u"}, wantCancel: true},
		{name: "kitty ctrl-c with lock bits cancels", reads: []string{"\x1b[99;69u"}, wantCancel: true},
		{name: "escape sequence is kept", reads: []string{"\x1b[A"}, wantResidual: "\x1b[A"},
		{name: "typed keys are kept in order", reads: []string{"l", "s", "\r"}, wantResidual: "ls\r"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reader := &inputTestReader{chunks: make(chan []byte, len(tt.reads))}
			pump := newTerminalInputPump(reader)
			pump.start()
			t.Cleanup(func() {
				pump.stop()
				close(reader.chunks)
			})
			lifetime := &terminalInputLifetime{eof: make(chan error, 1), pump: pump}
			watch := lifetime.watchResume()
			for _, read := range tt.reads {
				reader.chunks <- []byte(read)
			}
			if tt.wantCancel {
				select {
				case <-watch.cancelled:
				case <-time.After(5 * time.Second):
					t.Fatal("cancel key ignored")
				}
			} else {
				require.Eventually(t, func() bool { return watch.heldLen() == len(tt.wantResidual) }, 5*time.Second, time.Millisecond)
			}
			watch.release()
			pump.mu.Lock()
			defer pump.mu.Unlock()
			require.Equal(t, tt.wantResidual, string(pump.residual))
			require.Zero(t, pump.consumer, "release returns the claim")
		})
	}
}

// stallingSessionStream passes writes through until stall is set; stalled
// writes block until release closes, then fail with failErr when set.
type stallingSessionStream struct {
	*sessionTestStream
	stall   atomic.Bool
	entered chan struct{}
	release chan struct{}
	failErr error
}

func (s *stallingSessionStream) SendClient(message protocol.ClientMessage) error {
	if s.stall.Load() {
		select {
		case s.entered <- struct{}{}:
		default:
		}
		select {
		case <-s.release:
		case <-s.closed:
			return errSessionTestClosed
		}
		if s.failErr != nil {
			return s.failErr
		}
	}
	return s.sessionTestStream.SendClient(message)
}

// TestAttachmentWorkerStalledLink drives the real worker over a link that
// stops accepting writes after attach.
func TestAttachmentWorkerStalledLink(t *testing.T) {
	linkErr := errors.New("link lost")
	tests := []struct {
		name string
		run  func(t *testing.T, h *inputHarness, s *stallingSessionStream)
	}{
		{
			name: "output keeps painting while a write is stalled",
			run: func(t *testing.T, h *inputHarness, s *stallingSessionStream) {
				h.send("a")
				<-s.entered
				later := sessionTestOutput(2, "\x1b[Hlater")
				later.Epoch = 2
				h.stream.deliver(later)
				require.Eventually(t, func() bool { return strings.Contains(h.term.written(), "later") }, 5*time.Second, time.Millisecond, "a stalled write blocked painting")
				close(s.release)
				require.Equal(t, []string{"a"}, h.awaitInputs(t, 1))
				h.end(t)
			},
		},
		{
			name: "a read that queues nothing is acked while an earlier write is stalled",
			run: func(t *testing.T, h *inputHarness, s *stallingSessionStream) {
				// A queued output Ack stalls behind the link.
				next := sessionTestOutput(2, "\x1b[Hmore")
				next.Epoch = 2
				h.stream.deliver(next)
				<-s.entered
				// An open bracketed paste is held by the coalescer and queues
				// nothing, so there is no write to wait for.
				h.send("\x1b[200~abc")
				h.send("def")
				require.Eventually(t, func() bool {
					h.pump.mu.Lock()
					defer h.pump.mu.Unlock()
					return h.pump.pending == nil && h.pump.delivering == 0 && len(h.reader.chunks) == 0
				}, 5*time.Second, time.Millisecond, "an unqueued read held the pump behind a stalled write")
				close(s.release)
				h.end(t)
			},
		},
		{
			name: "keys behind a failed write are preserved for the next attempt",
			run: func(t *testing.T, h *inputHarness, s *stallingSessionStream) {
				s.failErr = linkErr
				h.send("a")
				<-s.entered
				close(s.release)
				select {
				case event := <-h.events:
					require.Equal(t, AttachmentEventFailed, event.Kind)
					require.ErrorIs(t, event.Err, linkErr)
				case <-time.After(5 * time.Second):
					t.Fatal("the failed write never settled the run")
				}
				h.pump.mu.Lock()
				defer h.pump.mu.Unlock()
				require.Equal(t, "a", string(h.pump.residual), "the unsent key is kept for the resume")
			},
		},
		{
			name: "detach on a stalled link is bounded",
			run: func(t *testing.T, h *inputHarness, s *stallingSessionStream) {
				h.send("a")
				<-s.entered
				h.stream.deliver(protocol.Detached{Reason: protocol.ReasonDetach})
				select {
				case event := <-h.events:
					require.Equal(t, AttachmentEventEnded, event.Kind)
				case <-time.After(5 * time.Second):
					t.Fatal("the daemon-confirmed detach never settled")
				}
				h.pump.mu.Lock()
				defer h.pump.mu.Unlock()
				require.Equal(t, "a", string(h.pump.residual), "a key the link never took is not lost")
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			base := newSessionTestStream()
			s := &stallingSessionStream{sessionTestStream: base, entered: make(chan struct{}, 1), release: make(chan struct{})}
			h := startInputHarnessOn(t, nil, sessionTestRequest(true), nil, base, s)
			require.Eventually(t, func() bool { return strings.Contains(h.term.written(), "ready") }, 5*time.Second, time.Millisecond)
			// Let the initial Ack and focus report through, then stall.
			require.Eventually(t, func() bool { return len(base.messages()) >= 2 }, 5*time.Second, time.Millisecond)
			s.stall.Store(true)
			tt.run(t, h, s)
		})
	}
}
