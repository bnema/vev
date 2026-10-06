package client

import (
	"errors"
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
