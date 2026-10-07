package client

import (
	"context"
	"testing"
	"time"

	"github.com/bnema/vev/internal/protocol"
	"github.com/stretchr/testify/require"
)

func TestOutboxFullCancellation(t *testing.T) {
	for _, foreground := range []bool{false, true} {
		t.Run(map[bool]string{false: "context", true: "foreground"}[foreground], func(t *testing.T) {
			stream := &gatedSessionStream{sessionTestStream: newSessionTestStream(), release: make(chan struct{})}
			box := newAttachmentOutbox(stream)
			defer box.join()
			for range outboxMaxPending {
				require.NoError(t, box.SendClient(protocol.Ack{}))
			}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			revoked := make(chan struct{})
			result := make(chan error, 1)
			go func() { result <- box.enqueue(ctx, revoked, protocol.Ack{}) }()
			if foreground {
				close(revoked)
			} else {
				cancel()
			}
			select {
			case err := <-result:
				if foreground {
					require.ErrorIs(t, err, errAttachmentForegroundRevoked)
				} else {
					require.ErrorIs(t, err, context.Canceled)
				}
			case <-time.After(time.Second):
				t.Fatal("full outbox ignored cancellation")
			}
		})
	}
}

// TestOutboxFullAdmitsAfterProgress checks that a caller blocked on a full
// outbox is admitted once the sender writes and frees space.
func TestOutboxFullAdmitsAfterProgress(t *testing.T) {
	stream := &gatedSessionStream{sessionTestStream: newSessionTestStream(), release: make(chan struct{})}
	box := newAttachmentOutbox(stream)
	defer box.join()
	for range outboxMaxPending {
		require.NoError(t, box.SendClient(protocol.Ack{}))
	}
	result := make(chan error, 1)
	go func() { result <- box.enqueue(t.Context(), nil, protocol.Detach{}) }()
	select {
	case err := <-result:
		t.Fatalf("full outbox admitted before progress: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	close(stream.release)
	select {
	case err := <-result:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("blocked caller was not admitted after progress")
	}
	require.Eventually(t, func() bool { return len(stream.messages()) == outboxMaxPending+1 }, time.Second, time.Millisecond)
}

func TestOutboxMixedUnsentInput(t *testing.T) {
	stream := &gatedSessionStream{sessionTestStream: newSessionTestStream(), release: make(chan struct{})}
	box := newAttachmentOutbox(stream)
	defer box.join()
	for _, message := range []protocol.ClientMessage{protocol.Ack{}, protocol.Input{Data: []byte("a")}, protocol.TerminalFocus{}, protocol.Input{Data: []byte("b")}} {
		require.NoError(t, box.SendClient(message))
	}
	require.Equal(t, []byte("ab"), box.unsentInput(0, box.accepted()))
	require.Equal(t, []byte("b"), box.unsentInput(2, box.accepted()))
}
