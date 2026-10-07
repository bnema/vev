package client

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/bnema/vev/internal/protocol"
	"github.com/stretchr/testify/require"
)

func TestOutboxDrain(t *testing.T) {
	linkErr := errors.New("link lost")
	for _, mode := range []string{"written", "timeout", "cancelled", "failed", "revoked"} {
		t.Run(mode, func(t *testing.T) {
			stream := &gatedSessionStream{sessionTestStream: newSessionTestStream(), release: make(chan struct{})}
			box := newAttachmentOutbox(stream)
			defer box.join()
			require.NoError(t, box.SendClient(protocol.Detach{}))
			clock := newSupervisorTestClock()
			worker := &sessionAttachmentWorker{cfg: sessionAttachmentConfig{Clock: clock}}
			host := newAttachmentHost(attachmentHostConfig{})
			fg := host.newForeground(AttachmentToken{Generation: 1}, stream)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			result := make(chan error, 1)
			go func() { result <- worker.drainOutbox(ctx, fg, box) }()
			timer := clock.awaitTimer(t)
			require.Equal(t, 2*time.Second, timer.delay)
			switch mode {
			case "written":
				close(stream.release)
			case "timeout":
				timer.fire()
			case "cancelled":
				cancel()
			case "failed":
				stream.fail = linkErr
				close(stream.release)
			case "revoked":
				fg.finalize()
			}
			select {
			case err := <-result:
				switch mode {
				case "cancelled":
					require.ErrorIs(t, err, context.Canceled)
				case "failed":
					require.ErrorIs(t, err, linkErr)
				case "revoked":
					require.ErrorIs(t, err, errAttachmentForegroundRevoked)
				default:
					require.NoError(t, err)
				}
			case <-time.After(time.Second):
				t.Fatal("drain did not settle")
			}
			if mode == "written" {
				require.Len(t, stream.messages(), 1)
			}
		})
	}
}
