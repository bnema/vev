package quic

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/bnema/vev/internal/protocol/wire"
	"github.com/bnema/vev/internal/testutil/replaytest"
	"github.com/stretchr/testify/require"
)

// TestTransportReplay runs the canonical typed transcript over one QUIC
// stream: transport differences must not alter the application
// conversation (P7.1).
func TestTransportReplay(t *testing.T) {
	replaytest.Run(t, func(t *testing.T, envelopes []wire.Envelope) []wire.Envelope {
		cert, fingerprint, err := GenerateEphemeralCert()
		require.NoError(t, err)
		listener, err := ListenConfig("127.0.0.1:0", cert, Config{}, 4)
		require.NoError(t, err)
		defer func() { _ = listener.Close() }()

		accepted := make(chan wire.Transport, 1)
		go func() {
			transport, err := listener.Accept()
			if err != nil {
				return
			}
			accepted <- transport
		}()
		dialer := DialConfig(listener.Addr(), "vev-bootstrap", fingerprint, Config{}, 10*time.Second)
		sender, err := dialer.Dial(context.Background())
		require.NoError(t, err)
		defer func() { _ = sender.Close() }()

		var receiver wire.Transport
		select {
		case receiver = <-accepted:
		case <-time.After(10 * time.Second):
			t.Fatal("QUIC listener did not accept")
		}
		defer func() { _ = receiver.Close() }()

		var wg sync.WaitGroup
		wg.Go(func() {
			for _, frame := range envelopes {
				if err := sender.Send(frame); err != nil {
					t.Errorf("Send: %v", err)
					return
				}
			}
		})
		got := make([]wire.Envelope, 0, len(envelopes))
		for range envelopes {
			frame, err := receiver.Recv()
			if err != nil {
				t.Errorf("Recv: %v", err)
				_ = receiver.Close()
				break
			}
			got = append(got, frame)
		}
		wg.Wait()
		return got
	})
}
