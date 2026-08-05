package app

import (
	"context"
	"encoding/base64"
	"fmt"
	"os"

	"github.com/bnema/vev/internal/adapters/ipc"
	"github.com/bnema/vev/internal/ports"
)

// runRemotePreview is the hidden SSH-side carriage. The request is encoded in
// one shell-safe argument, then the daemon returns one bounded binary response
// whose payload is written verbatim to stdout; no viewport bytes are logged.
func runRemotePreview(ctx context.Context, encoded string) error {
	request, err := base64.RawURLEncoding.DecodeString(encoded)
	if err != nil {
		return fmt.Errorf("vev: invalid remote preview request: %w", err)
	}
	if _, err := ports.UnmarshalRemotePreviewRequest(request); err != nil {
		return fmt.Errorf("vev: invalid remote preview request: %w", err)
	}
	transport, err := ensureDaemonWithLifecycle(ctx, ipc.SocketDir(), realDial, realSpawn, defaultBackoff)
	if err != nil {
		return err
	}
	defer func() { _ = transport.Close() }()
	if err := transport.Send(ports.Frame{Type: ports.MsgRemotePreviewRequest, Payload: request}); err != nil {
		return fmt.Errorf("vev: sending remote preview request: %w", err)
	}
	frameCh := make(chan struct {
		frame ports.Frame
		err   error
	}, 1)
	go func() {
		frame, err := transport.Recv()
		frameCh <- struct {
			frame ports.Frame
			err   error
		}{frame: frame, err: err}
	}()
	select {
	case <-ctx.Done():
		_ = transport.Close()
		return ctx.Err()
	case result := <-frameCh:
		if result.err != nil {
			return fmt.Errorf("vev: receiving remote preview: %w", result.err)
		}
		if result.frame.Type != ports.MsgRemotePreviewResponse {
			return fmt.Errorf("vev: unexpected remote preview response %d", result.frame.Type)
		}
		if _, err := ports.UnmarshalRemotePreview(result.frame.Payload); err != nil {
			return fmt.Errorf("vev: malformed remote preview response: %w", err)
		}
		_, err := os.Stdout.Write(result.frame.Payload)
		return err
	}
}
