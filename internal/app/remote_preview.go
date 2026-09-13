package app

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"os"

	"github.com/bnema/vev/internal/adapters/ipc"
	"github.com/bnema/vev/internal/adapters/sessionwire"
	"github.com/bnema/vev/internal/protocol"
)

// runRemotePreview is the hidden SSH-side carriage. The request is encoded in
// one shell-safe argument, then the daemon returns one bounded binary response
// whose payload is written verbatim to stdout; no viewport bytes are logged.
func runRemotePreview(ctx context.Context, encoded string) error {
	request, err := base64.RawURLEncoding.DecodeString(encoded)
	if err != nil {
		return fmt.Errorf("vev: invalid remote preview request: %w", err)
	}
	decoded, err := sessionwire.DecodeClientEnvelope(request)
	if err != nil {
		return fmt.Errorf("vev: invalid remote preview request: %w", err)
	}
	if _, ok := decoded.(protocol.RemotePreviewRequest); !ok {
		return fmt.Errorf("vev: invalid remote preview request: %w", sessionwire.ErrWrongDirection)
	}
	transport, err := ipc.DialContext(ctx, ipc.SocketDir())
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		return writeRemotePreviewStatus(protocol.RemotePreviewUnavailable)
	}
	defer func() { _ = transport.Close() }()
	connection := sessionwire.NewClientConnection(transport)
	if err := connection.SendClient(decoded); err != nil {
		return fmt.Errorf("vev: sending remote preview request: %w", err)
	}
	type previewReply struct {
		message protocol.ServerMessage
		err     error
	}
	replyCh := make(chan previewReply, 1)
	go func() {
		message, err := connection.ReceiveServer()
		replyCh <- previewReply{message: message, err: err}
	}()
	select {
	case <-ctx.Done():
		_ = transport.Close()
		return ctx.Err()
	case result := <-replyCh:
		if result.err != nil {
			return fmt.Errorf("vev: receiving remote preview: %w", result.err)
		}
		preview, ok := result.message.(protocol.RemotePreview)
		if !ok {
			return fmt.Errorf("vev: unexpected remote preview response %T", result.message)
		}
		raw, err := sessionwire.EncodeServerMessage(preview)
		if err != nil {
			return fmt.Errorf("vev: malformed remote preview response: %w", err)
		}
		_, err = os.Stdout.Write(raw)
		return err
	}
}

func writeRemotePreviewStatus(status protocol.RemotePreviewStatus) error {
	raw, err := sessionwire.EncodeServerMessage(protocol.RemotePreview{Version: protocol.RemotePreviewSchemaVersion, Status: status})
	if err != nil {
		return errors.New("vev: failed to encode remote preview status")
	}
	if _, err := os.Stdout.Write(raw); err != nil {
		return fmt.Errorf("vev: writing remote preview status: %w", err)
	}
	return nil
}
