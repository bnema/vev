package remote

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"os/exec"
	"time"

	"github.com/bnema/vev/internal/adapters/sshstdio"
	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
)

const (
	remotePreviewCommandTimeout = 5 * time.Second
	remotePreviewWaitDelay      = 500 * time.Millisecond
	remotePreviewMaxDiagnostic  = 4 << 10
)

var (
	errRemotePreviewSSH      = errors.New("remote preview: ssh command failed")
	errRemotePreviewTooLarge = errors.New("remote preview: output exceeds size limit")
)

// PreviewClient invokes the hidden remote preview command over SSH. The
// command receives only a base64url request and stdout is decoded as one
// bounded binary response.
type PreviewClient struct {
	command func(ctx context.Context, name string, args ...string) *exec.Cmd
	timeout time.Duration
}

var _ ports.RemotePreviewClient = (*PreviewClient)(nil)

func NewPreviewClient() ports.RemotePreviewClient {
	return &PreviewClient{command: exec.CommandContext}
}

func (c *PreviewClient) Preview(ctx context.Context, target domain.RemoteSessionTarget, width, height uint16) (ports.RemotePreview, error) {
	request := ports.RemotePreviewRequest{Version: ports.RemotePreviewSchemaVersion, Target: target, Width: width, Height: height}
	payload := ports.MarshalRemotePreviewRequest(request)
	if payload == nil {
		return ports.RemotePreview{}, ports.ErrInvalidRemotePreviewRequest
	}
	runCtx, cancel := context.WithTimeout(ctx, c.previewTimeout())
	defer cancel()
	command := c.command
	if command == nil {
		command = exec.CommandContext
	}
	encoded := base64.RawURLEncoding.EncodeToString(payload)
	spec := sshstdio.BuildCommandForRemoteCommand(target.Endpoint, "vev", "_remote-preview", encoded)
	cmd := command(runCtx, spec.Path, spec.Args...)
	stdout := boundedBuffer{limit: ports.RemotePreviewMaxBytes}
	stderr := boundedBuffer{limit: remotePreviewMaxDiagnostic}
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	cmd.WaitDelay = remotePreviewWaitDelay
	if err := cmd.Run(); err != nil {
		if ctxErr := runCtx.Err(); ctxErr != nil {
			return ports.RemotePreview{}, ctxErr
		}
		if stdout.overflow || stderr.overflow {
			return ports.RemotePreview{}, errRemotePreviewTooLarge
		}
		return ports.RemotePreview{}, fmt.Errorf("%w: %v", errRemotePreviewSSH, err)
	}
	if stdout.overflow || stderr.overflow {
		return ports.RemotePreview{}, errRemotePreviewTooLarge
	}
	preview, err := ports.UnmarshalRemotePreview(stdout.Bytes())
	if err != nil {
		return ports.RemotePreview{}, err
	}
	if preview.Status != ports.RemotePreviewOK {
		return ports.RemotePreview{}, fmt.Errorf("remote preview unavailable: status %d", preview.Status)
	}
	return preview, nil
}

func (c *PreviewClient) previewTimeout() time.Duration {
	if c != nil && c.timeout > 0 {
		return c.timeout
	}
	return remotePreviewCommandTimeout
}
