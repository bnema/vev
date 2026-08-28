package remote

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	renderer "github.com/bnema/vev-vt"
	"github.com/bnema/vev/internal/adapters/sshstdio"
	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol"
)

func previewClientTargetForTest() domain.RemoteSessionTarget {
	var lifecycle domain.SessionLifecycleID
	lifecycle[0] = 1
	return domain.RemoteSessionTarget{Endpoint: "arch", DisplayOrigin: "arch", LifecycleID: lifecycle, SessionName: "work", LiveTabID: "tab-1"}
}

func TestRemotePreviewClientBuildsExactSSHCommandAndDecodesResponse(t *testing.T) {
	var lifecycle domain.SessionLifecycleID
	lifecycle[0] = 9
	target := domain.RemoteSessionTarget{
		Endpoint: "build@mule", DisplayOrigin: "mule", LifecycleID: lifecycle,
		SessionName: "work", LiveTabID: "tab-1",
	}
	want := protocol.RemotePreview{
		Version: protocol.RemotePreviewSchemaVersion, Status: protocol.RemotePreviewOK,
		LifecycleID: lifecycle, TabID: target.LiveTabID, Revision: 7, Width: 1, Height: 1,
		Cells: []renderer.Cell{{Rune: 'x', Style: renderer.DefaultStyle()}},
	}
	payload := ports.MarshalRemotePreview(want)
	require.NotNil(t, payload)

	var gotPath string
	var gotArgs []string
	client := &PreviewClient{command: func(ctx context.Context, name string, args ...string) *exec.Cmd {
		gotPath = name
		gotArgs = append([]string(nil), args...)
		if len(args) != 3 {
			t.Fatalf("ssh args = %#v, want target and one remote command", args)
		}
		words := strings.Split(args[2], " ")
		if len(words) != 3 {
			t.Fatalf("remote command = %q, want three shell words", args[2])
		}
		encoded := strings.Trim(words[2], "'")
		requestPayload, err := base64.RawURLEncoding.DecodeString(encoded)
		if err != nil {
			t.Fatalf("decode request: %v", err)
		}
		request, err := ports.UnmarshalRemotePreviewRequest(requestPayload)
		if err != nil {
			t.Fatalf("decode request payload: %v", err)
		}
		if request.Target != target || request.Width != 1 || request.Height != 1 {
			t.Fatalf("request = %#v, want target and 1x1 dimensions", request)
		}
		return stdoutCmd(ctx, string(payload))
	}}

	got, err := client.Preview(context.Background(), target, 1, 1)
	require.NoError(t, err)
	require.Equal(t, want, got)
	wantCommand := sshstdio.BuildCommandForRemoteCommand(target.Endpoint, "vev", "_remote-preview", "request")
	require.Equal(t, wantCommand.Path, gotPath)
	require.Equal(t, target.Endpoint, gotArgs[1])
	require.Contains(t, gotArgs[2], "'_remote-preview'")
}

func TestRemotePreviewClientRejectsMismatchedResponseIdentity(t *testing.T) {
	target := previewClientTargetForTest()
	var otherLifecycle domain.SessionLifecycleID
	otherLifecycle[0] = 2
	valid := protocol.RemotePreview{
		Version: protocol.RemotePreviewSchemaVersion, Status: protocol.RemotePreviewOK,
		LifecycleID: target.LifecycleID, TabID: target.LiveTabID, Revision: 1, Width: 1, Height: 1,
		Cells: []renderer.Cell{{Rune: 'x', Style: renderer.DefaultStyle()}},
	}
	tests := []struct {
		name   string
		mutate func(*protocol.RemotePreview)
	}{
		{name: "lifecycle ID", mutate: func(preview *protocol.RemotePreview) { preview.LifecycleID = otherLifecycle }},
		{name: "tab ID", mutate: func(preview *protocol.RemotePreview) { preview.TabID = "tab-2" }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			preview := valid
			tt.mutate(&preview)
			payload := ports.MarshalRemotePreview(preview)
			require.NotNil(t, payload)

			client := &PreviewClient{command: func(ctx context.Context, _ string, _ ...string) *exec.Cmd {
				return stdoutCmd(ctx, string(payload))
			}}
			_, err := client.Preview(context.Background(), target, 1, 1)
			require.ErrorIs(t, err, errRemotePreviewIdentity)
		})
	}
}

func TestRemotePreviewClientRejectsMalformedResponse(t *testing.T) {
	client := &PreviewClient{command: func(ctx context.Context, _ string, _ ...string) *exec.Cmd {
		return stdoutCmd(ctx, "not-a-preview")
	}}
	var lifecycle domain.SessionLifecycleID
	lifecycle[0] = 1
	target := domain.RemoteSessionTarget{Endpoint: "arch", DisplayOrigin: "arch", LifecycleID: lifecycle, SessionName: "work", LiveTabID: "tab-1"}
	_, err := client.Preview(context.Background(), target, 1, 1)
	require.Error(t, err)
}

func TestRemotePreviewClientRejectsOversizedOutput(t *testing.T) {
	client := &PreviewClient{command: func(ctx context.Context, _ string, _ ...string) *exec.Cmd {
		return exec.CommandContext(ctx, "sh", "-c", fmt.Sprintf("head -c %d /dev/zero", protocol.RemotePreviewMaxBytes+1))
	}}
	target := previewClientTargetForTest()
	_, err := client.Preview(context.Background(), target, 1, 1)
	require.ErrorIs(t, err, errRemotePreviewTooLarge)
}

func TestRemotePreviewClientReturnsStderrDiagnostic(t *testing.T) {
	client := &PreviewClient{command: func(ctx context.Context, _ string, _ ...string) *exec.Cmd {
		return exec.CommandContext(ctx, "sh", "-c", "printf 'connection refused' >&2; exit 1")
	}}
	target := previewClientTargetForTest()
	_, err := client.Preview(context.Background(), target, 1, 1)
	require.ErrorIs(t, err, errRemotePreviewSSH)
	require.Contains(t, err.Error(), "connection refused")
}

func TestRemotePreviewClientReturnsInternalTimeoutSentinel(t *testing.T) {
	client := &PreviewClient{
		timeout: 20 * time.Millisecond,
		command: func(ctx context.Context, _ string, _ ...string) *exec.Cmd {
			return exec.CommandContext(ctx, "sleep", "10")
		},
	}
	_, err := client.Preview(context.Background(), previewClientTargetForTest(), 1, 1)
	require.ErrorIs(t, err, protocol.ErrRemotePreviewTimeout)
}

func TestRemotePreviewClientReturnsCallerCancellation(t *testing.T) {
	started := make(chan struct{})
	client := &PreviewClient{command: func(ctx context.Context, _ string, _ ...string) *exec.Cmd {
		close(started)
		return exec.CommandContext(ctx, "sleep", "10")
	}}
	target := previewClientTargetForTest()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	errCh := make(chan error, 1)
	go func() {
		_, err := client.Preview(ctx, target, 1, 1)
		errCh <- err
	}()
	<-started
	cancel()
	err := <-errCh
	require.ErrorIs(t, err, context.Canceled)
	require.False(t, errors.Is(err, errRemotePreviewSSH))
}
