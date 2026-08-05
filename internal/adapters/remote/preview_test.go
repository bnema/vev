package remote

import (
	"context"
	"encoding/base64"
	"os/exec"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/adapters/sshstdio"
	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/pkg/renderer"
)

func TestRemotePreviewClientBuildsExactSSHCommandAndDecodesResponse(t *testing.T) {
	var lifecycle domain.SessionLifecycleID
	lifecycle[0] = 9
	target := domain.RemoteSessionTarget{
		Endpoint: "build@mule", DisplayOrigin: "mule", LifecycleID: lifecycle,
		SessionName: "work", LiveTabID: "tab-1",
	}
	want := ports.RemotePreview{
		Version: ports.RemotePreviewSchemaVersion, Status: ports.RemotePreviewOK,
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
