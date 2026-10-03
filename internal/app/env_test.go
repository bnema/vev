package app

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/protocol/wire"
	"github.com/stretchr/testify/require"
)

func TestParseEnvArgs(t *testing.T) {
	tests := []struct {
		name    string
		args    []string
		want    envInvocation
		wantErr string
	}{
		{name: "fish", args: []string{"fish"}, want: envInvocation{shell: "fish"}},
		{name: "sh with session", args: []string{"sh", "-s", "work"}, want: envInvocation{shell: "sh", session: "work"}},
		{name: "session before shell", args: []string{"-s", "work", "fish"}, want: envInvocation{shell: "fish", session: "work"}},
		{name: "help without shell", args: []string{"--help"}, want: envInvocation{help: true}},
		{name: "missing shell", wantErr: "requires a shell"},
		{name: "two shells", args: []string{"fish", "sh"}, wantErr: "one shell"},
		{name: "same shell twice", args: []string{"fish", "fish"}, wantErr: "one shell"},
		{name: "unsupported shell", args: []string{"nu"}, wantErr: `unsupported shell "nu"`},
		{name: "json belongs to cmd env", args: []string{"--json"}, wantErr: `unknown flag "--json"`},
		{name: "old shell flag", args: []string{"--fish"}, wantErr: `unknown flag "--fish"`},
		{name: "missing session", args: []string{"fish", "-s"}, wantErr: "requires a session name"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseEnvArgs(tt.args)
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
				require.Equal(t, 2, exitCode(err))
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestWriteShellEnvironment(t *testing.T) {
	encoded := `{"WAYLAND_DISPLAY":"wayland-1","SSH_AUTH_SOCK":null,"ODD":"it's \\ $HOME"}`
	tests := []struct {
		name    string
		shell   string
		encoded string
		want    string
		wantErr string
	}{
		{
			name:    "fish sets and erases",
			shell:   "fish",
			encoded: encoded,
			want:    "set -gx ODD 'it\\'s \\\\ $HOME'\nset -e SSH_AUTH_SOCK\nset -gx WAYLAND_DISPLAY 'wayland-1'\n",
		},
		{
			name:    "sh exports and unsets",
			shell:   "sh",
			encoded: encoded,
			want:    "export ODD='it'\\''s \\ $HOME'\nunset SSH_AUTH_SOCK\nexport WAYLAND_DISPLAY='wayland-1'\n",
		},
		{name: "empty object", shell: "sh", encoded: `{}`, want: ""},
		{name: "injected name", shell: "sh", encoded: `{"BAD;rm":"x"}`, wantErr: "invalid session environment name"},
		{name: "leading digit", shell: "sh", encoded: `{"1X":"x"}`, wantErr: "invalid session environment name"},
		{name: "not json", shell: "sh", encoded: `not json`, wantErr: "decoding session environment"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var out bytes.Buffer
			err := writeShellEnvironment(&out, tt.shell, tt.encoded)
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
				require.Empty(t, out.String())
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.want, out.String())
		})
	}
}

func TestRunEnvWithDeps(t *testing.T) {
	const reply = `{"SSH_AUTH_SOCK":null,"WAYLAND_DISPLAY":"wayland-1"}` + "\n"
	tests := []struct {
		name        string
		invocation  envInvocation
		vev         string
		result      protocol.CommandResult
		want        string
		wantErr     string
		wantCode    int
		wantRequest protocol.CommandRequest
	}{
		{
			name:        "inside a pane follows the pane session",
			invocation:  envInvocation{shell: "fish"},
			vev:         "session=work,tab=t_1,pane=p_1",
			result:      protocol.CommandResult{RequestID: 1, Outcome: protocol.CommandSucceeded, Output: reply},
			want:        "set -e SSH_AUTH_SOCK\nset -gx WAYLAND_DISPLAY 'wayland-1'\n",
			wantRequest: protocol.CommandRequest{Slug: "env", JSON: true, TargetSession: "work", TargetTab: "t_1", TargetPane: "p_1"},
		},
		{
			name:        "explicit session outside a pane",
			invocation:  envInvocation{shell: "sh", session: "work"},
			result:      protocol.CommandResult{RequestID: 1, Outcome: protocol.CommandSucceeded, Output: reply},
			want:        "unset SSH_AUTH_SOCK\nexport WAYLAND_DISPLAY='wayland-1'\n",
			wantRequest: protocol.CommandRequest{Slug: "env", JSON: true, TargetSession: "work"},
		},
		{
			name:       "outside a pane without a session",
			invocation: envInvocation{shell: "fish"},
			wantErr:    "inside a vev pane",
			wantCode:   2,
		},
		{
			name:       "daemon failure prints no shell code",
			invocation: envInvocation{shell: "fish", session: "gone"},
			result:     protocol.CommandResult{RequestID: 1, Outcome: protocol.CommandFailed, Code: protocol.ErrNoSuchTarget, Text: "no such session: gone"},
			wantErr:    "no such session: gone",
			wantCode:   1,
		},
		{
			name:       "help",
			invocation: envInvocation{help: true},
			want:       envHelp + "\n",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stream := newSeamBrokerStream(tt.result)
			service := newSeamBrokerService(stream)
			var out bytes.Buffer
			err := runEnvWithDeps(context.Background(), tt.invocation, cmdDeps{
				stdout:  &out,
				getenv:  func(string) string { return tt.vev },
				connect: func(context.Context) (ports.BrokerService, error) { return service, nil },
				dial: func(context.Context, string) (wire.Transport, error) {
					return nil, errors.New("unexpected dial")
				},
			})
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
				require.Equal(t, tt.wantCode, exitCode(err))
				require.Empty(t, out.String())
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.want, out.String())
			if tt.wantRequest.Slug == "" {
				require.Zero(t, stream.sentCount())
				return
			}
			stream.mu.Lock()
			require.Len(t, stream.sent, 1)
			request, ok := stream.sent[0].(protocol.CommandRequest)
			stream.mu.Unlock()
			require.True(t, ok)
			require.Equal(t, tt.wantRequest.Slug, request.Slug)
			require.Equal(t, tt.wantRequest.JSON, request.JSON)
			require.Equal(t, tt.wantRequest.TargetSession, request.TargetSession)
			require.Equal(t, tt.wantRequest.TargetTab, request.TargetTab)
			require.Equal(t, tt.wantRequest.TargetPane, request.TargetPane)
		})
	}
}

func TestCmdEnvHelpUsesRegistryUsage(t *testing.T) {
	require.Contains(t, cmdHelp(cmdInvocation{slug: "env", help: true}), "usage: vev cmd [-s <session>] [--self] env [--json]")
}
