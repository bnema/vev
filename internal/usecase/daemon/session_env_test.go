package daemon

import (
	"testing"

	"github.com/bnema/vev/internal/protocol"
	snapcodec "github.com/bnema/vev/internal/usecase/snapshot"
	"github.com/stretchr/testify/require"
)

func TestRefreshSessionEnvironment(t *testing.T) {
	tests := []struct {
		name    string
		current []string
		client  []string
		want    []string
	}{
		{
			name:    "ordinary variables keep creation values",
			current: []string{"SHELL=/usr/bin/fish", "PATH=/a", "HOME=/home/user"},
			client:  []string{"SHELL=/bin/bash", "PATH=/b", "NEW=1"},
			want:    []string{"SHELL=/usr/bin/fish", "PATH=/a", "HOME=/home/user"},
		},
		{
			name:    "graphical attach adds desktop variables to a console session",
			current: []string{"SHELL=/usr/bin/fish", "XDG_RUNTIME_DIR=/run/user/1000"},
			client:  []string{"WAYLAND_DISPLAY=wayland-1", "DISPLAY=:0", "XDG_RUNTIME_DIR=/run/user/1000", "DBUS_SESSION_BUS_ADDRESS=unix:path=/run/user/1000/bus"},
			want:    []string{"SHELL=/usr/bin/fish", "WAYLAND_DISPLAY=wayland-1", "DISPLAY=:0", "DBUS_SESSION_BUS_ADDRESS=unix:path=/run/user/1000/bus", "XDG_RUNTIME_DIR=/run/user/1000"},
		},
		{
			name:    "attach without a desktop never strips desktop variables",
			current: []string{"WAYLAND_DISPLAY=wayland-1", "DISPLAY=:0", "KEEP=1"},
			client:  []string{"SSH_CONNECTION=10.0.0.2 1 10.0.0.1 22"},
			want:    []string{"WAYLAND_DISPLAY=wayland-1", "DISPLAY=:0", "KEEP=1", "SSH_CONNECTION=10.0.0.2 1 10.0.0.1 22"},
		},
		{
			name:    "desktop variables update to the latest attach",
			current: []string{"WAYLAND_DISPLAY=wayland-2", "KEEP=1"},
			client:  []string{"WAYLAND_DISPLAY=wayland-1"},
			want:    []string{"KEEP=1", "WAYLAND_DISPLAY=wayland-1"},
		},
		{
			name:    "login variables absent from the client are removed",
			current: []string{"SSH_AUTH_SOCK=/tmp/stale", "SSH_CONNECTION=old", "SSH_TTY=/dev/pts/1", "KEEP=1"},
			client:  []string{"WAYLAND_DISPLAY=wayland-1"},
			want:    []string{"KEEP=1", "WAYLAND_DISPLAY=wayland-1"},
		},
		{
			name:    "agent pid follows the agent socket",
			current: []string{"SSH_AUTH_SOCK=/tmp/old", "SSH_AGENT_PID=11"},
			client:  []string{"SSH_AUTH_SOCK=/tmp/new"},
			want:    []string{"SSH_AUTH_SOCK=/tmp/new"},
		},
		{
			name:    "login variables follow the client",
			current: []string{"SSH_AUTH_SOCK=/tmp/old"},
			client:  []string{"SSH_AUTH_SOCK=/run/user/1000/agent"},
			want:    []string{"SSH_AUTH_SOCK=/run/user/1000/agent"},
		},
		{
			name:    "duplicate current entries collapse and the first client value wins",
			current: []string{"WAYLAND_DISPLAY=a", "WAYLAND_DISPLAY=b", "PAIR=a=b"},
			client:  []string{"WAYLAND_DISPLAY=c", "WAYLAND_DISPLAY=d"},
			want:    []string{"PAIR=a=b", "WAYLAND_DISPLAY=c"},
		},
		{
			name:    "empty values are values",
			current: []string{"DISPLAY=:0"},
			client:  []string{"DISPLAY="},
			want:    []string{"DISPLAY="},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			current := append([]string(nil), tt.current...)
			got := refreshSessionEnvironment(current, tt.client)
			require.Equal(t, tt.want, got)
			require.Equal(t, tt.current, current, "the previous snapshot is never mutated")
		})
	}
}

func TestSessionEnvironmentExport(t *testing.T) {
	env := []string{"SHELL=/usr/bin/fish", "WAYLAND_DISPLAY=wayland-1", "SSH_AUTH_SOCK=/run/agent", "DBUS_SESSION_BUS_ADDRESS=unix:path=/run/bus"}
	tests := []struct {
		name   string
		env    []string
		asJSON bool
		want   string
	}{
		{
			name: "text lists set session-bound variables only",
			env:  env,
			want: "WAYLAND_DISPLAY=wayland-1\nDBUS_SESSION_BUS_ADDRESS=unix:path=/run/bus\nSSH_AUTH_SOCK=/run/agent\n",
		},
		{
			name:   "json unsets absent login variables and omits absent desktop variables",
			env:    env,
			asJSON: true,
			want:   `{"DBUS_SESSION_BUS_ADDRESS":"unix:path=/run/bus","SSH_AGENT_PID":null,"SSH_AUTH_SOCK":"/run/agent","SSH_CLIENT":null,"SSH_CONNECTION":null,"SSH_TTY":null,"WAYLAND_DISPLAY":"wayland-1"}` + "\n",
		},
		{
			name: "empty session exports nothing as text",
			env:  []string{"SHELL=/bin/sh"},
			want: "",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := sessionEnvironmentExport(tt.env, tt.asJSON)
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestAdoptClientEnvironmentLocked(t *testing.T) {
	tests := []struct {
		name            string
		env             []string
		provisional     bool
		client          []string
		want            []string
		wantProvisional bool
	}{
		{
			name:        "provisional daemon environment is replaced wholesale once",
			env:         []string{"SHELL=/bin/sh", "PATH=/daemon"},
			provisional: true,
			client:      []string{"SHELL=/usr/bin/fish", "PATH=/client", "WAYLAND_DISPLAY=wayland-1"},
			want:        []string{"SHELL=/usr/bin/fish", "PATH=/client", "WAYLAND_DISPLAY=wayland-1"},
		},
		{
			name:   "client environment refreshes only session-bound variables",
			env:    []string{"SHELL=/usr/bin/fish", "PATH=/first"},
			client: []string{"SHELL=/bin/bash", "PATH=/second", "WAYLAND_DISPLAY=wayland-1"},
			want:   []string{"SHELL=/usr/bin/fish", "PATH=/first", "WAYLAND_DISPLAY=wayland-1"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := append([]string(nil), tt.client...)
			sess := &session{env: tt.env, envProvisional: tt.provisional}
			sess.adoptClientEnvironmentLocked(client)
			require.Equal(t, tt.want, sess.env)
			require.Equal(t, tt.wantProvisional, sess.envProvisional)
			client[0] = "MUTATED=1"
			require.NotContains(t, sess.env, "MUTATED=1", "the session never aliases the client slice")
		})
	}
}

func TestApplyTargetStateRefreshesTargetEnvironment(t *testing.T) {
	// The source session last saw a desktop client; the switching attachment
	// is a separate SSH client. Only the switching attachment's own
	// environment may reach the target.
	sourceEnv := []string{"SHELL=/bin/bash", "PATH=/source", "WAYLAND_DISPLAY=wayland-desktop", "SSH_AUTH_SOCK=/run/desktop-agent"}
	sshEnv := []string{"SHELL=/bin/zsh", "PATH=/ssh", "SSH_AUTH_SOCK=/tmp/ssh-agent", "SSH_CONNECTION=10.0.0.2 1 10.0.0.1 22"}
	targetEnv := []string{"SHELL=/usr/bin/fish", "PATH=/target", "WAYLAND_DISPLAY=wayland-old"}
	tests := []struct {
		name        string
		refresh     bool
		provisional bool
		clientEnv   []string
		want        []string
	}{
		{
			name:      "handoff refreshes from the switching attachment, not the source session",
			refresh:   true,
			clientEnv: sshEnv,
			want:      []string{"SHELL=/usr/bin/fish", "PATH=/target", "WAYLAND_DISPLAY=wayland-old", "SSH_AUTH_SOCK=/tmp/ssh-agent", "SSH_CONNECTION=10.0.0.2 1 10.0.0.1 22"},
		},
		{
			name:        "handoff into a restored session adopts the attachment environment",
			refresh:     true,
			provisional: true,
			clientEnv:   sshEnv,
			want:        sshEnv,
		},
		{
			name:    "daemon-owned attachment never changes the target",
			refresh: true,
			want:    targetEnv,
		},
		{
			name:        "daemon-owned attachment keeps a restored target provisional",
			refresh:     true,
			provisional: true,
			want:        targetEnv,
		},
		{
			name:      "no refresh request leaves the target untouched",
			clientEnv: sshEnv,
			want:      targetEnv,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			source := &session{env: copyEnvironment(sourceEnv)}
			target := &session{env: copyEnvironment(targetEnv), envProvisional: tt.provisional}
			next := &attachedClient{}
			next.setClientEnvironment(tt.clientEnv)
			d := &Daemon{}
			d.applyTargetStateLocked(&attachmentPublication{
				req:    attachmentTransitionRequest{target: target, next: next, refreshTargetEnvironment: tt.refresh},
				source: source,
			})
			require.Equal(t, tt.want, target.env)
			require.Equal(t, tt.provisional && tt.clientEnv == nil, target.envProvisional)
			require.Equal(t, sourceEnv, source.env)
		})
	}
}

func TestAttachedClientEnvironment(t *testing.T) {
	tests := []struct {
		name   string
		hello  protocol.Hello
		want   []string
		wantOK bool
	}{
		{
			name:   "client-owned hello is recorded",
			hello:  protocol.Hello{Env: []string{"A=1"}, EnvironmentPolicy: protocol.EnvironmentPolicyClientOwned},
			want:   []string{"A=1"},
			wantOK: true,
		},
		{
			name:   "client-owned empty hello still owns an empty environment",
			hello:  protocol.Hello{EnvironmentPolicy: protocol.EnvironmentPolicyClientOwned},
			want:   []string{},
			wantOK: true,
		},
		{
			name:  "daemon-owned hello records nothing",
			hello: protocol.Hello{Env: []string{"A=1"}, EnvironmentPolicy: protocol.EnvironmentPolicyDaemonOwned},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			hello := tt.hello
			hello.Env = append([]string(nil), tt.hello.Env...)
			ac := &attachedClient{}
			ac.setClientEnvironment(helloClientEnvironment(hello))
			got, ok := ac.clientEnvironment()
			require.Equal(t, tt.wantOK, ok)
			require.Equal(t, tt.want, got)
			if ok && len(hello.Env) > 0 {
				hello.Env[0] = "MUTATED=1"
				again, _ := ac.clientEnvironment()
				require.Equal(t, tt.want, again, "the attachment owns a private copy")
			}
		})
	}
}

func TestNewRestoredSessionEnvironmentIsProvisional(t *testing.T) {
	d := newTestDaemon(t, nil, stubClock{})
	d.baseEnv = []string{"SHELL=/bin/sh", "PATH=/daemon"}
	sess := d.newRestoredSession(snapcodec.Session{Name: "work"}, d.serveCtx, func() {}, nil)
	require.Equal(t, d.baseEnv, sess.env)
	require.True(t, sess.envProvisional, "the first client-owned attach must replace the daemon's environment")
}
