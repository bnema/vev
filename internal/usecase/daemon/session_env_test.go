package daemon

import (
	"testing"

	"github.com/bnema/vev/internal/ports"
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
			name:    "refreshing from the creating environment keeps it byte-identical",
			current: []string{"A=1", "WAYLAND_DISPLAY=wayland-1", "PATH=/a", "DBUS_SESSION_BUS_ADDRESS=bus", "SSH_AUTH_SOCK=/agent", "Z=2"},
			client:  []string{"A=1", "WAYLAND_DISPLAY=wayland-1", "PATH=/a", "DBUS_SESSION_BUS_ADDRESS=bus", "SSH_AUTH_SOCK=/agent", "Z=2"},
			want:    []string{"A=1", "WAYLAND_DISPLAY=wayland-1", "PATH=/a", "DBUS_SESSION_BUS_ADDRESS=bus", "SSH_AUTH_SOCK=/agent", "Z=2"},
		},
		{
			name:    "ordinary variables keep creation values",
			current: []string{"SHELL=/usr/bin/fish", "PATH=/a", "HOME=/home/user"},
			client:  []string{"SHELL=/bin/bash", "PATH=/b", "NEW=1"},
			want:    []string{"SHELL=/usr/bin/fish", "PATH=/a", "HOME=/home/user"},
		},
		{
			name:    "graphical attach adds the display group to a console session",
			current: []string{"SHELL=/usr/bin/fish", "XDG_RUNTIME_DIR=/run/user/1000"},
			client:  []string{"WAYLAND_DISPLAY=wayland-1", "DISPLAY=:0", "XDG_RUNTIME_DIR=/run/user/1000", "DBUS_SESSION_BUS_ADDRESS=unix:path=/run/user/1000/bus"},
			want:    []string{"SHELL=/usr/bin/fish", "XDG_RUNTIME_DIR=/run/user/1000", "WAYLAND_DISPLAY=wayland-1", "DISPLAY=:0", "DBUS_SESSION_BUS_ADDRESS=unix:path=/run/user/1000/bus"},
		},
		{
			name:    "attach without a display leaves the display group untouched",
			current: []string{"WAYLAND_DISPLAY=wayland-1", "DISPLAY=:0", "XDG_SESSION_TYPE=wayland", "KEEP=1"},
			client:  []string{"XDG_SESSION_TYPE=tty", "XDG_SESSION_ID=7", "SSH_CONNECTION=10.0.0.2 1 10.0.0.1 22"},
			want:    []string{"WAYLAND_DISPLAY=wayland-1", "DISPLAY=:0", "XDG_SESSION_TYPE=wayland", "KEEP=1", "SSH_CONNECTION=10.0.0.2 1 10.0.0.1 22"},
		},
		{
			name:    "ssh with x forwarding leaves the local display group untouched",
			current: []string{"WAYLAND_DISPLAY=wayland-1", "XDG_SESSION_TYPE=wayland", "XDG_SESSION_ID=2"},
			client:  []string{"DISPLAY=localhost:10.0", "XDG_SESSION_TYPE=tty", "XDG_SESSION_ID=9", "SSH_CONNECTION=10.0.0.2 1 10.0.0.1 22", "SSH_AUTH_SOCK=/tmp/fwd-agent"},
			want:    []string{"WAYLAND_DISPLAY=wayland-1", "XDG_SESSION_TYPE=wayland", "XDG_SESSION_ID=2", "SSH_AUTH_SOCK=/tmp/fwd-agent", "SSH_CONNECTION=10.0.0.2 1 10.0.0.1 22"},
		},
		{
			name:    "second compositor on another console replaces the whole display group",
			current: []string{"KEEP=1", "WAYLAND_DISPLAY=wayland-0", "DISPLAY=:0", "XDG_CURRENT_DESKTOP=first", "XDG_SESSION_ID=2"},
			client:  []string{"WAYLAND_DISPLAY=wayland-1", "XDG_CURRENT_DESKTOP=second", "XDG_SESSION_ID=3"},
			want:    []string{"KEEP=1", "WAYLAND_DISPLAY=wayland-1", "XDG_CURRENT_DESKTOP=second", "XDG_SESSION_ID=3"},
		},
		{
			name:    "x11-only desktop removes the stale wayland socket",
			current: []string{"WAYLAND_DISPLAY=wayland-1", "XDG_SESSION_TYPE=wayland"},
			client:  []string{"DISPLAY=:1", "XAUTHORITY=/tmp/xauth", "XDG_SESSION_TYPE=x11"},
			want:    []string{"XDG_SESSION_TYPE=x11", "DISPLAY=:1", "XAUTHORITY=/tmp/xauth"},
		},
		{
			name:    "user bus variables follow a client that has them",
			current: []string{"DBUS_SESSION_BUS_ADDRESS=unix:path=/old/bus", "XDG_RUNTIME_DIR=/old"},
			client:  []string{"DBUS_SESSION_BUS_ADDRESS=unix:path=/run/user/1000/bus", "XDG_RUNTIME_DIR=/run/user/1000"},
			want:    []string{"DBUS_SESSION_BUS_ADDRESS=unix:path=/run/user/1000/bus", "XDG_RUNTIME_DIR=/run/user/1000"},
		},
		{
			name:    "user bus variables are kept by an attach without them",
			current: []string{"DBUS_SESSION_BUS_ADDRESS=unix:path=/run/user/1000/bus", "XDG_RUNTIME_DIR=/run/user/1000"},
			client:  []string{"WAYLAND_DISPLAY=wayland-1"},
			want:    []string{"DBUS_SESSION_BUS_ADDRESS=unix:path=/run/user/1000/bus", "XDG_RUNTIME_DIR=/run/user/1000", "WAYLAND_DISPLAY=wayland-1"},
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
			name:    "duplicate current entries collapse in place and the first client value wins",
			current: []string{"WAYLAND_DISPLAY=a", "WAYLAND_DISPLAY=b", "PAIR=a=b"},
			client:  []string{"WAYLAND_DISPLAY=c", "WAYLAND_DISPLAY=d"},
			want:    []string{"WAYLAND_DISPLAY=c", "PAIR=a=b"},
		},
		{
			name:    "an empty display value is not a graphical client",
			current: []string{"WAYLAND_DISPLAY=wayland-1", "DISPLAY=:0"},
			client:  []string{"DISPLAY=", "WAYLAND_DISPLAY="},
			want:    []string{"WAYLAND_DISPLAY=wayland-1", "DISPLAY=:0"},
		},
		{
			name:    "an empty member of a graphical client's group is copied",
			current: []string{"WAYLAND_DISPLAY=wayland-1", "DISPLAY=:0"},
			client:  []string{"WAYLAND_DISPLAY=wayland-2", "DISPLAY="},
			want:    []string{"WAYLAND_DISPLAY=wayland-2", "DISPLAY="},
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
	tests := []struct {
		name   string
		env    []string
		asJSON bool
		want   string
	}{
		{
			name: "text lists set session-bound variables only",
			env:  []string{"SHELL=/usr/bin/fish", "WAYLAND_DISPLAY=wayland-1", "SSH_AUTH_SOCK=/run/agent", "DBUS_SESSION_BUS_ADDRESS=unix:path=/run/bus"},
			want: "WAYLAND_DISPLAY=wayland-1\nDBUS_SESSION_BUS_ADDRESS=unix:path=/run/bus\nSSH_AUTH_SOCK=/run/agent\n",
		},
		{
			name:   "json with a display unsets the rest of the display group",
			env:    []string{"WAYLAND_DISPLAY=wayland-1", "DBUS_SESSION_BUS_ADDRESS=unix:path=/run/bus", "SSH_AUTH_SOCK=/run/agent"},
			asJSON: true,
			want:   `{"DBUS_SESSION_BUS_ADDRESS":"unix:path=/run/bus","DISPLAY":null,"SSH_AGENT_PID":null,"SSH_AUTH_SOCK":"/run/agent","SSH_CLIENT":null,"SSH_CONNECTION":null,"SSH_TTY":null,"WAYLAND_DISPLAY":"wayland-1","XAUTHORITY":null,"XDG_CURRENT_DESKTOP":null,"XDG_SESSION_CLASS":null,"XDG_SESSION_DESKTOP":null,"XDG_SESSION_ID":null,"XDG_SESSION_TYPE":null}` + "\n",
		},
		{
			name:   "json without a display leaves display variables alone",
			env:    []string{"SHELL=/bin/sh", "XDG_RUNTIME_DIR=/run/user/1000"},
			asJSON: true,
			want:   `{"SSH_AGENT_PID":null,"SSH_AUTH_SOCK":null,"SSH_CLIENT":null,"SSH_CONNECTION":null,"SSH_TTY":null,"XDG_RUNTIME_DIR":"/run/user/1000"}` + "\n",
		},
		{
			name:   "json with only x11 unsets a stale wayland socket",
			env:    []string{"DISPLAY=:1"},
			asJSON: true,
			want:   `{"DISPLAY":":1","SSH_AGENT_PID":null,"SSH_AUTH_SOCK":null,"SSH_CLIENT":null,"SSH_CONNECTION":null,"SSH_TTY":null,"WAYLAND_DISPLAY":null,"XAUTHORITY":null,"XDG_CURRENT_DESKTOP":null,"XDG_SESSION_CLASS":null,"XDG_SESSION_DESKTOP":null,"XDG_SESSION_ID":null,"XDG_SESSION_TYPE":null}` + "\n",
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
			name:        "provisional session keeps its display for an ssh client with x forwarding",
			env:         []string{"PATH=/daemon", "WAYLAND_DISPLAY=wayland-1", "XDG_SESSION_TYPE=wayland"},
			provisional: true,
			client:      []string{"PATH=/ssh", "DISPLAY=localhost:10.0", "XDG_SESSION_TYPE=tty", "SSH_CONNECTION=10.0.0.2 1 10.0.0.1 22"},
			want:        []string{"PATH=/ssh", "XDG_SESSION_TYPE=wayland", "SSH_CONNECTION=10.0.0.2 1 10.0.0.1 22", "WAYLAND_DISPLAY=wayland-1"},
		},
		{
			name:        "provisional session keeps its display for a console client",
			env:         []string{"PATH=/daemon", "WAYLAND_DISPLAY=wayland-1"},
			provisional: true,
			client:      []string{"PATH=/console"},
			want:        []string{"PATH=/console", "WAYLAND_DISPLAY=wayland-1"},
		},
		{
			name:        "provisional session without a display keeps the console client's session variables",
			env:         []string{"PATH=/daemon"},
			provisional: true,
			client:      []string{"PATH=/console", "XDG_SESSION_TYPE=tty", "XDG_SESSION_ID=4"},
			want:        []string{"PATH=/console", "XDG_SESSION_TYPE=tty", "XDG_SESSION_ID=4"},
		},
		{
			name:   "client environment refreshes only session-bound variables",
			env:    []string{"SHELL=/usr/bin/fish", "PATH=/first"},
			client: []string{"WAYLAND_DISPLAY=wayland-1", "SHELL=/bin/bash", "PATH=/second"},
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
			// client[0] is always copied (provisional) or session-bound
			// (refresh), so mutating it proves the session owns its entries.
			client[0] = "WAYLAND_DISPLAY=mutated"
			require.NotContains(t, sess.env, "WAYLAND_DISPLAY=mutated", "the session never aliases the client slice")
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
		preserve    bool
		provisional bool
		clientEnv   []string
		want        []string
	}{
		{
			name:      "handoff refreshes from the switching attachment, not the source session",
			clientEnv: sshEnv,
			want:      []string{"SHELL=/usr/bin/fish", "PATH=/target", "WAYLAND_DISPLAY=wayland-old", "SSH_AUTH_SOCK=/tmp/ssh-agent", "SSH_CONNECTION=10.0.0.2 1 10.0.0.1 22"},
		},
		{
			name:        "ssh handoff into a restored session adopts it but keeps the local display",
			provisional: true,
			clientEnv:   sshEnv,
			want:        append(append([]string(nil), sshEnv...), "WAYLAND_DISPLAY=wayland-old"),
		},
		{
			name: "daemon-owned attachment never changes the target",
			want: targetEnv,
		},
		{
			name:        "daemon-owned attachment keeps a restored target provisional",
			provisional: true,
			want:        targetEnv,
		},
		{
			name:      "a preserved attachment leaves the target untouched",
			preserve:  true,
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
				req:    attachmentTransitionRequest{target: target, next: next, preserveAttachment: tt.preserve},
				source: source,
			})
			require.Equal(t, tt.want, target.env)
			require.Equal(t, tt.provisional && (tt.clientEnv == nil || tt.preserve), target.envProvisional)
			require.Equal(t, sourceEnv, source.env, "the source session is never read or changed")
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

// TestFailedAttachTransitionLeavesSessionEnvironment proves a real attach
// applies the environment inside the publication: when the transition fails
// after the attachment and its client environment are prepared, the session
// environment and its provisional state stay untouched.
func TestFailedAttachTransitionLeavesSessionEnvironment(t *testing.T) {
	d := newTestDaemon(t, nil, stubClock{})
	sess := addControlSession(d, "work", "tab-1", "pane-1")
	sess.ephemeral = false
	sess.incarnation = newTestLifecycle(t)
	sess.mu.Lock()
	sess.env = []string{"PATH=/daemon", "WAYLAND_DISPLAY=wayland-1"}
	sess.envProvisional = true
	sess.mu.Unlock()
	// Fail the transition after finishAttach prepared the attachment, where
	// an eager adopt would already have rewritten the session.
	d.afterAttachmentEffectParticipantsSnapshotted = func(string, []*attachedClient) {
		d.mu.Lock()
		d.closing = true
		d.mu.Unlock()
	}

	target := protocol.ExactSessionTarget{LifecycleID: sess.incarnation, SessionName: sess.name}
	env := []string{"PATH=/client", "WAYLAND_DISPLAY=wayland-2"}
	hello := protocol.Hello{
		Version: protocol.Version, Intent: protocol.IntentAttach, Name: target.SessionName, Size: defaultSize,
		ExactTarget: &target, Env: env, EnvironmentPolicy: protocol.EnvironmentPolicyClientOwned,
	}
	admission := ports.SessionAdmission{
		Origin: ports.SessionOriginLocal, Policy: admissionTestPolicy(ports.SessionOriginLocal),
		Purpose: ports.BrokerStreamAttachment, Admission: ports.BrokerAdmissionExact, Target: target, Env: env,
	}
	_, _, err := d.route(hello, &admittedTransport{ServerConnection: &closeTrackingTransport{}, admission: admission, present: true})
	require.Error(t, err)

	sess.mu.Lock()
	defer sess.mu.Unlock()
	require.Equal(t, []string{"PATH=/daemon", "WAYLAND_DISPLAY=wayland-1"}, sess.env)
	require.True(t, sess.envProvisional)
}

func TestEnvironmentSeedCarriesProvisionalState(t *testing.T) {
	tests := []struct {
		name        string
		provisional bool
	}{
		{name: "client-owned source seeds a client-owned session"},
		{name: "provisional source seeds a provisional session", provisional: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			factory := &controlPTYFactory{}
			d := newTestDaemon(t, factory, stubClock{})
			t.Cleanup(func() { factory.close(); d.sessWg.Wait() })
			source := addControlSession(d, "work", "t_work", "p_work")
			source.mu.Lock()
			source.env = []string{"PATH=/source"}
			source.envProvisional = tt.provisional
			source.mu.Unlock()

			result := sendCommand(t, d, protocol.CommandRequest{Slug: "new-session", Args: []string{"seeded"}, TargetSession: "work"})
			require.True(t, result.Outcome == protocol.CommandSucceeded, result.Text)
			d.mu.Lock()
			seeded := d.findByNameLocked("seeded")
			d.mu.Unlock()
			require.NotNil(t, seeded)
			seeded.mu.Lock()
			defer seeded.mu.Unlock()
			require.Equal(t, []string{"PATH=/source"}, seeded.env)
			require.Equal(t, tt.provisional, seeded.envProvisional)
		})
	}
}
