package daemon

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol"
)

// admittedTransport wraps a typed server connection and stamps the provisioned
// admission an accepting side would have resolved from its binding. It
// implements the optional ports.SessionAdmissionProvider seam the daemon asserts
// structurally, returning a defensive copy exactly as a real accepted
// connection does. present=false models a legacy connection that carries no
// provisioned admission at all.
type admittedTransport struct {
	ports.ServerConnection
	admission ports.SessionAdmission
	present   bool
}

func (t *admittedTransport) SessionAdmission() (ports.SessionAdmission, bool) {
	if !t.present {
		return ports.SessionAdmission{}, false
	}
	return t.admission.Clone(), true
}

// admissionTestPolicy builds one valid exact policy whose environment-policy
// owner matches the supplied carriage locality, as a provisioned binding does.
func admissionTestPolicy(origin ports.SessionConnectionOrigin) ports.BrokerPolicy {
	environment := protocol.EnvironmentPolicyClientOwned
	transport := "unix-mux"
	if origin == ports.SessionOriginRemote {
		environment = protocol.EnvironmentPolicyDaemonOwned
		transport = "ssh-quic"
	}
	return ports.BrokerPolicy{
		ProtocolVersion: protocol.Version, CatalogSchemaVersion: 3,
		EnvironmentPolicy: environment, Transport: transport,
		Trust: "same-user", Launch: "explicit", Isolation: "per-user",
	}
}

func admissionTestExact(name string, marker byte) protocol.ExactSessionTarget {
	return protocol.ExactSessionTarget{LifecycleID: domain.SessionLifecycleID{marker}, SessionName: name}
}

func ptrExactTarget(target protocol.ExactSessionTarget) *protocol.ExactSessionTarget {
	return &target
}

// TestValidateSessionHelloAdmissionMatrix pins the closed admission contract:
// every admission variant maps to exactly one intent and credential shape, and a
// lying locality, policy, environment, name, target, purpose, or admission
// variant is refused rather than narrowed.
func TestValidateSessionHelloAdmissionMatrix(t *testing.T) {
	localPolicy := admissionTestPolicy(ports.SessionOriginLocal)
	remotePolicy := admissionTestPolicy(ports.SessionOriginRemote)
	target := admissionTestExact("work", 1)
	localEnv := []string{"TERM=xterm-256color", "PATH=/bin"}

	local := func(admission ports.SessionAdmission) ports.SessionAdmission {
		admission.Origin = ports.SessionOriginLocal
		admission.Policy = localPolicy
		admission.Purpose = ports.BrokerStreamAttachment
		return admission
	}
	remote := func(admission ports.SessionAdmission) ports.SessionAdmission {
		admission.Origin = ports.SessionOriginRemote
		admission.Policy = remotePolicy
		admission.Purpose = ports.BrokerStreamAttachment
		return admission
	}

	tests := []struct {
		name      string
		hello     protocol.Hello
		admission ports.SessionAdmission
		wantErr   bool
	}{
		{
			name: "local ephemeral creation",
			hello: protocol.Hello{
				Intent: protocol.IntentEphemeral, Cwd: "/client/cwd", Env: localEnv,
			},
			admission: local(ports.SessionAdmission{Admission: ports.BrokerAdmissionCreateEphemeral, Env: localEnv}),
		},
		{
			name: "local named creation",
			hello: protocol.Hello{
				Intent: protocol.IntentNew, Name: "work", Cwd: "/client/cwd", Env: localEnv,
			},
			admission: local(ports.SessionAdmission{Admission: ports.BrokerAdmissionCreateNamed, Name: "work", Env: localEnv}),
		},
		{
			name:      "local named attach",
			hello:     protocol.Hello{Intent: protocol.IntentAttach, Name: "work", Cwd: "/client/cwd", Env: localEnv},
			admission: local(ports.SessionAdmission{Admission: ports.BrokerAdmissionAttachNamed, Name: "work", Env: localEnv}),
		},
		{
			name: "remote named attach",
			hello: protocol.Hello{
				Intent: protocol.IntentAttach, Name: "work", Remote: true,
				EnvironmentPolicy: protocol.EnvironmentPolicyDaemonOwned,
			},
			admission: remote(ports.SessionAdmission{Admission: ports.BrokerAdmissionAttachNamed, Name: "work"}),
		},
		{
			name:      "named attach cannot create",
			hello:     protocol.Hello{Intent: protocol.IntentNew, Name: "work"},
			admission: local(ports.SessionAdmission{Admission: ports.BrokerAdmissionAttachNamed, Name: "work"}),
			wantErr:   true,
		},
		{
			name:      "liar name on a named attach",
			hello:     protocol.Hello{Intent: protocol.IntentAttach, Name: "other"},
			admission: local(ports.SessionAdmission{Admission: ports.BrokerAdmissionAttachNamed, Name: "work"}),
			wantErr:   true,
		},
		{
			name:      "named attach smuggles an exact target",
			hello:     protocol.Hello{Intent: protocol.IntentAttach, Name: "work", ExactTarget: &target},
			admission: local(ports.SessionAdmission{Admission: ports.BrokerAdmissionAttachNamed, Name: "work"}),
			wantErr:   true,
		},
		{
			name:      "named attach smuggles a resume token",
			hello:     protocol.Hello{Intent: protocol.IntentAttach, Name: "work", ResumeToken: 4},
			admission: local(ports.SessionAdmission{Admission: ports.BrokerAdmissionAttachNamed, Name: "work"}),
			wantErr:   true,
		},
		{
			name:      "local fresh exact attach",
			hello:     protocol.Hello{Intent: protocol.IntentAttach, Name: "work", ExactTarget: &target},
			admission: local(ports.SessionAdmission{Admission: ports.BrokerAdmissionExact, Target: target}),
		},
		{
			name: "local resumed exact attach",
			hello: protocol.Hello{
				Intent: protocol.IntentResume, Name: "work", ResumeToken: 7, ExactTarget: &target,
			},
			admission: local(ports.SessionAdmission{Admission: ports.BrokerAdmissionExact, Target: target}),
		},
		{
			name: "remote named creation",
			hello: protocol.Hello{
				Intent: protocol.IntentNew, Name: "work", Remote: true,
				EnvironmentPolicy: protocol.EnvironmentPolicyDaemonOwned,
			},
			admission: remote(ports.SessionAdmission{Admission: ports.BrokerAdmissionCreateNamed, Name: "work"}),
		},
		{
			name: "remote ephemeral creation",
			hello: protocol.Hello{
				Intent: protocol.IntentEphemeral, Remote: true,
				EnvironmentPolicy: protocol.EnvironmentPolicyDaemonOwned,
			},
			admission: remote(ports.SessionAdmission{Admission: ports.BrokerAdmissionCreateEphemeral}),
		},
		{
			name: "remote resume exact",
			hello: protocol.Hello{
				Intent: protocol.IntentResume, Name: "work", ResumeToken: 7, ExactTarget: &target,
				Remote: true, EnvironmentPolicy: protocol.EnvironmentPolicyDaemonOwned,
			},
			admission: remote(ports.SessionAdmission{Admission: ports.BrokerAdmissionExact, Target: target}),
		},

		{
			name:      "control purpose is refused",
			hello:     protocol.Hello{Intent: protocol.IntentEphemeral, Cwd: "/client/cwd"},
			admission: ports.SessionAdmission{Origin: ports.SessionOriginLocal, Policy: localPolicy, Purpose: ports.BrokerStreamControl},
			wantErr:   true,
		},
		{
			name:      "observation purpose is refused",
			hello:     protocol.Hello{Intent: protocol.IntentEphemeral, Cwd: "/client/cwd"},
			admission: ports.SessionAdmission{Origin: ports.SessionOriginLocal, Policy: localPolicy, Purpose: ports.BrokerStreamObservation},
			wantErr:   true,
		},
		{
			name:      "zero admission is refused",
			hello:     protocol.Hello{Intent: protocol.IntentEphemeral},
			admission: ports.SessionAdmission{},
			wantErr:   true,
		},
		{
			name:      "unknown admission variant is refused",
			hello:     protocol.Hello{Intent: protocol.IntentEphemeral},
			admission: local(ports.SessionAdmission{Admission: ports.BrokerStreamAdmission(9)}),
			wantErr:   true,
		},
		{
			name: "liar locality claims remote for a local admission",
			hello: protocol.Hello{
				Intent: protocol.IntentEphemeral, Remote: true, EnvironmentPolicy: protocol.EnvironmentPolicyClientOwned,
			},
			admission: local(ports.SessionAdmission{Admission: ports.BrokerAdmissionCreateEphemeral}),
			wantErr:   true,
		},
		{
			name: "liar locality claims local for a remote admission",
			hello: protocol.Hello{
				Intent: protocol.IntentEphemeral, Remote: false, EnvironmentPolicy: protocol.EnvironmentPolicyDaemonOwned,
			},
			admission: remote(ports.SessionAdmission{Admission: ports.BrokerAdmissionCreateEphemeral}),
			wantErr:   true,
		},
		{
			name: "liar environment policy for a local admission",
			hello: protocol.Hello{
				Intent: protocol.IntentEphemeral, EnvironmentPolicy: protocol.EnvironmentPolicyDaemonOwned,
			},
			admission: local(ports.SessionAdmission{Admission: ports.BrokerAdmissionCreateEphemeral, Env: localEnv}),
			wantErr:   true,
		},
		{
			name: "liar environment content",
			hello: protocol.Hello{
				Intent: protocol.IntentEphemeral, Env: []string{"TERM=xterm-256color", "PATH=/evil"},
			},
			admission: local(ports.SessionAdmission{Admission: ports.BrokerAdmissionCreateEphemeral, Env: localEnv}),
			wantErr:   true,
		},
		{
			name: "liar environment order",
			hello: protocol.Hello{
				Intent: protocol.IntentEphemeral, Env: []string{"PATH=/bin", "TERM=xterm-256color"},
			},
			admission: local(ports.SessionAdmission{Admission: ports.BrokerAdmissionCreateEphemeral, Env: localEnv}),
			wantErr:   true,
		},
		{
			name: "remote admission rejects a client environment",
			hello: protocol.Hello{
				Intent: protocol.IntentNew, Name: "work", Remote: true,
				EnvironmentPolicy: protocol.EnvironmentPolicyDaemonOwned, Env: []string{"HOME=/client"},
			},
			admission: remote(ports.SessionAdmission{Admission: ports.BrokerAdmissionCreateNamed, Name: "work"}),
			wantErr:   true,
		},
		{
			name: "remote admission rejects a client cwd",
			hello: protocol.Hello{
				Intent: protocol.IntentNew, Name: "work", Remote: true, Cwd: "/client",
				EnvironmentPolicy: protocol.EnvironmentPolicyDaemonOwned,
			},
			admission: remote(ports.SessionAdmission{Admission: ports.BrokerAdmissionCreateNamed, Name: "work"}),
			wantErr:   true,
		},
		{
			name:      "local admission rejects a relative cwd",
			hello:     protocol.Hello{Intent: protocol.IntentNew, Name: "work", Cwd: "relative/path"},
			admission: local(ports.SessionAdmission{Admission: ports.BrokerAdmissionCreateNamed, Name: "work"}),
			wantErr:   true,
		},
		{
			name:      "liar name on a named creation",
			hello:     protocol.Hello{Intent: protocol.IntentNew, Name: "other"},
			admission: local(ports.SessionAdmission{Admission: ports.BrokerAdmissionCreateNamed, Name: "work"}),
			wantErr:   true,
		},
		{
			name:      "named creation smuggles a resume token",
			hello:     protocol.Hello{Intent: protocol.IntentNew, Name: "work", ResumeToken: 9},
			admission: local(ports.SessionAdmission{Admission: ports.BrokerAdmissionCreateNamed, Name: "work"}),
			wantErr:   true,
		},
		{
			name:      "named creation smuggles an exact target",
			hello:     protocol.Hello{Intent: protocol.IntentNew, Name: "work", ExactTarget: &target},
			admission: local(ports.SessionAdmission{Admission: ports.BrokerAdmissionCreateNamed, Name: "work"}),
			wantErr:   true,
		},
		{
			name:      "ephemeral creation smuggles a name",
			hello:     protocol.Hello{Intent: protocol.IntentEphemeral, Name: "work"},
			admission: local(ports.SessionAdmission{Admission: ports.BrokerAdmissionCreateEphemeral}),
			wantErr:   true,
		},
		{
			name:      "ephemeral creation smuggles a resume token",
			hello:     protocol.Hello{Intent: protocol.IntentEphemeral, ResumeToken: 3},
			admission: local(ports.SessionAdmission{Admission: ports.BrokerAdmissionCreateEphemeral}),
			wantErr:   true,
		},
		{
			name:      "wrong intent for a named creation",
			hello:     protocol.Hello{Intent: protocol.IntentAttach, Name: "work"},
			admission: local(ports.SessionAdmission{Admission: ports.BrokerAdmissionCreateNamed, Name: "work"}),
			wantErr:   true,
		},
		{
			name: "liar exact target",
			hello: protocol.Hello{
				Intent: protocol.IntentAttach, Name: "work", ExactTarget: ptrExactTarget(admissionTestExact("work", 2)),
			},
			admission: local(ports.SessionAdmission{Admission: ports.BrokerAdmissionExact, Target: target}),
			wantErr:   true,
		},
		{
			name:      "missing exact target",
			hello:     protocol.Hello{Intent: protocol.IntentAttach, Name: "work"},
			admission: local(ports.SessionAdmission{Admission: ports.BrokerAdmissionExact, Target: target}),
			wantErr:   true,
		},
		{
			name: "exact admission requires resume intent for a token",
			hello: protocol.Hello{
				Intent: protocol.IntentAttach, Name: "work", ResumeToken: 5, ExactTarget: &target,
			},
			admission: local(ports.SessionAdmission{Admission: ports.BrokerAdmissionExact, Target: target}),
			wantErr:   true,
		},
		{
			name:      "fresh exact admission rejects a resume intent",
			hello:     protocol.Hello{Intent: protocol.IntentResume, Name: "work", ExactTarget: &target},
			admission: local(ports.SessionAdmission{Admission: ports.BrokerAdmissionExact, Target: target}),
			wantErr:   true,
		},
		{
			name: "session target contradicts the admitted target",
			hello: protocol.Hello{
				Intent: protocol.IntentAttach, Name: "work", ExactTarget: &target,
				SessionTarget: ptrSessionAttachTarget(protocol.SessionAttachTarget{
					LifecycleID: domain.SessionLifecycleID{2}, SessionName: "work",
					TabID: "tab-1", TabIndex: protocol.NoTabIndex,
				}),
			},
			admission: local(ports.SessionAdmission{Admission: ports.BrokerAdmissionExact, Target: target}),
			wantErr:   true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateSessionHelloAdmission(tt.hello, tt.admission)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
		})
	}
}

// TestSessionHelloAdmissionIsDefensiveCopy proves the daemon consumes an
// independent snapshot of the connection's admitted authority: mutating what the
// daemon read can never alter what the connection still holds.
func TestSessionHelloAdmissionIsDefensiveCopy(t *testing.T) {
	stored := ports.SessionAdmission{
		Origin: ports.SessionOriginLocal, Policy: admissionTestPolicy(ports.SessionOriginLocal),
		Purpose: ports.BrokerStreamAttachment, Admission: ports.BrokerAdmissionCreateNamed,
		Name: "work", Env: []string{"TERM=xterm-256color"},
	}
	tr := &admittedTransport{ServerConnection: &closeTrackingTransport{}, admission: stored, present: true}

	first, ok := sessionHelloAdmission(tr)
	require.True(t, ok)
	first.Env[0] = "TERM=mutated"
	first.Name = "mutated"
	first.Policy.Trust = "mutated"

	second, ok := sessionHelloAdmission(tr)
	require.True(t, ok)
	require.Equal(t, []string{"TERM=xterm-256color"}, second.Env)
	require.Equal(t, "work", second.Name)
	require.Equal(t, "same-user", second.Policy.Trust)

	legacy, ok := sessionHelloAdmission(&closeTrackingTransport{})
	require.False(t, ok)
	require.Equal(t, ports.SessionAdmission{}, legacy, "a legacy connection carries no admission")
}

// TestRouteRejectsSessionAdmissionContradictionsBeforeMutation proves the
// admission gate runs at the very start of routing: a contradictory Hello is
// refused without creating, restoring, or mutating any session, and a control
// stream can never become an attachment by sending a Hello.
func TestRouteRejectsSessionAdmissionContradictionsBeforeMutation(t *testing.T) {
	tests := []struct {
		name      string
		hello     protocol.Hello
		admission ports.SessionAdmission
	}{
		{
			name:  "control purpose cannot attach",
			hello: protocol.Hello{Version: protocol.Version, Intent: protocol.IntentNew, Name: "work", Size: defaultSize},
			admission: ports.SessionAdmission{
				Origin: ports.SessionOriginLocal, Policy: admissionTestPolicy(ports.SessionOriginLocal),
				Purpose: ports.BrokerStreamControl,
			},
		},
		{
			name: "liar locality",
			hello: protocol.Hello{
				Version: protocol.Version, Intent: protocol.IntentNew, Name: "work", Size: defaultSize,
				Remote: true, EnvironmentPolicy: protocol.EnvironmentPolicyClientOwned,
			},
			admission: ports.SessionAdmission{
				Origin: ports.SessionOriginLocal, Policy: admissionTestPolicy(ports.SessionOriginLocal),
				Purpose: ports.BrokerStreamAttachment, Admission: ports.BrokerAdmissionCreateNamed, Name: "work",
			},
		},
		{
			name: "liar name",
			hello: protocol.Hello{
				Version: protocol.Version, Intent: protocol.IntentNew, Name: "other", Size: defaultSize,
			},
			admission: ports.SessionAdmission{
				Origin: ports.SessionOriginLocal, Policy: admissionTestPolicy(ports.SessionOriginLocal),
				Purpose: ports.BrokerStreamAttachment, Admission: ports.BrokerAdmissionCreateNamed, Name: "work",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := newTestDaemon(t, newFactory(t, newQuietPTY()), stubClock{})
			tr := &admittedTransport{ServerConnection: &closeTrackingTransport{}, admission: tt.admission, present: true}
			_, _, err := d.route(tt.hello, tr)
			var protocolErr *protoErr
			require.ErrorAs(t, err, &protocolErr)
			require.Equal(t, protocol.ErrNoSuchTarget, protocolErr.code)
			require.Zero(t, sessionCount(d), "a refused admission must never create a session")
		})
	}
}

// TestRouteRemoteEphemeralCreateRequiresAdmissionProof proves an authenticated
// remote ephemeral creation is admitted with the daemon's own environment and
// home, while the same Hello without a provisioned admission keeps the legacy
// refusal: a forged Hello never gains the remote create exception.
func TestRouteRemoteEphemeralCreateRequiresAdmissionProof(t *testing.T) {
	remotePoison := []string{"HOME=/daemon/home", "SHELL=/daemon/shell", "PATH=/daemon/bin"}
	hello := protocol.Hello{
		Version: protocol.Version, Intent: protocol.IntentEphemeral, Size: defaultSize,
		Remote: true, EnvironmentPolicy: protocol.EnvironmentPolicyDaemonOwned,
	}
	admission := ports.SessionAdmission{
		Origin: ports.SessionOriginRemote, Policy: admissionTestPolicy(ports.SessionOriginRemote),
		Purpose: ports.BrokerStreamAttachment, Admission: ports.BrokerAdmissionCreateEphemeral,
	}

	proven := newTestDaemon(t, newFactory(t, newQuietPTY()), stubClock{})
	proven.baseEnv = remotePoison
	proven.dirOrHome = func(cwd string) string {
		require.Empty(t, cwd, "a remote creation must not resolve a client working directory")
		return "/daemon/home"
	}
	sess, ac, err := proven.route(hello, &admittedTransport{ServerConnection: &closeTrackingTransport{}, admission: admission, present: true})
	require.NoError(t, err)
	require.NotNil(t, ac)
	sess.mu.Lock()
	require.Equal(t, remotePoison, sess.env, "future PTYs must use the daemon environment")
	require.Equal(t, "/daemon/home", sess.cwd)
	sess.mu.Unlock()
	require.NoError(t, proven.killSession(sess, protocol.ReasonSessionKilled, true))

	// The same Hello without a provisioned admission is the legacy shape and
	// must stay refused rather than inherit the remote create exception.
	unproven := newTestDaemon(t, newFactory(t, newQuietPTY()), stubClock{})
	legacyTr := &admittedTransport{ServerConnection: &closeTrackingTransport{}, present: false}
	_, _, err = unproven.route(hello, legacyTr)
	var protocolErr *protoErr
	require.ErrorAs(t, err, &protocolErr)
	require.Equal(t, protocol.ErrNoSuchTarget, protocolErr.code)
	require.Zero(t, sessionCount(unproven), "an unproven remote creation must never create a session")
}

// TestRouteRemoteNamedCreateUsesDaemonEnvironment proves an authenticated remote
// named creation uses the daemon's own environment and home rather than any
// client value.
func TestRouteRemoteNamedCreateUsesDaemonEnvironment(t *testing.T) {
	remotePoison := []string{"HOME=/daemon/home", "SHELL=/daemon/shell", "PATH=/daemon/bin"}
	hello := protocol.Hello{
		Version: protocol.Version, Intent: protocol.IntentNew, Name: "work", Size: defaultSize,
		Remote: true, EnvironmentPolicy: protocol.EnvironmentPolicyDaemonOwned,
	}
	admission := ports.SessionAdmission{
		Origin: ports.SessionOriginRemote, Policy: admissionTestPolicy(ports.SessionOriginRemote),
		Purpose: ports.BrokerStreamAttachment, Admission: ports.BrokerAdmissionCreateNamed, Name: "work",
	}

	d := newTestDaemon(t, newFactory(t, newQuietPTY()), stubClock{})
	d.baseEnv = remotePoison
	d.dirOrHome = func(cwd string) string {
		require.Empty(t, cwd, "a remote creation must not resolve a client working directory")
		return "/daemon/home"
	}
	sess, ac, err := d.route(hello, &admittedTransport{ServerConnection: &closeTrackingTransport{}, admission: admission, present: true})
	require.NoError(t, err)
	require.NotNil(t, ac)
	sess.mu.Lock()
	require.Equal(t, remotePoison, sess.env, "future PTYs must use the daemon environment")
	require.Equal(t, "/daemon/home", sess.cwd)
	sess.mu.Unlock()
	require.NoError(t, d.killSession(sess, protocol.ReasonSessionKilled, true))
}

// TestRouteLocalCreateUsesAdmittedClientEnvironment proves a local admitted
// creation uses the client's provisioned environment and working directory, and
// never the daemon's own base environment.
func TestRouteLocalCreateUsesAdmittedClientEnvironment(t *testing.T) {
	daemonPoison := []string{"HOME=/daemon/home", "PATH=/daemon/bin"}
	clientEnv := []string{"HOME=/client/home", "PATH=/client/bin"}
	cwd := t.TempDir()

	for _, tt := range []struct {
		name      string
		admission ports.SessionAdmission
		hello     protocol.Hello
	}{
		{
			name: "named",
			admission: ports.SessionAdmission{
				Origin: ports.SessionOriginLocal, Policy: admissionTestPolicy(ports.SessionOriginLocal),
				Purpose: ports.BrokerStreamAttachment, Admission: ports.BrokerAdmissionCreateNamed,
				Name: "work", Env: clientEnv,
			},
			hello: protocol.Hello{
				Version: protocol.Version, Intent: protocol.IntentNew, Name: "work", Size: defaultSize,
				Cwd: cwd, Env: clientEnv, EnvironmentPolicy: protocol.EnvironmentPolicyClientOwned,
			},
		},
		{
			name: "ephemeral",
			admission: ports.SessionAdmission{
				Origin: ports.SessionOriginLocal, Policy: admissionTestPolicy(ports.SessionOriginLocal),
				Purpose: ports.BrokerStreamAttachment, Admission: ports.BrokerAdmissionCreateEphemeral,
				Env: clientEnv,
			},
			hello: protocol.Hello{
				Version: protocol.Version, Intent: protocol.IntentEphemeral, Size: defaultSize,
				Cwd: cwd, Env: clientEnv, EnvironmentPolicy: protocol.EnvironmentPolicyClientOwned,
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			d := newTestDaemon(t, newFactory(t, newQuietPTY()), stubClock{})
			d.baseEnv = daemonPoison
			tr := &admittedTransport{ServerConnection: &closeTrackingTransport{}, admission: tt.admission, present: true}
			sess, ac, err := d.route(tt.hello, tr)
			require.NoError(t, err)
			require.NotNil(t, ac)
			sess.mu.Lock()
			require.Equal(t, clientEnv, sess.env, "the local creation must adopt the admitted client environment")
			require.Equal(t, cwd, sess.cwd)
			sess.mu.Unlock()
			require.NoError(t, d.killSession(sess, protocol.ReasonSessionKilled, true))
		})
	}
}

// TestRouteLocalAttachUpdatesOnlyFutureChildEnvironment proves a local admitted
// attach refreshes the session's environment for future PTY children while the
// already-running PTY is untouched, which is the existing shared-session
// semantics the admission must preserve.
func TestRouteLocalAttachUpdatesOnlyFutureChildEnvironment(t *testing.T) {
	firstEnv := []string{"MARK=first"}
	pty, release := newBlockingPTY(t)
	defer release()
	d := newTestDaemon(t, newFactorySeq(t, pty, newQuietPTY()), stubClock{})

	create := protocol.Hello{
		Version: protocol.Version, Intent: protocol.IntentNew, Name: "work", Size: defaultSize,
		Env: firstEnv, EnvironmentPolicy: protocol.EnvironmentPolicyClientOwned,
	}
	createAdmission := ports.SessionAdmission{
		Origin: ports.SessionOriginLocal, Policy: admissionTestPolicy(ports.SessionOriginLocal),
		Purpose: ports.BrokerStreamAttachment, Admission: ports.BrokerAdmissionCreateNamed, Name: "work", Env: firstEnv,
	}
	sess, _, err := d.route(create, &admittedTransport{ServerConnection: &closeTrackingTransport{}, admission: createAdmission, present: true})
	require.NoError(t, err)
	sess.mu.Lock()
	require.Equal(t, firstEnv, sess.env)
	existingPane := sess.tabs[0].focusedPane()
	sess.mu.Unlock()

	// A later attach refreshes only the session's future-child environment. A
	// named admission authorizes creation only, so the attach rides a fresh
	// exact admission for the live session's own lifecycle.
	attachEnv := []string{"MARK=attach"}
	target := protocol.ExactSessionTarget{LifecycleID: sess.incarnation, SessionName: sess.name}
	attach := protocol.Hello{
		Version: protocol.Version, Intent: protocol.IntentAttach, Name: target.SessionName, Size: defaultSize,
		ExactTarget: &target, Env: attachEnv, EnvironmentPolicy: protocol.EnvironmentPolicyClientOwned,
	}
	exactAdmission := ports.SessionAdmission{
		Origin: ports.SessionOriginLocal, Policy: admissionTestPolicy(ports.SessionOriginLocal),
		Purpose: ports.BrokerStreamAttachment, Admission: ports.BrokerAdmissionExact, Target: target, Env: attachEnv,
	}
	_, _, err = d.route(attach, &admittedTransport{ServerConnection: &closeTrackingTransport{}, admission: exactAdmission, present: true})
	require.NoError(t, err)

	sess.mu.Lock()
	require.Equal(t, attachEnv, sess.env, "a later client-owned attach refreshes future child environment")
	require.Same(t, existingPane, sess.tabs[0].focusedPane(), "the existing PTY is untouched")
	sess.mu.Unlock()

	require.NoError(t, d.killSession(sess, protocol.ReasonSessionKilled, true))
}

// TestRouteRemoteExactAttachPreservesSessionEnvironment proves a remote exact
// attach never rewrites the live session's environment or working directory:
// the daemon-owned session keeps what it was created with, and only a
// client-owned local attach refreshes future-child state.
func TestRouteRemoteExactAttachPreservesSessionEnvironment(t *testing.T) {
	d := newTestDaemon(t, nil, stubClock{})
	sess := addControlSession(d, "work", "tab-1", "pane-1")
	sess.ephemeral = false
	sess.incarnation = newTestLifecycle(t)
	sess.mu.Lock()
	sess.env = []string{"SESSION=owned"}
	sess.mu.Unlock()

	target := protocol.ExactSessionTarget{LifecycleID: sess.incarnation, SessionName: sess.name}
	hello := protocol.Hello{
		Version: protocol.Version, Intent: protocol.IntentAttach, Name: target.SessionName,
		Size: defaultSize, ExactTarget: &target,
		Remote: true, EnvironmentPolicy: protocol.EnvironmentPolicyDaemonOwned,
	}
	admission := ports.SessionAdmission{
		Origin: ports.SessionOriginRemote, Policy: admissionTestPolicy(ports.SessionOriginRemote),
		Purpose: ports.BrokerStreamAttachment, Admission: ports.BrokerAdmissionExact, Target: target,
	}
	tr := &admittedTransport{ServerConnection: &closeTrackingTransport{}, admission: admission, present: true}
	routed, ac, err := d.route(hello, tr)
	require.NoError(t, err)
	require.Same(t, sess, routed)
	sess.mu.Lock()
	require.Equal(t, []string{"SESSION=owned"}, sess.env, "a remote attach preserves the session environment")
	require.Equal(t, "/tmp/work", sess.cwd, "a remote attach preserves the session working directory")
	sess.mu.Unlock()
	d.clientGone(sess, ac, tr, false)
}

// TestRouteResumeRejectsMismatchedAdmittedTargetBeforeClaim proves an admitted
// resume whose target does not name the exact lifecycle the token owns is
// refused before any claim or transport replacement: the parked attachment
// remains unclaimed and its transport is preserved.
func TestRouteResumeRejectsMismatchedAdmittedTargetBeforeClaim(t *testing.T) {
	remotePolicy := admissionTestPolicy(ports.SessionOriginRemote)

	tests := []struct {
		name   string
		target func(sess *session) protocol.ExactSessionTarget
	}{
		{name: "wrong lifecycle", target: func(sess *session) protocol.ExactSessionTarget {
			lifecycle := sess.incarnation
			lifecycle[0]++
			return protocol.ExactSessionTarget{LifecycleID: lifecycle, SessionName: sess.name}
		}},
		{name: "wrong name", target: func(sess *session) protocol.ExactSessionTarget {
			return protocol.ExactSessionTarget{LifecycleID: sess.incarnation, SessionName: "other"}
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pty, release := newBlockingPTY(t)
			defer release()
			d := newTestDaemon(t, newFactory(t, pty), stubClock{})
			oldTransport := &closeTrackingTransport{}
			sess, ac, err := d.route(helloResumeCapable(protocol.IntentNew, "work", 0), oldTransport)
			require.NoError(t, err)
			token := ac.resumeToken
			d.clientGone(sess, ac, oldTransport, false)

			target := tt.target(sess)
			resume := protocol.Hello{
				Version: protocol.Version, Intent: protocol.IntentResume, Name: target.SessionName,
				ResumeToken: token, Size: defaultSize, ExactTarget: &target,
				Remote: true, EnvironmentPolicy: protocol.EnvironmentPolicyDaemonOwned,
			}
			admission := ports.SessionAdmission{
				Origin: ports.SessionOriginRemote, Policy: remotePolicy,
				Purpose: ports.BrokerStreamAttachment, Admission: ports.BrokerAdmissionExact, Target: target,
			}
			tr := &admittedTransport{ServerConnection: &closeTrackingTransport{}, admission: admission, present: true}
			_, _, err = d.route(resume, tr)
			var protocolErr *protoErr
			require.ErrorAs(t, err, &protocolErr)
			require.Equal(t, protocol.ErrNoSuchTarget, protocolErr.code)

			d.mu.Lock()
			parked := d.parked[token]
			claimed := parked != nil && parked.claimed
			d.mu.Unlock()
			require.NotNil(t, parked, "a mismatched admitted target must not remove the parked credential")
			require.False(t, claimed, "a mismatched admitted target must not claim the parked attachment")
			require.Same(t, oldTransport, ac.transport(), "a mismatched admitted target must not replace the transport")

			require.NoError(t, d.killSession(sess, protocol.ReasonSessionKilled, true))
		})
	}
}
