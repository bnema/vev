package daemon

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
)

func TestDaemonOwnedNoExactTargetRejectsNewAndEphemeral(t *testing.T) {
	d := newTestDaemon(t, nil, stubClock{})
	for _, intent := range []uint8{ports.IntentNew, ports.IntentEphemeral} {
		hello := ports.Hello{
			Version: ports.ProtocolVersion, Intent: intent, Name: "work",
			Size: domain.Size{Cols: 80, Rows: 24}, Cwd: "/untrusted/cwd",
			Env: []string{"UNTRUSTED=client"}, EnvironmentPolicy: ports.EnvironmentPolicyDaemonOwned,
		}
		_, _, err := d.route(hello, &closeTrackingTransport{})
		var protocol *protoErr
		require.ErrorAs(t, err, &protocol)
		require.Equal(t, ports.ErrNoSuchTarget, protocol.code)
	}
	require.Empty(t, d.sessions)
}

func TestLegacyDaemonOwnedStoppedAttachUsesDaemonEnvironment(t *testing.T) {
	d := newTestDaemon(t, newFactory(t, newQuietPTY()), stubClock{})
	d.baseEnv = []string{"DAEMON=owned"}
	d.mu.Lock()
	d.stopped["work"] = stoppedSession{name: "work", cwd: "/remote/work", tabNames: []string{"main"}, state: ports.SessionDown}
	d.mu.Unlock()

	hello := ports.Hello{
		Version: ports.ProtocolVersion, Intent: ports.IntentAttach, Name: "work",
		Size: domain.Size{Cols: 80, Rows: 24}, Cwd: "/untrusted/cwd",
		Env: []string{"UNTRUSTED=client"}, EnvironmentPolicy: ports.EnvironmentPolicyDaemonOwned,
	}
	sess, ac, err := d.routeWithContext(context.Background(), hello, &closeTrackingTransport{})
	require.NoError(t, err)
	require.NotNil(t, ac)
	sess.mu.Lock()
	require.Equal(t, []string{"DAEMON=owned"}, sess.env)
	sess.mu.Unlock()
	require.NoError(t, d.killSession(sess, ports.ReasonSessionKilled, true))
}

func TestRouteRemoteTargetRestoresStoppedStableTabAndOwnsEnvironment(t *testing.T) {
	d := newTestDaemon(t, newFactory(t, newQuietPTY()), stubClock{})
	lifecycle := remoteLifecycleForTest()
	d.mu.Lock()
	d.stopped["work"] = stoppedSession{
		name: "work", cwd: "/remote/work", incarnation: lifecycle,
		tabNames: []string{"alpha", "beta"},
		tabRecords: []domain.CatalogueTabRecord{
			{StableID: "tab-a", Name: "alpha"},
			{StableID: "tab-b", Name: "beta"},
		},
	}
	d.mu.Unlock()

	target := domain.RemoteSessionTarget{
		Endpoint: "arch", DisplayOrigin: "arch", LifecycleID: lifecycle,
		SessionName: "work", Stopped: true,
		StoppedTab: domain.NewStableTabSelector("tab-b"),
	}
	tr, _ := newCapturingTransport(t)
	hello := ports.Hello{
		Version: ports.ProtocolVersion, Intent: ports.IntentAttach, Name: "work",
		Size: domain.Size{Cols: 80, Rows: 24}, RemoteTarget: &target,
		EnvironmentPolicy: ports.EnvironmentPolicyDaemonOwned,
		Env:               []string{"VEV_REMOTE_PICKER_SENTINEL=untrusted"},
	}
	sess, ac, err := d.routeWithContext(context.Background(), hello, tr)
	require.NoError(t, err)
	require.NotNil(t, sess)
	require.NotNil(t, ac)
	require.Equal(t, lifecycle, sess.incarnation)
	require.Equal(t, domain.TabStableID("tab-b"), ac.viewSnapshot().tabID)
	require.NotContains(t, sess.env, "VEV_REMOTE_PICKER_SENTINEL=untrusted")
	d.clientGone(sess, ac, tr, false)
}
