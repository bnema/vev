package daemon

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
)

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
