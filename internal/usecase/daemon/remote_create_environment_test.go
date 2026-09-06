package daemon

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	portsmocks "github.com/bnema/vev/internal/ports/mocks"
	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/protocol/catalogue"
	"github.com/bnema/vev/internal/protocol/wire"
	"github.com/bnema/vev/internal/usecase/palette"
)

func TestPaletteRemoteCreateUsesDaemonEnvironment(t *testing.T) {
	local := newRemotePickerDaemon(&remoteRefreshHostStore{hosts: []string{"remote.example"}})
	local.remoteCatalog.replaceCache([]catalogue.RemoteCatalogCacheEntry{{Host: "remote.example", FetchedAt: time.Unix(10, 0)}})
	local.remoteCatalog.mu.Lock()
	local.remoteCatalog.status["remote.example"] = remoteHostFresh
	local.remoteCatalog.mu.Unlock()
	sess, ac, sends := addRemoteRefreshPickerOwner(t, local, "local")
	effect := beginRecentRoutePaletteEffect(t, local, sess, ac)
	result := palette.NewCreateSessionDestination(palette.CreateSessionOnRemoteHost, "remote.example", "remote.example", protocol.RouteRef{}, 0)
	err := (paletteExec{d: local, sess: sess, ac: ac, effect: effect}).createSessionOnDestination(effect, result, "work")
	require.NoError(t, err)
	frame := awaitFrame(t, sends, wire.MsgAttachTarget)
	handoff, err := wire.UnmarshalAttachTarget(frame.Payload)
	require.NoError(t, err)
	require.Equal(t, protocol.IntentNew, handoff.Intent)
	require.NotZero(t, handoff.RequestID)
	require.Equal(t, protocol.EnvironmentPolicyDaemonOwned, handoff.EnvironmentPolicy)
	require.Nil(t, ac.currentAttachmentSession())
}

func TestNewSessionEnvironmentOwnership(t *testing.T) {
	for _, tt := range []struct {
		name   string
		policy protocol.EnvironmentPolicy
	}{
		{name: "direct client", policy: protocol.EnvironmentPolicyClientOwned},
		{name: "remote palette", policy: protocol.EnvironmentPolicyDaemonOwned},
	} {
		t.Run(tt.name, func(t *testing.T) {
			clientCwd, remoteHome := t.TempDir(), t.TempDir()
			clientEnv := []string{"HOME=/client/home", "SHELL=/client/shell", "PATH=/client/bin", "XDG_CONFIG_HOME=/client/config"}
			remoteEnv := []string{"HOME=" + remoteHome, "SHELL=/remote/shell", "PATH=/remote/bin", "XDG_CONFIG_HOME=/remote/config"}
			wantEnv, wantCwd, wantShell := clientEnv, clientCwd, "/client/shell"
			if tt.policy == protocol.EnvironmentPolicyDaemonOwned {
				wantEnv, wantCwd, wantShell = remoteEnv, remoteHome, "/remote/shell"
			}
			factory := portsmocks.NewMockPTYFactory(t)
			factory.EXPECT().Open(mock.Anything, wantShell, mock.Anything, mock.Anything, wantCwd, mock.Anything).RunAndReturn(
				func(_ context.Context, _ string, _ []string, env []string, _ string, _ domain.Geometry) (ports.PTY, error) {
					for _, value := range wantEnv {
						require.Contains(t, env, value)
					}
					return newQuietPTY(), nil
				},
			)
			d := newTestDaemon(t, factory, stubClock{})
			d.baseEnv = remoteEnv
			d.dirOrHome = func(cwd string) string {
				require.Empty(t, cwd, "remote creation must not resolve the client's working directory")
				return remoteHome
			}
			sess, ac, err := d.route(protocol.Hello{
				Version: protocol.Version, Intent: protocol.IntentNew, Name: "work",
				Size: domain.Size{Cols: 80, Rows: 24}, Cwd: clientCwd, Env: clientEnv,
				EnvironmentPolicy: tt.policy,
			}, &closeTrackingTransport{})
			require.NoError(t, err)
			require.NotNil(t, ac)
			sess.mu.Lock()
			require.Equal(t, wantEnv, sess.env, "future PTYs must retain the selected environment")
			sess.mu.Unlock()
			require.NoError(t, d.killSession(sess, protocol.ReasonSessionKilled, true))
		})
	}
}
