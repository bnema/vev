package app

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/bnema/vev/internal/adapters/brokerconfig"
	"github.com/bnema/vev/internal/adapters/brokeripc"
	"github.com/bnema/vev/internal/adapters/brokerstore"
	"github.com/bnema/vev/internal/adapters/clock"
	"github.com/bnema/vev/internal/adapters/daemonmux"
	"github.com/bnema/vev/internal/adapters/ipc"
	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/usecase/broker"
	"github.com/bnema/vev/pkg/safedir"
	"github.com/stretchr/testify/require"
)

type routeTestHosts struct {
	record ports.BrokerHostRecord
	found  bool
}

func (h *routeTestHosts) LookupHost(ctx context.Context, endpoint string) (ports.BrokerHostRecord, bool, error) {
	return h.record, h.found && endpoint == h.record.Registration.Endpoint, ctx.Err()
}

func TestBrokerRoutesDurableFirstAddAndRestart(t *testing.T) {
	ctx := context.Background()
	dir := filepath.Join(t.TempDir(), "state")
	store, err := brokerstore.Open(brokerstore.Options{Dir: dir})
	require.NoError(t, err)
	defer func() { require.NoError(t, store.Close()) }()
	routes := &brokerRoutes{}
	probe := &brokerRemoteProbe{epoch: 1, routes: routes}
	registry, err := broker.NewRegistryWithConfig(1, store, probe, clock.New(), nil, broker.RegistryConfig{MembershipMode: broker.MembershipMutable})
	require.NoError(t, err)
	routes.hosts = registry
	probe.hosts = registry
	policy := remoteBrokerPolicy("stdio")
	reg, err := registry.AddHost(ctx, "user@example.test", policy)
	require.NoError(t, err)
	request, err := probe.request(ctx, reg)
	require.NoError(t, err)
	target, err := routes.ResolveDialTarget(ctx, request)
	require.NoError(t, err)
	require.False(t, target.ExpectedIdentity.Bound)
	route, err := routes.remoteRoute(ctx, target)
	require.NoError(t, err)
	require.Equal(t, brokerconfig.RouteSSHStdio, route.Kind())
	require.Equal(t, []string{"vev", brokerMuxStdioCommand, "--production"}, route.Argv())
	identity, err := registry.BindAuthenticatedIdentity(ctx, ports.BrokerIdentityBindingRequest{Fence: target.Fence, Policy: policy, Identity: "daemon-test"})
	require.NoError(t, err)
	_, err = routes.remoteRoute(ctx, target)
	require.Error(t, err, "an unbound target cannot bypass a newly committed identity")
	target, err = routes.ResolveDialTarget(ctx, request)
	require.NoError(t, err)
	require.Equal(t, identity, target.ExpectedIdentity.Identity)
	require.True(t, target.ExpectedIdentity.Bound)
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	registry.Run(cancelled) // flush advisory writes before closing durable ownership
	require.NoError(t, store.Close())
	store, err = brokerstore.Open(brokerstore.Options{Dir: dir})
	require.NoError(t, err)
	restarted, err := broker.NewRegistryWithConfig(2, store, probe, clock.New(), nil, broker.RegistryConfig{MembershipMode: broker.MembershipMutable})
	require.NoError(t, err)
	routes.hosts = restarted
	recovered, err := routes.ResolveDialTarget(ctx, request)
	require.NoError(t, err)
	require.Equal(t, target, recovered)
	restarted.Run(cancelled)
}

func TestBrokerRoutesRejectChangedAuthority(t *testing.T) {
	reg := domain.RemoteRegistration{Endpoint: "user@example.test", Incarnation: [16]byte{1}, Generation: 1}
	policy := remoteBrokerPolicy("stdio")
	spec, err := ports.BrokerRouteForTransport(policy.Transport, reg.Endpoint)
	require.NoError(t, err)
	original := ports.BrokerHostRecord{Registration: reg, Policy: policy, Route: spec}
	for _, tc := range []struct {
		name   string
		change func(*routeTestHosts)
	}{
		{"removed", func(h *routeTestHosts) { h.found = false }},
		{"readded", func(h *routeTestHosts) { h.record.Registration.Incarnation[0]++ }},
		{"policy", func(h *routeTestHosts) { h.record.Policy.Trust = "other" }},
		{"route", func(h *routeTestHosts) { h.record.Route.Target = "other@example.test" }},
		{"identity", func(h *routeTestHosts) { h.record.Identity = "other-daemon" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			hosts := &routeTestHosts{record: original, found: true}
			routes := &brokerRoutes{hosts: hosts}
			req := ports.BrokerOpenStreamRequest{Endpoint: reg.Endpoint, Registration: reg, Policy: policy, StartMode: ports.BrokerDaemonExistingOnly}
			target, err := routes.ResolveDialTarget(context.Background(), req)
			require.NoError(t, err)
			tc.change(hosts)
			_, err = routes.remoteRoute(context.Background(), target)
			require.Error(t, err)
		})
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = (&brokerRoutes{}).ResolveDialTarget(ctx, ports.BrokerOpenStreamRequest{})
	require.ErrorIs(t, err, context.Canceled)
}

func TestCanonicalBrokerRoutesSelectProductionHelpers(t *testing.T) {
	for _, transport := range []string{"ssh-stdio", "ssh-quic"} {
		t.Run(transport, func(t *testing.T) {
			spec, err := ports.BrokerRouteForTransport(transport, "user@example.test")
			require.NoError(t, err)
			cmd, err := parseArgs(spec.Argv[1:])
			require.NoError(t, err)
			require.Equal(t, ports.BrokerDaemonExistingOnly, cmd.brokerMux.startMode)
			for _, args := range [][]string{{"--production", "--production"}, {"--production", "--offline-root", "/tmp/root"}, {"--offline-root", "/tmp/root", "--production"}} {
				_, err = parseBrokerMuxArgs(spec.Argv[1], cmd.kind, args)
				require.Error(t, err)
			}
		})
	}
}

// The real production composition starts with no remote import. The first IPC
// add must succeed and the same running registry must retain it across restart.
func TestProductionBrokerFirstHostAdd(t *testing.T) {
	t.Setenv("XDG_RUNTIME_DIR", shortTempDir(t, "vevroute"))
	t.Setenv("XDG_STATE_HOME", t.TempDir())
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
	require.NoError(t, safedir.EnsurePrivate(filepath.Dir(productionBrokerConfigPath())))
	raw, err := json.Marshal(map[string]any{"marker": brokerconfig.Marker, "registrations": []any{}})
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(productionBrokerConfigPath(), raw, 0600))
	var registered domain.RemoteRegistration
	for i := 0; i < 2; i++ {
		deps, ready := testBrokerServeDeps(newSandboxClock())
		ctx, cancel := context.WithCancel(context.Background())
		done := runSandbox(ctx, brokerServeOptions{}, deps)
		socket := awaitSandboxReady(t, ready, done)
		service, err := brokeripc.NewConnector(socket, brokeripc.Config{}).Connect(ctx)
		require.NoError(t, err)
		reg, err := service.AddHost(ctx, "user@127.0.0.1:1", remoteBrokerPolicy("stdio"))
		require.NoError(t, err)
		if i == 0 {
			registered = reg
		} else {
			require.Equal(t, registered, reg)
		}
		require.NoError(t, service.Close())
		awaitSandboxCancel(t, cancel, done)
	}
}

func TestBrokerDynamicConnectorDialsDurableRouteWithoutConfigEntry(t *testing.T) {
	policy := brokerTestPolicy()
	fixture := newBrokerMuxFixture(t, policy)
	reg := brokerTestRegistration()
	hosts := &routeTestHosts{found: true, record: ports.BrokerHostRecord{
		Registration: reg, Policy: policy, Route: ports.BrokerRouteSpec{Kind: ports.BrokerRouteUnix, Path: fixture.route},
	}}
	routes := &brokerRoutes{hosts: hosts}
	target, err := routes.ResolveDialTarget(context.Background(), ports.BrokerOpenStreamRequest{
		Endpoint: reg.Endpoint, Registration: reg, Policy: policy, StartMode: ports.BrokerDaemonExistingOnly,
	})
	require.NoError(t, err)
	connector, err := brokerDynamicMuxConnector(nil, routes, nil)
	require.NoError(t, err)
	physical, err := connector.Connect(context.Background(), target)
	require.NoError(t, err)
	require.Equal(t, ports.BrokerDaemonIdentity(brokerTestIdentity), physical.Identity())
	require.NoError(t, physical.Close())
	hosts.found = false
	_, err = connector.Connect(context.Background(), target)
	require.Error(t, err)
}

func TestProductionMuxHelperExistingOnlyNeverSpawns(t *testing.T) {
	t.Setenv("XDG_RUNTIME_DIR", shortTempDir(t, "vevhelper"))
	previous := localDaemonStarter
	defer func() { localDaemonStarter = previous }()
	dials, spawns := 0, 0
	localDaemonStarter = brokerDaemonStarter{
		dial: func(_ context.Context, path string) (daemonmux.RawFramedTransport, error) {
			dials++
			require.Equal(t, daemonmux.SocketPath(ipc.SocketDir()), path)
			return nil, os.ErrNotExist
		},
		spawn: func() error { spawns++; return nil },
	}
	_, err := dialBrokerMuxHelper(context.Background(), brokerMuxOptions{startMode: ports.BrokerDaemonExistingOnly})
	require.ErrorIs(t, err, os.ErrNotExist)
	require.Equal(t, 1, dials)
	require.Zero(t, spawns)
}

func TestBrokerRoutesLocalRemainsConfigured(t *testing.T) {
	config, binding := testLocalSandboxConfigForCarriage(t, filepath.Join(t.TempDir(), "daemon.sock"))
	routes := &brokerRoutes{local: config.Resolver()} // no remote authority at all
	request := ports.BrokerOpenStreamRequest{Local: true, Policy: binding.Policy, StartMode: ports.BrokerDaemonExistingOnly}
	target, err := routes.ResolveDialTarget(context.Background(), request)
	require.NoError(t, err)
	require.Equal(t, binding.Route.Address(), target.Address)
	require.Equal(t, binding.Identity, target.ExpectedIdentity.Identity)
	request.Policy.Trust = "caller-selected"
	_, err = routes.ResolveDialTarget(context.Background(), request)
	require.Error(t, err)
}
