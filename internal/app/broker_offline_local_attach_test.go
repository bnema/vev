//go:build linux

package app

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/adapters/brokerconfig"
	"github.com/bnema/vev/internal/adapters/clock"
	"github.com/bnema/vev/internal/adapters/daemonidentity"
	"github.com/bnema/vev/internal/adapters/daemonmux"
	"github.com/bnema/vev/internal/adapters/ipc"
	"github.com/bnema/vev/internal/adapters/pty"
	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/protocol/catalogue"
	"github.com/bnema/vev/internal/usecase/daemon"
)

// P7.4b real-composition coverage for the broker-owned local probe.
//
// The scripted tests prove the probe's exact stream semantics. These tests run
// the same probe against a real daemon serving a real private Unix daemonmux
// carriage, so the authenticated physical preamble, the observation open, the
// typed remote-catalog command, and the catalogue validation all compose against
// production daemon code. They prove the observation is real: it creates no
// session, no process, and no durable identity, and the catalogue it publishes
// is exactly what makes an exact terminal attach resolvable.

// realLocalDaemonFixture is a real daemon serving one private Unix daemonmux
// carriage, plus the sandbox state directory a probe must never write to.
type realLocalDaemonFixture struct {
	binding   brokerconfig.LocalBinding
	route     string
	stateDir  string
	connector *daemonmux.EndpointConnector
	// nextStream allocates the strictly increasing logical stream identity each
	// helper attachment names, as the daemonmux engine requires within one
	// physical connection.
	nextStream ports.BrokerStreamID
}

// startRealLocalDaemon starts one real daemon over a private local daemonmux
// carriage and returns the fixture the probe dials.
func startRealLocalDaemon(t *testing.T) *realLocalDaemonFixture {
	t.Helper()
	_, prodRuntime, prodState := isolateSandboxEnv(t)
	root := filepath.Join(shortTempDir(t, "vevl"), "sandbox")
	require.NoError(t, os.MkdirAll(root, 0o700))
	policy := brokerTestPolicy()
	route := filepath.Join(root, "local-mux.sock")
	stateDir := filepath.Join(root, "state")
	require.NoError(t, os.MkdirAll(stateDir, 0o700))
	writeSandboxConfig(t, root, map[string]any{
		"marker":        brokerconfig.Marker,
		"registrations": []any{},
		"local": map[string]any{
			"identity":      brokerLocalTestIdentity,
			"displayOrigin": "local",
			"route":         route,
			"policy": map[string]any{
				"protocolVersion":      policy.ProtocolVersion,
				"catalogSchemaVersion": policy.CatalogSchemaVersion,
				"environmentPolicy":    "client-owned",
				"transport":            policy.Transport,
				"trust":                policy.Trust,
				"launch":               policy.Launch,
				"isolation":            policy.Isolation,
			},
		},
	})
	layout, err := offlineLayout(root)
	require.NoError(t, err)
	config, err := brokerconfig.Load(layout)
	require.NoError(t, err)
	binding, ok := config.LocalBinding()
	require.True(t, ok)

	listener, err := ipc.ListenMux(route)
	require.NoError(t, err)
	aggregate := daemonmux.NewAggregateListener()
	serverBinding := testServerBinding(t, binding)
	supervisor, err := daemonmux.NewServerSupervisor(aggregate, serverBinding, daemonmux.DefaultMuxCeilings(), 0)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	d := daemon.New(pty.NewFactory(), clock.New(), discardLog(),
		daemon.WithShell("/bin/sh", []string{"-c", "trap : TERM; while :; do read line || exit; done"}))
	go func() { _ = d.Serve(ctx, aggregate) }()
	go func() {
		for {
			raw, acceptErr := listener.Accept()
			if acceptErr != nil {
				return
			}
			go func() { _ = supervisor.Adopt(ctx, raw) }()
		}
	}()
	t.Cleanup(func() {
		cancel()
		_ = listener.Close()
		_ = supervisor.Close()
		_ = aggregate.Close()
		requireProductionUntouched(t, prodRuntime, prodState)
	})

	dial := func(ctx context.Context, _ ports.BrokerDialTarget) (daemonmux.RawFramedTransport, error) {
		return ipc.DialMuxContext(ctx, route)
	}
	connector, err := daemonmux.NewEndpointConnector(daemonmux.RawCarrierDialer(dial), daemonmux.DefaultMuxCeilings())
	require.NoError(t, err)
	return &realLocalDaemonFixture{binding: binding, route: route, stateDir: stateDir, connector: connector}
}

// probe returns one probe over the fixture's real carriage.
func (f *realLocalDaemonFixture) probe(t *testing.T) *localRouteProbe {
	t.Helper()
	dial := func(ctx context.Context, target ports.BrokerDialTarget) (daemonmux.RawFramedTransport, error) {
		require.Equal(t, f.binding.Route.Address(), target.Address, "the probe dials only its provisioned local address")
		return ipc.DialMuxContext(ctx, f.route)
	}
	probe, err := newLocalRouteProbe(f.binding, brokerLocalTestEpoch, dial, daemonmux.DefaultMuxCeilings())
	require.NoError(t, err)
	return probe
}

// openStream opens one logical stream over the fixture's real carriage under
// the fixture's authenticated local endpoint.
func (f *realLocalDaemonFixture) openStream(t *testing.T, request ports.BrokerOpenStreamRequest) ports.BrokerLogicalConnection {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), brokerTestWait)
	t.Cleanup(cancel)
	physical, err := f.connector.Connect(ctx, ports.BrokerDialTarget{
		Fence: ports.BrokerEndpointFence{Local: true}, Policy: f.binding.Policy, Address: f.binding.Route.Address(),
		StartMode:        ports.BrokerDaemonExistingOnly,
		ExpectedIdentity: ports.BrokerExpectedIdentity{Identity: f.binding.Identity, Bound: true},
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = physical.Close() })

	request.Epoch = brokerLocalTestEpoch
	request.Local = true
	request.Policy = f.binding.Policy
	request.Connection = ports.BrokerConnectionID{4, 4}
	if request.Stream == 0 {
		f.nextStream++
		request.Stream = f.nextStream
	}
	stream, err := physical.OpenStream(ctx, request)
	require.NoError(t, err)
	t.Cleanup(func() { _ = stream.Close() })
	return stream
}

// awaitWelcome receives server messages until the typed Welcome arrives.
func awaitWelcome(t *testing.T, stream ports.BrokerLogicalConnection) protocol.Welcome {
	t.Helper()
	type outcome struct {
		message protocol.ServerMessage
		err     error
	}
	results := make(chan outcome, 1)
	go func() {
		for {
			message, err := stream.ReceiveServer()
			if err != nil {
				results <- outcome{err: err}
				return
			}
			if welcome, ok := message.(protocol.Welcome); ok {
				results <- outcome{message: welcome}
				return
			}
		}
	}()
	select {
	case result := <-results:
		require.NoError(t, result.err, "waiting for Welcome")
		return result.message.(protocol.Welcome)
	case <-time.After(brokerTestWait):
		t.Fatal("timed out waiting for Welcome")
		return protocol.Welcome{}
	}
}

// TestBrokerLocalProbeRealCatalogueEnablesExactAttach proves the broker-owned
// local probe reads a real daemon's catalogue and that the catalogue it
// publishes is exactly what makes an exact terminal attach resolvable.
func TestBrokerLocalProbeRealCatalogueEnablesExactAttach(t *testing.T) {
	fixture := startRealLocalDaemon(t)
	probe := fixture.probe(t)

	// An observation of a daemon that owns no session reports reachable, known,
	// and empty inventory: observing never creates a session.
	empty, err := probe.ProbeLocal(context.Background())
	require.NoError(t, err)
	require.Equal(t, domain.RemoteAvailabilityReachable, empty.Availability)
	require.True(t, empty.InventoryKnown)
	require.Empty(t, empty.Sessions, "an observation must never create a session")
	require.Equal(t, fixture.binding.Identity, empty.Identity)
	require.NotZero(t, empty.ProtocolVersion)

	// One real session is created through one real attachment stream.
	attachment := fixture.openStream(t, ports.BrokerOpenStreamRequest{
		Purpose:   ports.BrokerStreamAttachment,
		Admission: ports.BrokerAdmissionCreateNamed,
		Name:      "probe-target",
		StartMode: ports.BrokerDaemonStartIfNeeded,
	})
	require.NoError(t, attachment.SendClient(protocol.Hello{
		Version: protocol.Version, Intent: protocol.IntentNew, Name: "probe-target",
		Size: domain.Size{Cols: 80, Rows: 24}, EnvironmentPolicy: protocol.EnvironmentPolicyClientOwned,
	}))
	welcome := awaitWelcome(t, attachment)
	require.NotNil(t, welcome.CommittedIdentity)
	target := welcome.CommittedIdentity.Target

	// The observation reports the real daemon's catalogue: the exact session
	// lifecycle the daemon committed, so the observation is the authority an
	// exact attach resolves against.
	observed, err := probe.ProbeLocal(context.Background())
	require.NoError(t, err)
	require.Equal(t, domain.RemoteAvailabilityReachable, observed.Availability)
	require.True(t, observed.InventoryKnown)
	require.Equal(t, fixture.binding.Identity, observed.Identity)
	require.Len(t, observed.Sessions, 1)
	require.Equal(t, "probe-target", observed.Sessions[0].Name)
	require.Equal(t, target.LifecycleID, observed.Sessions[0].LifecycleID)
	require.Equal(t, catalogue.RemoteCatalogSessionUp, observed.Sessions[0].State)

	// The published catalogue resolves the exact attach target the client
	// composition uses, and that exact target attaches successfully.
	resolved, ok := brokerObservationExactTarget(observed, "probe-target")
	require.True(t, ok)
	require.Equal(t, target, resolved)

	exact := fixture.openStream(t, ports.BrokerOpenStreamRequest{
		Purpose:   ports.BrokerStreamAttachment,
		Admission: ports.BrokerAdmissionExact,
		Target:    resolved,
		StartMode: ports.BrokerDaemonStartIfNeeded,
	})
	exactTarget := resolved
	require.NoError(t, exact.SendClient(protocol.Hello{
		Version: protocol.Version, Intent: protocol.IntentAttach, Name: resolved.SessionName, ExactTarget: &exactTarget,
		Size: domain.Size{Cols: 80, Rows: 24}, EnvironmentPolicy: protocol.EnvironmentPolicyClientOwned,
	}))
	exactWelcome := awaitWelcome(t, exact)
	require.NotNil(t, exactWelcome.CommittedIdentity)
	require.Equal(t, resolved, exactWelcome.CommittedIdentity.Target)

	// Probing creates no durable identity: only a daemon-owned startup path ever
	// creates the identity file, and the probe is not one.
	_, statErr := os.Lstat(daemonidentity.Path(fixture.stateDir))
	require.ErrorIs(t, statErr, os.ErrNotExist, "an observation must never create daemon identity")
}

// TestBrokerLocalProbeAbsentDaemonStaysUnavailable proves an absence daemon
// leaves the local observation unavailable with zero identity and creates
// nothing: no session, no process, no durable identity. brokerconfig refuses a
// missing or malformed configuration before any probe exists, and an absent
// carriage is reported, never started.
func TestBrokerLocalProbeAbsentDaemonStaysUnavailable(t *testing.T) {
	_, prodRuntime, prodState := isolateSandboxEnv(t)
	root := filepath.Join(shortTempDir(t, "vevl"), "sandbox")
	require.NoError(t, os.MkdirAll(root, 0o700))
	stateDir := filepath.Join(root, "state")
	require.NoError(t, os.MkdirAll(stateDir, 0o700))
	// A local binding provisioned on a socket nothing listens on: the probe must
	// report unavailable rather than start anything.
	route := filepath.Join(root, "absent-mux.sock")
	writeSandboxConfig(t, root, map[string]any{
		"marker":        brokerconfig.Marker,
		"registrations": []any{},
		"local": map[string]any{
			"identity": brokerLocalTestIdentity, "displayOrigin": "local", "route": route,
			"policy": map[string]any{
				"protocolVersion": brokerTestPolicy().ProtocolVersion, "catalogSchemaVersion": brokerTestPolicy().CatalogSchemaVersion,
				"environmentPolicy": "client-owned", "transport": brokerTestPolicy().Transport,
				"trust": brokerTestPolicy().Trust, "launch": brokerTestPolicy().Launch, "isolation": brokerTestPolicy().Isolation,
			},
		},
	})
	layout, err := offlineLayout(root)
	require.NoError(t, err)
	config, err := brokerconfig.Load(layout)
	require.NoError(t, err)
	binding, ok := config.LocalBinding()
	require.True(t, ok)

	var dialed []string
	var mu sync.Mutex
	probe, err := newLocalRouteProbe(binding, brokerLocalTestEpoch, func(ctx context.Context, target ports.BrokerDialTarget) (daemonmux.RawFramedTransport, error) {
		mu.Lock()
		dialed = append(dialed, target.Address)
		mu.Unlock()
		return ipc.DialMuxContext(ctx, target.Address)
	}, daemonmux.DefaultMuxCeilings())
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), brokerTestWait)
	defer cancel()
	observation, err := probe.ProbeLocal(ctx)
	require.Error(t, err)
	require.Equal(t, domain.RemoteAvailabilityUnreachable, observation.Availability)
	require.Zero(t, observation.Identity)
	require.False(t, observation.InventoryKnown)
	require.Empty(t, observation.Sessions)

	mu.Lock()
	require.Equal(t, []string{binding.Route.Address()}, dialed, "the probe dials only the address its provisioned local binding published")
	mu.Unlock()
	// Nothing was created: no socket, no identity, no production path entry.
	_, statErr := os.Lstat(route)
	require.ErrorIs(t, statErr, os.ErrNotExist, "a probe must never create a carriage")
	_, identityErr := os.Lstat(daemonidentity.Path(stateDir))
	require.ErrorIs(t, identityErr, os.ErrNotExist, "a probe must never create daemon identity")
	requireProductionUntouched(t, prodRuntime, prodState)
}
