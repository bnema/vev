package app

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/adapters/brokerconfig"
	"github.com/bnema/vev/internal/adapters/brokerstore"
	"github.com/bnema/vev/internal/adapters/clock"
	"github.com/bnema/vev/internal/adapters/daemonmux"
	"github.com/bnema/vev/internal/adapters/ipc"
	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/usecase/broker"
)

const brokerLocalTestIdentity = "offline-local-daemon"

// testLocalSandboxConfig writes and loads one marker-valid sandbox whose only
// provisioned carriage is the broker-owned local binding.
func testLocalSandboxConfig(t *testing.T) (*brokerconfig.Config, brokerconfig.LocalBinding) {
	t.Helper()
	root := filepath.Join(shortTempDir(t, "vevl"), "sandbox")
	policy := brokerTestPolicy()
	require.NoError(t, os.MkdirAll(root, 0o700))
	writeSandboxConfig(t, root, map[string]any{
		"marker":        brokerconfig.Marker,
		"registrations": []any{},
		"local": map[string]any{
			"identity":      brokerLocalTestIdentity,
			"displayOrigin": "local",
			"route":         filepath.Join(root, "local-mux.sock"),
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
	layout, err := brokerconfig.ResolveLayout(root, nil)
	require.NoError(t, err)
	config, err := brokerconfig.Load(layout)
	require.NoError(t, err)
	binding, ok := config.LocalBinding()
	require.True(t, ok)
	return config, binding
}

// rawFramedTransport adapts one in-memory pipe end to the raw framed carriage
// the daemonmux bridge consumes. No real socket is opened.
func rawFramedTransport(raw net.Conn) daemonmux.RawFramedTransport {
	return ipc.NewTransport(raw).(daemonmux.RawFramedTransport)
}

// testServerBinding builds the daemon-side binding matching one local binding.
func testServerBinding(t *testing.T, binding brokerconfig.LocalBinding) daemonmux.ServerBinding {
	t.Helper()
	serverBinding, err := daemonmux.NewServerBinding(binding.Identity, ports.BrokerDaemonIncarnation{7, 1}, binding.Policy)
	require.NoError(t, err)
	return serverBinding
}

// TestBrokerLocalProbeObservesWithoutAttaching proves the local probe completes
// only the daemonmux physical preamble: it reports the daemon's authenticated
// identity, incarnation, and negotiated protocol version, and then closes the
// carriage without ever opening a logical stream (no attach). Capabilities and
// the session catalogue stay unknown because the preamble carries neither.
func TestBrokerLocalProbeObservesWithoutAttaching(t *testing.T) {
	_, binding := testLocalSandboxConfig(t)
	serverBinding := testServerBinding(t, binding)

	clientConn, serverConn := net.Pipe()
	t.Cleanup(func() {
		_ = clientConn.Close()
		_ = serverConn.Close()
	})

	var dialed atomic.Value
	dial := func(_ context.Context, address string) (daemonmux.RawFramedTransport, error) {
		dialed.Store(address)
		return rawFramedTransport(clientConn), nil
	}
	probe, err := newLocalRouteProbe(binding, dial, daemonmux.DefaultMuxCeilings())
	require.NoError(t, err)

	// The daemon side completes one preamble and then blocks reading the next
	// frame. The probe must never send one: the read ends only when the probe
	// closes the carriage, which is the proof that no attachment stream opened.
	serverDone := make(chan error, 1)
	go func() {
		bridge, err := daemonmux.NewPreambleCarrier(rawFramedTransport(serverConn))
		if err != nil {
			serverDone <- err
			return
		}
		if _, err := daemonmux.RunServerHandshake(context.Background(), bridge, serverBinding, daemonmux.DefaultMuxCeilings()); err != nil {
			serverDone <- err
			return
		}
		readCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_, recvErr := bridge.Receive(readCtx)
		serverDone <- recvErr
	}()

	observation, err := probe.ProbeLocal(context.Background())
	require.NoError(t, err)
	require.Equal(t, binding.Identity, observation.Identity)
	require.Equal(t, ports.BrokerDaemonIncarnation{7, 1}, observation.Incarnation)
	require.Equal(t, binding.Policy.ProtocolVersion, observation.ProtocolVersion)
	require.Equal(t, domain.RemoteAvailabilityReachable, observation.Availability)
	require.Zero(t, observation.Capabilities, "the preamble carries no capabilities; they must stay unknown")
	require.Empty(t, observation.Sessions)
	require.False(t, observation.InventoryKnown)
	require.False(t, observation.Local, "the probe supplies observed state only, never local authority")
	require.Empty(t, observation.Endpoint)
	require.Equal(t, binding.Route.Address(), dialed.Load())

	serverErr := <-serverDone
	require.Error(t, serverErr, "the probe sent no frame after the preamble")
	require.True(t, errors.Is(serverErr, io.EOF) || errors.Is(serverErr, io.ErrClosedPipe) || errors.Is(serverErr, context.Canceled),
		"expected a closed carriage, got %v", serverErr)
}

// TestBrokerLocalProbeFailsClosed proves an unavailable carriage yields zero
// observed identity and an explicit availability, never an invented one, and
// that the probe only dials the provisioned local address: it has no spawn path
// and never starts a stopped daemon.
func TestBrokerLocalProbeFailsClosed(t *testing.T) {
	_, binding := testLocalSandboxConfig(t)

	var addresses []string
	dial := func(_ context.Context, address string) (daemonmux.RawFramedTransport, error) {
		addresses = append(addresses, address)
		return nil, errors.New("no carriage")
	}
	probe, err := newLocalRouteProbe(binding, dial, daemonmux.DefaultMuxCeilings())
	require.NoError(t, err)

	observation, err := probe.ProbeLocal(context.Background())
	require.Error(t, err)
	require.Equal(t, domain.RemoteAvailabilityUnreachable, observation.Availability)
	require.Zero(t, observation.Identity)
	require.True(t, observation.Incarnation.IsZero())
	require.Zero(t, observation.ProtocolVersion)
	require.Nil(t, observation.Sessions)
	require.Equal(t, []string{binding.Route.Address()}, addresses)

	t.Run("a nil carriage is refused", func(t *testing.T) {
		probe, err := newLocalRouteProbe(binding, func(context.Context, string) (daemonmux.RawFramedTransport, error) {
			return nil, nil
		}, daemonmux.DefaultMuxCeilings())
		require.NoError(t, err)
		observation, err := probe.ProbeLocal(context.Background())
		require.Error(t, err)
		require.Zero(t, observation.Identity)
	})

	t.Run("construction refuses a missing dialer", func(t *testing.T) {
		_, err := newLocalRouteProbe(binding, nil, daemonmux.DefaultMuxCeilings())
		require.Error(t, err)
	})
}

// TestBrokerLocalObservationComposition proves the composition derives the
// local producer's authority from the validated configuration and refuses a
// configuration that provisions no local binding.
func TestBrokerLocalObservationComposition(t *testing.T) {
	config, binding := testLocalSandboxConfig(t)
	observation, err := brokerLocalObservation(config, nil)
	require.NoError(t, err)
	require.Equal(t, binding.DisplayOrigin, observation.DisplayOrigin)
	require.Equal(t, binding.Policy, observation.Policy)
	require.NotNil(t, observation.Probe)

	t.Run("no local binding provisions no producer", func(t *testing.T) {
		root := filepath.Join(shortTempDir(t, "vevl"), "sandbox")
		require.NoError(t, os.MkdirAll(root, 0o700))
		writeSandboxConfig(t, root, sandboxRegistrationDocument("/tmp/vev-local-test/mux.sock"))
		layout, err := brokerconfig.ResolveLayout(root, nil)
		require.NoError(t, err)
		config, err := brokerconfig.Load(layout)
		require.NoError(t, err)
		_, err = brokerLocalObservation(config, nil)
		require.Error(t, err)
	})
}

// TestBrokerLocalObservationRegistryPublishesLocalFirst proves the composition
// end to end: the registry observes the local daemon through the broker-owned
// probe and publishes it at index zero, and the durable store persists no local
// entry (the local observation is never durable).
func TestBrokerLocalObservationRegistryPublishesLocalFirst(t *testing.T) {
	_, binding := testLocalSandboxConfig(t)
	serverBinding := testServerBinding(t, binding)

	var dials atomic.Int32
	dial := func(ctx context.Context, _ string) (daemonmux.RawFramedTransport, error) {
		dials.Add(1)
		clientConn, serverConn := net.Pipe()
		go func() {
			defer serverConn.Close()
			bridge, err := daemonmux.NewPreambleCarrier(rawFramedTransport(serverConn))
			if err != nil {
				return
			}
			if _, err := daemonmux.RunServerHandshake(ctx, bridge, serverBinding, daemonmux.DefaultMuxCeilings()); err != nil {
				return
			}
			// Hold the carriage until the probe closes it or the attempt is
			// retired, so the daemon side owns no logical stream either.
			<-ctx.Done()
			_ = bridge.Close()
		}()
		return rawFramedTransport(clientConn), nil
	}
	probe, err := newLocalRouteProbe(binding, dial, daemonmux.DefaultMuxCeilings())
	require.NoError(t, err)

	stateDir := filepath.Join(shortTempDir(t, "vevs"), "state")
	require.NoError(t, os.MkdirAll(stateDir, 0o700))
	store, err := brokerstore.OpenOffline(brokerstore.Options{Dir: stateDir})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })

	registry, err := broker.NewRegistryWithConfig(7, store, nil, clock.New(), slog.New(slog.NewTextHandler(io.Discard, nil)), broker.RegistryConfig{
		Local: &broker.LocalObservation{DisplayOrigin: binding.DisplayOrigin, Policy: binding.Policy, Probe: probe},
	})
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); registry.Run(ctx) }()
	t.Cleanup(func() { cancel(); <-done })

	require.Eventually(t, func() bool {
		snapshot := registry.Snapshot()
		return len(snapshot.Daemons) == 1 && snapshot.Daemons[0].Local &&
			!snapshot.Daemons[0].Checking && snapshot.Daemons[0].Availability == domain.RemoteAvailabilityReachable
	}, 3*time.Second, 2*time.Millisecond)

	snapshot := registry.Snapshot()
	require.Len(t, snapshot.Daemons, 1)
	require.True(t, snapshot.Daemons[0].Local)
	require.Equal(t, binding.Identity, snapshot.Daemons[0].Identity)
	require.Positive(t, dials.Load())

	// The durable store persists remote observations only: the local entry never
	// reaches disk.
	require.Eventually(t, func() bool {
		stored, err := store.Load()
		return err == nil && stored.Revision > 0
	}, 3*time.Second, 2*time.Millisecond)
	stored, err := store.Load()
	require.NoError(t, err)
	for _, daemon := range stored.Daemons {
		require.False(t, daemon.Local, "a local observation must never be durable")
	}
}

// TestBrokerLocalProbeRefusesNonLocalRoute proves the probe refuses a
// hand-built binding whose route is an ssh helper route: probing must never
// start a child process, so only a Unix route this process dials directly is
// acceptable. brokerconfig exposes no exported route constructor and its route
// fields are unexported, so the ssh route is obtained the only way this package
// can: through a provisioned ssh registration (loadTestRoute, shared with the
// mux tests) and used directly as the local binding's route.
func TestBrokerLocalProbeRefusesNonLocalRoute(t *testing.T) {
	isolateSandboxEnv(t)
	sshRoute := loadTestRoute(t, map[string]any{
		"kind":   "ssh-stdio",
		"target": "user@host:2222",
		"argv":   []any{"vev", brokerMuxStdioCommand, "--offline-root", "/srv/remote"},
	})
	require.Equal(t, brokerconfig.RouteSSHStdio, sshRoute.Kind())
	require.False(t, sshRoute.IsLocal(), "an ssh helper route is never dialed directly")

	binding := brokerconfig.LocalBinding{
		Identity:      brokerLocalTestIdentity,
		DisplayOrigin: "local",
		Policy:        brokerTestPolicy(),
		Route:         sshRoute,
	}
	_, err := newLocalRouteProbe(binding, func(context.Context, string) (daemonmux.RawFramedTransport, error) {
		t.Fatal("an ssh route must never be dialed by the local probe")
		return nil, nil
	}, daemonmux.DefaultMuxCeilings())
	require.Error(t, err)
	require.Contains(t, err.Error(), "local route")
}

// TestBrokerLocalProbeNilReceiverFailsClosed proves a nil probe reports an
// explicit error and an observation whose availability is never zero.
func TestBrokerLocalProbeNilReceiverFailsClosed(t *testing.T) {
	var probe *localRouteProbe
	observation, err := probe.ProbeLocal(context.Background())
	require.Error(t, err)
	require.Equal(t, domain.RemoteAvailabilityUnreachable, observation.Availability)
	require.NotZero(t, observation.Availability, "availability is never zero")
}
