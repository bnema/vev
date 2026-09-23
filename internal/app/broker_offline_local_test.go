package app

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/adapters/brokerconfig"
	"github.com/bnema/vev/internal/adapters/brokerstore"
	"github.com/bnema/vev/internal/adapters/clock"
	"github.com/bnema/vev/internal/adapters/daemonidentity"
	"github.com/bnema/vev/internal/adapters/daemonmux"
	"github.com/bnema/vev/internal/adapters/ipc"
	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/protocol/catalogue"
	"github.com/bnema/vev/internal/usecase/broker"
)

const (
	brokerLocalTestIdentity = "offline-local-daemon"
	brokerLocalTestEpoch    = ports.BrokerEpoch(11)
)

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
	layout, err := offlineLayout(root)
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

// testLocalConnector builds the broker-owned endpoint connector a probe
// composes, over an explicit dial seam. It is the same connector type the pool
// uses, so a test exercises the real composition path.
func testLocalConnector(t *testing.T, dial localCarrierDialer) ports.BrokerEndpointConnector {
	t.Helper()
	connector, err := daemonmux.NewEndpointConnector(daemonmux.RawCarrierDialer(dial), daemonmux.DefaultMuxCeilings())
	require.NoError(t, err)
	return connector
}

// unreachableCarriageConnector builds a connector whose dial always fails, so a
// composition test can build a probe without a scripted daemon.
func unreachableCarriageConnector(t *testing.T) ports.BrokerEndpointConnector {
	t.Helper()
	return testLocalConnector(t, func(context.Context, ports.BrokerDialTarget) (daemonmux.RawFramedTransport, error) {
		return nil, errors.New("no carriage")
	})
}

// testLocalCatalogue builds one exact-schema catalogue carrying the supplied
// sessions, marshalled exactly as the daemon's remote-catalog command does.
func testLocalCatalogue(t *testing.T, sessions ...catalogue.RemoteCatalogSession) string {
	t.Helper()
	encoded, err := json.Marshal(catalogue.RemoteCatalog{
		ProtocolVersion: protocol.Version,
		SchemaVersion:   catalogue.RemoteCatalogSchemaVersion,
		Sessions:        sessions,
	})
	require.NoError(t, err)
	return string(encoded) + "\n"
}

// testLocalUpSession builds one exact, attachable catalogue session with an
// ordered tab list.
func testLocalUpSession(lifecycle domain.SessionLifecycleID, name string) catalogue.RemoteCatalogSession {
	return catalogue.RemoteCatalogSession{
		LifecycleID: lifecycle,
		Name:        name,
		State:       catalogue.RemoteCatalogSessionUp,
		Tabs:        []catalogue.RemoteCatalogTab{},
	}
}

// scriptedLocalDaemon is a scripted daemonmux daemon for the broker-owned local
// probe: it completes the authenticated physical preamble for the supplied
// binding, accepts the logical streams the probe opens, records every typed
// client message, and answers from a scripted responder. It never creates a
// session or a process and never interprets the probe's traffic beyond the
// responder.
type scriptedLocalDaemon struct {
	supervisor *daemonmux.ServerSupervisor
	respond    func(protocol.ClientMessage) protocol.ServerMessage

	dials atomic.Int32

	mu       sync.Mutex
	streams  int
	received []protocol.ClientMessage
}

// newScriptedLocalDaemon serves one scripted daemonmux daemon over in-memory
// carriages. The responder answers one typed client message; a nil reply ends
// the stream without an answer.
func newScriptedLocalDaemon(t *testing.T, binding daemonmux.ServerBinding, respond func(protocol.ClientMessage) protocol.ServerMessage) *scriptedLocalDaemon {
	t.Helper()
	daemon := &scriptedLocalDaemon{respond: respond}
	aggregate := daemonmux.NewAggregateListener()
	supervisor, err := daemonmux.NewServerSupervisor(aggregate, binding, daemonmux.DefaultMuxCeilings(), 0)
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = supervisor.Close()
		_ = aggregate.Close()
	})
	go func() {
		for {
			connection, acceptErr := aggregate.Accept()
			if acceptErr != nil {
				return
			}
			daemon.mu.Lock()
			daemon.streams++
			daemon.mu.Unlock()
			go daemon.serveStream(connection)
		}
	}()
	daemon.supervisor = supervisor
	return daemon
}

// dial returns the probe's carrier dialer: one fresh in-memory carriage whose
// daemon end is adopted by the scripted daemon.
func (d *scriptedLocalDaemon) dial() localCarrierDialer {
	return func(ctx context.Context, _ ports.BrokerDialTarget) (daemonmux.RawFramedTransport, error) {
		d.dials.Add(1)
		client, server := net.Pipe()
		go func() { _ = d.supervisor.Adopt(context.Background(), rawFramedTransport(server)) }()
		return rawFramedTransport(client), nil
	}
}

// serveStream records every typed client message of one admitted stream and
// answers it from the responder. A responder that returns nil ends the stream
// without an answer, so a test can exercise the peer-closed-without-replying
// path; a responder that blocks holds the stream open, so a test can exercise
// the caller-bounded wait.
func (d *scriptedLocalDaemon) serveStream(connection ports.ServerConnection) {
	defer func() { _ = connection.Close() }()
	for {
		message, err := connection.ReceiveClient()
		if err != nil {
			return
		}
		d.mu.Lock()
		d.received = append(d.received, message)
		d.mu.Unlock()
		if d.respond == nil {
			return
		}
		reply := d.respond(message)
		if reply == nil {
			return
		}
		if err := connection.SendServer(reply); err != nil {
			return
		}
	}
}

// messages returns a defensive copy of every recorded client message.
func (d *scriptedLocalDaemon) messages() []protocol.ClientMessage {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([]protocol.ClientMessage(nil), d.received...)
}

// awaitMessages waits until the daemon recorded at least count messages.
func (d *scriptedLocalDaemon) awaitMessages(t *testing.T, count int) []protocol.ClientMessage {
	t.Helper()
	require.Eventually(t, func() bool {
		d.mu.Lock()
		defer d.mu.Unlock()
		return len(d.received) >= count
	}, brokerTestWait, time.Millisecond, "scripted local daemon recorded %d messages", len(d.messages()))
	return d.messages()
}

// awaitStreams waits until the daemon admitted at least count logical streams.
func (d *scriptedLocalDaemon) awaitStreams(t *testing.T, count int) {
	t.Helper()
	require.Eventually(t, func() bool {
		d.mu.Lock()
		defer d.mu.Unlock()
		return d.streams >= count
	}, brokerTestWait, time.Millisecond, "scripted local daemon admitted too few streams")
}

// TestBrokerLocalProbeObservesAuthenticatedCatalogue proves the broker-owned
// local probe opens exactly one logical observation stream over the
// authenticated daemonmux carriage, asks the daemon for its own catalogue
// through the existing typed control protocol, and publishes the observed
// identity, incarnation, protocol version, and exact session inventory. It
// never sends a Hello, requests no admission, claims no geometry, and creates
// nothing.
func TestBrokerLocalProbeObservesAuthenticatedCatalogue(t *testing.T) {
	_, binding := testLocalSandboxConfig(t)
	serverBinding := testServerBinding(t, binding)
	lifecycle := domain.SessionLifecycleID{1}
	session := testLocalUpSession(lifecycle, "work")

	daemon := newScriptedLocalDaemon(t, serverBinding, func(message protocol.ClientMessage) protocol.ServerMessage {
		request, ok := message.(protocol.CommandRequest)
		if !ok {
			return nil
		}
		return protocol.CommandResult{RequestID: request.RequestID, Outcome: protocol.CommandSucceeded, Output: testLocalCatalogue(t, session)}
	})

	probe, err := newLocalRouteProbe(binding, brokerLocalTestEpoch, daemon.dial(), daemonmux.DefaultMuxCeilings())
	require.NoError(t, err)

	observation, err := probe.ProbeLocal(context.Background())
	require.NoError(t, err)
	require.Equal(t, binding.Identity, observation.Identity)
	require.Equal(t, serverBinding.Incarnation(), observation.Incarnation)
	require.Equal(t, protocol.Version, observation.ProtocolVersion)
	require.Equal(t, domain.RemoteAvailabilityReachable, observation.Availability)
	require.True(t, observation.InventoryKnown, "an answered catalogue is known inventory")
	require.Equal(t, []catalogue.RemoteCatalogSession{session}, observation.Sessions)
	require.Zero(t, observation.Capabilities, "the catalogue command carries no capabilities; they must stay unknown")
	require.False(t, observation.Local, "the probe supplies observed state only, never configured authority")
	require.Empty(t, observation.Endpoint)
	require.Equal(t, domain.RemoteRegistration{}, observation.Registration)

	// Exactly one observation stream opened, and its only traffic is one typed,
	// correlated remote-catalog request: no Hello means no attachment, no
	// session creation, and no geometry claim.
	daemon.awaitStreams(t, 1)
	messages := daemon.awaitMessages(t, 1)
	require.Len(t, messages, 1)
	request, ok := messages[0].(protocol.CommandRequest)
	require.True(t, ok, "the observation stream must send only a typed command, got %#v", messages[0])
	require.Equal(t, "remote-catalog", request.Slug)
	require.True(t, request.JSON)
	require.False(t, request.Attached, "an observation stream never claims an attachment")
	require.NotZero(t, request.RequestID)
	require.Equal(t, protocol.Version, request.Version)
	for _, message := range messages {
		_, hello := message.(protocol.Hello)
		require.False(t, hello, "an observation must never send a Hello")
	}
	require.EqualValues(t, 1, daemon.dials.Load())
}

// TestBrokerLocalProbeRequestIsObservationOnly proves the one open the local
// probe performs is a local observation that may never start the daemon: it
// carries no admission, no name, no target, no environment, no endpoint, and
// no registration, and it is refused outright when it authorizes a spawn.
func TestBrokerLocalProbeRequestIsObservationOnly(t *testing.T) {
	_, binding := testLocalSandboxConfig(t)
	probe, err := newLocalRouteProbe(binding, brokerLocalTestEpoch, func(context.Context, ports.BrokerDialTarget) (daemonmux.RawFramedTransport, error) {
		return nil, nil
	}, daemonmux.DefaultMuxCeilings())
	require.NoError(t, err)

	request, err := probe.request()
	require.NoError(t, err)
	require.NoError(t, request.Validate())
	require.Equal(t, ports.BrokerStreamObservation, request.Purpose)
	require.Equal(t, brokerLocalTestEpoch, request.Epoch)
	require.True(t, request.Local)
	require.Zero(t, request.Admission)
	require.Empty(t, request.Name)
	require.Empty(t, request.Endpoint)
	require.Equal(t, domain.RemoteRegistration{}, request.Registration)
	require.Empty(t, request.Env)
	require.Equal(t, protocol.ExactSessionTarget{}, request.Target)
	require.Equal(t, binding.Policy, request.Policy)
	require.NotZero(t, request.Connection)
	require.NotZero(t, request.Stream)
	// Observation must never start the target: the request carries the
	// existing-only authorization, and a spawn-capable one is refused rather
	// than silently narrowed.
	require.Equal(t, ports.BrokerDaemonExistingOnly, request.StartMode)
	startable := request
	startable.StartMode = ports.BrokerDaemonStartIfNeeded
	require.Error(t, startable.Validate())
}

// TestBrokerLocalProbeFailsClosed proves an unavailable carriage yields zero
// observed identity, no inventory, and an explicit availability, never an
// invented one, and that the probe only dials the provisioned local address: it
// has no spawn path and never starts a stopped daemon.
func TestBrokerLocalProbeFailsClosed(t *testing.T) {
	_, binding := testLocalSandboxConfig(t)

	var addresses []string
	dial := func(_ context.Context, target ports.BrokerDialTarget) (daemonmux.RawFramedTransport, error) {
		require.Equal(t, ports.BrokerDaemonExistingOnly, target.StartMode, "an observation dial never authorizes starting the daemon")
		addresses = append(addresses, target.Address)
		return nil, errors.New("no carriage")
	}
	probe, err := newLocalRouteProbe(binding, brokerLocalTestEpoch, dial, daemonmux.DefaultMuxCeilings())
	require.NoError(t, err)

	observation, err := probe.ProbeLocal(context.Background())
	require.Error(t, err)
	require.Equal(t, domain.RemoteAvailabilityUnreachable, observation.Availability)
	require.Zero(t, observation.Identity)
	require.True(t, observation.Incarnation.IsZero())
	require.Zero(t, observation.ProtocolVersion)
	require.False(t, observation.InventoryKnown)
	require.Nil(t, observation.Sessions)
	require.Equal(t, []string{binding.Route.Address()}, addresses)

	t.Run("a nil carriage is refused", func(t *testing.T) {
		probe, err := newLocalRouteProbe(binding, brokerLocalTestEpoch, func(context.Context, ports.BrokerDialTarget) (daemonmux.RawFramedTransport, error) {
			return nil, nil
		}, daemonmux.DefaultMuxCeilings())
		require.NoError(t, err)
		observation, err := probe.ProbeLocal(context.Background())
		require.Error(t, err)
		require.Zero(t, observation.Identity)
		require.Equal(t, domain.RemoteAvailabilityUnreachable, observation.Availability)
	})

	t.Run("construction refuses a missing dialer", func(t *testing.T) {
		_, err := newLocalRouteProbe(binding, brokerLocalTestEpoch, nil, daemonmux.DefaultMuxCeilings())
		require.Error(t, err)
	})

	t.Run("construction refuses a zero epoch", func(t *testing.T) {
		_, err := newLocalRouteProbe(binding, 0, func(context.Context, ports.BrokerDialTarget) (daemonmux.RawFramedTransport, error) {
			return nil, nil
		}, daemonmux.DefaultMuxCeilings())
		require.Error(t, err)
	})
}

// TestBrokerLocalProbeRejectsInvalidObservation proves every malformed or
// mistyped answer fails closed: an uncorrelated result, an explicit failure, a
// result that is not a result at all, malformed JSON, and a catalogue outside
// the exact current schema all yield an error and an explicit unreachable
// observation with zero identity, zero version, and no inventory. A probe never
// publishes inventory it could not validate.
func TestBrokerLocalProbeRejectsInvalidObservation(t *testing.T) {
	_, binding := testLocalSandboxConfig(t)
	serverBinding := testServerBinding(t, binding)
	session := testLocalUpSession(domain.SessionLifecycleID{1}, "work")

	// stalled is released when the subtest ends, so a responder that holds an
	// answer open never leaks its goroutine past the test that needed it.
	stalled := make(chan struct{})
	t.Cleanup(func() { close(stalled) })

	valid := testLocalCatalogue(t, session)
	badSchema, err := json.Marshal(catalogue.RemoteCatalog{
		ProtocolVersion: protocol.Version,
		SchemaVersion:   catalogue.RemoteCatalogSchemaVersion + 1,
		Sessions:        []catalogue.RemoteCatalogSession{session},
	})
	require.NoError(t, err)

	tests := []struct {
		name    string
		respond func(protocol.ClientMessage) protocol.ServerMessage
		timeout time.Duration
	}{
		{
			name: "uncorrelated result",
			respond: func(message protocol.ClientMessage) protocol.ServerMessage {
				request, _ := message.(protocol.CommandRequest)
				return protocol.CommandResult{RequestID: request.RequestID + 1, Outcome: protocol.CommandSucceeded, Output: valid}
			},
		},
		{
			name: "explicit failure",
			respond: func(protocol.ClientMessage) protocol.ServerMessage {
				return protocol.CommandResult{RequestID: 0, Outcome: protocol.CommandFailed, Code: protocol.ErrInternal, Text: "catalogue unavailable"}
			},
		},
		{
			name:    "wrong message type",
			respond: func(protocol.ClientMessage) protocol.ServerMessage { return protocol.Pong{} },
		},
		{
			name: "malformed json",
			respond: func(message protocol.ClientMessage) protocol.ServerMessage {
				request, _ := message.(protocol.CommandRequest)
				return protocol.CommandResult{RequestID: request.RequestID, Outcome: protocol.CommandSucceeded, Output: "{not json"}
			},
		},
		{
			name: "schema mismatch",
			respond: func(message protocol.ClientMessage) protocol.ServerMessage {
				request, _ := message.(protocol.CommandRequest)
				return protocol.CommandResult{RequestID: request.RequestID, Outcome: protocol.CommandSucceeded, Output: string(badSchema)}
			},
		},
		{
			name:    "peer closes without answering",
			respond: func(protocol.ClientMessage) protocol.ServerMessage { return nil },
		},
		{
			name: "daemon stalls and the attempt deadline expires",
			respond: func(protocol.ClientMessage) protocol.ServerMessage {
				<-stalled
				return nil
			},
			timeout: 200 * time.Millisecond,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			daemon := newScriptedLocalDaemon(t, serverBinding, tt.respond)
			probe, err := newLocalRouteProbe(binding, brokerLocalTestEpoch, daemon.dial(), daemonmux.DefaultMuxCeilings())
			require.NoError(t, err)

			wait := brokerTestWait
			if tt.timeout > 0 {
				wait = tt.timeout
			}
			ctx, cancel := context.WithTimeout(context.Background(), wait)
			defer cancel()
			observation, err := probe.ProbeLocal(ctx)
			require.Error(t, err)
			require.Equal(t, domain.RemoteAvailabilityUnreachable, observation.Availability)
			require.Zero(t, observation.Identity)
			require.True(t, observation.Incarnation.IsZero())
			require.Zero(t, observation.ProtocolVersion)
			require.False(t, observation.InventoryKnown)
			require.Empty(t, observation.Sessions)
		})
	}
}

// TestBrokerLocalObservationComposition proves the composition derives the
// local producer's authority from the validated configuration and refuses a
// configuration that provisions no local binding.
func TestBrokerLocalObservationComposition(t *testing.T) {
	config, binding := testLocalSandboxConfig(t)
	connector := unreachableCarriageConnector(t)
	observation, err := brokerLocalObservation(config, brokerLocalTestEpoch, connector)
	require.NoError(t, err)
	require.Equal(t, binding.DisplayOrigin, observation.DisplayOrigin)
	require.Equal(t, binding.Policy, observation.Policy)
	require.NotNil(t, observation.Probe)
	// Shared transport: the probe composes the exact connector the pool already
	// owns instead of dialing its own, so one broker process holds one transport
	// owner per route.
	probe, ok := observation.Probe.(*localRouteProbe)
	require.True(t, ok)
	require.Same(t, connector, probe.connector)
	require.Equal(t, brokerLocalTestEpoch, probe.epoch)

	t.Run("no local binding provisions no producer", func(t *testing.T) {
		root := filepath.Join(shortTempDir(t, "vevl"), "sandbox")
		require.NoError(t, os.MkdirAll(root, 0o700))
		writeSandboxConfig(t, root, sandboxRegistrationDocument("/tmp/vev-local-test/mux.sock"))
		layout, err := brokerconfig.ResolveLayout(root, nil)
		require.NoError(t, err)
		config, err := brokerconfig.Load(layout)
		require.NoError(t, err)
		_, err = brokerLocalObservation(config, brokerLocalTestEpoch, connector)
		require.Error(t, err)
	})
}

// TestOfflineRegistryConfigSelectsLocalObservationOnlyWhenProvisioned proves the
// composition's producer selection is decided by the provisioned configuration
// alone, never by the presence of an identity: a provisioned local binding
// always observes and runs with mutable membership (so the broker owns host
// membership for its whole run, including a zero-host membership), while a
// configuration with no local binding is an explicitly configured fixture that
// stays observation-disabled.
func TestOfflineRegistryConfigSelectsLocalObservationOnlyWhenProvisioned(t *testing.T) {
	localConfig, binding := testLocalSandboxConfig(t)
	selected, err := offlineRegistryConfig(localConfig, brokerLocalTestEpoch, unreachableCarriageConnector(t))
	require.NoError(t, err)
	require.False(t, selected.ObservationDisabled)
	require.Equal(t, broker.MembershipMutable, selected.MembershipMode, "a provisioned local binding always owns mutable membership")
	require.NotNil(t, selected.Local)
	require.Equal(t, binding.DisplayOrigin, selected.Local.DisplayOrigin)
	require.Equal(t, binding.Policy, selected.Local.Policy)

	root := filepath.Join(shortTempDir(t, "vevl"), "sandbox")
	require.NoError(t, os.MkdirAll(root, 0o700))
	writeSandboxConfig(t, root, sandboxRegistrationDocument("/tmp/vev-local-test/mux.sock"))
	layout, err := brokerconfig.ResolveLayout(root, nil)
	require.NoError(t, err)
	remoteOnly, err := brokerconfig.Load(layout)
	require.NoError(t, err)
	disabled, err := offlineRegistryConfig(remoteOnly, brokerLocalTestEpoch, unreachableCarriageConnector(t))
	require.NoError(t, err)
	require.True(t, disabled.ObservationDisabled, "a fixture with no local binding stays explicitly disabled")
	require.Nil(t, disabled.Local)
}

// productionLocalConfig builds one production-shaped broker configuration whose
// local binding carries an empty identity, exactly as the broker composes it
// before the daemon's first start. The identity is supplied only by the loader.
func productionLocalConfig(t *testing.T) (*brokerconfig.Config, brokerconfig.LocalBinding) {
	t.Helper()
	t.Setenv("XDG_RUNTIME_DIR", t.TempDir())
	t.Setenv("XDG_STATE_HOME", t.TempDir())
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
	layout := productionBrokerLayout()
	path := productionBrokerConfigPath()
	require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o700))
	raw, err := json.Marshal(sandboxEmptyDocument("3s"))
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(path, raw, 0o600))
	config, err := brokerconfig.LoadProduction(layout, path, "", localDaemonPolicy(), daemonmux.SocketPath(ipc.SocketDir()))
	require.NoError(t, err)
	binding, ok := config.LocalBinding()
	require.True(t, ok)
	require.Empty(t, binding.Identity, "production local authority starts with no identity")
	return config, binding
}

// TestBrokerLocalObservationWithoutIdentityPublishesLocalUnknown proves the
// composition observes the local daemon even before an identity exists: the
// published snapshot carries exactly one local entry with the explicit unknown
// availability and zero observed identity, at a nonzero epoch and revision. A
// broker may therefore precede its daemon and still publish a valid catalogue.
func TestBrokerLocalObservationWithoutIdentityPublishesLocalUnknown(t *testing.T) {
	config, _ := productionLocalConfig(t)
	// The loader re-reads an absent identity on every attempt, so the probe is
	// dynamic: it never dials and never spawns while no identity exists.
	loader := localIdentityLoader(func() (ports.BrokerDaemonIdentity, error) { return "", nil })
	var dials atomic.Int32
	connector := testLocalConnector(t, func(context.Context, ports.BrokerDialTarget) (daemonmux.RawFramedTransport, error) {
		dials.Add(1)
		return nil, errors.New("an absent identity must never dial")
	})
	local, err := brokerLocalObservation(config, brokerLocalTestEpoch, connector, loader)
	require.NoError(t, err)

	stateDir := filepath.Join(shortTempDir(t, "vevl"), "state")
	store, err := brokerstore.Open(brokerstore.Options{Dir: stateDir})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	registry, err := broker.NewRegistryWithConfig(brokerLocalTestEpoch, store, nil, clock.New(), discardLog(), broker.RegistryConfig{Local: local, MembershipMode: broker.MembershipMutable})
	require.NoError(t, err)
	startLocalRegistry(t, registry)

	observation := waitLocalEntry(t, registry, func(entry ports.BrokerDaemonObservation) bool { return !entry.Checking })
	require.True(t, observation.Local)
	require.Equal(t, domain.RemoteAvailabilityUnknown, observation.Availability, "an unobserved local daemon is explicitly unknown, never invented")
	require.Empty(t, observation.Identity)
	require.True(t, observation.Incarnation.IsZero())
	require.Zero(t, observation.ProtocolVersion)
	require.False(t, observation.InventoryKnown)
	require.Empty(t, observation.Sessions)

	snapshot := registry.Snapshot()
	require.NotZero(t, snapshot.Epoch, "a published snapshot always carries an epoch")
	require.NotZero(t, snapshot.Revision, "a published snapshot always carries a nonzero revision")
	require.Len(t, snapshot.Daemons, 1, "exactly one local entry is published")
	require.True(t, snapshot.Daemons[0].Local)
	require.Equal(t, int32(0), dials.Load(), "an absent identity never dials and never spawns")

	// Never durable: the store is asked for its durable snapshot and the local
	// entry is absent from it.
	durable, err := store.Load()
	require.NoError(t, err)
	require.Empty(t, durable.Daemons, "a local observation is never durable")
}

// TestBrokerLocalObservationPublishesReachableCatalogueWhenIdentityAppears
// proves the same broker starts observing without a restart once the daemon
// publishes its identity: the loader is re-read on every attempt, so the next
// observation authenticates the local carriage and publishes the reachable
// catalogue.
func TestBrokerLocalObservationPublishesReachableCatalogueWhenIdentityAppears(t *testing.T) {
	config, binding := productionLocalConfig(t)
	// The daemon publishes its own identity; the composition's binding starts
	// empty and is filled by the loader the moment the daemon first starts.
	daemonIdentity := ports.BrokerDaemonIdentity(brokerLocalTestIdentity)
	serverBinding, err := daemonmux.NewServerBinding(daemonIdentity, ports.BrokerDaemonIncarnation{7, 1}, binding.Policy)
	require.NoError(t, err)
	session := testLocalUpSession(domain.SessionLifecycleID{5}, "appeared")
	daemon := newScriptedLocalDaemon(t, serverBinding, func(message protocol.ClientMessage) protocol.ServerMessage {
		request, ok := message.(protocol.CommandRequest)
		if !ok {
			return nil
		}
		return protocol.CommandResult{RequestID: request.RequestID, Outcome: protocol.CommandSucceeded, Output: testLocalCatalogue(t, session)}
	})

	// The identity is absent at first and appears before the next attempt; the
	// scripted daemon answers under that identity.
	var identity atomic.Value
	identity.Store(ports.BrokerDaemonIdentity(""))
	connector := testLocalConnector(t, daemon.dial())
	local, err := brokerLocalObservation(config, brokerLocalTestEpoch, connector, func() (ports.BrokerDaemonIdentity, error) {
		return identity.Load().(ports.BrokerDaemonIdentity), nil
	})
	require.NoError(t, err)

	stateDir := filepath.Join(shortTempDir(t, "vevl"), "state")
	store, err := brokerstore.Open(brokerstore.Options{Dir: stateDir})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	registry, err := broker.NewRegistryWithConfig(brokerLocalTestEpoch, store, nil, clock.New(), discardLog(), broker.RegistryConfig{Local: local, MembershipMode: broker.MembershipMutable})
	require.NoError(t, err)
	startLocalRegistry(t, registry)

	waitLocalEntry(t, registry, func(entry ports.BrokerDaemonObservation) bool { return !entry.Checking })
	require.Equal(t, int32(0), daemon.dials.Load(), "no dial happens while no identity exists")

	identity.Store(daemonIdentity)
	observed := waitLocalEntry(t, registry, func(entry ports.BrokerDaemonObservation) bool {
		return entry.Availability == domain.RemoteAvailabilityReachable
	})
	require.Equal(t, daemonIdentity, observed.Identity)
	require.Equal(t, serverBinding.Incarnation(), observed.Incarnation)
	require.Equal(t, protocol.Version, observed.ProtocolVersion)
	require.True(t, observed.InventoryKnown)
	require.Equal(t, []catalogue.RemoteCatalogSession{session}, observed.Sessions)
	require.Positive(t, daemon.dials.Load())

	// The published catalogue resolves the exact attach target the client
	// composition uses, so the appearing daemon is immediately usable.
	_, ok := brokerObservationExactTarget(observed, "appeared")
	require.True(t, ok)
}

// TestBrokerLocalObservationNeverSpawnsDaemon proves the local observation is
// spawn-free: every dial carries the existing-only authorization, so observing
// a stopped daemon can never start one, and no dial happens without an identity.
func TestBrokerLocalObservationNeverSpawnsDaemon(t *testing.T) {
	_, binding := testLocalSandboxConfig(t)
	var modes []ports.BrokerDaemonStartMode
	var mu sync.Mutex
	probe, err := newDynamicLocalRouteProbe(binding.Route, binding.Policy, brokerLocalTestEpoch, testEndpointConnectorFunc(func(_ context.Context, target ports.BrokerDialTarget) (ports.BrokerPhysicalConnection, error) {
		mu.Lock()
		modes = append(modes, target.StartMode)
		mu.Unlock()
		return nil, errors.New("socket absent")
	}), func() (ports.BrokerDaemonIdentity, error) { return binding.Identity, nil })
	require.NoError(t, err)

	_, err = probe.ProbeLocal(context.Background())
	require.Error(t, err)
	mu.Lock()
	require.Equal(t, []ports.BrokerDaemonStartMode{ports.BrokerDaemonExistingOnly}, modes, "an observation never authorizes starting the daemon")
	mu.Unlock()
}

// TestOfflineRegistryConfigExplicitDisabledFixtureStaysDisabled proves an
// explicitly configured fixture that provisions no local binding remains a
// read-only snapshot owner: it issues no probes, arms no timers, and never
// reconciles, so its disabled mode is never inferred from an identity.
func TestOfflineRegistryConfigExplicitDisabledFixtureStaysDisabled(t *testing.T) {
	root := filepath.Join(shortTempDir(t, "vevl"), "sandbox")
	require.NoError(t, os.MkdirAll(root, 0o700))
	writeSandboxConfig(t, root, sandboxEmptyDocument("3s"))
	layout, err := brokerconfig.ResolveLayout(root, nil)
	require.NoError(t, err)
	config, err := brokerconfig.Load(layout)
	require.NoError(t, err)

	selected, err := offlineRegistryConfig(config, brokerLocalTestEpoch, unreachableCarriageConnector(t))
	require.NoError(t, err)
	require.True(t, selected.ObservationDisabled)
	require.Nil(t, selected.Local)

	stateDir := filepath.Join(shortTempDir(t, "vevl"), "state")
	store, err := brokerstore.Open(brokerstore.Options{Dir: stateDir})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	registry, err := broker.NewRegistryWithConfig(brokerLocalTestEpoch, store, nil, clock.New(), discardLog(), selected)
	require.NoError(t, err)
	startLocalRegistry(t, registry)

	// A disabled registry publishes its restored durable state and then stays
	// read-only: no probe, no reconcile, and a zero-host mutation is refused.
	require.Eventually(t, func() bool { return registry.Snapshot().Epoch != 0 }, brokerTestWait, time.Millisecond)
	_, err = registry.AddHost(context.Background(), "user@example.com", localDaemonPolicy())
	require.ErrorIs(t, err, ports.ErrBrokerMembershipImmutable, "an explicitly disabled fixture owns no mutable membership")
}

// TestProductionRegistryOwnsMutableMembershipWithZeroHosts proves the production
// composition owns host membership for its whole run even when it starts with
// no configured host at all: the broker may precede every registration, so a
// first host can be added and removed through the same authority the zero-host
// run already published.
func TestProductionRegistryOwnsMutableMembershipWithZeroHosts(t *testing.T) {
	config, _ := productionLocalConfig(t)
	local, err := brokerLocalObservation(config, brokerLocalTestEpoch, unreachableCarriageConnector(t), func() (ports.BrokerDaemonIdentity, error) { return "", nil })
	require.NoError(t, err)
	selected, err := offlineRegistryConfig(config, brokerLocalTestEpoch, unreachableCarriageConnector(t))
	require.NoError(t, err)
	require.Equal(t, broker.MembershipMutable, selected.MembershipMode)

	stateDir := filepath.Join(shortTempDir(t, "vevl"), "state")
	store, err := brokerstore.Open(brokerstore.Options{Dir: stateDir})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	// The production remote probe is required exactly when membership is added,
	// so this fixture supplies one that never dials: the test asserts membership
	// authority, not observation.
	registry, err := broker.NewRegistryWithConfig(brokerLocalTestEpoch, store, unreachableHostProbe(t), clock.New(), discardLog(), broker.RegistryConfig{Local: local, MembershipMode: broker.MembershipMutable})
	require.NoError(t, err)
	startLocalRegistry(t, registry)

	require.Empty(t, registry.Snapshot().Daemons[0].Registration, "the local entry carries no configured registration")
	registration, err := registry.AddHost(context.Background(), "user@example.com", remoteBrokerPolicy("quic"))
	require.NoError(t, err, "a zero-host production run must admit a first host")
	require.Equal(t, "user@example.com", registration.Endpoint)

	removed, err := registry.RemoveHost(context.Background(), registration)
	require.NoError(t, err)
	require.True(t, removed)
}

// TestBrokerLocalObservationRegistryPublishesLocalFirst proves the composition
// end to end: the registry observes the local daemon through the broker-owned
// probe and publishes it at index zero with the observed inventory, and the
// durable store persists no local entry (the local observation is never
// durable).
func TestBrokerLocalObservationRegistryPublishesLocalFirst(t *testing.T) {
	config, binding := testLocalSandboxConfig(t)
	serverBinding := testServerBinding(t, binding)
	session := testLocalUpSession(domain.SessionLifecycleID{9}, "registry")

	daemon := newScriptedLocalDaemon(t, serverBinding, func(message protocol.ClientMessage) protocol.ServerMessage {
		request, ok := message.(protocol.CommandRequest)
		if !ok {
			return nil
		}
		return protocol.CommandResult{RequestID: request.RequestID, Outcome: protocol.CommandSucceeded, Output: testLocalCatalogue(t, session)}
	})
	local, err := brokerLocalObservation(config, brokerLocalTestEpoch, testLocalConnector(t, daemon.dial()))
	require.NoError(t, err)

	stateDir := filepath.Join(shortTempDir(t, "vevs"), "state")
	require.NoError(t, os.MkdirAll(stateDir, 0o700))
	store, err := brokerstore.Open(brokerstore.Options{Dir: stateDir})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })

	registry, err := broker.NewRegistryWithConfig(brokerLocalTestEpoch, store, nil, clock.New(), slog.New(slog.NewTextHandler(io.Discard, nil)), broker.RegistryConfig{Local: local})
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); registry.Run(ctx) }()
	t.Cleanup(func() { cancel(); <-done })
	registry.RequestProbe("")

	require.Eventually(t, func() bool {
		snapshot := registry.Snapshot()
		return len(snapshot.Daemons) == 1 && snapshot.Daemons[0].Local &&
			!snapshot.Daemons[0].Checking && snapshot.Daemons[0].Availability == domain.RemoteAvailabilityReachable
	}, 3*time.Second, 2*time.Millisecond)

	snapshot := registry.Snapshot()
	require.Len(t, snapshot.Daemons, 1)
	require.True(t, snapshot.Daemons[0].Local)
	require.Equal(t, binding.Identity, snapshot.Daemons[0].Identity)
	require.Equal(t, serverBinding.Incarnation(), snapshot.Daemons[0].Incarnation)
	require.True(t, snapshot.Daemons[0].InventoryKnown)
	require.Equal(t, []catalogue.RemoteCatalogSession{session}, snapshot.Daemons[0].Sessions)
	require.Positive(t, daemon.dials.Load())

	// The durable store persists remote observations only: the local entry never
	// reaches disk.
	require.Eventually(t, func() bool {
		stored, err := store.Load()
		return err == nil && stored.Revision > 0
	}, 3*time.Second, 2*time.Millisecond)
	stored, err := store.Load()
	require.NoError(t, err)
	for _, entry := range stored.Daemons {
		require.False(t, entry.Local, "a local observation must never be durable")
	}
}

// TestBrokerLocalProbeDialTargetIsExistingOnly proves the broker-owned local
// observation dial target carries the existing-only authorization: an
// observation must never start a stopped daemon, and the resolved target is
// where that decision is carried to the transport.
type testEndpointConnectorFunc func(context.Context, ports.BrokerDialTarget) (ports.BrokerPhysicalConnection, error)

func (f testEndpointConnectorFunc) Connect(ctx context.Context, target ports.BrokerDialTarget) (ports.BrokerPhysicalConnection, error) {
	return f(ctx, target)
}

func TestBrokerLocalProbeLoadsIdentityOnEveryAttempt(t *testing.T) {
	_, binding := testLocalSandboxConfig(t)
	var identity ports.BrokerDaemonIdentity
	var dials []ports.BrokerDialTarget
	probe, err := newDynamicLocalRouteProbe(binding.Route, binding.Policy, brokerLocalTestEpoch, testEndpointConnectorFunc(func(_ context.Context, target ports.BrokerDialTarget) (ports.BrokerPhysicalConnection, error) {
		dials = append(dials, target)
		return nil, errors.New("socket absent")
	}), func() (ports.BrokerDaemonIdentity, error) {
		return identity, nil
	})
	require.NoError(t, err)

	observation, err := probe.ProbeLocal(context.Background())
	require.NoError(t, err)
	require.Equal(t, domain.RemoteAvailabilityUnknown, observation.Availability)
	require.Empty(t, dials, "an absent identity cannot dial or spawn")

	identity = binding.Identity
	observation, err = probe.ProbeLocal(context.Background())
	require.Error(t, err)
	require.Equal(t, domain.RemoteAvailabilityUnreachable, observation.Availability)
	require.Len(t, dials, 1)
	require.Equal(t, ports.BrokerExpectedIdentity{Identity: binding.Identity, Bound: true}, dials[0].ExpectedIdentity)
	require.Equal(t, ports.BrokerDaemonExistingOnly, dials[0].StartMode)
}

func TestBrokerLocalProbeIdentityCorruptionIsDiagnosticWithoutDial(t *testing.T) {
	_, binding := testLocalSandboxConfig(t)
	dialed := false
	probe, err := newDynamicLocalRouteProbe(binding.Route, binding.Policy, brokerLocalTestEpoch, testEndpointConnectorFunc(func(context.Context, ports.BrokerDialTarget) (ports.BrokerPhysicalConnection, error) {
		dialed = true
		return nil, nil
	}), func() (ports.BrokerDaemonIdentity, error) {
		return "", daemonidentity.ErrInvalid
	})
	require.NoError(t, err)
	observation, err := probe.ProbeLocal(context.Background())
	require.ErrorIs(t, err, daemonidentity.ErrInvalid)
	require.Equal(t, domain.RemoteAvailabilityUnreachable, observation.Availability)
	require.False(t, dialed)
}

func TestBrokerLocalProbeDialTargetIsExistingOnly(t *testing.T) {
	_, binding := testLocalSandboxConfig(t)
	var target ports.BrokerDialTarget
	probe, err := newLocalRouteProbe(binding, brokerLocalTestEpoch, func(_ context.Context, observed ports.BrokerDialTarget) (daemonmux.RawFramedTransport, error) {
		target = observed
		return nil, errors.New("unreachable")
	}, daemonmux.DefaultMuxCeilings())
	require.NoError(t, err)
	_, err = probe.ProbeLocal(context.Background())
	require.Error(t, err)
	require.Equal(t, ports.BrokerDaemonExistingOnly, target.StartMode,
		"an observation dial target never authorizes starting the daemon")
	require.True(t, target.Fence.Local)
	require.Equal(t, binding.Identity, target.ExpectedIdentity.Identity)
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
	_, err := newLocalRouteProbe(binding, brokerLocalTestEpoch, func(context.Context, ports.BrokerDialTarget) (daemonmux.RawFramedTransport, error) {
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

// startLocalRegistry runs one registry for the duration of a test.
func startLocalRegistry(t *testing.T, registry *broker.Registry) {
	t.Helper()
	registry.SetDemand(true)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); registry.Run(ctx) }()
	t.Cleanup(func() { cancel(); <-done; registry.SetDemand(false) })
}

// waitLocalEntry waits for the published local entry (always index zero) to
// satisfy predicate and returns it.
func waitLocalEntry(t *testing.T, registry *broker.Registry, predicate func(ports.BrokerDaemonObservation) bool) ports.BrokerDaemonObservation {
	t.Helper()
	sub := registry.Subscribe()
	defer sub.Close()
	deadline := time.After(brokerTestWait)
	for {
		snapshot := registry.Snapshot()
		if len(snapshot.Daemons) > 0 && snapshot.Daemons[0].Local && predicate(snapshot.Daemons[0]) {
			return snapshot.Daemons[0]
		}
		select {
		case <-sub.Changed():
		case <-deadline:
			t.Fatal("timed out waiting for the published local entry")
			return ports.BrokerDaemonObservation{}
		}
	}
}

// noDialHostProbe is a ports.BrokerHostProbe that never dials: a membership test
// supplies it only because the registry requires a remote probe to observe
// hosts, never to own their authority.
type noDialHostProbe struct{}

func (noDialHostProbe) Probe(context.Context, domain.RemoteRegistration) (ports.BrokerDaemonObservation, error) {
	return ports.BrokerDaemonObservation{}, errors.New("membership authority must not dial")
}

func unreachableHostProbe(t *testing.T) ports.BrokerHostProbe {
	t.Helper()
	return noDialHostProbe{}
}
