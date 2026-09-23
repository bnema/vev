package app

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/adapters/brokerconfig"
	"github.com/bnema/vev/internal/adapters/brokeripc"
	"github.com/bnema/vev/internal/adapters/daemonmux"
	"github.com/bnema/vev/internal/adapters/ipc"
	"github.com/bnema/vev/internal/adapters/lifecycle"
	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol"
)

const (
	brokerTestEndpoint    = "offline@daemon:2222"
	brokerTestIdentity    = "offline-daemon"
	brokerTestWait        = 5 * time.Second
	brokerTestPollTick    = 5 * time.Millisecond
	brokerTestIdleAdvance = time.Minute
)

// brokerTestPolicy is the exact policy provisioned and offered by the fixture.
func brokerTestPolicy() ports.BrokerPolicy {
	return ports.BrokerPolicy{
		ProtocolVersion:      protocol.Version,
		CatalogSchemaVersion: 3,
		EnvironmentPolicy:    protocol.EnvironmentPolicyClientOwned,
		Transport:            "unix-mux",
		Trust:                "offline-trust",
		Launch:               "offline-launch",
		Isolation:            "offline-isolation",
	}
}

// brokerTestRegistration is the exact registration provisioned by the sandbox
// config and carried by the request.
func brokerTestRegistration() domain.RemoteRegistration {
	var registration domain.RemoteRegistration
	registration.Endpoint = brokerTestEndpoint
	for i := range registration.Incarnation {
		registration.Incarnation[i] = byte(i + 1)
	}
	registration.Generation = 1
	return registration
}

// sandboxRegistrationDocument renders one registration document with the given
// route (a Unix path string or an explicit route object).
func sandboxRegistrationDocument(route any) map[string]any {
	policy := brokerTestPolicy()
	return map[string]any{
		"marker": brokerconfig.Marker,
		"registrations": []any{map[string]any{
			"endpoint":    brokerTestEndpoint,
			"incarnation": "0102030405060708090a0b0c0d0e0f10",
			"generation":  1,
			"identity":    brokerTestIdentity,
			"route":       route,
			"policy": map[string]any{
				"protocolVersion":      policy.ProtocolVersion,
				"catalogSchemaVersion": policy.CatalogSchemaVersion,
				"environmentPolicy":    "client-owned",
				"transport":            policy.Transport,
				"trust":                policy.Trust,
				"launch":               policy.Launch,
				"isolation":            policy.Isolation,
			},
		}},
	}
}

// writeSandboxConfig marshals one document into root/config.json owner-only.
func writeSandboxConfig(t *testing.T, root string, document any) {
	t.Helper()
	raw, err := json.Marshal(document)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(root, brokerconfig.ConfigFileName), raw, 0o600))
}

// isolateSandboxEnv redirects production XDG roots into private temporary
// directories and returns one fresh offline root beside them, so the sandbox
// can never touch the real runtime or state paths.
func isolateSandboxEnv(t *testing.T) (root, prodRuntime, prodState string) {
	t.Helper()
	prodRuntime = t.TempDir()
	prodState = t.TempDir()
	t.Setenv("VEV_ENV", "")
	t.Setenv("VEV_ENV_ROOT", "")
	t.Setenv("XDG_RUNTIME_DIR", prodRuntime)
	t.Setenv("XDG_STATE_HOME", prodState)
	root = filepath.Join(shortTempDir(t, "vevs"), "sandbox")
	return root, prodRuntime, prodState
}

// requireProductionUntouched asserts no production runtime or state entry was
// created.
func requireProductionUntouched(t *testing.T, prodRuntime, prodState string) {
	t.Helper()
	for _, dir := range []string{prodRuntime, prodState} {
		entries, err := os.ReadDir(dir)
		require.NoError(t, err)
		require.Empty(t, entries, "production directory %s was touched", dir)
	}
}

// testBrokerServeDeps builds sandbox deps with a deterministic clock and a
// discard logger, and reports readiness on the returned channel.
func testBrokerServeDeps(clk ports.Clock) (brokerServeDeps, <-chan string) {
	deps := defaultBrokerServeDeps()
	deps.clock = func() ports.Clock { return clk }
	deps.newLogger = func(string) (*slog.Logger, io.Closer, error) {
		return slog.New(slog.NewTextHandler(io.Discard, nil)), io.NopCloser(strings.NewReader("")), nil
	}
	ready := make(chan string, 1)
	deps.onReady = func(socketPath string) {
		select {
		case ready <- socketPath:
		default:
		}
	}
	return deps, ready
}

// runSandbox starts one sandbox serve goroutine and returns its result channel.
func runSandbox(ctx context.Context, options brokerServeOptions, deps brokerServeDeps) <-chan error {
	done := make(chan error, 1)
	go func() { done <- runBrokerServe(ctx, options, deps) }()
	return done
}

// awaitSandboxCancel cancels the sandbox and requires an orderly shutdown.
func awaitSandboxCancel(t *testing.T, cancel context.CancelFunc, done <-chan error) {
	t.Helper()
	cancel()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(brokerTestWait):
		t.Fatal("sandbox did not shut down after cancellation")
	}
}

// fatalListener is a ports.BrokerListener whose Accept always reports one
// fixed terminal error, so a test can drive the accept drain without a carriage.
type fatalListener struct{ err error }

func (l fatalListener) Accept() (ports.BrokerService, error) { return nil, l.err }
func (l fatalListener) Close() error                         { return nil }
func (l fatalListener) Addr() string                         { return "fatal" }

// TestDrainBrokerAcceptClassifiesTerminalFailure proves the drain treats a
// listener close as an orderly end with no log and no failure signal, while any
// other terminal accept failure is logged and reported on the failure channel so
// the caller can commit shutdown.
func TestDrainBrokerAcceptClassifiesTerminalFailure(t *testing.T) {
	tests := []struct {
		name       string
		err        error
		wantLogged bool
		wantFailed bool
	}{
		{name: "listener close is orderly and quiet", err: brokeripc.ErrListenerClosed},
		{name: "unexpected terminal failure is logged and signaled", err: errors.New("carriage accept failed"), wantLogged: true, wantFailed: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buffer bytes.Buffer
			log := slog.New(slog.NewTextHandler(&buffer, nil))
			started := make(chan struct{})
			drained := make(chan struct{})
			failed := make(chan struct{})
			go drainBrokerAccept(fatalListener{err: tt.err}, log, started, drained, failed)

			select {
			case <-started:
			case <-time.After(brokerTestWait):
				t.Fatal("the drain never started")
			}
			select {
			case <-drained:
			case <-time.After(brokerTestWait):
				t.Fatal("the drain did not stop on a terminal accept failure")
			}

			select {
			case <-failed:
				require.True(t, tt.wantFailed, "an orderly listener close must not signal failure")
			default:
				require.False(t, tt.wantFailed, "an unexpected terminal failure must signal shutdown")
			}

			if tt.wantLogged {
				require.Contains(t, buffer.String(), "broker_offline_accept_failed")
				return
			}
			require.Empty(t, strings.TrimSpace(buffer.String()), "an orderly listener close must not be logged")
		})
	}
}

func TestProductionBrokerServeRejectsUnsafeRoot(t *testing.T) {
	layout := emptyProductionBrokerLayout(t, "")
	require.NoError(t, os.MkdirAll(layout.Root, 0o755))
	deps, _ := testBrokerServeDeps(newSandboxClock())
	err := runBrokerServe(context.Background(), brokerServeOptions{production: true}, deps)
	require.ErrorContains(t, err, "0755")
	require.NoFileExists(t, filepath.Join(layout.Runtime, "lifecycle.lock"))
}

func TestBrokerServeRoundTrip(t *testing.T) {
	emptyProductionBrokerLayout(t, "")
	policy := brokerTestPolicy()
	fixture := newBrokerMuxFixture(t, policy)
	writeProductionBrokerDocument(t, sandboxRegistrationDocument(fixture.route))

	clk := newSandboxClock()
	deps, ready := testBrokerServeDeps(clk)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := runSandbox(ctx, brokerServeOptions{production: true}, deps)

	socketPath := awaitSandboxReady(t, ready, done)

	clientCtx, clientCancel := context.WithTimeout(context.Background(), brokerTestWait)
	defer clientCancel()
	service, err := brokeripc.Dial(clientCtx, socketPath, brokeripc.Config{})
	require.NoError(t, err)
	defer func() { _ = service.Close() }()

	streamID, err := service.NextStreamID()
	require.NoError(t, err)
	stream, err := service.OpenStream(clientCtx, ports.BrokerOpenStreamRequest{
		Purpose:      ports.BrokerStreamControl,
		Stream:       streamID,
		Endpoint:     brokerTestEndpoint,
		Registration: brokerTestRegistration(),
		Policy:       policy,
		StartMode:    ports.BrokerDaemonStartIfNeeded,
	})
	require.NoError(t, err)
	defer func() { _ = stream.Close() }()

	type pingResult struct {
		message protocol.ServerMessage
		err     error
	}
	result := make(chan pingResult, 1)
	go func() {
		if sendErr := stream.SendClient(protocol.Ping{}); sendErr != nil {
			result <- pingResult{err: sendErr}
			return
		}
		message, recvErr := stream.ReceiveServer()
		result <- pingResult{message: message, err: recvErr}
	}()
	select {
	case outcome := <-result:
		require.NoError(t, outcome.err)
		require.Equal(t, protocol.Pong{}, outcome.message)
	case <-time.After(brokerTestWait):
		t.Fatal("typed IPC -> pool -> mux round trip timed out")
	}
	require.Positive(t, fixture.streams.Load())

	awaitSandboxCancel(t, cancel, done)
	requireSocketRemoved(t, socketPath)
}

// writeProductionBrokerDocument replaces only the private per-test bootstrap.
func writeProductionBrokerDocument(t *testing.T, document any) {
	t.Helper()
	raw, err := json.Marshal(document)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(productionBrokerConfigPath(), raw, 0o600))
}

// awaitSandboxReady waits for the sandbox readiness hook and returns the
// reported socket path.
func awaitSandboxReady(t *testing.T, ready <-chan string, done <-chan error) string {
	t.Helper()
	select {
	case socketPath := <-ready:
		require.NotEmpty(t, socketPath)
		return socketPath
	case err := <-done:
		t.Fatalf("sandbox exited before readiness: %v", err)
		return ""
	case <-time.After(brokerTestWait):
		t.Fatal("sandbox never reported readiness and did not exit")
		return ""
	}
}

// requireSocketRemoved asserts the sandbox socket inode was unlinked.
func requireSocketRemoved(t *testing.T, socketPath string) {
	t.Helper()
	_, err := os.Lstat(socketPath)
	require.ErrorIs(t, err, os.ErrNotExist)
}

// waitForSandboxShutdown advances the manual clock until the sandbox has shut
// down, so a real idle grace is never awaited.
func waitForSandboxShutdown(t *testing.T, clk *sandboxClock, done <-chan error) {
	t.Helper()
	deadline := time.After(brokerTestWait)
	for {
		select {
		case err := <-done:
			require.NoError(t, err)
			return
		case <-time.After(brokerTestPollTick):
			clk.Advance(brokerTestIdleAdvance)
		case <-deadline:
			t.Fatal("idle sandbox did not shut down")
		}
	}
}

func TestBrokerServeIdleShutdownOnEmptyConfig(t *testing.T) {
	layout := emptyProductionBrokerLayout(t, "")
	clk := newSandboxClock()

	deps, ready := testBrokerServeDeps(clk)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := runSandbox(ctx, brokerServeOptions{production: true}, deps)

	socketPath := awaitSandboxReady(t, ready, done)
	waitForSandboxShutdown(t, clk, done)

	requireSocketRemoved(t, socketPath)
	requireOwnerLockReleased(t, layout.Runtime)

}

// requireOwnerLockReleased proves the lifetime lock is no longer held.
func requireOwnerLockReleased(t *testing.T, runtimeDir string) {
	t.Helper()
	owner, err := lifecycle.TryAcquire(runtimeDir)
	require.NoError(t, err)
	require.NoError(t, owner.Release())
}

func TestBrokerServeCancelCleanup(t *testing.T) {
	layout := emptyProductionBrokerLayout(t, "")
	clk := newSandboxClock()

	deps, ready := testBrokerServeDeps(clk)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := runSandbox(ctx, brokerServeOptions{production: true}, deps)

	socketPath := awaitSandboxReady(t, ready, done)
	awaitSandboxCancel(t, cancel, done)

	requireSocketRemoved(t, socketPath)
	requireOwnerLockReleased(t, layout.Runtime)
	_, err := os.Lstat(filepath.Join(layout.State, "state.json"))
	require.NoError(t, err, "durable broker state must remain after shutdown")

}

// wedgedListener is a ports.BrokerListener whose Accept always reports one
// unexpected terminal failure and whose Close records that the sandbox tore it
// down, so a test can prove an accept failure does not leave a bound broker.
type wedgedListener struct {
	err    error
	addr   string
	closed atomic.Bool
}

func (l *wedgedListener) Accept() (ports.BrokerService, error) { return nil, l.err }
func (l *wedgedListener) Close() error {
	l.closed.Store(true)
	return nil
}
func (l *wedgedListener) Addr() string { return l.addr }

// TestBrokerServeShutsDownOnUnexpectedAcceptFailure proves an unexpected
// terminal accept failure commits shutdown and drains the listener, instead of
// leaving a bound broker whose socket can no longer admit work.
func TestBrokerServeShutsDownOnUnexpectedAcceptFailure(t *testing.T) {
	layout := emptyProductionBrokerLayout(t, "")
	clk := newSandboxClock()

	listener := &wedgedListener{err: errors.New("carriage accept failed"), addr: "wedged"}
	deps, ready := testBrokerServeDeps(clk)
	deps.listen = func(string, ports.BrokerEpoch, ports.BrokerAuthority, brokeripc.Config) (ports.BrokerListener, error) {
		return listener, nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := runSandbox(ctx, brokerServeOptions{production: true}, deps)

	awaitSandboxReady(t, ready, done)
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(brokerTestWait):
		t.Fatal("the broker wedged on an unexpected accept failure")
	}
	require.True(t, listener.closed.Load(), "shutdown must close the wedged listener")
	requireOwnerLockReleased(t, layout.Runtime)

}

func TestBrokerServeRefusesDuplicateOwnership(t *testing.T) {
	emptyProductionBrokerLayout(t, "")
	clk := newSandboxClock()

	deps, ready := testBrokerServeDeps(clk)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := runSandbox(ctx, brokerServeOptions{production: true}, deps)
	socketPath := awaitSandboxReady(t, ready, done)

	// A second foreground owner fails fast on the lifetime lock.
	secondDeps, _ := testBrokerServeDeps(newSandboxClock())
	err := runBrokerServe(context.Background(), brokerServeOptions{production: true}, secondDeps)
	require.ErrorIs(t, err, lifecycle.ErrBusy)

	// A duplicate listener on the live socket is refused as well.
	duplicate, err := ipc.ListenMux(socketPath)
	if duplicate != nil {
		_ = duplicate.Close()
	}
	require.ErrorIs(t, err, ipc.ErrDaemonRunning)

	awaitSandboxCancel(t, cancel, done)
}

// serveTestPongs answers every typed Ping of one accepted daemonmux stream with
// exactly one Pong until the stream ends.
func serveTestPongs(connection ports.ServerConnection) {
	for {
		message, err := connection.ReceiveClient()
		if err != nil {
			return
		}
		if _, ok := message.(protocol.Ping); !ok {
			continue
		}
		if err := connection.SendServer(protocol.Pong{}); err != nil {
			return
		}
	}
}

// brokerMuxFixture is a real Unix daemonmux server: one private mux carriage
// adopted by a supervisor into an aggregate listener that answers typed Pings.
// streams counts admitted typed logical streams so a test can prove the
// request actually reached the mux fixture.
type brokerMuxFixture struct {
	route   string
	streams atomic.Int64
}

// newBrokerMuxFixture binds one real private Unix mux carriage and serves typed
// Pings on every admitted logical stream.
func newBrokerMuxFixture(t *testing.T, policy ports.BrokerPolicy) *brokerMuxFixture {
	t.Helper()
	routeDir := t.TempDir()
	require.NoError(t, os.Chmod(routeDir, 0o700))
	route := filepath.Join(routeDir, "mux.sock")
	listener, err := ipc.ListenMux(route)
	require.NoError(t, err)

	aggregate := daemonmux.NewAggregateListener()
	var incarnation ports.BrokerDaemonIncarnation
	for i := range incarnation {
		incarnation[i] = byte(0x10 + i)
	}
	binding, err := daemonmux.NewServerBindings(ports.BrokerDaemonIdentity(brokerTestIdentity), incarnation, []daemonmux.ServerPolicyAdmission{{Policy: policy, Origin: ports.SessionOriginRemote}})
	require.NoError(t, err)
	supervisor, err := daemonmux.NewServerSupervisor(aggregate, binding, daemonmux.DefaultMuxCeilings(), 8)
	require.NoError(t, err)

	fixture := &brokerMuxFixture{route: route}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(func() {
		cancel()
		_ = supervisor.Close()
		_ = aggregate.Close()
		_ = listener.Close()
	})

	go func() {
		for {
			raw, acceptErr := listener.Accept()
			if acceptErr != nil {
				return
			}
			go func() { _ = supervisor.Adopt(ctx, raw) }()
		}
	}()
	go func() {
		for {
			connection, acceptErr := aggregate.Accept()
			if acceptErr != nil {
				return
			}
			fixture.streams.Add(1)
			go serveTestPongs(connection)
		}
	}()
	return fixture
}

// sandboxClock is a deterministic ports.Clock/ports.Timer pair: time only moves
// when Advance is called, so idle shutdown is exercised without wall time.
type sandboxClock struct {
	mu     sync.Mutex
	now    time.Time
	order  uint64
	timers map[*sandboxTimer]struct{}
}

func newSandboxClock() *sandboxClock {
	return &sandboxClock{now: time.Unix(0, 0), timers: make(map[*sandboxTimer]struct{})}
}

func (c *sandboxClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *sandboxClock) NewTimer(d time.Duration) ports.Timer {
	timer := &sandboxTimer{clock: c, ch: make(chan time.Time, 1)}
	c.mu.Lock()
	defer c.mu.Unlock()
	timer.arm(d)
	return timer
}

// Advance moves time forward and fires every timer that came due.
func (c *sandboxClock) Advance(d time.Duration) {
	if d < 0 {
		panic("sandboxClock.Advance requires a non-negative duration")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
	due := make([]*sandboxTimer, 0, len(c.timers))
	for timer := range c.timers {
		if !timer.when.After(c.now) {
			due = append(due, timer)
		}
	}
	sort.Slice(due, func(i, j int) bool {
		if due[i].when.Equal(due[j].when) {
			return due[i].order < due[j].order
		}
		return due[i].when.Before(due[j].when)
	})
	for _, timer := range due {
		timer.active = false
		delete(c.timers, timer)
		select {
		case timer.ch <- c.now:
		default:
		}
	}
}

type sandboxTimer struct {
	clock  *sandboxClock
	ch     chan time.Time
	when   time.Time
	order  uint64
	active bool
}

func (t *sandboxTimer) C() <-chan time.Time { return t.ch }

func (t *sandboxTimer) Stop() bool {
	t.clock.mu.Lock()
	defer t.clock.mu.Unlock()
	if !t.active {
		return false
	}
	t.active = false
	delete(t.clock.timers, t)
	return true
}

func (t *sandboxTimer) Reset(d time.Duration) bool {
	t.clock.mu.Lock()
	defer t.clock.mu.Unlock()
	wasActive := t.active
	if t.active {
		t.active = false
		delete(t.clock.timers, t)
	}
	select {
	case <-t.ch:
	default:
	}
	t.arm(d)
	return wasActive
}

func (t *sandboxTimer) arm(d time.Duration) {
	t.order = t.clock.order
	t.clock.order++
	t.when = t.clock.now.Add(d)
	t.active = true
	t.clock.timers[t] = struct{}{}
}

func TestSandboxClockFiresOnAdvance(t *testing.T) {
	clk := newSandboxClock()
	timer := clk.NewTimer(time.Minute)
	require.True(t, timer.Stop())
	timer = clk.NewTimer(time.Minute)
	clk.Advance(time.Minute)
	select {
	case <-timer.C():
	case <-time.After(brokerTestWait):
		t.Fatal("manual timer did not fire")
	}
}
