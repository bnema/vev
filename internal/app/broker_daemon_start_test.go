package app

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/adapters/brokerconfig"
	"github.com/bnema/vev/internal/adapters/daemonmux"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol/wire"
)

// Broker-owned daemon start coverage (Plan 001 P7, UI-driver slice 1).
//
// These tests drive the real start decision — the closed start authorization,
// the launch policy, the shared election, and the carriage dial — through the
// scripted starter seams. Nothing here re-executes a daemon, binds a socket, or
// reads a session: every assertion is about whether a spawn was elected and
// which carriage was dialed, which is exactly the contract the broker owns.

// stubRawCarriage is one inert raw framed carriage: enough to satisfy the dial
// seam without a socket, a peer, or a goroutine.
type stubRawCarriage struct{ closed atomic.Bool }

func (c *stubRawCarriage) Send(wire.Envelope) error { return nil }

func (c *stubRawCarriage) RecvBounded(uint64) (wire.Envelope, error) { return wire.Envelope{}, io.EOF }

func (c *stubRawCarriage) Close() error {
	c.closed.Store(true)
	return nil
}

// scriptedDaemonStarter is a goroutine-safe recording starter: it counts dials
// and spawns, keeps the exact target each dial received, and fails every dial
// until the scripted spawn (or the test) makes the carriage available.
type scriptedDaemonStarter struct {
	mu       sync.Mutex
	carriage string
	// published is the carriage the scripted daemon advertises as the endpoint
	// this process can start. Empty means the same carriage it answers at.
	published string
	dialed    []ports.BrokerDialTarget
	dials     int
	spawns    int
	// running reports that the daemon behind the carriage is published, as the
	// elected spawner's daemon is once it binds its endpoint.
	running bool
	// available reports whether the carriage answers. A nil func means "answer
	// only once a spawn happened".
	available func() bool
	// dialErr overrides the failure a dial reports while unavailable.
	dialErr error
	// spawnErr fails the elected spawn.
	spawnErr error
	// daemonOwner is the lifecycle ownership the spawned daemon holds, exactly
	// as the real daemon does for its whole run. It is what makes a concurrent
	// second caller wait instead of electing itself.
	daemonOwner lifecycleOwnership
}

func (s *scriptedDaemonStarter) starter(backoff backoffConfig) brokerDaemonStarter {
	return brokerDaemonStarter{
		dial:    s.dial,
		spawn:   s.spawn,
		backoff: backoff,
		// The scripted daemon publishes exactly the carriage the test declared,
		// so the start path sees one reachable daemon endpoint like production.
		published: func() string { return s.publishedCarriage() },
	}
}

func (s *scriptedDaemonStarter) dial(_ context.Context, carriage string) (daemonmux.RawFramedTransport, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.dials++
	s.dialed = append(s.dialed, ports.BrokerDialTarget{Address: carriage})
	if s.carriage != carriage {
		return nil, os.ErrNotExist
	}
	if s.available == nil {
		if s.spawns == 0 && !s.running {
			return nil, os.ErrNotExist
		}
	} else if !s.available() {
		if s.dialErr != nil {
			return nil, s.dialErr
		}
		return nil, os.ErrNotExist
	}
	return &stubRawCarriage{}, nil
}

func (s *scriptedDaemonStarter) spawn() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.spawns++
	if s.spawnErr != nil {
		return s.spawnErr
	}
	// A real spawned daemon immediately takes lifecycle ownership of the
	// runtime directory and holds it for its whole run; a concurrent caller
	// therefore observes ErrBusy and waits for the carriage rather than
	// electing a second spawner.
	lockDir := filepath.Dir(s.publishedCarriageLocked())
	if owner, err := daemonLifecycleProbe.TryAcquire(lockDir); err == nil {
		s.daemonOwner = owner
	}
	return nil
}

// markRunning publishes the daemon the elected spawner started, so the callers
// waiting on the held election converge on its carriage.
func (s *scriptedDaemonStarter) markRunning() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.running = true
}

// isRunning reports whether the daemon is published.
func (s *scriptedDaemonStarter) isRunning() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.running
}

// publishedCarriageLocked reports the published carriage while the mutex is
// already held.
func (s *scriptedDaemonStarter) publishedCarriageLocked() string {
	if s.published != "" {
		return s.published
	}
	return s.carriage
}

// publishedCarriage reports the carriage the scripted daemon publishes as the
// endpoint this process can start.
func (s *scriptedDaemonStarter) publishedCarriage() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.publishedCarriageLocked()
}

func (s *scriptedDaemonStarter) counters() (dials, spawns int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.dials, s.spawns
}

// fastDaemonStartBackoff bounds the election the way an unresponsive daemon
// would, without the production five-second budget.
var fastDaemonStartBackoff = backoffConfig{initial: time.Millisecond, max: 2 * time.Millisecond, total: 150 * time.Millisecond}

// withDaemonStarter swaps the package start seam for one test and restores it.
// The start seam is process state, so no caller of this helper runs in parallel.
func withDaemonStarter(t *testing.T, starter brokerDaemonStarter) {
	t.Helper()
	restore := localDaemonStarter
	localDaemonStarter = starter
	t.Cleanup(func() { localDaemonStarter = restore })
}

// testDaemonCarriage returns a fresh owner-only daemon runtime directory and the
// exact private carriage a daemon bound to it publishes. It is the carriage the
// scripted starter declares, so the start path sees one reachable endpoint.
func testDaemonCarriage(t *testing.T) (dir, carriage string) {
	t.Helper()
	dir = t.TempDir()
	// The lifecycle lock refuses a directory with group or other access, and
	// t.TempDir() is 0755; the daemon runtime directory is owner-only.
	require.NoError(t, os.Chmod(dir, 0o700))
	return dir, daemonmux.SocketPath(dir)
}

// startableLocalTarget builds the resolved local target the broker publishes for
// its own machine daemon under the supplied start authorization. The policy is
// the production local daemon authority, which explicitly names the launch
// token; a test that needs a non-launching policy overrides it. The address is
// the bounded token the resolver would publish for the provisioned carriage.
func startableLocalTarget(t *testing.T, carriage string, mode ports.BrokerDaemonStartMode) ports.BrokerDialTarget {
	t.Helper()
	_, binding := testLocalSandboxConfigForCarriage(t, carriage)
	target := ports.BrokerDialTarget{
		Fence:            ports.BrokerEndpointFence{Local: true},
		Address:          binding.Route.Address(),
		Policy:           localDaemonPolicy(),
		StartMode:        mode,
		ExpectedIdentity: ports.BrokerExpectedIdentity{Identity: brokerLocalTestIdentity, Bound: true},
	}
	require.NoError(t, target.Validate())
	return target
}

// TestDaemonStartExistingOnlyNeverSpawns proves an existing-only acquisition is
// exactly one carriage dial: no lifecycle probe, no spawn lock, no process, and
// no fallback of any kind, even though the carriage is absent.
func TestDaemonStartExistingOnlyNeverSpawns(t *testing.T) {
	isolateSandboxEnv(t)
	dir, carriage := testDaemonCarriage(t)
	scripted := &scriptedDaemonStarter{carriage: carriage}
	withDaemonStarter(t, scripted.starter(fastDaemonStartBackoff))

	target := startableLocalTarget(t, carriage, ports.BrokerDaemonExistingOnly)
	raw, err := dialBrokerLocalDaemon(context.Background(), carriage, target)
	require.ErrorIs(t, err, os.ErrNotExist)
	var brokerErr ports.BrokerError
	require.ErrorAs(t, err, &brokerErr)
	require.Equal(t, ports.BrokerErrorNoDaemon, brokerErr.Code)
	require.Nil(t, raw)

	dials, spawns := scripted.counters()
	require.Equal(t, 1, dials, "existing-only is exactly one carriage dial")
	require.Zero(t, spawns, "existing-only must never spawn")
	_, statErr := os.Stat(filepath.Join(dir, spawnLockName))
	require.ErrorIs(t, statErr, os.ErrNotExist, "existing-only must not take the spawn lock")
	_, probeErr := os.Stat(filepath.Join(dir, "lifecycle.lock"))
	require.ErrorIs(t, probeErr, os.ErrNotExist, "existing-only must not probe lifecycle ownership")
}

// TestDaemonStartExistingOnlyReturnsRunningDaemonWithoutProbing proves a
// reachable carriage is the whole result: the running daemon is never restarted
// and no election is entered.
func TestDaemonStartExistingOnlyReturnsRunningDaemonWithoutProbing(t *testing.T) {
	isolateSandboxEnv(t)
	dir, carriage := testDaemonCarriage(t)
	scripted := &scriptedDaemonStarter{carriage: carriage, available: func() bool { return true }}
	withDaemonStarter(t, scripted.starter(fastDaemonStartBackoff))

	target := startableLocalTarget(t, carriage, ports.BrokerDaemonExistingOnly)
	raw, err := dialBrokerLocalDaemon(context.Background(), carriage, target)
	require.NoError(t, err)
	require.NotNil(t, raw)

	// A start-if-needed acquisition against the same running daemon also
	// returns it unchanged: the mode authorizes a start, it never demands one.
	startIfNeeded := startableLocalTarget(t, carriage, ports.BrokerDaemonStartIfNeeded)
	rawStartIfNeeded, err := dialBrokerLocalDaemon(context.Background(), carriage, startIfNeeded)
	require.NoError(t, err)
	require.NotNil(t, rawStartIfNeeded)

	dials, spawns := scripted.counters()
	require.Equal(t, 2, dials)
	require.Zero(t, spawns, "a running daemon is never restarted")
	_, probeErr := os.Stat(filepath.Join(dir, "lifecycle.lock"))
	require.ErrorIs(t, probeErr, os.ErrNotExist, "a reachable carriage never enters the election")
}

// TestDaemonStartIfNeededSpawnsExactlyOnce proves the authorized start path
// elects one spawner, then returns the carriage the spawned daemon published.
func TestDaemonStartIfNeededSpawnsExactlyOnce(t *testing.T) {
	isolateSandboxEnv(t)
	dir, carriage := testDaemonCarriage(t)
	scripted := &scriptedDaemonStarter{carriage: carriage}
	withDaemonStarter(t, scripted.starter(fastDaemonStartBackoff))

	target := startableLocalTarget(t, carriage, ports.BrokerDaemonStartIfNeeded)
	raw, err := dialBrokerLocalDaemon(context.Background(), carriage, target)
	require.NoError(t, err)
	require.NotNil(t, raw)

	dials, spawns := scripted.counters()
	require.GreaterOrEqual(t, dials, 2, "the start path dials before and after the spawn")
	require.Equal(t, 1, spawns)
	// The election released the spawn lock.
	require.NoFileExists(t, filepath.Join(dir, spawnLockName))
}

// TestDaemonStartRefusesPolicyWithoutLaunchAuthority proves the closed start
// authorization can only narrow the resolved policy: a policy that does not name
// the broker launch authority refuses the start outright, and nothing spawns.
func TestDaemonStartRefusesPolicyWithoutLaunchAuthority(t *testing.T) {
	isolateSandboxEnv(t)
	dir, carriage := testDaemonCarriage(t)
	scripted := &scriptedDaemonStarter{carriage: carriage}
	withDaemonStarter(t, scripted.starter(fastDaemonStartBackoff))

	target := startableLocalTarget(t, carriage, ports.BrokerDaemonStartIfNeeded)
	target.Policy.Launch = "sandbox-private"
	require.NoError(t, target.Validate())

	raw, err := dialBrokerLocalDaemon(context.Background(), carriage, target)
	require.Error(t, err)
	require.Nil(t, raw)
	var typed ports.BrokerError
	require.ErrorAs(t, err, &typed)
	require.Equal(t, ports.BrokerErrorConflictingPolicy, typed.Code)

	dials, spawns := scripted.counters()
	require.Equal(t, 1, dials, "the carriage is dialed once before the refusal")
	require.Zero(t, spawns, "a non-launching policy must never spawn")
	require.NoFileExists(t, filepath.Join(dir, spawnLockName))
}

// TestDaemonStartRefusesRegisteredEndpoint proves the local start mechanics
// refuse a registered endpoint even on a local carriage: the broker owns the
// start authority of its own machine daemon only.
func TestDaemonStartRefusesRegisteredEndpoint(t *testing.T) {
	isolateSandboxEnv(t)
	_, carriage := testDaemonCarriage(t)
	scripted := &scriptedDaemonStarter{carriage: carriage}
	withDaemonStarter(t, scripted.starter(fastDaemonStartBackoff))

	target := startableLocalTarget(t, carriage, ports.BrokerDaemonStartIfNeeded)
	target.Fence = ports.BrokerEndpointFence{Registration: brokerTestRegistration()}
	require.NoError(t, target.Validate())

	_, err := dialBrokerLocalDaemon(context.Background(), carriage, target)
	require.Error(t, err)
	dials, spawns := scripted.counters()
	require.Zero(t, dials, "a registered endpoint is refused before any dial")
	require.Zero(t, spawns)
}

// TestDaemonStartRefusesCarriageOutsideTheProvisionedRoute proves a carriage that
// is not an absolute cleaned private mux path is refused before any dial, so a
// hand-built target can never make the launcher start a daemon for a foreign
// endpoint.
func TestDaemonStartRefusesCarriageOutsideTheProvisionedRoute(t *testing.T) {
	isolateSandboxEnv(t)
	_, carriage := testDaemonCarriage(t)
	scripted := &scriptedDaemonStarter{carriage: carriage}
	withDaemonStarter(t, scripted.starter(fastDaemonStartBackoff))

	target := startableLocalTarget(t, carriage, ports.BrokerDaemonStartIfNeeded)
	for _, invalid := range []string{"", "relative/daemonmux.sock", carriage + "/.."} {
		_, err := dialBrokerLocalDaemon(context.Background(), invalid, target)
		require.Error(t, err)
		var typed ports.BrokerError
		require.ErrorAs(t, err, &typed)
		require.Equal(t, ports.BrokerErrorIncompatible, typed.Code)
	}
	dials, spawns := scripted.counters()
	require.Zero(t, dials)
	require.Zero(t, spawns)
}

// TestDaemonStartRefusesForeignCarriageStart proves a start is refused for a
// carriage this process cannot publish: the launcher re-executes this binary,
// which binds its own private endpoint, so starting a daemon for a foreign
// carriage would spawn a process the caller could never reach.
func TestDaemonStartRefusesForeignCarriageStart(t *testing.T) {
	isolateSandboxEnv(t)
	_, own := testDaemonCarriage(t)
	foreignDir := t.TempDir()
	require.NoError(t, os.Chmod(foreignDir, 0o700))
	foreign := filepath.Join(foreignDir, "daemonmux.sock")

	// The scripted daemon publishes and answers at the process' own carriage;
	// the dial target names a different one.
	scripted := &scriptedDaemonStarter{carriage: own, published: own}
	withDaemonStarter(t, scripted.starter(fastDaemonStartBackoff))

	target := startableLocalTarget(t, foreign, ports.BrokerDaemonStartIfNeeded)
	_, err := dialBrokerDaemonCarriage(context.Background(), foreign, target)
	require.Error(t, err)
	var typed ports.BrokerError
	require.ErrorAs(t, err, &typed)
	require.Equal(t, ports.BrokerErrorIncompatible, typed.Code)

	dials, spawns := scripted.counters()
	require.Equal(t, 1, dials, "the carriage is dialed once before the refusal")
	require.Zero(t, spawns, "a foreign carriage must never be started")
	require.NoFileExists(t, filepath.Join(filepath.Dir(own), spawnLockName))
}

// TestDaemonStartDoesNotSpawnOnNonAbsenceFailure proves only proven absence can
// authorize a start: a permission, path, peer, or cancellation failure is
// reported unchanged and never repaired by spawning a second daemon.
func TestDaemonStartDoesNotSpawnOnNonAbsenceFailure(t *testing.T) {
	tests := []struct {
		name string
		err  error
	}{
		{name: "permission", err: os.ErrPermission},
		{name: "context canceled", err: context.Canceled},
		{name: "invalid path", err: os.ErrInvalid},
		{name: "unclassified", err: errors.New("carriage refused")},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			isolateSandboxEnv(t)
			dir, carriage := testDaemonCarriage(t)
			scripted := &scriptedDaemonStarter{
				carriage:  carriage,
				available: func() bool { return false },
				dialErr:   tt.err,
			}
			withDaemonStarter(t, scripted.starter(fastDaemonStartBackoff))

			target := startableLocalTarget(t, carriage, ports.BrokerDaemonStartIfNeeded)
			_, err := dialBrokerLocalDaemon(context.Background(), carriage, target)
			require.ErrorIs(t, err, tt.err)

			dials, spawns := scripted.counters()
			require.Equal(t, 1, dials)
			require.Zero(t, spawns)
			require.NoFileExists(t, filepath.Join(dir, spawnLockName))
		})
	}
}

// TestDaemonStartConcurrentRequestsShareOneElection proves N concurrent
// start-if-needed requests never elect a second spawner while one election is
// already in progress: the spawn lock is held by the elected spawner, so every
// other caller waits for the carriage it publishes instead of spawning again.
//
// The spawn lock is taken here exactly as another client's elected spawner
// would take it, and the carriage becomes dialable once that "spawner" finishes.
// The starter therefore records zero spawns of its own, every caller converges
// on the published carriage, and no caller ever holds the lock itself.
func TestDaemonStartConcurrentRequestsShareOneElection(t *testing.T) {
	isolateSandboxEnv(t)
	dir, carriage := testDaemonCarriage(t)
	scripted := &scriptedDaemonStarter{carriage: carriage}
	withDaemonStarter(t, scripted.starter(fastDaemonStartBackoff))

	// Another client's elected spawner holds the lock; the daemon it starts
	// publishes the carriage shortly afterwards.
	release, acquired, err := acquireSpawnLock(dir)
	require.NoError(t, err)
	require.True(t, acquired, "the test must hold the spawn lock")
	t.Cleanup(release)

	// The target is resolved once: require.* is not goroutine-safe, and the
	// resolved target is immutable and shared by every caller.
	target := startableLocalTarget(t, carriage, ports.BrokerDaemonStartIfNeeded)

	const callers = 8
	var wg sync.WaitGroup
	results := make([]daemonmux.RawFramedTransport, callers)
	errs := make([]error, callers)
	start := make(chan struct{})
	for i := range callers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			results[i], errs[i] = dialBrokerLocalDaemon(context.Background(), carriage, target)
		}()
	}
	// Publish the daemon the held election is starting, as its spawner does.
	go func() {
		<-start
		scripted.markRunning()
	}()
	close(start)
	// The held election ends once the carriage answers.
	require.Eventually(t, func() bool { return scripted.isRunning() }, brokerTestWait, time.Millisecond)
	release()
	wg.Wait()

	for i := range callers {
		require.NoError(t, errs[i], "caller %d", i)
		require.NotNil(t, results[i], "caller %d", i)
	}
	_, spawns := scripted.counters()
	require.Zero(t, spawns, "a caller must never spawn while another election holds the lock")
}

// TestDaemonStartConcurrentExistingOnlyNeverSpawns proves concurrent
// existing-only acquisitions never share a start: each one dials, fails on the
// same absent carriage, and spawns nothing.
func TestDaemonStartConcurrentExistingOnlyNeverSpawns(t *testing.T) {
	isolateSandboxEnv(t)
	_, carriage := testDaemonCarriage(t)
	scripted := &scriptedDaemonStarter{carriage: carriage}
	withDaemonStarter(t, scripted.starter(fastDaemonStartBackoff))

	target := startableLocalTarget(t, carriage, ports.BrokerDaemonExistingOnly)
	const callers = 8
	var wg sync.WaitGroup
	start := make(chan struct{})
	for range callers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			_, err := dialBrokerLocalDaemon(context.Background(), carriage, target)
			require.ErrorIs(t, err, os.ErrNotExist)
		}()
	}
	close(start)
	wg.Wait()

	dials, spawns := scripted.counters()
	require.Equal(t, callers, dials)
	require.Zero(t, spawns)
}

// TestDaemonStartPropagatesTheWholeTarget proves the resolved target reaches the
// carriage owner whole: address, fence, policy, start authorization, and expected
// identity all travel together, so a dial can never act on an authorization the
// connector did not validate.
func TestDaemonStartPropagatesTheWholeTarget(t *testing.T) {
	isolateSandboxEnv(t)
	_, carriage := testDaemonCarriage(t)

	var seen []ports.BrokerDialTarget
	var mu sync.Mutex
	connectorDial := func(ctx context.Context, target ports.BrokerDialTarget) (daemonmux.RawFramedTransport, error) {
		mu.Lock()
		seen = append(seen, target)
		mu.Unlock()
		return nil, os.ErrNotExist
	}
	connector, err := daemonmux.NewEndpointConnector(daemonmux.RawCarrierDialer(connectorDial), daemonmux.DefaultMuxCeilings())
	require.NoError(t, err)

	// The pool-side connector already owns the whole target; the local start
	// helper is what turns it into the carriage decision.
	scripted := &scriptedDaemonStarter{carriage: carriage}
	withDaemonStarter(t, scripted.starter(fastDaemonStartBackoff))

	target := startableLocalTarget(t, carriage, ports.BrokerDaemonExistingOnly)
	raw, err := dialBrokerLocalDaemon(context.Background(), carriage, target)
	require.Error(t, err)
	require.Nil(t, raw)

	// The exact same target value is what a connector hands to its dial
	// function, so the local helper observes the identical authorization.
	_, err = connector.Connect(context.Background(), target)
	require.Error(t, err)
	mu.Lock()
	require.Equal(t, []ports.BrokerDialTarget{target}, seen, "the connector dials the whole resolved target")
	mu.Unlock()
	require.Equal(t, ports.BrokerDaemonExistingOnly, seen[0].StartMode)
	require.True(t, seen[0].Fence.Local)
	require.Equal(t, target.Policy, seen[0].Policy)
	require.Equal(t, target.ExpectedIdentity, seen[0].ExpectedIdentity)
}

// TestDaemonStartNeverFallsBackToASessionDial proves structurally that the
// start path owns no session carriage: it never dials the daemon's public
// session socket and never wraps a session transport.
func TestDaemonStartNeverFallsBackToASessionDial(t *testing.T) {
	sources := loadAppSources(t, false)
	body, ok := sources["broker_daemon_start.go"]
	require.True(t, ok, "the daemon start owner must exist")

	for _, forbidden := range []string{
		"ipc.DialContext(",
		"ipc.SocketPath(",
		"sessionwire.",
		"localDaemonDialer",
		"ensureDaemonWithLifecycle",
		"ensureDaemon(",
	} {
		require.NotContains(t, body, forbidden, "the daemon start owner must not reach a session carriage")
	}
	// The only transport it hands out is the daemonmux carriage.
	require.Contains(t, body, "daemonmux.RawFramedTransport")
}

// TestDaemonStartRefusesIncompleteStarter proves a start seam that cannot dial
// or spawn fails closed instead of dialing a session or panicking.
func TestDaemonStartRefusesIncompleteStarter(t *testing.T) {
	isolateSandboxEnv(t)
	_, carriage := testDaemonCarriage(t)
	target := startableLocalTarget(t, carriage, ports.BrokerDaemonStartIfNeeded)

	for _, starter := range []brokerDaemonStarter{{}, {dial: dialLocalDaemonCarriage}, {spawn: realSpawn}} {
		withDaemonStarter(t, starter)
		_, err := dialBrokerLocalDaemon(context.Background(), carriage, target)
		require.Error(t, err)
		var typed ports.BrokerError
		require.ErrorAs(t, err, &typed)
		require.Equal(t, ports.BrokerErrorIncompatible, typed.Code)
	}
}

// TestDaemonStartRefusesInvalidTarget proves an unvalidated resolved target is
// refused before any dial or spawn.
func TestDaemonStartRefusesInvalidTarget(t *testing.T) {
	isolateSandboxEnv(t)
	_, carriage := testDaemonCarriage(t)
	scripted := &scriptedDaemonStarter{carriage: carriage}
	withDaemonStarter(t, scripted.starter(fastDaemonStartBackoff))

	invalid := startableLocalTarget(t, carriage, ports.BrokerDaemonStartIfNeeded)
	invalid.StartMode = 0
	_, err := dialBrokerLocalDaemon(context.Background(), carriage, invalid)
	require.Error(t, err)
	var typed ports.BrokerError
	require.ErrorAs(t, err, &typed)
	require.Equal(t, ports.BrokerErrorIncompatible, typed.Code)
	dials, spawns := scripted.counters()
	require.Zero(t, dials)
	require.Zero(t, spawns)
}

// TestDaemonStartArgvFlagRoundTrips proves the helper argument spelling is one
// closed value both sides agree on, and that an unknown value is refused in
// either direction.
func TestDaemonStartArgvFlagRoundTrips(t *testing.T) {
	tests := []struct {
		mode ports.BrokerDaemonStartMode
		flag string
	}{
		{mode: ports.BrokerDaemonExistingOnly, flag: "existing-only"},
		{mode: ports.BrokerDaemonStartIfNeeded, flag: "if-needed"},
	}
	for _, tt := range tests {
		flag, err := brokerDaemonStartArgvFlag(tt.mode)
		require.NoError(t, err)
		require.Equal(t, tt.flag, flag)
		parsed, err := parseBrokerDaemonStartArgvFlag(flag)
		require.NoError(t, err)
		require.Equal(t, tt.mode, parsed)
	}
	_, err := brokerDaemonStartArgvFlag(ports.BrokerDaemonStartMode(0))
	require.Error(t, err)
	for _, invalid := range []string{"", "existing_only", "if_needed", "always", "Existing-Only"} {
		_, err := parseBrokerDaemonStartArgvFlag(invalid)
		require.Error(t, err, "value %q must be refused", invalid)
	}
}

// testLocalSandboxConfigForCarriage derives local authority from production state.
func testLocalSandboxConfigForCarriage(t *testing.T, carriage string) (*brokerconfig.Config, brokerconfig.LocalBinding) {
	t.Helper()
	layout := emptyProductionBrokerLayout(t, "")
	config, err := brokerconfig.LoadProduction(layout, productionBrokerConfigPath(), brokerLocalTestIdentity, brokerTestPolicy(), carriage)
	require.NoError(t, err)
	binding, ok := config.LocalBinding()
	require.True(t, ok)
	return config, binding
}
