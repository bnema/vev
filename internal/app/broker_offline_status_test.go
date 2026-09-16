package app

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/adapters/brokerconfig"
	"github.com/bnema/vev/internal/adapters/brokeripc"
	"github.com/bnema/vev/internal/adapters/lifecycle"
)

// sandboxEmptyDocument is a marker-valid sandbox configuration with no
// registrations and an optional provisioned idle grace.
func sandboxEmptyDocument(idleGrace string) map[string]any {
	document := map[string]any{"marker": brokerconfig.Marker, "registrations": []any{}}
	if idleGrace != "" {
		document["idleGrace"] = idleGrace
	}
	return document
}

// emptySandboxRoot writes one marker-valid sandbox root and returns its layout.
func emptySandboxRoot(t *testing.T, idleGrace string) brokerconfig.Layout {
	t.Helper()
	isolateSandboxEnv(t)
	root := filepath.Join(shortTempDir(t, "vevu"), "s")
	require.NoError(t, os.MkdirAll(root, 0o700))
	writeSandboxConfig(t, root, sandboxEmptyDocument(idleGrace))
	layout, err := offlineLayout(root)
	require.NoError(t, err)
	return layout
}

// fakeStatusLock records releases for one held election.
type fakeStatusLock struct{ releases atomic.Int32 }

func (l *fakeStatusLock) Release() error {
	l.releases.Add(1)
	return nil
}

// readyReport is the report returned when a scripted probe succeeds.
func readyReport(socketPath string) brokerStatusReport {
	return brokerStatusReport{Status: "ready", Endpoint: socketPath, Epoch: 7, Revision: 3, HostCount: 2}
}

// testStatusDeps builds deterministic status seams. Virtual time starts at the
// real instant and only advances when the loop waits, so a short timeout
// exhausts without real sleeping while the production context deadline stays
// comfortably in the future. The default probe always reports absence.
func testStatusDeps(t *testing.T) (brokerStatusDeps, *statusRecorder) {
	t.Helper()
	rec := &statusRecorder{
		now:    time.Now(),
		stdout: &bytes.Buffer{},
	}
	deps := brokerStatusDeps{
		now: func() time.Time { return rec.now },
		wait: func(ctx context.Context, d time.Duration) error {
			rec.now = rec.now.Add(d)
			return ctx.Err()
		},
		acquireElection: func(string) (brokerStatusLock, error) {
			rec.elections.Add(1)
			lock := &fakeStatusLock{}
			rec.locks = append(rec.locks, lock)
			return lock, nil
		},
		lifetimeOwner: func(string) (bool, error) { return false, nil },
		spawn: func(context.Context, string, time.Duration, bool) error {
			rec.spawns.Add(1)
			return nil
		},
		stdout: rec.stdout,
	}
	rec.setProbe(&deps, func(_ int, socketPath string) (brokerStatusReport, error) {
		return brokerStatusReport{}, absentError(socketPath)
	})
	return deps, rec
}

// statusRecorder captures one status run's observable effects.
type statusRecorder struct {
	now       time.Time
	probes    atomic.Int32
	spawns    atomic.Int32
	elections atomic.Int32
	locks     []*fakeStatusLock
	stdout    *bytes.Buffer
}

// setProbe installs one scripted probe that sees its 1-based attempt number.
func (r *statusRecorder) setProbe(deps *brokerStatusDeps, probe func(attempt int, socketPath string) (brokerStatusReport, error)) {
	deps.probe = func(_ context.Context, socketPath string) (brokerStatusReport, error) {
		return probe(int(r.probes.Add(1)), socketPath)
	}
}

func (r *statusRecorder) report(t *testing.T) brokerStatusReport {
	t.Helper()
	var report brokerStatusReport
	require.NoError(t, json.Unmarshal([]byte(strings.TrimSpace(r.stdout.String())), &report))
	return report
}

// decodeOneReport asserts stdout holds exactly one JSON document and decodes it.
func decodeOneReport(t *testing.T, raw string) brokerStatusReport {
	t.Helper()
	trimmed := strings.TrimSpace(raw)
	require.Equal(t, 1, strings.Count(trimmed, "\n")+1, "stdout must hold exactly one report line")
	var report brokerStatusReport
	require.NoError(t, json.Unmarshal([]byte(trimmed), &report))
	return report
}

func TestParseBrokerLauncherArgs(t *testing.T) {
	tests := []struct {
		name    string
		args    []string
		want    brokerLauncherOptions
		wantErr string
	}{
		{name: "root only", args: []string{"--offline-root", "/srv/sandbox"}, want: brokerLauncherOptions{offlineRoot: "/srv/sandbox"}},
		{
			name: "root and grace",
			args: []string{"--offline-root", "/srv/sandbox", "--idle-grace", "90s"},
			want: brokerLauncherOptions{offlineRoot: "/srv/sandbox", idleGrace: 90 * time.Second},
		},
		{name: "missing root", args: nil, wantErr: "requires --offline-root"},
		{name: "missing root value", args: []string{"--offline-root"}, wantErr: "requires a path"},
		{name: "duplicate root", args: []string{"--offline-root", "/a", "--offline-root", "/b"}, wantErr: "duplicate --offline-root"},
		{name: "duplicate grace", args: []string{"--offline-root", "/a", "--idle-grace", "1s", "--idle-grace", "2s"}, wantErr: "duplicate --idle-grace"},
		{name: "zero grace", args: []string{"--offline-root", "/a", "--idle-grace", "0s"}, wantErr: "must be positive"},
		{name: "invalid grace", args: []string{"--offline-root", "/a", "--idle-grace", "soon"}, wantErr: "not a duration"},
		{name: "ensure is not a launcher flag", args: []string{"--offline-root", "/a", "--ensure"}, wantErr: "unknown flag"},
		{name: "positional", args: []string{"--offline-root", "/a", "extra"}, wantErr: "positional"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			command, err := parseBrokerLauncherArgs(tt.args)
			if tt.wantErr != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tt.wantErr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, kindBrokerLauncher, command.kind)
			require.Equal(t, tt.want, command.brokerLauncher)
		})
	}
}

func TestParseBrokerStatusArgs(t *testing.T) {
	tests := []struct {
		name    string
		args    []string
		want    brokerStatusOptions
		wantErr string
	}{
		{
			name: "root only defaults the timeout",
			args: []string{"--offline-root", "/srv/sandbox"},
			want: brokerStatusOptions{offlineRoot: "/srv/sandbox", timeout: brokerStatusDefaultTimeout},
		},
		{
			name: "ensure and timeout",
			args: []string{"--offline-root", "/a", "--ensure", "--timeout", "250ms"},
			want: brokerStatusOptions{offlineRoot: "/a", ensure: true, timeout: 250 * time.Millisecond},
		},
		{name: "missing root", args: []string{"--ensure"}, wantErr: "requires --offline-root"},
		{name: "duplicate root", args: []string{"--offline-root", "/a", "--offline-root", "/b"}, wantErr: "duplicate --offline-root"},
		{name: "duplicate ensure", args: []string{"--offline-root", "/a", "--ensure", "--ensure"}, wantErr: "duplicate --ensure"},
		{name: "duplicate timeout", args: []string{"--offline-root", "/a", "--timeout", "1s", "--timeout", "2s"}, wantErr: "duplicate --timeout"},
		{name: "missing timeout value", args: []string{"--offline-root", "/a", "--timeout"}, wantErr: "requires a duration"},
		{name: "zero timeout", args: []string{"--offline-root", "/a", "--timeout", "0s"}, wantErr: "must be positive"},
		{name: "invalid timeout", args: []string{"--offline-root", "/a", "--timeout", "soon"}, wantErr: "not a duration"},
		{name: "idle grace is not a status flag", args: []string{"--offline-root", "/a", "--idle-grace", "1s"}, wantErr: "unknown flag"},
		{name: "positional", args: []string{"--offline-root", "/a", "extra"}, wantErr: "positional"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			command, err := parseBrokerStatusArgs(tt.args)
			if tt.wantErr != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tt.wantErr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, kindBrokerStatus, command.kind)
			require.Equal(t, tt.want, command.brokerStatus)
		})
	}
}

func TestBrokerLauncherAndStatusAreHiddenFromPublicHelp(t *testing.T) {
	require.NotContains(t, usageText, brokerLauncherCommand)
	require.NotContains(t, usageText, brokerStatusCommand)
	_, err := parseArgs([]string{brokerLauncherCommand, "--offline-root", "/srv/sandbox"})
	require.NoError(t, err)
	_, err = parseArgs([]string{brokerStatusCommand, "--offline-root", "/srv/sandbox", "--ensure"})
	require.NoError(t, err)
	_, err = parseArgs([]string{"--help"})
	require.NoError(t, err)
}

func TestEffectiveIdleGrace(t *testing.T) {
	layout := emptySandboxRoot(t, "90s")
	config, err := brokerconfig.Load(layout)
	require.NoError(t, err)
	unprovisioned := emptySandboxRoot(t, "")
	plain, err := brokerconfig.Load(unprovisioned)
	require.NoError(t, err)

	tests := []struct {
		name     string
		config   *brokerconfig.Config
		explicit time.Duration
		want     time.Duration
		wantErr  string
	}{
		{name: "unset uses provisioned", config: config, want: 90 * time.Second},
		{name: "matching explicit is accepted", config: config, explicit: 90 * time.Second, want: 90 * time.Second},
		{name: "conflicting explicit is diagnosed", config: config, explicit: time.Minute, wantErr: "conflicts with the provisioned idle grace"},
		{name: "unprovisioned explicit wins", config: plain, explicit: 42 * time.Second, want: 42 * time.Second},
		{name: "unprovisioned unset defers to the default", config: plain, want: 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := effectiveIdleGrace(tt.config, tt.explicit)
			if tt.wantErr != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tt.wantErr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestBrokerServeRefusesConflictingIdleGrace(t *testing.T) {
	layout := emptySandboxRoot(t, "90s")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	deps, _ := testBrokerServeDeps(newSandboxClock())
	err := runBrokerServe(ctx, brokerServeOptions{offlineRoot: layout.Root, idleGrace: time.Minute}, deps)
	require.Error(t, err)
	require.Contains(t, err.Error(), "conflicts with the provisioned idle grace")
	require.NoFileExists(t, filepath.Join(layout.Runtime, "lifecycle.lock"), "a refused serve must not take the lifetime lock")
}

func TestBrokerLauncherRefusesConflictingIdleGraceBeforeSpawning(t *testing.T) {
	layout := emptySandboxRoot(t, "90s")
	err := runBrokerLauncher(brokerLauncherOptions{offlineRoot: layout.Root, idleGrace: time.Minute})
	require.Error(t, err)
	require.Contains(t, err.Error(), "conflicts with the provisioned idle grace")
}

func TestBrokerStatusDialOnlyReportsOfflineWithoutCreatingAnything(t *testing.T) {
	layout := emptySandboxRoot(t, "")
	deps := defaultBrokerStatusDeps()
	var out bytes.Buffer
	deps.stdout = &out

	require.NoError(t, runBrokerStatus(context.Background(), brokerStatusOptions{offlineRoot: layout.Root, timeout: time.Second}, deps))
	report := decodeOneReport(t, out.String())
	require.Equal(t, "offline", report.Status)
	require.Equal(t, brokeripc.SocketPath(layout.Runtime), report.Endpoint)
	require.Zero(t, report.Epoch)
	require.Zero(t, report.Revision)
	require.Zero(t, report.HostCount)

	require.NoDirExists(t, layout.Runtime, "dial-only status must not create the runtime directory")
	require.NoDirExists(t, layout.Spawn, "dial-only status must not create the spawn directory")
}

func TestBrokerStatusDialOnlyReadyAgainstLiveSandbox(t *testing.T) {
	root, prodRuntime, prodState := isolateSandboxEnv(t)
	fixture := newBrokerMuxFixture(t, brokerTestPolicy())
	require.NoError(t, os.MkdirAll(root, 0o700))
	writeSandboxConfig(t, root, sandboxRegistrationDocument(fixture.route))

	clk := newSandboxClock()
	serveDeps, ready := testBrokerServeDeps(clk)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := runSandbox(ctx, brokerServeOptions{offlineRoot: root}, serveDeps)
	awaitSandboxReady(t, ready)

	deps := defaultBrokerStatusDeps()
	var out bytes.Buffer
	deps.stdout = &out
	require.NoError(t, runBrokerStatus(context.Background(), brokerStatusOptions{offlineRoot: root, timeout: time.Second}, deps))
	report := decodeOneReport(t, out.String())
	require.Equal(t, "ready", report.Status)
	require.Positive(t, report.Epoch)
	require.Positive(t, report.Revision)
	// The sandbox registry restores from its own empty durable store; the
	// provisioned endpoints feed the resolver, not the host inventory.
	require.Zero(t, report.HostCount)

	awaitSandboxCancel(t, cancel, done)
	requireProductionUntouched(t, prodRuntime, prodState)
}

func TestBrokerStatusEnsureSpawnsOnceThenReportsReady(t *testing.T) {
	layout := emptySandboxRoot(t, "")
	deps, rec := testStatusDeps(t)
	rec.setProbe(&deps, func(_ int, socketPath string) (brokerStatusReport, error) {
		if rec.spawns.Load() > 0 {
			return readyReport(socketPath), nil
		}
		return brokerStatusReport{}, absentError(socketPath)
	})
	require.NoError(t, runBrokerStatus(context.Background(), brokerStatusOptions{offlineRoot: layout.Root, ensure: true, timeout: time.Second}, deps))

	require.Equal(t, int32(1), rec.spawns.Load(), "exactly one spawn is expected")
	require.Equal(t, int32(1), rec.elections.Load(), "exactly one election is expected")
	require.Len(t, rec.locks, 1)
	require.Equal(t, int32(1), rec.locks[0].releases.Load(), "the election must be released after readiness")
	require.Equal(t, "ready", rec.report(t).Status)
}

func TestBrokerStatusEnsureWaitsForAnotherSpawner(t *testing.T) {
	layout := emptySandboxRoot(t, "")
	var readyAfter int32 = 4
	deps, rec := testStatusDeps(t)
	rec.setProbe(&deps, func(attempt int, socketPath string) (brokerStatusReport, error) {
		if int32(attempt) >= readyAfter {
			return readyReport(socketPath), nil
		}
		return brokerStatusReport{}, absentError(socketPath)
	})
	deps.acquireElection = func(string) (brokerStatusLock, error) {
		rec.elections.Add(1)
		return nil, lifecycle.ErrBusy
	}
	require.NoError(t, runBrokerStatus(context.Background(), brokerStatusOptions{offlineRoot: layout.Root, ensure: true, timeout: time.Second}, deps))

	require.Zero(t, rec.spawns.Load(), "a waiter must never spawn while another caller is elected")
	require.Positive(t, rec.elections.Load())
	require.Equal(t, "ready", rec.report(t).Status)
}

func TestBrokerStatusEnsureDefersToLifetimeOwnerBeforeSpawning(t *testing.T) {
	layout := emptySandboxRoot(t, "")
	var ownerCalls int32
	deps, rec := testStatusDeps(t)
	rec.setProbe(&deps, func(_ int, socketPath string) (brokerStatusReport, error) {
		if rec.spawns.Load() > 0 {
			return readyReport(socketPath), nil
		}
		return brokerStatusReport{}, absentError(socketPath)
	})
	deps.lifetimeOwner = func(string) (bool, error) {
		if atomic.AddInt32(&ownerCalls, 1) <= 3 {
			return true, nil
		}
		return false, nil
	}
	require.NoError(t, runBrokerStatus(context.Background(), brokerStatusOptions{offlineRoot: layout.Root, ensure: true, timeout: time.Second}, deps))
	require.Equal(t, int32(1), rec.spawns.Load(), "a live lifetime owner must suppress the spawn until it is gone")
	require.Equal(t, int32(1), rec.elections.Load(), "the election is taken only after the live owner releases the sandbox")
}

func TestBrokerStatusEnsureRecoversAfterSpawnerDeath(t *testing.T) {
	layout := emptySandboxRoot(t, "")
	var electionAttempts int32
	deps, rec := testStatusDeps(t)
	rec.setProbe(&deps, func(_ int, socketPath string) (brokerStatusReport, error) {
		if rec.spawns.Load() > 0 {
			return readyReport(socketPath), nil
		}
		return brokerStatusReport{}, absentError(socketPath)
	})
	deps.acquireElection = func(string) (brokerStatusLock, error) {
		rec.elections.Add(1)
		if atomic.AddInt32(&electionAttempts, 1) <= 3 {
			// The previous elected spawner is still alive (or mid-startup).
			return nil, lifecycle.ErrBusy
		}
		lock := &fakeStatusLock{}
		rec.locks = append(rec.locks, lock)
		return lock, nil
	}
	require.NoError(t, runBrokerStatus(context.Background(), brokerStatusOptions{offlineRoot: layout.Root, ensure: true, timeout: time.Second}, deps))
	require.Equal(t, int32(1), rec.spawns.Load(), "a waiter must take over once the elected spawner dies")
	require.Equal(t, "ready", rec.report(t).Status)
}

func TestBrokerStatusEnsureIncompatibleEndpointFailsWithoutSpawning(t *testing.T) {
	layout := emptySandboxRoot(t, "")
	deps, rec := testStatusDeps(t)
	rec.setProbe(&deps, func(_ int, socketPath string) (brokerStatusReport, error) {
		return brokerStatusReport{}, incompatibleError(socketPath, errors.New("foreign endpoint"))
	})
	err := runBrokerStatus(context.Background(), brokerStatusOptions{offlineRoot: layout.Root, ensure: true, timeout: time.Second}, deps)
	require.ErrorIs(t, err, errBrokerIncompatible)
	require.Zero(t, rec.spawns.Load())
	require.Zero(t, rec.elections.Load())
	report := rec.report(t)
	require.Equal(t, "offline", report.Status)
	require.Equal(t, 3, exitCode(err))
}

func TestBrokerStatusEnsureBoundedTimeoutReportsOffline(t *testing.T) {
	layout := emptySandboxRoot(t, "")
	deps, rec := testStatusDeps(t)
	err := runBrokerStatus(context.Background(), brokerStatusOptions{offlineRoot: layout.Root, ensure: true, timeout: 50 * time.Millisecond}, deps)
	require.ErrorIs(t, err, errBrokerNotReady)
	require.Equal(t, 3, exitCode(err))
	require.Equal(t, int32(1), rec.spawns.Load())
	require.Equal(t, "offline", rec.report(t).Status)
}

func TestBrokerStatusEnsureSpawnFailureIsReported(t *testing.T) {
	layout := emptySandboxRoot(t, "")
	spawnErr := errors.New("launcher exec failed")
	deps, rec := testStatusDeps(t)
	deps.spawn = func(context.Context, string, time.Duration, bool) error {
		rec.spawns.Add(1)
		return spawnErr
	}
	err := runBrokerStatus(context.Background(), brokerStatusOptions{offlineRoot: layout.Root, ensure: true, timeout: time.Second}, deps)
	require.ErrorIs(t, err, spawnErr)
	require.Equal(t, 3, exitCode(err))
	require.Equal(t, "offline", rec.report(t).Status)
}

func TestBrokerStatusEnsureRejectsUnsafeRoot(t *testing.T) {
	isolateSandboxEnv(t)
	tests := []struct {
		name string
		root string
	}{
		{name: "relative root", root: "relative"},
		{name: "production overlap", root: brokeripc.SocketDir()},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			deps, rec := testStatusDeps(t)
			err := runBrokerStatus(context.Background(), brokerStatusOptions{offlineRoot: tt.root, ensure: true, timeout: time.Second}, deps)
			require.Error(t, err)
			require.Equal(t, 3, exitCode(err), "a layout failure must still exit 3")
			require.Equal(t, "offline", rec.report(t).Status, "a layout failure must still emit one offline report")
			require.Zero(t, rec.spawns.Load())
		})
	}
}

// TestBrokerStatusEnsureConfigFailureReportsOffline proves a malformed or
// missing configuration still leaves the caller with one offline JSON document
// and a code-3 failure instead of a bare error.
func TestBrokerStatusEnsureConfigFailureReportsOffline(t *testing.T) {
	isolateSandboxEnv(t)
	root := filepath.Join(shortTempDir(t, "vevm"), "s")
	require.NoError(t, os.MkdirAll(root, 0o700))
	deps, rec := testStatusDeps(t)
	err := runBrokerStatus(context.Background(), brokerStatusOptions{offlineRoot: root, ensure: true, timeout: time.Second}, deps)
	require.Error(t, err)
	require.Contains(t, err.Error(), brokerconfig.ConfigFileName)
	require.Equal(t, 3, exitCode(err))
	require.Equal(t, "offline", rec.report(t).Status)
	require.Zero(t, rec.spawns.Load())
}

// TestBrokerStatusEnsureSecuresRootBeforeSpawning proves the ensure path
// validates and secures the sandbox root exactly as the launcher does, before
// the spawn directory is created or any election lock is taken.
func TestBrokerStatusEnsureSecuresRootBeforeSpawning(t *testing.T) {
	layout := emptySandboxRoot(t, "")
	require.NoError(t, os.Chmod(layout.Root, 0o755))
	deps, rec := testStatusDeps(t)
	err := runBrokerStatus(context.Background(), brokerStatusOptions{offlineRoot: layout.Root, ensure: true, timeout: time.Second}, deps)
	require.Error(t, err)
	require.Contains(t, err.Error(), "0755")
	require.Equal(t, 3, exitCode(err))
	require.Equal(t, "offline", rec.report(t).Status)
	require.Zero(t, rec.spawns.Load())
	require.Zero(t, rec.elections.Load())
	require.NoFileExists(t, filepath.Join(layout.Runtime, brokerconfig.SpawnDirName), "an insecure root must be refused before the spawn directory is created")
}

// TestBrokerStatusEnsureDeadlineBoundsSpawnWait proves the overall ensure
// deadline bounds a launcher spawn that never returns, so a blocking launcher
// cannot wedge the status command.
func TestBrokerStatusEnsureDeadlineBoundsSpawnWait(t *testing.T) {
	layout := emptySandboxRoot(t, "")
	deps, rec := testStatusDeps(t)
	deps.spawn = func(ctx context.Context, _ string, _ time.Duration, _ bool) error {
		<-ctx.Done()
		return ctx.Err()
	}
	start := time.Now()
	err := runBrokerStatus(context.Background(), brokerStatusOptions{offlineRoot: layout.Root, ensure: true, timeout: 200 * time.Millisecond}, deps)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Equal(t, 3, exitCode(err))
	require.Less(t, time.Since(start), 5*time.Second, "the overall deadline must bound the launcher spawn wait")
	require.Equal(t, "offline", rec.report(t).Status)
}

// TestBrokerStatusDialOnlyAppliesTimeout proves the dial-only probe is bounded
// by --timeout as one absolute bound and is never silently replaced by the
// ensure probe budget.
func TestBrokerStatusDialOnlyAppliesTimeout(t *testing.T) {
	tests := []struct {
		name    string
		timeout time.Duration
	}{
		{name: "short timeout is the probe bound", timeout: 250 * time.Millisecond},
		{name: "default timeout is the probe bound", timeout: brokerStatusDefaultTimeout},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			layout := emptySandboxRoot(t, "")
			deps, _ := testStatusDeps(t)
			var observed time.Duration
			sawDeadline := false
			deps.probe = func(ctx context.Context, socketPath string) (brokerStatusReport, error) {
				deadline, ok := ctx.Deadline()
				sawDeadline = ok
				if ok {
					observed = time.Until(deadline)
				}
				return brokerStatusReport{}, absentError(socketPath)
			}
			require.NoError(t, runBrokerStatus(context.Background(), brokerStatusOptions{offlineRoot: layout.Root, timeout: tt.timeout}, deps))
			require.True(t, sawDeadline, "dial-only must bound the probe with the timeout context")
			require.InDelta(t, tt.timeout.Seconds(), observed.Seconds(), 0.5)
		})
	}
}

// TestBrokerStatusDialOnlyBoundedByTimeout proves a live-but-silent endpoint is
// bounded by --timeout rather than the ensure probe budget.
func TestBrokerStatusDialOnlyBoundedByTimeout(t *testing.T) {
	layout := emptySandboxRoot(t, "")
	require.NoError(t, os.MkdirAll(layout.Runtime, 0o700))
	_ = bindUnixSocket(t, brokeripc.SocketPath(layout.Runtime))
	deps := defaultBrokerStatusDeps()
	var out bytes.Buffer
	deps.stdout = &out
	start := time.Now()
	require.NoError(t, runBrokerStatus(context.Background(), brokerStatusOptions{offlineRoot: layout.Root, timeout: 300 * time.Millisecond}, deps))
	require.Less(t, time.Since(start), time.Second, "the dial-only probe must be bounded by --timeout, not the ensure budget")
	require.Equal(t, "offline", decodeOneReport(t, out.String()).Status)
}

func TestBrokerStatusEnsureHonorsCancellation(t *testing.T) {
	layout := emptySandboxRoot(t, "")
	deps, _ := testStatusDeps(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := runBrokerStatus(ctx, brokerStatusOptions{offlineRoot: layout.Root, ensure: true, timeout: time.Second}, deps)
	require.ErrorIs(t, err, context.Canceled)
}

func TestBrokerStatusReportIsBoundedJSON(t *testing.T) {
	var out bytes.Buffer
	require.NoError(t, writeBrokerStatusReport(&out, brokerStatusReport{
		Status: "ready", Endpoint: brokeripc.SocketPath("/tmp/sandbox/runtime"), Epoch: 3, Revision: 9, HostCount: 4,
	}))
	var fields map[string]json.RawMessage
	require.NoError(t, json.Unmarshal([]byte(strings.TrimSpace(out.String())), &fields))
	require.Equal(t, []string{"endpoint", "epoch", "host_count", "revision", "status"}, sortedKeys(fields))
	require.LessOrEqual(t, out.Len(), brokerStatusMaxReportBytes)
}

// sortedKeys returns the sorted keys of one JSON object.
func sortedKeys(fields map[string]json.RawMessage) []string {
	keys := make([]string, 0, len(fields))
	for key := range fields {
		keys = append(keys, key)
	}
	slicesSort(keys)
	return keys
}

// slicesSort is a tiny insertion sort so the test file needs no extra import.
func slicesSort(values []string) {
	for i := 1; i < len(values); i++ {
		for j := i; j > 0 && values[j] < values[j-1]; j-- {
			values[j], values[j-1] = values[j-1], values[j]
		}
	}
}

func TestProbeBrokerStatusClassifiesAbsenceAndIncompatibility(t *testing.T) {
	layout := emptySandboxRoot(t, "")
	socketPath := brokeripc.SocketPath(layout.Runtime)

	t.Run("missing endpoint is absence", func(t *testing.T) {
		_, err := probeBrokerStatus(context.Background(), socketPath)
		require.ErrorIs(t, err, errBrokerAbsent)
	})

	t.Run("foreign path is incompatible", func(t *testing.T) {
		require.NoError(t, os.MkdirAll(layout.Runtime, 0o700))
		require.NoError(t, os.WriteFile(socketPath, []byte("not a socket"), 0o600))
		_, err := probeBrokerStatus(context.Background(), socketPath)
		require.ErrorIs(t, err, errBrokerIncompatible)
	})

	t.Run("stale socket is absence", func(t *testing.T) {
		require.NoError(t, os.Remove(socketPath))
		bindStaleUnixSocket(t, socketPath)
		_, err := probeBrokerStatus(context.Background(), socketPath)
		require.ErrorIs(t, err, errBrokerAbsent)
	})

	t.Run("live foreign endpoint is incompatible", func(t *testing.T) {
		require.NoError(t, os.Remove(socketPath))
		listener := bindUnixSocket(t, socketPath)
		go func() {
			for {
				conn, err := listener.Accept()
				if err != nil {
					return
				}
				_ = conn.Close()
			}
		}()
		_, err := probeBrokerStatus(context.Background(), socketPath)
		require.ErrorIs(t, err, errBrokerIncompatible)
	})
}

func TestProbeBrokerStatusSilentRegistrationIsBounded(t *testing.T) {
	layout := emptySandboxRoot(t, "")
	socketPath := brokeripc.SocketPath(layout.Runtime)
	require.NoError(t, os.MkdirAll(layout.Runtime, 0o700))
	// A listener that never reads or answers: the dial succeeds and the broker
	// handshake stalls until the probe budget expires.
	_ = bindUnixSocket(t, socketPath)

	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err := probeBrokerStatus(ctx, socketPath)
	require.Error(t, err)
	require.False(t, errors.Is(err, errBrokerAbsent), "a stalled handshake must not be classified as absence")
	require.Less(t, time.Since(start), 5*time.Second)
}

// bindUnixSocket binds one real Unix listener at path.
func bindUnixSocket(t *testing.T, path string) net.Listener {
	t.Helper()
	listener, err := net.Listen("unix", path)
	require.NoError(t, err)
	t.Cleanup(func() { _ = listener.Close() })
	return listener
}

// bindStaleUnixSocket leaves a socket file behind with no live listener, exactly
// like an owner that died without unlinking its endpoint.
func bindStaleUnixSocket(t *testing.T, path string) {
	t.Helper()
	listener, err := net.Listen("unix", path)
	require.NoError(t, err)
	if unix, ok := listener.(*net.UnixListener); ok {
		unix.SetUnlinkOnClose(false)
	}
	require.NoError(t, listener.Close())
	require.FileExists(t, path)
}

// Ensure the helper fixture contract used elsewhere stays coherent.
func TestStatusFixtureDocument(t *testing.T) {
	document := sandboxEmptyDocument("1s")
	require.Equal(t, brokerconfig.Marker, document["marker"])
	require.Equal(t, "1s", document["idleGrace"])
	require.Empty(t, sandboxEmptyDocument("")["idleGrace"])
}
