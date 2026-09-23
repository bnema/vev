package app

import (
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
	"github.com/bnema/vev/internal/adapters/daemonmux"
	"github.com/bnema/vev/internal/adapters/ipc"
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

// emptyProductionBrokerLayout provisions the real broker config in private XDG
// directories; each test receives an independent runtime, state, and config.
func emptyProductionBrokerLayout(t *testing.T, idleGrace string) brokerconfig.Layout {
	t.Helper()
	t.Setenv("VEV_ENV", "")
	t.Setenv("VEV_ENV_ROOT", "")
	t.Setenv("XDG_RUNTIME_DIR", shortTempDir(t, "vb"))
	t.Setenv("XDG_STATE_HOME", shortTempDir(t, "vb"))
	t.Setenv("XDG_CONFIG_HOME", shortTempDir(t, "vb"))
	require.NoError(t, ensureProductionBrokerConfig())
	if idleGrace != "" {
		raw, err := json.Marshal(sandboxEmptyDocument(idleGrace))
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(productionBrokerConfigPath(), raw, 0o600))
	}
	return productionBrokerLayout()
}

// productionStatusRequest provides the inputs used by production connect-or-spawn.
func productionStatusRequest(layout brokerconfig.Layout, timeout time.Duration) brokerStatusRequest {
	return brokerStatusRequest{
		layout: layout, socketPath: brokeripc.SocketPath(layout.Runtime),
		deadline: time.Now().Add(timeout), timeout: timeout,
	}
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
		now: time.Now(),
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
}

// setProbe installs one scripted probe that sees its 1-based attempt number.
func (r *statusRecorder) setProbe(deps *brokerStatusDeps, probe func(attempt int, socketPath string) (brokerStatusReport, error)) {
	deps.probe = func(_ context.Context, socketPath string) (brokerStatusReport, error) {
		return probe(int(r.probes.Add(1)), socketPath)
	}
}

// decodeOneReport remains shared with the subprocess broker tests until those
// are converted to the production launcher.
func decodeOneReport(t *testing.T, raw string) brokerStatusReport {
	t.Helper()
	trimmed := strings.TrimSpace(raw)
	require.Equal(t, 1, strings.Count(trimmed, "\n")+1)
	var report brokerStatusReport
	require.NoError(t, json.Unmarshal([]byte(trimmed), &report))
	return report
}

func TestEffectiveIdleGrace(t *testing.T) {
	layout := emptyProductionBrokerLayout(t, "90s")
	config, err := brokerconfig.LoadProduction(layout, productionBrokerConfigPath(), "", localDaemonPolicy(), daemonmux.SocketPath(ipc.SocketDir()))
	require.NoError(t, err)
	unprovisioned := emptyProductionBrokerLayout(t, "")
	plain, err := brokerconfig.LoadProduction(unprovisioned, productionBrokerConfigPath(), "", localDaemonPolicy(), daemonmux.SocketPath(ipc.SocketDir()))
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
	layout := emptyProductionBrokerLayout(t, "90s")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	deps, _ := testBrokerServeDeps(newSandboxClock())
	err := runBrokerServe(ctx, brokerServeOptions{production: true, idleGrace: time.Minute}, deps)
	require.Error(t, err)
	require.Contains(t, err.Error(), "conflicts with the provisioned idle grace")
	require.NoFileExists(t, filepath.Join(layout.Runtime, "lifecycle.lock"), "a refused serve must not take the lifetime lock")
}

func TestBrokerLauncherRefusesConflictingIdleGraceBeforeSpawning(t *testing.T) {
	layout := emptyProductionBrokerLayout(t, "90s")
	config, err := brokerconfig.LoadProduction(layout, productionBrokerConfigPath(), "", localDaemonPolicy(), daemonmux.SocketPath(ipc.SocketDir()))
	require.NoError(t, err)
	_, err = effectiveIdleGrace(config, time.Minute)
	require.Error(t, err)
	require.Contains(t, err.Error(), "conflicts with the provisioned idle grace")
}

func TestBrokerStatusEnsureSpawnsOnceThenReportsReady(t *testing.T) {
	layout := emptyProductionBrokerLayout(t, "")
	deps, rec := testStatusDeps(t)
	rec.setProbe(&deps, func(_ int, socketPath string) (brokerStatusReport, error) {
		if rec.spawns.Load() > 0 {
			return readyReport(socketPath), nil
		}
		return brokerStatusReport{}, absentError(socketPath)
	})
	report, err := ensureBrokerReady(context.Background(), productionStatusRequest(layout, time.Second), deps)
	require.NoError(t, err)

	require.Equal(t, int32(1), rec.spawns.Load(), "exactly one spawn is expected")
	require.Equal(t, int32(1), rec.elections.Load(), "exactly one election is expected")
	require.Len(t, rec.locks, 1)
	require.Equal(t, int32(1), rec.locks[0].releases.Load(), "the election must be released after readiness")
	require.Equal(t, "ready", report.Status)
}

func TestBrokerStatusEnsureWaitsForAnotherSpawner(t *testing.T) {
	layout := emptyProductionBrokerLayout(t, "")
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
	report, err := ensureBrokerReady(context.Background(), productionStatusRequest(layout, time.Second), deps)
	require.NoError(t, err)

	require.Zero(t, rec.spawns.Load(), "a waiter must never spawn while another caller is elected")
	require.Positive(t, rec.elections.Load())
	require.Equal(t, "ready", report.Status)
}

func TestBrokerStatusEnsureDefersToLifetimeOwnerBeforeSpawning(t *testing.T) {
	layout := emptyProductionBrokerLayout(t, "")
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
	_, err := ensureBrokerReady(context.Background(), productionStatusRequest(layout, time.Second), deps)
	require.NoError(t, err)
	require.Equal(t, int32(1), rec.spawns.Load(), "a live lifetime owner must suppress the spawn until it is gone")
	require.Equal(t, int32(1), rec.elections.Load(), "the election is taken only after the live owner releases the broker")
}

func TestBrokerStatusEnsureRecoversAfterSpawnerDeath(t *testing.T) {
	layout := emptyProductionBrokerLayout(t, "")
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
	report, err := ensureBrokerReady(context.Background(), productionStatusRequest(layout, time.Second), deps)
	require.NoError(t, err)
	require.Equal(t, int32(1), rec.spawns.Load(), "a waiter must take over once the elected spawner dies")
	require.Equal(t, "ready", report.Status)
}

func TestBrokerStatusEnsureIncompatibleEndpointFailsWithoutSpawning(t *testing.T) {
	layout := emptyProductionBrokerLayout(t, "")
	deps, rec := testStatusDeps(t)
	rec.setProbe(&deps, func(_ int, socketPath string) (brokerStatusReport, error) {
		return brokerStatusReport{}, incompatibleError(socketPath, errors.New("foreign endpoint"))
	})
	_, err := ensureBrokerReady(context.Background(), productionStatusRequest(layout, time.Second), deps)
	require.ErrorIs(t, err, errBrokerIncompatible)
	require.Zero(t, rec.spawns.Load())
	require.Zero(t, rec.elections.Load())
}

func TestBrokerStatusEnsureBoundedTimeoutReportsOffline(t *testing.T) {
	layout := emptyProductionBrokerLayout(t, "")
	deps, rec := testStatusDeps(t)
	_, err := ensureBrokerReady(context.Background(), productionStatusRequest(layout, 50*time.Millisecond), deps)
	require.ErrorIs(t, err, errBrokerNotReady)
	require.Equal(t, int32(1), rec.spawns.Load())
}

func TestBrokerStatusEnsureSpawnFailureIsReported(t *testing.T) {
	layout := emptyProductionBrokerLayout(t, "")
	spawnErr := errors.New("launcher exec failed")
	deps, _ := testStatusDeps(t)
	deps.spawn = func(context.Context, string, time.Duration, bool) error {
		return spawnErr
	}
	_, err := ensureBrokerReady(context.Background(), productionStatusRequest(layout, time.Second), deps)
	require.ErrorIs(t, err, spawnErr)
}

// A corrupt production broker.json is rejected before broker ownership.
func TestBrokerEnsureProductionConfigFailure(t *testing.T) {
	layout := emptyProductionBrokerLayout(t, "")
	require.NoError(t, os.WriteFile(productionBrokerConfigPath(), []byte("not json"), 0o600))
	deps, _ := testBrokerServeDeps(newSandboxClock())
	err := runBrokerServe(context.Background(), brokerServeOptions{production: true}, deps)
	require.Error(t, err)
	require.NoFileExists(t, filepath.Join(layout.Runtime, "lifecycle.lock"))
}

// TestBrokerStatusEnsureSecuresRootBeforeSpawning rejects an insecure
// production spawn directory before electing a launcher.
func TestBrokerStatusEnsureSecuresRootBeforeSpawning(t *testing.T) {
	layout := emptyProductionBrokerLayout(t, "")
	require.NoError(t, os.MkdirAll(layout.Runtime, 0o700))
	require.NoError(t, os.MkdirAll(layout.Spawn, 0o700))
	require.NoError(t, os.Chmod(layout.Spawn, 0o755))
	deps, rec := testStatusDeps(t)
	_, err := ensureBrokerReady(context.Background(), productionStatusRequest(layout, time.Second), deps)
	require.Error(t, err)
	require.Contains(t, err.Error(), "0755")
	require.Zero(t, rec.spawns.Load())
	require.Zero(t, rec.elections.Load())
	require.NoFileExists(t, filepath.Join(layout.Spawn, "lifecycle.lock"), "an insecure spawn directory must be refused before election")
}

// TestBrokerStatusEnsureDeadlineBoundsSpawnWait proves the overall ensure
// deadline bounds a launcher spawn that never returns, so a blocking launcher
// cannot wedge connect-or-spawn.
func TestBrokerStatusEnsureDeadlineBoundsSpawnWait(t *testing.T) {
	layout := emptyProductionBrokerLayout(t, "")
	deps, _ := testStatusDeps(t)
	deps.spawn = func(ctx context.Context, _ string, _ time.Duration, _ bool) error {
		<-ctx.Done()
		return ctx.Err()
	}
	start := time.Now()
	_, err := ensureBrokerReady(context.Background(), productionStatusRequest(layout, 200*time.Millisecond), deps)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Less(t, time.Since(start), 5*time.Second, "the overall deadline must bound the launcher spawn wait")
}

func TestBrokerStatusEnsureHonorsCancellation(t *testing.T) {
	layout := emptyProductionBrokerLayout(t, "")
	deps, _ := testStatusDeps(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := ensureBrokerReady(ctx, productionStatusRequest(layout, time.Second), deps)
	require.ErrorIs(t, err, context.Canceled)
}

func TestProbeBrokerStatusClassifiesAbsenceAndIncompatibility(t *testing.T) {
	layout := emptyProductionBrokerLayout(t, "")
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
	layout := emptyProductionBrokerLayout(t, "")
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
