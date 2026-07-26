package app

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bnema/vev/internal/adapters/lifecycle"
	"github.com/bnema/vev/internal/adapters/noticefile"
	"github.com/bnema/vev/internal/adapters/recoveryfs"
	"github.com/bnema/vev/internal/adapters/snapshot"
	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/persist"
	"github.com/bnema/vev/internal/ports"
	portsmocks "github.com/bnema/vev/internal/ports/mocks"
	"github.com/bnema/vev/internal/usecase/client"
	"github.com/bnema/vev/internal/usecase/confirm"
	"github.com/bnema/vev/internal/usecase/daemon"
	"github.com/bnema/vev/internal/usecase/recovery"
	"github.com/bnema/vev/pkg/kv"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func decodeJSONLogs(t *testing.T, data []byte) []map[string]any {
	t.Helper()
	var entries []map[string]any
	scanner := bufio.NewScanner(bytes.NewReader(data))
	for scanner.Scan() {
		var entry map[string]any
		require.NoError(t, json.Unmarshal(scanner.Bytes(), &entry))
		entries = append(entries, entry)
	}
	require.NoError(t, scanner.Err())
	return entries
}

func eventNames(entries []map[string]any) []string {
	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		if name, ok := entry["msg"].(string); ok {
			names = append(names, name)
		}
	}
	return names
}

func requireEvent(t *testing.T, entries []map[string]any, name string) map[string]any {
	t.Helper()
	for _, entry := range entries {
		if entry["msg"] == name {
			return entry
		}
	}
	t.Fatalf("event %q not found in %v", name, eventNames(entries))
	return nil
}

func TestRecoveryObservability(t *testing.T) {
	var logBuffer bytes.Buffer
	log := slog.New(slog.NewJSONHandler(&logBuffer, nil))
	owner := fakeLifecycleOwnership{release: func() error { return nil }}
	deps := lifecycleStartupDeps{
		ensurePrivate: func(string) error { return nil },
		acquire: func(context.Context, string, time.Duration) (lifecycleOwnership, error) {
			return owner, nil
		},
		log: log,
	}
	records := []domain.CatalogueRecord{
		{RecoveryState: domain.RecoveryHealthy},
		{RecoveryState: domain.RecoveryHealthy},
		{RecoveryState: domain.RecoveryFresh},
		{RecoveryState: domain.RecoveryDegraded},
	}
	noticeStore := portsmocks.NewMockNoticeStore(t)
	var recoveryNotice domain.Notification
	noticeStore.EXPECT().Append(mock.Anything).Run(func(n domain.Notification) { recoveryNotice = n }).Return(nil).Once()
	require.NoError(t, runWithLifecycleOwnerDeps(context.Background(), "/runtime/vev", "/state/vev", func(context.Context) error {
		logCatalogueRecovery(log, records, "current")
		logTransactionRecovery(log, 2, 0)
		require.NoError(t, persistTransactionRecoveryNotices(noticeStore, []interruptedRecoveryIdentity{{name: "work", id: domain.IncarnationID{9}}}, time.Unix(1, 0)))
		logStartupRecoveryCounts(log, records, 0)
		return nil
	}, deps))

	entries := decodeJSONLogs(t, logBuffer.Bytes())
	for _, name := range []string{"lifecycle_owner_wait", "lifecycle_owner_acquired", "catalogue_validated", "catalogue_compaction_recovery_complete", "interrupted_transaction_recovery_complete", "daemon_startup_complete", "lifecycle_owner_released"} {
		require.Contains(t, eventNames(entries), name)
	}
	startup := requireEvent(t, entries, "daemon_startup_complete")
	require.EqualValues(t, 2, startup["healthy"])
	require.EqualValues(t, 1, startup["fresh"])
	require.EqualValues(t, 0, startup["restoring"])
	require.EqualValues(t, 1, startup["degraded"])
	recovered := requireEvent(t, entries, "interrupted_transaction_recovery_complete")
	require.EqualValues(t, 0, recovered["sessions_fenced"])
	require.Equal(t, "clean", recovered["outcome"])
	require.NotContains(t, eventNames(entries), "interrupted_transaction_fenced")

	require.NotContains(t, fmt.Sprint(requireEvent(t, entries, "interrupted_transaction_recovery_complete")), "terminal contents")
	require.Equal(t, domain.NoticeSnapshotRestore, recoveryNotice.Code)
	require.Equal(t, domain.SessionID(domain.IncarnationID{9}.String()), recoveryNotice.SessionID)
	require.NotContains(t, fmt.Sprint(recoveryNotice), "terminal contents")
}

// A session-scoped conflict no longer aborts startup, so the daemon comes up
// looking healthy. Startup must therefore name the fenced session in the log,
// persist a notice for it, and never report the recovery as clean.
func TestStartupReloadsCatalogueAfterRolledForwardDeletion(t *testing.T) {
	ctx := t.Context()
	stateDir := filepath.Join(t.TempDir(), "vev")
	catalogue := newTestPersister(t, stateDir)
	t.Cleanup(func() { _ = catalogue.Close() })
	repository := snapshot.NewRepository(filepath.Join(stateDir, "snapshots"))
	record := domain.CatalogueRecord{
		Name: "work", IncarnationID: domain.IncarnationID{9}, Cwd: t.TempDir(),
		CreatedAt: 1, UpdatedAt: 1, RecoveryState: domain.RecoveryDeleting,
	}
	require.NoError(t, catalogue.Save(record))
	coordinator := recovery.NewCoordinator(catalogue, repository, recoveryfs.New(stateDir), rand.Reader)

	records, err := recoverAuthoritativeCatalogue(ctx, coordinator, catalogue)
	require.NoError(t, err)
	require.Empty(t, records, "daemon startup must not retain the pre-recovery reservation")
	_, exists, err := catalogue.Record(record.Name)
	require.NoError(t, err)
	require.False(t, exists, "rolled-forward deletion must remain absent in authoritative state")
}

func TestFencedRecoveryObservability(t *testing.T) {
	// Keep the runtime path short enough for Darwin's smaller unix socket limit.
	runtimeRoot, err := os.MkdirTemp("/tmp", "vev-r-")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, os.RemoveAll(runtimeRoot)) })
	stateRoot := t.TempDir()
	t.Setenv("XDG_RUNTIME_DIR", runtimeRoot)
	t.Setenv("XDG_STATE_HOME", stateRoot)
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
	t.Setenv("VEV_PPROF_ADDR", "")

	stateDir := filepath.Join(stateRoot, "vev")
	repository := snapshot.NewRepository(filepath.Join(stateDir, "snapshots"))
	catalogue := newTestPersister(t, stateDir)
	live := domain.CatalogueRecord{
		Name: "work", IncarnationID: domain.IncarnationID{7}, Cwd: t.TempDir(),
		CreatedAt: 1, UpdatedAt: 1, RecoveryState: domain.RecoveryHealthy,
		Committed: &domain.CheckpointRef{Generation: 1, ManifestDigest: [32]byte{1}},
	}
	require.NoError(t, catalogue.Save(live))
	require.NoError(t, catalogue.Close(), "startup must reopen the on-disk catalogue")
	require.NoError(t, repository.WriteDeletionTombstone(t.Context(), domain.DeletionTombstone{
		Name: live.Name, IncarnationID: live.IncarnationID,
	}))

	// Pause immediately before socket publication so the test can reopen and
	// claim the notice before the daemon's restore worker consumes it.
	originalListenDaemon := listenDaemon
	listenEntered := make(chan struct{})
	allowListen := make(chan struct{})
	var allowListenOnce sync.Once
	releaseListen := func() { allowListenOnce.Do(func() { close(allowListen) }) }
	listenDaemon = func(dir string, observer ports.SerializedRuntimeObserver) (ports.Listener, error) {
		close(listenEntered)
		<-allowListen
		return originalListenDaemon(dir, observer)
	}
	t.Cleanup(func() {
		releaseListen()
		listenDaemon = originalListenDaemon
	})

	var logBuffer bytes.Buffer
	log := slog.New(slog.NewJSONHandler(&logBuffer, nil))
	ctx, cancel := context.WithCancel(t.Context())
	started := make(chan error, 1)
	go func() { started <- runDaemonOwnedWithLogger(ctx, log) }()

	var shutdownErr error
	var shutdownOnce sync.Once
	shutdown := func() error {
		shutdownOnce.Do(func() {
			releaseListen()
			cancel()
			select {
			case shutdownErr = <-started:
			case <-time.After(5 * time.Second):
				shutdownErr = errors.New("daemon did not stop after cancellation")
			}
		})
		return shutdownErr
	}
	t.Cleanup(func() { require.NoError(t, shutdown()) })

	select {
	case <-listenEntered:
	case <-time.After(5 * time.Second):
		t.Fatal("startup did not reach socket publication")
	}
	reopenedNotices := noticefile.New(stateDir)
	pending, err := reopenedNotices.Claim()
	require.NoError(t, err)
	require.Len(t, pending, 2, "startup persists both the interrupted and fenced recovery notices")
	var fencedNotice *domain.Notification
	for i := range pending {
		if strings.Contains(pending[i].Details, "reason_code=deletion-tombstone-conflict") {
			fencedNotice = &pending[i]
		}
	}
	require.NotNil(t, fencedNotice)
	require.Equal(t, domain.NoticeSnapshotRestore, fencedNotice.Code)
	require.Equal(t, domain.NoticeWarn, fencedNotice.Severity)
	require.Equal(t, domain.SessionID(live.IncarnationID.String()), fencedNotice.SessionID)
	require.Contains(t, fencedNotice.Details, "session="+live.Name)
	require.NotContains(t, fmt.Sprint(fencedNotice), "conflicts with catalogue")
	require.NoError(t, reopenedNotices.Ack(), "notice claim lock must be released before daemon startup resumes")

	releaseListen()
	socketPath := filepath.Join(runtimeRoot, "vev", "daemon.sock")
	require.Eventually(t, func() bool {
		_, statErr := os.Stat(socketPath)
		return statErr == nil
	}, 5*time.Second, time.Millisecond, "fenced recovery must not prevent socket publication")
	require.NoError(t, shutdown(), "fenced recovery must allow clean daemon startup and shutdown")

	entries := decodeJSONLogs(t, logBuffer.Bytes())
	event := requireEvent(t, entries, "interrupted_transaction_fenced")
	require.Equal(t, "WARN", event["level"])
	require.Equal(t, live.Name, event["session"])
	require.Equal(t, live.IncarnationID.String(), event["incarnation"])
	require.Equal(t, "deletion-tombstone", event["kind"])
	require.Equal(t, "deletion-tombstone-conflict", event["reason_code"])
	recovered := requireEvent(t, entries, "interrupted_transaction_recovery_complete")
	require.EqualValues(t, 1, recovered["sessions_fenced"])
	require.Equal(t, "fenced", recovered["outcome"])
	startup := requireEvent(t, entries, "daemon_startup_complete")
	require.EqualValues(t, 1, startup["degraded"])
	require.NotContains(t, logBuffer.String(), "conflicts with catalogue", "raw conflict text must not be logged")

	reopenedCatalogue := newTestPersister(t, stateDir)
	t.Cleanup(func() { require.NoError(t, reopenedCatalogue.Close()) })
	degraded, ok, err := reopenedCatalogue.Record(live.Name)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, domain.RecoveryDegraded, degraded.RecoveryState)
}

func TestFencedRecoveryNoticeFailurePropagates(t *testing.T) {
	store := portsmocks.NewMockNoticeStore(t)
	store.EXPECT().Append(mock.Anything).Return(errors.New("disk full")).Once()
	err := persistFencedRecoveryNotices(store, []fencedRecoveryIdentity{{
		name: "work", id: domain.IncarnationID{1}, kind: "discard-intent", reasonCode: "discard-intent-conflict",
	}}, time.Unix(1, 0))
	require.ErrorContains(t, err, "session \"work\"")
	require.ErrorContains(t, err, domain.IncarnationID{1}.String())
}

func TestTransactionRecoveryNoticeFailurePropagates(t *testing.T) {
	store := portsmocks.NewMockNoticeStore(t)
	store.EXPECT().Append(mock.Anything).Return(errors.New("disk full")).Once()
	err := persistTransactionRecoveryNotices(store, []interruptedRecoveryIdentity{{name: "work", id: domain.IncarnationID{1}}}, time.Unix(1, 0))
	require.ErrorContains(t, err, "session \"work\"")
	require.ErrorContains(t, err, domain.IncarnationID{1}.String())
}

type fakeLifecycleOwnership struct {
	release func() error
}

func (o fakeLifecycleOwnership) Release() error { return o.release() }

type fakeLifecycleProbe struct {
	owner lifecycleOwnership
	err   error
}

func (p fakeLifecycleProbe) TryAcquire(string) (lifecycleOwnership, error) {
	return p.owner, p.err
}

func TestJoinLifecycleReleaseError(t *testing.T) {
	operationErr := errors.New("operation failed")
	releaseErr := errors.New("release failed")

	tests := []struct {
		name         string
		operationErr error
		releaseErr   error
		wantErrors   []error
	}{
		{
			name:       "successful operation preserves release failure",
			releaseErr: releaseErr,
			wantErrors: []error{releaseErr},
		},
		{
			name:         "operation and release failures are joined",
			operationErr: operationErr,
			releaseErr:   releaseErr,
			wantErrors:   []error{operationErr, releaseErr},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.operationErr
			joinLifecycleReleaseError(&got, fakeLifecycleOwnership{release: func() error {
				return tt.releaseErr
			}})

			for _, wantErr := range tt.wantErrors {
				require.ErrorIs(t, got, wantErr)
			}
		})
	}
}

func TestOfflineCommandsPropagateLifecycleReleaseErrors(t *testing.T) {
	releaseErr := errors.New("release failed")

	tests := []struct {
		name             string
		run              func(context.Context) error
		wantOperationErr string
	}{
		{
			name: "list joins release failure to success",
			run:  runList,
		},
		{
			name: "kill joins release failure to operation failure",
			run: func(ctx context.Context) error {
				return runKill(ctx, "", false, false)
			},
			wantOperationErr: "vev: no daemon running",
		},
	}

	originalProbe := daemonLifecycleProbe
	t.Cleanup(func() { daemonLifecycleProbe = originalProbe })
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("XDG_RUNTIME_DIR", t.TempDir())
			t.Setenv("XDG_STATE_HOME", t.TempDir())
			daemonLifecycleProbe = fakeLifecycleProbe{
				owner: fakeLifecycleOwnership{release: func() error { return releaseErr }},
			}

			err := tt.run(context.Background())
			require.ErrorIs(t, err, releaseErr)
			if tt.wantOperationErr != "" {
				require.ErrorContains(t, err, tt.wantOperationErr)
			}
		})
	}
}

func TestCatalogueFailureDoesNotListen(t *testing.T) {
	originalOpenOrMigrate, originalListenDaemon := openOrMigrate, listenDaemon
	t.Cleanup(func() {
		openOrMigrate = originalOpenOrMigrate
		listenDaemon = originalListenDaemon
	})

	for _, catalogueErr := range []error{errors.New("catalogue corrupt"), errors.New("catalogue unavailable")} {
		t.Run(catalogueErr.Error(), func(t *testing.T) {
			runtimeRoot, stateRoot := t.TempDir(), t.TempDir()
			runtimeDir := filepath.Join(runtimeRoot, "vev")
			stateDir := filepath.Join(stateRoot, "vev")
			t.Setenv("XDG_RUNTIME_DIR", runtimeRoot)
			t.Setenv("XDG_STATE_HOME", stateRoot)

			listenCalls := 0
			openOrMigrate = func(_ context.Context, deps persist.OpenDeps) (persist.OpenResult, error) {
				require.Equal(t, stateDir, deps.StateDir)
				return persist.OpenResult{}, catalogueErr
			}
			listenDaemon = func(string, ports.SerializedRuntimeObserver) (ports.Listener, error) {
				listenCalls++
				return nil, errors.New("IPC listen must not run after catalogue failure")
			}

			err := runWithLifecycleOwner(context.Background(), runtimeDir, stateDir, func(ctx context.Context) error {
				return runDaemonOwnedWithLogger(ctx, slog.New(slog.NewTextHandler(io.Discard, nil)))
			})
			require.ErrorIs(t, err, catalogueErr)
			require.Zero(t, listenCalls)
			require.NoFileExists(t, filepath.Join(runtimeDir, "daemon.sock"))

			owner, acquireErr := lifecycle.TryAcquire(runtimeDir)
			require.NoError(t, acquireErr, "catalogue failure must release lifecycle ownership")
			require.NoError(t, owner.Release())
		})
	}
}

func TestCatalogueRegistryConstructionPrecedesSocketPublication(t *testing.T) {
	var events []string
	_, _, err := constructDaemonBeforeSocketPublication(
		func() *daemon.Daemon {
			events = append(events, "catalogue-registry")
			return nil
		},
		func() (ports.Listener, error) {
			require.Equal(t, []string{"catalogue-registry"}, events)
			events = append(events, "socket-publication")
			return nil, nil
		},
	)
	require.NoError(t, err)
	require.Equal(t, []string{"catalogue-registry", "socket-publication"}, events)
}

func TestLifecycleOwnershipPrecedesDaemonStartup(t *testing.T) {
	t.Run("ownership prefix is exact", func(t *testing.T) {
		var events []string
		owner := fakeLifecycleOwnership{release: func() error {
			events = append(events, "unlock")
			return nil
		}}
		deps := lifecycleStartupDeps{
			ensurePrivate: func(path string) error {
				if path == "runtime" {
					events = append(events, "ensure-runtime")
				} else {
					events = append(events, "ensure-state")
				}
				return nil
			},
			acquire: func(context.Context, string, time.Duration) (lifecycleOwnership, error) {
				events = append(events, "lock")
				return owner, nil
			},
		}

		err := runWithLifecycleOwnerDeps(context.Background(), "runtime", "state", func(context.Context) error {
			events = append(events, "durable-open", "listen", "serve")
			return nil
		}, deps)
		require.NoError(t, err)
		require.Equal(t, []string{"ensure-runtime", "ensure-state", "lock"}, events[:3])
		require.Equal(t, []string{"ensure-runtime", "ensure-state", "lock", "durable-open", "listen", "serve", "unlock"}, events)
	})

	t.Run("callback observes held lock and failure releases it", func(t *testing.T) {
		runtimeDir := filepath.Join(t.TempDir(), "runtime")
		stateDir := filepath.Join(t.TempDir(), "state")
		startErr := errors.New("catalogue corrupt")

		err := runWithLifecycleOwner(context.Background(), runtimeDir, stateDir, func(context.Context) error {
			_, lockErr := lifecycle.TryAcquire(runtimeDir)
			require.ErrorIs(t, lockErr, lifecycle.ErrBusy)
			return startErr
		})
		require.ErrorIs(t, err, startErr)

		owner, err := lifecycle.TryAcquire(runtimeDir)
		require.NoError(t, err, "callback failure must release lifecycle ownership")
		require.NoError(t, owner.Release())
	})

	t.Run("busy and unavailable ownership fail before startup", func(t *testing.T) {
		runtimeDir := filepath.Join(t.TempDir(), "runtime")
		stateDir := filepath.Join(t.TempDir(), "state")
		owner, err := lifecycle.TryAcquire(runtimeDir)
		require.NoError(t, err)
		defer func() { require.NoError(t, owner.Release()) }()

		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		started := false
		err = runWithLifecycleOwner(ctx, runtimeDir, stateDir, func(context.Context) error {
			started = true
			return nil
		})
		require.ErrorIs(t, err, context.Canceled)
		require.False(t, started)

		badState := filepath.Join(t.TempDir(), "state-file")
		require.NoError(t, os.WriteFile(badState, []byte("unavailable"), 0o600))
		err = runWithLifecycleOwner(context.Background(), filepath.Join(t.TempDir(), "other-runtime"), badState, func(context.Context) error {
			started = true
			return nil
		})
		require.Error(t, err)
		require.False(t, started)
	})

	t.Run("corrupt catalogue fails before socket publication", func(t *testing.T) {
		runtimeRoot, stateRoot := t.TempDir(), t.TempDir()
		runtimeDir := filepath.Join(runtimeRoot, "vev")
		stateDir := filepath.Join(stateRoot, "vev")
		require.NoError(t, os.Mkdir(stateDir, 0o700))
		store, err := kv.Open(persist.StorePath(stateDir))
		require.NoError(t, err)
		require.NoError(t, store.Set([]byte("work"), []byte("malformed catalogue value")))
		require.NoError(t, store.Close())

		t.Setenv("XDG_RUNTIME_DIR", runtimeRoot)
		t.Setenv("XDG_STATE_HOME", stateRoot)
		err = runWithLifecycleOwner(context.Background(), runtimeDir, stateDir, runDaemonOwned)
		require.Error(t, err)
		require.Contains(t, err.Error(), "open or migrate durable session state")
		_, statErr := os.Stat(filepath.Join(runtimeDir, "daemon.sock"))
		require.ErrorIs(t, statErr, os.ErrNotExist)
	})

	t.Run("proven absence creates the catalogue before socket publication", func(t *testing.T) {
		runtimeRoot, err := os.MkdirTemp("/tmp", "vev-")
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, os.RemoveAll(runtimeRoot)) })
		stateRoot := t.TempDir()
		runtimeDir := filepath.Join(runtimeRoot, "vev")
		stateDir := filepath.Join(stateRoot, "vev")
		t.Setenv("XDG_RUNTIME_DIR", runtimeRoot)
		t.Setenv("XDG_STATE_HOME", stateRoot)
		ctx, cancel := context.WithCancel(t.Context())
		result := make(chan error, 1)
		go func() {
			result <- runWithLifecycleOwner(ctx, runtimeDir, stateDir, runDaemonOwned)
		}()
		t.Cleanup(func() {
			cancel()
			select {
			case err := <-result:
				require.NoError(t, err)
			case <-time.After(5 * time.Second):
				t.Error("daemon lifecycle did not stop after cancellation")
			}
		})

		require.Eventually(t, func() bool {
			_, err := os.Stat(filepath.Join(runtimeDir, "daemon.sock"))
			return err == nil
		}, 5*time.Second, time.Millisecond, "daemon socket was not published")
		require.FileExists(t, persist.StorePath(stateDir))
	})

	t.Run("callback and release errors are joined", func(t *testing.T) {
		startErr := errors.New("catalogue unavailable")
		releaseErr := errors.New("unlock failed")
		deps := lifecycleStartupDeps{
			ensurePrivate: func(string) error { return nil },
			acquire: func(context.Context, string, time.Duration) (lifecycleOwnership, error) {
				return fakeLifecycleOwnership{release: func() error { return releaseErr }}, nil
			},
		}

		err := runWithLifecycleOwnerDeps(context.Background(), "runtime", "state", func(context.Context) error {
			return startErr
		}, deps)
		require.ErrorIs(t, err, startErr)
		require.ErrorIs(t, err, releaseErr)
	})
}

// scriptRecv makes a transport yield frames in order, then wait for done
// before returning EOF. The shared done channel coordinates proxy readers
// without relying on scheduling or sleeps.
func scriptRecv(tr *portsmocks.MockTransport, done <-chan struct{}, frames ...ports.Frame) *portsmocks.MockTransport_Recv_Call {
	script := make(chan ports.Frame, len(frames))
	for _, frame := range frames {
		script <- frame
	}
	return tr.EXPECT().Recv().RunAndReturn(func() (ports.Frame, error) {
		select {
		case frame := <-script:
			return frame, nil
		case <-done:
			return ports.Frame{}, io.EOF
		}
	})
}

func TestFirstHelloTransportReplacesRemoteEnvironment(t *testing.T) {
	for _, tt := range []struct {
		name     string
		session  string
		wantName string
	}{
		{name: "stdio replaces environment", wantName: ""},
		{name: "udp proxy replaces environment and retains session rewrite", session: "work", wantName: "work"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			original := ports.Hello{
				Version:           ports.ProtocolVersion,
				Intent:            ports.IntentAttach,
				ClientID:          [16]byte{1},
				ResumeToken:       2,
				Size:              domain.Size{Cols: 80, Rows: 24},
				TermEnv:           "client-term",
				Cwd:               "/client",
				TrueColor:         true,
				MaxOutputInFlight: 8,
				Env:               []string{"CLIENT=value"},
			}
			nonHello := ports.Frame{Type: ports.MsgInput, Payload: []byte("unchanged")}
			transport := portsmocks.NewMockTransport(t)
			done := make(chan struct{})
			scriptRecv(transport, done,
				ports.Frame{Type: ports.MsgHello, Payload: ports.MarshalHello(original)},
				nonHello,
			).Times(2)
			wrapped := newFirstHelloTransport(transport, tt.session, []string{"REMOTE=one", "TOKEN=a=b=c"})

			frame, err := wrapped.Recv()
			require.NoError(t, err)
			got, err := ports.UnmarshalHello(frame.Payload)
			require.NoError(t, err)
			original.Name = tt.wantName
			original.Env = []string{"REMOTE=one", "TOKEN=a=b=c"}
			require.Equal(t, original, got)

			frame, err = wrapped.Recv()
			require.NoError(t, err)
			require.Equal(t, nonHello, frame)
		})
	}
}

func TestRunStdioProxyReplacesHelloEnvironmentAtDaemonBoundary(t *testing.T) {
	original := remoteProxyHello()
	client, daemon, sent := newRemoteProxyTransports(t, original)

	err := runStdioProxy(context.Background(), client, daemon, []string{"REMOTE=one", "TOKEN=a=b=c"}, nil)
	require.NoError(t, err)

	got := firstHello(t, sent)
	original.Env = []string{"REMOTE=one", "TOKEN=a=b=c"}
	require.Equal(t, original, got)
}

func TestRunUDPProxyRuntimeReplacesHelloEnvironmentAndSessionAtDaemonBoundary(t *testing.T) {
	original := remoteProxyHello()
	client, daemon, sent := newRemoteProxyTransports(t, original)

	err := runUDPProxyRuntime(context.Background(), "remote-work", client, daemon, []string{"REMOTE=udp", "TOKEN=a=b=c"}, nil)
	require.NoError(t, err)

	got := firstHello(t, sent)
	original.Name = "remote-work"
	original.Env = []string{"REMOTE=udp", "TOKEN=a=b=c"}
	require.Equal(t, original, got)
}

func remoteProxyHello() ports.Hello {
	return ports.Hello{
		Version:           ports.ProtocolVersion,
		Intent:            ports.IntentAttach,
		ClientID:          [16]byte{1},
		ResumeToken:       2,
		Size:              domain.Size{Cols: 80, Rows: 24},
		TermEnv:           "client-term",
		Cwd:               "/client",
		TrueColor:         true,
		MaxOutputInFlight: 1,
		Env:               []string{"CLIENT=value"},
	}
}

func newRemoteProxyTransports(t *testing.T, hello ports.Hello) (*portsmocks.MockTransport, *portsmocks.MockTransport, <-chan ports.Frame) {
	t.Helper()
	client := portsmocks.NewMockTransport(t)
	daemon := portsmocks.NewMockTransport(t)
	clientDone := make(chan struct{})
	daemonDone := make(chan struct{})
	sent := make(chan ports.Frame, 1)

	scriptRecv(client, clientDone, ports.Frame{Type: ports.MsgHello, Payload: ports.MarshalHello(hello)}).Maybe()
	client.EXPECT().Close().RunAndReturn(func() error {
		close(clientDone)
		return nil
	}).Once()
	daemon.EXPECT().Send(mock.Anything).RunAndReturn(func(frame ports.Frame) error {
		sent <- frame
		close(daemonDone)
		return nil
	}).Once()
	scriptRecv(daemon, daemonDone).Once()
	daemon.EXPECT().Close().Return(nil).Once()

	return client, daemon, sent
}

func firstHello(t *testing.T, sent <-chan ports.Frame) ports.Hello {
	t.Helper()
	frame := <-sent
	hello, err := ports.UnmarshalHello(frame.Payload)
	require.NoError(t, err)
	return hello
}

func TestRunUDPProxyUsesBoundedClientMaxPending(t *testing.T) {
	require.Equal(t, 32, udpProxyClientTransportOptions.MaxPending)
}

func TestRunUDPProxyClientDeadAfterExceedsIdleTTL(t *testing.T) {
	require.Positive(t, udpProxyIdleTTL)
	require.Greater(t, udpProxyClientTransportOptions.DeadAfter, udpProxyIdleTTL)
}

func TestParseArgs(t *testing.T) {
	tests := []struct {
		name       string
		args       []string
		wantKind   cmdKind
		wantIntent uint8
		wantName   string
		wantRemote string
		wantAll    bool
		wantDaemon bool
		wantErr    bool
	}{
		{name: "no args -> ephemeral attach", args: nil, wantKind: kindAttach, wantIntent: ports.IntentEphemeral},
		{name: "empty slice -> ephemeral attach", args: []string{}, wantKind: kindAttach, wantIntent: ports.IntentEphemeral},
		{name: "new named", args: []string{"new", "work"}, wantKind: kindAttach, wantIntent: ports.IntentNew, wantName: "work"},
		{name: "new without name", args: []string{"new"}, wantErr: true},
		{name: "new empty name", args: []string{"new", ""}, wantErr: true},
		{name: "new command override unsupported", args: []string{"new", "work", "--", "sh"}, wantErr: true},
		{name: "attach named", args: []string{"attach", "work"}, wantKind: kindAttach, wantIntent: ports.IntentAttach, wantName: "work"},
		{name: "attach preserves legacy unsafe name", args: []string{"attach", "my work"}, wantKind: kindAttach, wantIntent: ports.IntentAttach, wantName: "my work"},
		{name: "attach alias a", args: []string{"a", "work"}, wantKind: kindAttach, wantIntent: ports.IntentAttach, wantName: "work"},
		{name: "attach remote without session uses ephemeral", args: []string{"attach", "user@example.com"}, wantKind: kindAttach, wantIntent: ports.IntentEphemeral, wantRemote: "user@example.com"},
		{name: "attach remote with empty session uses ephemeral", args: []string{"attach", "user@example.com:"}, wantKind: kindAttach, wantIntent: ports.IntentEphemeral, wantRemote: "user@example.com"},
		{name: "attach remote with session", args: []string{"attach", "user@example.com:work"}, wantKind: kindAttach, wantIntent: ports.IntentAttach, wantName: "work", wantRemote: "user@example.com"},
		{name: "attach extra arg", args: []string{"attach", "work", "extra"}, wantErr: true},
		{name: "attach without name", args: []string{"attach"}, wantErr: true},
		{name: "ls", args: []string{"ls"}, wantKind: kindList},
		{name: "list", args: []string{"list"}, wantKind: kindList},
		{name: "kill named", args: []string{"kill", "work"}, wantKind: kindKill, wantName: "work"},
		{name: "kill preserves legacy unsafe name via terminator", args: []string{"kill", "--", "my work"}, wantKind: kindKill, wantName: "my work"},
		{name: "kill dashed name via terminator", args: []string{"kill", "--", "--all"}, wantKind: kindKill, wantName: "--all"},
		{name: "kill all", args: []string{"kill", "--all"}, wantKind: kindKill, wantAll: true},
		{name: "kill daemon", args: []string{"kill", "--daemon"}, wantKind: kindKill, wantDaemon: true},
		{name: "kill without name", args: []string{"kill"}, wantErr: true},
		{name: "kill terminator without name", args: []string{"kill", "--"}, wantErr: true},
		{name: "kill all rejects extra arg", args: []string{"kill", "--all", "extra"}, wantErr: true},
		{name: "kill daemon rejects extra arg", args: []string{"kill", "--daemon", "extra"}, wantErr: true},
		{name: "kill extra arg", args: []string{"kill", "work", "extra"}, wantErr: true},
		{name: "daemon", args: []string{"--daemon"}, wantKind: kindDaemon},
		{name: "stdio", args: []string{"_stdio"}, wantKind: kindStdio},
		{name: "stdio with session", args: []string{"_stdio", "work"}, wantKind: kindStdio, wantName: "work"},
		{name: "stdio preserves legacy unsafe name", args: []string{"_stdio", "my work"}, wantKind: kindStdio, wantName: "my work"},
		{name: "stdio too many args", args: []string{"_stdio", "work", "extra"}, wantErr: true},
		{name: "udp bootstrap", args: []string{"_udp-bootstrap", "work"}, wantKind: kindUDPBootstrap, wantName: "work"},
		{name: "udp proxy", args: []string{"_udp-proxy", "work"}, wantKind: kindUDPProxy, wantName: "work"},
		{name: "udp proxy too many args", args: []string{"_udp-proxy", "work", "extra"}, wantErr: true},
		{name: "help", args: []string{"--help"}, wantKind: kindHelp},
		{name: "help subcommand", args: []string{"help"}, wantKind: kindHelp},
		{name: "version", args: []string{"--version"}, wantKind: kindVersion},
		{name: "unknown", args: []string{"frobnicate"}, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseArgs(tt.args)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("parseArgs(%q) = %+v, want error", tt.args, got)
				}
				var ue *usageError
				if !errors.As(err, &ue) {
					t.Fatalf("parseArgs(%q) error = %T, want *usageError", tt.args, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("parseArgs(%q) unexpected error: %v", tt.args, err)
			}
			if got.kind != tt.wantKind {
				t.Errorf("kind = %v, want %v", got.kind, tt.wantKind)
			}
			if got.kind == kindAttach && got.intent != tt.wantIntent {
				t.Errorf("intent = %d, want %d", got.intent, tt.wantIntent)
			}
			if got.name != tt.wantName {
				t.Errorf("name = %q, want %q", got.name, tt.wantName)
			}
			if got.remoteTarget != tt.wantRemote {
				t.Errorf("remoteTarget = %q, want %q", got.remoteTarget, tt.wantRemote)
			}
			if got.killAll != tt.wantAll {
				t.Errorf("killAll = %v, want %v", got.killAll, tt.wantAll)
			}
			if got.killDaemon != tt.wantDaemon {
				t.Errorf("killDaemon = %v, want %v", got.killDaemon, tt.wantDaemon)
			}
		})
	}
}

func TestParseArgsNewRejectsUnsafeSessionName(t *testing.T) {
	_, err := parseArgs([]string{"new", "my work"})
	if !errors.Is(err, domain.ErrInvalidSessionName) {
		t.Fatalf("parseArgs new unsafe error = %v, want %v", err, domain.ErrInvalidSessionName)
	}
}

func TestListShowsDegraded(t *testing.T) {
	var out bytes.Buffer
	printSessions(&out, []ports.SessionInfo{
		{Name: "fresh", State: ports.SessionStopped},
		{Name: "loading", State: ports.SessionRestoring},
		{Name: "broken", State: ports.SessionDegraded},
	})
	require.Contains(t, out.String(), "fresh")
	require.Contains(t, out.String(), "stopped")
	require.Contains(t, out.String(), "loading")
	require.Contains(t, out.String(), "restoring")
	require.Contains(t, out.String(), "broken")
	require.Contains(t, out.String(), "degraded")
}

func TestPrintSessionsShowsStoppedState(t *testing.T) {
	var out bytes.Buffer
	printSessions(&out, []ports.SessionInfo{
		{Name: "main", State: ports.SessionRunning, Tabs: 2, Attached: true},
		{Name: "old", State: ports.SessionStopped},
	})
	got := out.String()
	for _, want := range []string{"NAME", "STATE", "main", "running", "2", "yes", "old", "stopped", "-"} {
		if !strings.Contains(got, want) {
			t.Fatalf("printSessions output %q missing %q", got, want)
		}
	}
}

func TestPrintSessionsMarksEphemeral(t *testing.T) {
	var buf bytes.Buffer
	printSessions(&buf, []ports.SessionInfo{
		{Name: "0", State: ports.SessionRunning, Ephemeral: true, Tabs: 1, Attached: false},
		{Name: "work", State: ports.SessionRunning, Tabs: 2, Attached: true},
		{Name: "old", State: ports.SessionStopped},
	})
	out := buf.String()
	for _, want := range []string{"0", "temporary", "work", "running", "old", "stopped"} {
		require.Contains(t, out, want)
	}
}

func newTestPersister(t *testing.T, stateDir string) *persist.Persister {
	t.Helper()
	store, err := persist.OpenStore(persist.StorePath(stateDir))
	require.NoError(t, err)
	return persist.New(store)
}

func TestRunListReadsStoppedSessionsWithoutDaemon(t *testing.T) {
	stateRoot, runtimeRoot := t.TempDir(), t.TempDir()
	t.Setenv("XDG_STATE_HOME", stateRoot)
	t.Setenv("XDG_RUNTIME_DIR", runtimeRoot)

	p := newTestPersister(t, filepath.Join(stateRoot, "vev"))
	now := time.Now().UnixNano()
	if err := p.Save(persist.Record{Name: "stored", IncarnationID: domain.IncarnationID{1}, Cwd: t.TempDir(), CreatedAt: now, UpdatedAt: now, RecoveryState: domain.RecoveryFresh}); err != nil {
		t.Fatalf("Save error = %v", err)
	}
	if err := p.Close(); err != nil {
		t.Fatalf("Close error = %v", err)
	}

	got := captureStdout(t, func() {
		if err := runList(context.Background()); err != nil {
			t.Fatalf("runList error = %v", err)
		}
	})
	for _, want := range []string{"stored", "stopped", "-"} {
		if !strings.Contains(got, want) {
			t.Fatalf("runList output %q missing %q", got, want)
		}
	}
}

func TestRunKillMissingStoppedSessionDoesNotCreateStore(t *testing.T) {
	stateRoot, runtimeRoot := t.TempDir(), t.TempDir()
	t.Setenv("XDG_STATE_HOME", stateRoot)
	t.Setenv("XDG_RUNTIME_DIR", runtimeRoot)

	err := runKill(context.Background(), "missing", false, false)
	if err == nil || !strings.Contains(err.Error(), "no such session: missing") {
		t.Fatalf("runKill error = %v, want no such session", err)
	}
	if _, statErr := os.Stat(filepath.Join(stateRoot, "vev", "sessions.kv")); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("sessions.kv stat error = %v, want not exist", statErr)
	}
}

func TestRunKillDeletesStoppedSessionWithoutDaemon(t *testing.T) {
	stateRoot, runtimeRoot := t.TempDir(), t.TempDir()
	t.Setenv("XDG_STATE_HOME", stateRoot)
	t.Setenv("XDG_RUNTIME_DIR", runtimeRoot)

	p := newTestPersister(t, filepath.Join(stateRoot, "vev"))
	now := time.Now().UnixNano()
	if err := p.Save(persist.Record{Name: "stored", IncarnationID: domain.IncarnationID{1}, Cwd: t.TempDir(), CreatedAt: now, UpdatedAt: now, RecoveryState: domain.RecoveryFresh}); err != nil {
		t.Fatalf("Save error = %v", err)
	}
	if err := p.Close(); err != nil {
		t.Fatalf("Close error = %v", err)
	}
	snapshots := snapshot.NewRepository(filepath.Join(stateRoot, "vev", "snapshots"))

	got := captureStdout(t, func() {
		if err := runKill(context.Background(), "stored", false, false); err != nil {
			t.Fatalf("runKill error = %v", err)
		}
	})
	if !strings.Contains(got, "killed stored") {
		t.Fatalf("runKill output = %q, want success", got)
	}
	records, err := persist.LoadReadOnly(filepath.Join(stateRoot, "vev"))
	if err != nil {
		t.Fatalf("LoadReadOnly error = %v", err)
	}
	if len(records) != 0 {
		t.Fatalf("records after kill = %#v, want none", records)
	}
	page, err := snapshots.ListDeletionTombstones(context.Background(), ports.DeletionTombstoneCursor{}, ports.MaintenanceBudget{Entries: 1, Bytes: 1024})
	if err != nil {
		t.Fatalf("list deletion tombstones: %v", err)
	}
	if len(page.Tombstones) != 0 {
		t.Fatalf("deletion tombstones after kill = %#v, want none", page.Tombstones)
	}
}

func captureStdout(t *testing.T, fn func()) string {
	t.Helper()
	old := os.Stdout
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("os.Pipe error = %v", err)
	}
	os.Stdout = w
	defer func() { os.Stdout = old }()
	defer func() { _ = r.Close() }()
	outCh := make(chan []byte, 1)
	errCh := make(chan error, 1)
	go func() {
		out, err := io.ReadAll(r)
		outCh <- out
		errCh <- err
	}()

	fn()
	_ = w.Close()
	out := <-outCh
	if err := <-errCh; err != nil {
		t.Fatalf("ReadAll stdout error = %v", err)
	}
	return string(out)
}

func TestRunLocalAttachPromptsAndRestartsOnProtocolMismatch(t *testing.T) {
	var prompts bytes.Buffer
	answers := strings.NewReader("y\n")
	attachCalls := 0
	killCalls := 0

	err := runLocalAttachWithRecovery(context.Background(), ports.IntentEphemeral, "", attachRecoveryDeps{
		confirmer: confirm.NewConfirmer(answers, &prompts),
		attach: func(context.Context, uint8, string) error {
			attachCalls++
			if attachCalls == 1 {
				return &client.ProtocolError{Code: ports.ErrVersionMismatch, Text: "protocol version mismatch"}
			}
			return nil
		},
		killDaemon: func(context.Context) error {
			killCalls++
			return nil
		},
	})
	if err != nil {
		t.Fatalf("runLocalAttachWithRecovery returned error: %v", err)
	}
	if attachCalls != 2 {
		t.Fatalf("attach calls = %d, want 2", attachCalls)
	}
	if killCalls != 1 {
		t.Fatalf("kill calls = %d, want 1", killCalls)
	}
	if got := prompts.String(); !strings.Contains(got, "Your vev version differs") || !strings.Contains(got, "kill it") {
		t.Fatalf("prompt = %q, want version/kill prompt", got)
	}
}

func TestRunLocalAttachPropagatesPromptError(t *testing.T) {
	readErr := errors.New("read failed")
	err := runLocalAttachWithRecovery(context.Background(), ports.IntentEphemeral, "", attachRecoveryDeps{
		confirmer: confirm.NewConfirmer(errorReader{err: readErr}, &bytes.Buffer{}),
		attach: func(context.Context, uint8, string) error {
			return &client.ProtocolError{Code: ports.ErrVersionMismatch, Text: "protocol version mismatch"}
		},
		killDaemon: func(context.Context) error {
			t.Fatal("killDaemon should not be called after prompt error")
			return nil
		},
	})
	if !errors.Is(err, readErr) {
		t.Fatalf("error = %v, want wrapped %v", err, readErr)
	}
}

func TestRunLocalAttachPropagatesKillError(t *testing.T) {
	killErr := errors.New("kill failed")
	err := runLocalAttachWithRecovery(context.Background(), ports.IntentEphemeral, "", attachRecoveryDeps{
		confirmer: confirm.NewConfirmer(strings.NewReader("y\n"), &bytes.Buffer{}),
		attach: func(context.Context, uint8, string) error {
			return &client.ProtocolError{Code: ports.ErrVersionMismatch, Text: "protocol version mismatch"}
		},
		killDaemon: func(context.Context) error { return killErr },
	})
	if !errors.Is(err, killErr) {
		t.Fatalf("error = %v, want %v", err, killErr)
	}
}

func TestRunLocalAttachSettlesBeforeRetry(t *testing.T) {
	var order []string
	err := runLocalAttachWithRecovery(context.Background(), ports.IntentEphemeral, "", attachRecoveryDeps{
		confirmer: confirm.NewConfirmer(strings.NewReader("y\n"), &bytes.Buffer{}),
		attach: func(context.Context, uint8, string) error {
			order = append(order, "attach")
			if len(order) == 1 {
				return &client.ProtocolError{Code: ports.ErrVersionMismatch, Text: "protocol version mismatch"}
			}
			return nil
		},
		killDaemon: func(context.Context) error {
			order = append(order, "kill")
			return nil
		},
		settleAfterKill: func(context.Context) error {
			order = append(order, "settle")
			return nil
		},
	})
	if err != nil {
		t.Fatalf("runLocalAttachWithRecovery returned error: %v", err)
	}
	if got, want := strings.Join(order, ","), "attach,kill,settle,attach"; got != want {
		t.Fatalf("order = %s, want %s", got, want)
	}
}

type errorReader struct{ err error }

func (r errorReader) Read([]byte) (int, error) { return 0, r.err }

func TestRunLocalAttachDeclineKeepsOriginalError(t *testing.T) {
	answers := strings.NewReader("n\n")
	wantErr := &client.ProtocolError{Code: ports.ErrInternal, Text: "malformed hello"}
	attachCalls := 0
	killCalls := 0

	err := runLocalAttachWithRecovery(context.Background(), ports.IntentEphemeral, "", attachRecoveryDeps{
		confirmer: confirm.NewConfirmer(answers, &bytes.Buffer{}),
		attach: func(context.Context, uint8, string) error {
			attachCalls++
			return wantErr
		},
		killDaemon: func(context.Context) error {
			killCalls++
			return nil
		},
	})
	if !errors.Is(err, wantErr) {
		t.Fatalf("error = %v, want original %v", err, wantErr)
	}
	if attachCalls != 1 {
		t.Fatalf("attach calls = %d, want 1", attachCalls)
	}
	if killCalls != 0 {
		t.Fatalf("kill calls = %d, want 0", killCalls)
	}
}

func TestRunAttachRejectsNestedVEVBeforeDial(t *testing.T) {
	called := false
	err := runAttachWithDeps(context.Background(), ports.IntentEphemeral, "", "", "outer", nil, runAttachDeps{
		createDetached: func(context.Context, string) error {
			called = true
			return nil
		},
	})
	if err == nil {
		t.Fatal("runAttach with VEV set returned nil error")
	}
	if called {
		t.Fatal("runAttach dialed while nested attach should be rejected")
	}
	if !strings.Contains(err.Error(), "sessions should be nested with care") {
		t.Fatalf("runAttach error = %q, want nested-session warning", err)
	}
}

func TestRunAttachNestedNewCreatesDetachedSession(t *testing.T) {
	var gotName string
	err := runAttachWithDeps(context.Background(), ports.IntentNew, "scratch", "", "outer", nil, runAttachDeps{
		createDetached: func(_ context.Context, name string) error {
			gotName = name
			return nil
		},
	})
	if err != nil {
		t.Fatalf("runAttachWithDeps returned error: %v", err)
	}
	if gotName != "scratch" {
		t.Fatalf("detached name = %q, want scratch", gotName)
	}
}

func TestDetachedLocalHelloIncludesTrueColor(t *testing.T) {
	t.Setenv("TERM", "xterm-direct")
	t.Setenv("COLORTERM", "")

	hello := detachedLocalHello("scratch", "/tmp/work")

	require.Equal(t, ports.IntentNew, hello.Intent)
	require.Equal(t, "scratch", hello.Name)
	require.Equal(t, "xterm-direct", hello.TermEnv)
	require.Equal(t, "/tmp/work", hello.Cwd)
	require.True(t, hello.TrueColor)
	require.Equal(t, os.Environ(), hello.Env)
}

type namedDialer struct{ name string }

func (d namedDialer) Dial(context.Context) (ports.Transport, error) {
	return nil, errors.New("not used")
}

// fakeClipboardReader is a distinguishable ports.ClipboardReader used only to
// verify identity (that runAttachWithDeps threads the *same* reader through),
// never actually invoked in these wiring tests.
type fakeClipboardReader struct{ ports.ClipboardReader }

func TestRunAttachWithDepsSelectsRemoteTransport(t *testing.T) {
	tests := []struct {
		name              string
		selectedTransport string
		wantMode          ports.RemoteTransportMode
	}{
		{name: "default remote mode is udp", selectedTransport: "", wantMode: ports.RemoteTransportUDP},
		{name: "explicit stdio mode", selectedTransport: "stdio", wantMode: ports.RemoteTransportStdio},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var gotDialer string
			var gotRemote bool
			var gotClipboard ports.ClipboardReader
			clip := &fakeClipboardReader{}
			factory := portsmocks.NewMockRemoteDialerFactory(t)
			factory.EXPECT().DialerForRemote("remote.example", "work", tt.wantMode, mock.Anything).Return(namedDialer{name: "remote"}, nil)

			err := runAttachWithDeps(context.Background(), ports.IntentAttach, "work", "remote.example", "", nil, runAttachDeps{
				remoteDialerFactory:     factory,
				selectedRemoteTransport: tt.selectedTransport,
				clipboard:               clip,
				runClient: func(_ context.Context, deps client.Dependencies, request client.AttachRequest) error {
					nd, ok := deps.Dialer.(namedDialer)
					if !ok {
						t.Fatalf("dialer type = %T, want namedDialer", deps.Dialer)
					}
					gotDialer = nd.name
					gotRemote = request.Remote
					gotClipboard = deps.Clipboard
					if request.Intent != ports.IntentAttach || request.SessionName != "work" {
						t.Fatalf("intent/name = %d/%q, want attach/work", request.Intent, request.SessionName)
					}
					return nil
				},
			})
			if err != nil {
				t.Fatalf("runAttachWithDeps returned error: %v", err)
			}
			if gotDialer != "remote" {
				t.Fatalf("remote dialer = %q, want remote", gotDialer)
			}
			if !gotRemote {
				t.Fatal("remote attach must pass remote=true to runClient")
			}
			if gotClipboard != ports.ClipboardReader(clip) {
				t.Fatalf("remote attach must thread the configured ClipboardReader through, got %#v", gotClipboard)
			}
		})
	}
}

func TestRunAttachWithDepsRejectsInvalidRemoteTransportBeforeDialing(t *testing.T) {
	factory := portsmocks.NewMockRemoteDialerFactory(t)
	runClientCalled := false

	err := runAttachWithDeps(context.Background(), ports.IntentAttach, "work", "remote.example", "", nil, runAttachDeps{
		remoteDialerFactory:     factory,
		selectedRemoteTransport: "serial",
		runClient: func(context.Context, client.Dependencies, client.AttachRequest) error {
			runClientCalled = true
			return nil
		},
	})
	if err == nil || err.Error() != `vev: invalid remote transport "serial" (want "udp" or "stdio")` {
		t.Fatalf("runAttachWithDeps error = %v, want invalid transport", err)
	}
	if runClientCalled {
		t.Fatal("runClient called after invalid remote transport")
	}
}

func TestRunAttachWithDepsReturnsFactoryErrorBeforeRunClient(t *testing.T) {
	factoryErr := errors.New("factory failed")
	factory := portsmocks.NewMockRemoteDialerFactory(t)
	factory.EXPECT().DialerForRemote("remote.example", "work", ports.RemoteTransportUDP, mock.Anything).Return(nil, factoryErr)
	runClientCalled := false

	err := runAttachWithDeps(context.Background(), ports.IntentAttach, "work", "remote.example", "", nil, runAttachDeps{
		remoteDialerFactory: factory,
		runClient: func(context.Context, client.Dependencies, client.AttachRequest) error {
			runClientCalled = true
			return nil
		},
	})
	if !errors.Is(err, factoryErr) {
		t.Fatalf("runAttachWithDeps error = %v, want %v", err, factoryErr)
	}
	if runClientCalled {
		t.Fatal("runClient called after remote factory error")
	}
}

func TestRunAttachWithDepsBuildsLocalDialer(t *testing.T) {
	var gotDialer string
	gotRemote := true
	gotClipboard := ports.ClipboardReader(&fakeClipboardReader{})
	factory := portsmocks.NewMockRemoteDialerFactory(t)
	err := runAttachWithDeps(context.Background(), ports.IntentEphemeral, "", "", "", nil, runAttachDeps{
		localDialer:         func() ports.Dialer { return namedDialer{name: "local"} },
		remoteDialerFactory: factory,
		clipboard:           &fakeClipboardReader{}, // must NOT reach runClient for a local attach
		runClient: func(_ context.Context, deps client.Dependencies, request client.AttachRequest) error {
			nd, ok := deps.Dialer.(namedDialer)
			if !ok {
				t.Fatalf("dialer type = %T, want namedDialer", deps.Dialer)
			}
			gotDialer = nd.name
			gotRemote = request.Remote
			gotClipboard = deps.Clipboard
			if request.Intent != ports.IntentEphemeral || request.SessionName != "" {
				t.Fatalf("intent/name = %d/%q, want ephemeral/empty", request.Intent, request.SessionName)
			}
			return nil
		},
	})
	if err != nil {
		t.Fatalf("runAttachWithDeps returned error: %v", err)
	}
	if gotDialer != "local" {
		t.Fatalf("local dialer = %q, want local", gotDialer)
	}
	if gotRemote {
		t.Fatal("local attach must pass remote=false to runClient")
	}
	if gotClipboard != nil {
		t.Fatalf("local attach must not thread a ClipboardReader through, got %#v", gotClipboard)
	}
}

func TestRunUDPBootstrapForwardsReadinessAndExits(t *testing.T) {
	oldTimeout := udpBootstrapTimeout
	udpBootstrapTimeout = time.Second
	t.Cleanup(func() { udpBootstrapTimeout = oldTimeout })
	oldCommand := udpProxyCommand
	var gotCmd *exec.Cmd
	udpProxyCommand = func(context.Context, string, ...string) *exec.Cmd {
		cmd := exec.Command("/bin/sh", "-c", "printf 'VEV-UDP 4242 AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=\\n'")
		gotCmd = cmd
		return cmd
	}
	t.Cleanup(func() { udpProxyCommand = oldCommand })

	got := captureStdout(t, func() {
		if err := runUDPBootstrap(context.Background(), "work"); err != nil {
			t.Fatalf("runUDPBootstrap() error = %v", err)
		}
	})
	if got != "VEV-UDP 4242 AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=\n" {
		t.Fatalf("readiness = %q", got)
	}
	if gotCmd.Stdin == nil || gotCmd.Stderr == nil {
		t.Fatalf("proxy stdio = stdin %T stderr %T, want detached from SSH channels", gotCmd.Stdin, gotCmd.Stderr)
	}
	if gotCmd.Stdout == os.Stdout || gotCmd.Stderr == os.Stderr || gotCmd.Stdin == os.Stdin {
		t.Fatalf("proxy inherited SSH stdio handles")
	}
}

func TestRunUDPBootstrapReturnsReadinessEOF(t *testing.T) {
	oldTimeout := udpBootstrapTimeout
	udpBootstrapTimeout = time.Second
	t.Cleanup(func() { udpBootstrapTimeout = oldTimeout })
	oldCommand := udpProxyCommand
	udpProxyCommand = func(context.Context, string, ...string) *exec.Cmd {
		return exec.Command("/bin/sh", "-c", "exit 7")
	}
	t.Cleanup(func() { udpProxyCommand = oldCommand })

	err := runUDPBootstrap(context.Background(), "work")
	if !errors.Is(err, io.EOF) {
		t.Fatalf("runUDPBootstrap() error = %v, want EOF", err)
	}
}

func TestRunUDPBootstrapTimesOutWaitingForReadiness(t *testing.T) {
	oldTimeout := udpBootstrapTimeout
	udpBootstrapTimeout = 10 * time.Millisecond
	t.Cleanup(func() { udpBootstrapTimeout = oldTimeout })
	oldCommand := udpProxyCommand
	udpProxyCommand = func(context.Context, string, ...string) *exec.Cmd {
		return exec.Command("/bin/sh", "-c", "sleep 5")
	}
	t.Cleanup(func() { udpProxyCommand = oldCommand })

	err := runUDPBootstrap(context.Background(), "work")
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("runUDPBootstrap() error = %v, want deadline exceeded", err)
	}
}

func TestParseUDPPortRange(t *testing.T) {
	tests := []struct {
		name      string
		value     string
		want      udpPortRange
		wantError bool
	}{
		{name: "empty uses default", value: "", want: udpPortRange{start: defaultUDPPortStart, end: defaultUDPPortEnd}},
		{name: "zero is ephemeral", value: "0", want: udpPortRange{start: 0, end: 0}},
		{name: "zero range rejected", value: "0-100", wantError: true},
		{name: "single port", value: "61000", want: udpPortRange{start: 61000, end: 61000}},
		{name: "range", value: "61000-61023", want: udpPortRange{start: 61000, end: 61023}},
		{name: "range with spaces", value: " 61000 - 61023 ", want: udpPortRange{start: 61000, end: 61023}},
		{name: "reversed range", value: "61023-61000", wantError: true},
		{name: "non-numeric", value: "abc", wantError: true},
		{name: "missing end", value: "61000-", wantError: true},
		{name: "port too high", value: "70000", wantError: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseUDPPortRange(tt.value)
			if tt.wantError {
				if err == nil {
					t.Fatalf("parseUDPPortRange(%q) error = nil, want error", tt.value)
				}
				return
			}
			if err != nil {
				t.Fatalf("parseUDPPortRange(%q) error = %v", tt.value, err)
			}
			if got != tt.want {
				t.Fatalf("parseUDPPortRange(%q) = %+v, want %+v", tt.value, got, tt.want)
			}
		})
	}
}

func TestListenUDPInRange(t *testing.T) {
	// Occupy a wildcard UDP port, then assert listenUDPInRange skips it.
	var lc net.ListenConfig
	occupied, err := lc.ListenPacket(context.Background(), "udp", ":0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = occupied.Close() }()
	udpPort := func(t *testing.T, conn net.PacketConn) int {
		t.Helper()
		addr, ok := conn.LocalAddr().(*net.UDPAddr)
		if !ok {
			t.Fatalf("LocalAddr() = %T, want *net.UDPAddr", conn.LocalAddr())
		}
		return addr.Port
	}
	busyPort := udpPort(t, occupied)

	t.Run("skips busy port", func(t *testing.T) {
		conn, err := listenUDPInRange(context.Background(), udpPortRange{start: busyPort, end: busyPort + 8})
		if err != nil {
			t.Fatalf("listenUDPInRange error = %v", err)
		}
		defer func() { _ = conn.Close() }()
		got := udpPort(t, conn)
		if got == busyPort {
			t.Fatalf("bound busy port %d, want a different port in range", busyPort)
		}
		if got < busyPort || got > busyPort+8 {
			t.Fatalf("bound port %d outside range %d-%d", got, busyPort, busyPort+8)
		}
	})

	t.Run("exhausted range errors", func(t *testing.T) {
		_, err := listenUDPInRange(context.Background(), udpPortRange{start: busyPort, end: busyPort})
		if err == nil {
			t.Fatal("listenUDPInRange error = nil, want exhausted-range error")
		}
		if !strings.Contains(err.Error(), "no free UDP port in range") {
			t.Fatalf("error = %v, want 'no free UDP port in range'", err)
		}
	})

	t.Run("invalid direct range errors", func(t *testing.T) {
		_, err := listenUDPInRange(context.Background(), udpPortRange{start: busyPort + 8, end: busyPort})
		if err == nil {
			t.Fatal("listenUDPInRange error = nil, want invalid-range error")
		}
		if !strings.Contains(err.Error(), "invalid UDP port range") {
			t.Fatalf("error = %v, want 'invalid UDP port range'", err)
		}
	})

	t.Run("ephemeral binds", func(t *testing.T) {
		conn, err := listenUDPInRange(context.Background(), udpPortRange{start: 0, end: 0})
		if err != nil {
			t.Fatalf("listenUDPInRange ephemeral error = %v", err)
		}
		defer func() { _ = conn.Close() }()
		if udpPort(t, conn) == 0 {
			t.Fatal("ephemeral bind returned port 0")
		}
	})
}

func TestPprofAddrIsLoopback(t *testing.T) {
	tests := []struct {
		addr string
		want bool
	}{
		{addr: "127.0.0.1:6060", want: true},
		{addr: "localhost:6060", want: true},
		{addr: "[::1]:6060", want: true},
		{addr: ":6060", want: false},
		{addr: "0.0.0.0:6060", want: false},
		{addr: "192.168.1.5:6060", want: false},
		{addr: "garbage", want: false},
	}

	for _, tt := range tests {
		t.Run(tt.addr, func(t *testing.T) {
			if got := pprofAddrIsLoopback(tt.addr); got != tt.want {
				t.Fatalf("pprofAddrIsLoopback(%q) = %v, want %v", tt.addr, got, tt.want)
			}
		})
	}
}
