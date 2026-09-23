package app

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/bnema/vev/internal/adapters/daemonmux"
	"github.com/bnema/vev/internal/adapters/lifecycle"
	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/persist"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/protocol/wire"
	"github.com/bnema/vev/internal/usecase/daemon"
	"github.com/bnema/vev/pkg/kv"
	"github.com/bnema/vev/pkg/safedir"
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
	ref := &domain.CheckpointRef{Generation: 1, ManifestDigest: [32]byte{1}}
	records := []domain.CatalogueRecord{
		{Committed: ref},
		{Committed: ref},
		{},
		{Committed: ref, DegradedReason: "checkpoint validation failed"},
	}
	require.NoError(t, runWithLifecycleOwnerDeps(context.Background(), "/runtime/vev", "/state/vev", func(context.Context) error {
		logCatalogueRecovery(log, records, "current")
		logStartupRecoveryCounts(log, records, 0)
		return nil
	}, deps))

	entries := decodeJSONLogs(t, logBuffer.Bytes())
	for _, name := range []string{"lifecycle_owner_wait", "lifecycle_owner_acquired", "catalogue_validated", "daemon_startup_complete", "lifecycle_owner_released"} {
		require.Contains(t, eventNames(entries), name)
	}
	startup := requireEvent(t, entries, "daemon_startup_complete")
	require.EqualValues(t, 2, startup["healthy"])
	require.EqualValues(t, 1, startup["fresh"])
	require.EqualValues(t, 0, startup["restoring"])
	require.EqualValues(t, 1, startup["broken"])
	require.NotContains(t, eventNames(entries), "interrupted_transaction_recovery_complete")
}

// A session-scoped conflict no longer aborts startup, so the daemon comes up
// looking healthy. Startup must therefore name the fenced session in the log,
// persist a notice for it, and never report the recovery as clean.
type fakeLifecycleOwnership struct {
	release func() error
}

func (o fakeLifecycleOwnership) Release() error { return o.release() }

func TestCatalogueFailureDoesNotListen(t *testing.T) {
	originalOpenCatalogue, originalListenDaemon := openCatalogue, listenDaemon
	t.Cleanup(func() {
		openCatalogue = originalOpenCatalogue
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
			openCatalogue = func(gotStateDir string) (persist.OpenResult, error) {
				require.Equal(t, stateDir, gotStateDir)
				return persist.OpenResult{}, catalogueErr
			}
			listenDaemon = func(string, ports.SerializedRuntimeObserver) (wire.Listener, error) {
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

// TestLifecycleOwnershipAcceptsOwnerOnlyVariantLock is the production-daemon
// regression for the relaxed lock-permission rule: a pre-existing lock written
// with an owner-only mode other than 0600 must still let the daemon start, while
// group/other access stays refused (covered by the lifecycle unit tests).
func TestLifecycleOwnershipAcceptsOwnerOnlyVariantLock(t *testing.T) {
	runtimeDir := filepath.Join(t.TempDir(), "runtime")
	stateDir := filepath.Join(t.TempDir(), "state")
	require.NoError(t, safedir.EnsurePrivate(runtimeDir))
	require.NoError(t, os.WriteFile(lifecycle.Path(runtimeDir), nil, 0o600))
	require.NoError(t, os.Chmod(lifecycle.Path(runtimeDir), 0o700))

	started := false
	err := runWithLifecycleOwner(context.Background(), runtimeDir, stateDir, func(context.Context) error {
		started = true
		return nil
	})
	require.NoError(t, err, "an owner-only variant lock must not block production daemon startup")
	require.True(t, started)

	owner, err := lifecycle.TryAcquire(runtimeDir)
	require.NoError(t, err, "lifecycle ownership must be released after startup")
	require.NoError(t, owner.Release())
}

func TestCatalogueRegistryConstructionPrecedesSocketPublication(t *testing.T) {
	var events []string
	_, _, err := constructDaemonBeforeSocketPublication(
		func() *daemon.Daemon {
			events = append(events, "catalogue-registry")
			return nil
		},
		func(*daemon.Daemon) error {
			require.Equal(t, []string{"catalogue-registry"}, events)
			events = append(events, "startup-garbage-collection")
			return nil
		},
		func() (wire.Listener, error) {
			require.Equal(t, []string{"catalogue-registry", "startup-garbage-collection"}, events)
			events = append(events, "socket-publication")
			return nil, nil
		},
	)
	require.NoError(t, err)
	require.Equal(t, []string{"catalogue-registry", "startup-garbage-collection", "socket-publication"}, events)
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
		require.NoError(t, safedir.EnsurePrivate(stateDir))
		store, err := kv.Open(persist.StorePath(stateDir))
		require.NoError(t, err)
		require.NoError(t, store.Set([]byte("work"), []byte("malformed catalogue value")))
		require.NoError(t, store.Close())

		t.Setenv("XDG_RUNTIME_DIR", runtimeRoot)
		t.Setenv("XDG_STATE_HOME", stateRoot)
		before, err := os.ReadFile(persist.StorePath(stateDir))
		require.NoError(t, err)
		err = runWithLifecycleOwner(context.Background(), runtimeDir, stateDir, runDaemonOwned)
		require.Error(t, err)
		require.Contains(t, err.Error(), stateDir)
		require.Contains(t, err.Error(), "rm -rf "+stateDir)
		after, readErr := os.ReadFile(persist.StorePath(stateDir))
		require.NoError(t, readErr)
		require.Equal(t, before, after, "failed startup must leave the catalogue untouched")
		_, statErr := os.Stat(daemonmux.SocketPath(runtimeDir))
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
			_, err := os.Stat(daemonmux.SocketPath(runtimeDir))
			return err == nil
		}, 5*time.Second, time.Millisecond, "daemon mux socket was not published")
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

func TestDevelopmentTempDirOption(t *testing.T) {
	absoluteBase := filepath.Join(t.TempDir(), ".dev")
	creatorErr := errors.New("cannot secure directory")
	tests := []struct {
		name        string
		environment string
		base        string
		creatorErr  error
		wantPath    string
		wantOption  bool
		wantErr     error
	}{
		{
			name: "inactive environment",
			base: absoluteBase,
		},
		{
			name:        "active environment",
			environment: "work",
			base:        absoluteBase,
			wantPath:    filepath.Join(absoluteBase, "work", "tmp"),
			wantOption:  true,
		},
		{
			name:        "creator error",
			environment: "work",
			base:        absoluteBase,
			creatorErr:  creatorErr,
			wantPath:    filepath.Join(absoluteBase, "work", "tmp"),
			wantErr:     creatorErr,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("VEV_ENV", tt.environment)
			t.Setenv("VEV_ENV_ROOT", tt.base)
			var creatorCalls []string

			option, err := developmentTempDirOption(func(path string) error {
				creatorCalls = append(creatorCalls, path)
				return tt.creatorErr
			})

			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
				require.Nil(t, option)
			} else {
				require.NoError(t, err)
				if tt.wantOption {
					require.NotNil(t, option)
				} else {
					require.Nil(t, option)
				}
			}
			if tt.wantPath == "" {
				require.Empty(t, creatorCalls)
			} else {
				require.Equal(t, []string{tt.wantPath}, creatorCalls)
				require.True(t, filepath.IsAbs(tt.wantPath))
			}
		})
	}
}

func TestRunActivatesDevelopmentEnvironmentBeforeParsing(t *testing.T) {
	t.Setenv("VEV_ENV", "../live")
	t.Setenv("XDG_CONFIG_HOME", "/existing/config")
	t.Setenv("XDG_STATE_HOME", "/existing/state")
	t.Setenv("XDG_RUNTIME_DIR", "/existing/runtime")

	err := Run([]string{"unknown"})
	require.ErrorContains(t, err, "invalid VEV_ENV")
	require.NotContains(t, err.Error(), "unknown command")
	require.Equal(t, "/existing/config", os.Getenv("XDG_CONFIG_HOME"))
	require.Equal(t, "/existing/state", os.Getenv("XDG_STATE_HOME"))
	require.Equal(t, "/existing/runtime", os.Getenv("XDG_RUNTIME_DIR"))
}

func TestParseArgs(t *testing.T) {
	tests := []struct {
		name         string
		args         []string
		wantKind     cmdKind
		wantIntent   uint8
		wantName     string
		wantRemote   string
		wantListHost string
		wantListAll  bool
		wantHostAct  string
		wantHostTgt  string
		wantAll      bool
		wantSessions bool
		wantErr      bool
		nonUsageErr  bool
	}{
		{name: "no args -> ephemeral attach", args: nil, wantKind: kindAttach, wantIntent: protocol.IntentEphemeral},
		{name: "empty slice -> ephemeral attach", args: []string{}, wantKind: kindAttach, wantIntent: protocol.IntentEphemeral},
		{name: "new named", args: []string{"new", "work"}, wantKind: kindAttach, wantIntent: protocol.IntentNew, wantName: "work"},
		{name: "new remote", args: []string{"new", "work", "user@example.com"}, wantKind: kindAttach, wantIntent: protocol.IntentNew, wantName: "work", wantRemote: "user@example.com"},
		{name: "new invalid remote", args: []string{"new", "work", "bad host"}, wantErr: true, nonUsageErr: true},
		{name: "new without name", args: []string{"new"}, wantErr: true},
		{name: "new empty name", args: []string{"new", ""}, wantErr: true},
		{name: "new command override unsupported", args: []string{"new", "work", "--", "sh"}, wantErr: true},
		{name: "attach named", args: []string{"attach", "work"}, wantKind: kindAttach, wantIntent: protocol.IntentAttach, wantName: "work"},
		{name: "attach preserves legacy unsafe name", args: []string{"attach", "my work"}, wantKind: kindAttach, wantIntent: protocol.IntentAttach, wantName: "my work"},
		{name: "attach alias a", args: []string{"a", "work"}, wantKind: kindAttach, wantIntent: protocol.IntentAttach, wantName: "work"},
		{name: "attach remote host uses ephemeral", args: []string{"attach", "user@example.com"}, wantKind: kindAttach, wantIntent: protocol.IntentEphemeral, wantRemote: "user@example.com"},
		{name: "attach remote host with empty session uses ephemeral", args: []string{"attach", "user@example.com:"}, wantKind: kindAttach, wantIntent: protocol.IntentEphemeral, wantRemote: "user@example.com"},
		{name: "attach remote host with session", args: []string{"attach", "user@example.com:work"}, wantKind: kindAttach, wantIntent: protocol.IntentAttach, wantName: "work", wantRemote: "user@example.com"},
		{name: "attach remote ipv4 with session", args: []string{"attach", "user@192.0.2.10:work"}, wantKind: kindAttach, wantIntent: protocol.IntentAttach, wantName: "work", wantRemote: "user@192.0.2.10"},
		{name: "attach remote ipv6 with session", args: []string{"attach", "user@[2001:db8::1]:work"}, wantKind: kindAttach, wantIntent: protocol.IntentAttach, wantName: "work", wantRemote: "user@[2001:db8::1]"},
		{name: "attach remote invalid session", args: []string{"attach", "user@example.com:my work"}, wantErr: true, nonUsageErr: true},
		{name: "attach extra arg", args: []string{"attach", "work", "extra"}, wantErr: true},
		{name: "attach without name", args: []string{"attach"}, wantErr: true},
		{name: "ls", args: []string{"ls"}, wantKind: kindList},
		{name: "list", args: []string{"list"}, wantKind: kindList},
		{name: "ls host", args: []string{"ls", "arch"}, wantKind: kindList, wantListHost: "arch"},
		{name: "list host", args: []string{"list", "build@mule"}, wantKind: kindList, wantListHost: "build@mule"},
		{name: "ls --all", args: []string{"ls", "--all"}, wantKind: kindList, wantListAll: true},
		{name: "list --all", args: []string{"list", "--all"}, wantKind: kindList, wantListAll: true},
		{name: "ls host extra", args: []string{"ls", "arch", "extra"}, wantErr: true},
		{name: "ls --all host", args: []string{"ls", "--all", "arch"}, wantErr: true},
		{name: "ls invalid host", args: []string{"ls", "bad host"}, wantErr: true, nonUsageErr: true},
		{name: "host add", args: []string{"host", "add", "arch"}, wantKind: kindHost, wantHostAct: "add", wantHostTgt: "arch"},
		{name: "host rm", args: []string{"host", "rm", "build@mule"}, wantKind: kindHost, wantHostAct: "rm", wantHostTgt: "build@mule"},
		{name: "host list", args: []string{"host", "list"}, wantKind: kindHost, wantHostAct: "list"},
		{name: "host alone", args: []string{"host"}, wantErr: true},
		{name: "host add missing target", args: []string{"host", "add"}, wantErr: true},
		{name: "host add extra", args: []string{"host", "add", "arch", "extra"}, wantErr: true},
		{name: "host add invalid", args: []string{"host", "add", " bad"}, wantErr: true, nonUsageErr: true},
		{name: "host unknown action", args: []string{"host", "rename", "arch"}, wantErr: true},
		{name: "kill named", args: []string{"kill", "work"}, wantKind: kindKill, wantName: "work"},
		{name: "kill preserves legacy unsafe name via terminator", args: []string{"kill", "--", "my work"}, wantKind: kindKill, wantName: "my work"},
		{name: "kill dashed name via terminator", args: []string{"kill", "--", "--all"}, wantKind: kindKill, wantName: "--all"},
		{name: "kill all", args: []string{"kill", "--all"}, wantKind: kindKill, wantAll: true},
		{name: "kill sessions", args: []string{"kill", "--sessions"}, wantKind: kindKill, wantSessions: true},
		{name: "kill sessions rejects extra arg", args: []string{"kill", "--sessions", "extra"}, wantErr: true},
		{name: "kill removed daemon flag", args: []string{"kill", "--daemon"}, wantErr: true},
		{name: "kill removed broker flag", args: []string{"kill", "--broker"}, wantErr: true},
		{name: "kill without name", args: []string{"kill"}, wantErr: true},
		{name: "kill terminator without name", args: []string{"kill", "--"}, wantErr: true},
		{name: "kill all rejects extra arg", args: []string{"kill", "--all", "extra"}, wantErr: true},
		{name: "kill extra arg", args: []string{"kill", "work", "extra"}, wantErr: true},
		{name: "daemon", args: []string{"--daemon"}, wantKind: kindDaemon},
		{name: "removed stdio", args: []string{"_stdio"}, wantErr: true},
		{name: "removed quic bootstrap", args: []string{"_quic-bootstrap"}, wantErr: true},
		{name: "removed quic proxy", args: []string{"_quic-proxy"}, wantErr: true},
		{name: "removed udp bootstrap", args: []string{"_udp-bootstrap"}, wantErr: true},
		{name: "removed udp proxy", args: []string{"_udp-proxy"}, wantErr: true},
		{name: "removed udp proxy rejects session", args: []string{"_udp-proxy", "work"}, wantErr: true},
		{name: "removed udp proxy rejects extra args", args: []string{"_udp-proxy", "work", "extra"}, wantErr: true},
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
				if tt.nonUsageErr {
					_, usage := errors.AsType[*usageError](err)
					if usage {
						t.Fatalf("parseArgs(%q) error = *usageError, want non-usage error", tt.args)
					}
				} else {
					_, usage := errors.AsType[*usageError](err)
					if !usage {
						t.Fatalf("parseArgs(%q) error = %T, want *usageError", tt.args, err)
					}
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
			if got.listHost != tt.wantListHost {
				t.Errorf("listHost = %q, want %q", got.listHost, tt.wantListHost)
			}
			if got.listAll != tt.wantListAll {
				t.Errorf("listAll = %v, want %v", got.listAll, tt.wantListAll)
			}
			if got.hostAction != tt.wantHostAct {
				t.Errorf("hostAction = %q, want %q", got.hostAction, tt.wantHostAct)
			}
			if got.hostTarget != tt.wantHostTgt {
				t.Errorf("hostTarget = %q, want %q", got.hostTarget, tt.wantHostTgt)
			}
			if got.killAll != tt.wantAll {
				t.Errorf("killAll = %v, want %v", got.killAll, tt.wantAll)
			}
			if got.killSessions != tt.wantSessions {
				t.Errorf("killSessions = %v, want %v", got.killSessions, tt.wantSessions)
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

func TestListShowsBroken(t *testing.T) {
	var out bytes.Buffer
	printSessions(&out, []protocol.SessionInfo{
		{Name: "fresh", State: protocol.SessionDown},
		{Name: "loading", State: protocol.SessionDown},
		{Name: "broken", State: protocol.SessionBroken},
	})
	require.Contains(t, out.String(), "fresh")
	require.Contains(t, out.String(), "down")
	require.Contains(t, out.String(), "loading")
	require.Contains(t, out.String(), "broken")
	require.NotContains(t, out.String(), "restoring")
	require.NotContains(t, out.String(), "degraded")
}

func TestPrintSessionsShowsDownState(t *testing.T) {
	var out bytes.Buffer
	printSessions(&out, []protocol.SessionInfo{
		{Name: "main", State: protocol.SessionUp, Tabs: 2, Attached: true},
		{Name: "old", State: protocol.SessionDown},
	})
	got := out.String()
	for _, want := range []string{"NAME", "STATE", "main", "up", "2", "yes", "old", "down", "-"} {
		if !strings.Contains(got, want) {
			t.Fatalf("printSessions output %q missing %q", got, want)
		}
	}
}

func TestPrintSessionsMarksEphemeral(t *testing.T) {
	var buf bytes.Buffer
	printSessions(&buf, []protocol.SessionInfo{
		{Name: "0", State: protocol.SessionUp, Ephemeral: true, Tabs: 1, Attached: false},
		{Name: "work", State: protocol.SessionUp, Tabs: 2, Attached: true},
		{Name: "old", State: protocol.SessionDown},
	})
	out := buf.String()
	for _, want := range []string{"0", "temporary", "work", "up", "old", "down"} {
		require.Contains(t, out, want)
	}
}

func newTestPersister(t *testing.T, stateDir string) *persist.Persister {
	t.Helper()
	store, err := persist.OpenStore(persist.StorePath(stateDir))
	require.NoError(t, err)
	return persist.New(store)
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
