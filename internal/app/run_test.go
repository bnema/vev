package app

import (
	"bufio"
	"bytes"
	"context"
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
	"testing"
	"time"

	"github.com/bnema/vev/internal/adapters/lifecycle"
	remoteadapter "github.com/bnema/vev/internal/adapters/remote"
	"github.com/bnema/vev/internal/adapters/sessionwire"
	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/persist"
	"github.com/bnema/vev/internal/ports"
	portsmocks "github.com/bnema/vev/internal/ports/mocks"
	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/usecase/client"
	"github.com/bnema/vev/internal/usecase/daemon"
	"github.com/bnema/vev/pkg/kv"
	"github.com/bnema/vev/pkg/safedir"
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
			run: func(ctx context.Context) error {
				return runList(ctx, command{kind: kindList})
			},
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
		func(*daemon.Daemon) error {
			require.Equal(t, []string{"catalogue-registry"}, events)
			events = append(events, "startup-garbage-collection")
			return nil
		},
		func() (ports.Listener, error) {
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
func TestRunUDPProxyUsesBoundedClientMaxPending(t *testing.T) {
	require.Equal(t, 32, udpProxyClientTransportOptions.MaxPending)
}

func TestRunUDPProxyClientDeadAfterExceedsIdleTTL(t *testing.T) {
	require.Positive(t, udpProxyIdleTTL)
	require.Greater(t, udpProxyClientTransportOptions.DeadAfter, udpProxyIdleTTL)
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
		wantDaemon   bool
		wantErr      bool
		nonUsageErr  bool
	}{
		{name: "no args -> ephemeral attach", args: nil, wantKind: kindAttach, wantIntent: protocol.IntentEphemeral},
		{name: "empty slice -> ephemeral attach", args: []string{}, wantKind: kindAttach, wantIntent: protocol.IntentEphemeral},
		{name: "new named", args: []string{"new", "work"}, wantKind: kindAttach, wantIntent: protocol.IntentNew, wantName: "work"},
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
		{name: "kill daemon", args: []string{"kill", "--daemon"}, wantKind: kindKill, wantDaemon: true},
		{name: "kill without name", args: []string{"kill"}, wantErr: true},
		{name: "kill terminator without name", args: []string{"kill", "--"}, wantErr: true},
		{name: "kill all rejects extra arg", args: []string{"kill", "--all", "extra"}, wantErr: true},
		{name: "kill daemon rejects extra arg", args: []string{"kill", "--daemon", "extra"}, wantErr: true},
		{name: "kill extra arg", args: []string{"kill", "work", "extra"}, wantErr: true},
		{name: "daemon", args: []string{"--daemon"}, wantKind: kindDaemon},
		{name: "stdio", args: []string{"_stdio"}, wantKind: kindStdio},
		{name: "stdio rejects session", args: []string{"_stdio", "work"}, wantErr: true},
		{name: "stdio rejects extra args", args: []string{"_stdio", "work", "extra"}, wantErr: true},
		{name: "udp bootstrap", args: []string{"_udp-bootstrap"}, wantKind: kindUDPBootstrap},
		{name: "udp bootstrap rejects session", args: []string{"_udp-bootstrap", "work"}, wantErr: true},
		{name: "udp proxy", args: []string{"_udp-proxy"}, wantKind: kindUDPProxy},
		{name: "udp proxy rejects session", args: []string{"_udp-proxy", "work"}, wantErr: true},
		{name: "udp proxy rejects extra args", args: []string{"_udp-proxy", "work", "extra"}, wantErr: true},
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

func TestAttachTargetsCreateCommonSessionRequest(t *testing.T) {
	tests := []struct {
		name string
		arg  string
		want client.AttachRequest
	}{
		{name: "local host", arg: "work", want: client.AttachRequest{Intent: protocol.IntentAttach, SessionName: "work"}},
		{name: "remote host", arg: "user@example.com:work", want: client.AttachRequest{Intent: protocol.IntentAttach, SessionName: "work"}},
		{name: "remote ipv4", arg: "user@192.0.2.10:work", want: client.AttachRequest{Intent: protocol.IntentAttach, SessionName: "work"}},
		{name: "remote ipv6", arg: "user@[2001:db8::1]:work", want: client.AttachRequest{Intent: protocol.IntentAttach, SessionName: "work"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cmd, err := parseArgs([]string{"attach", tt.arg})
			require.NoError(t, err)
			connection, err := client.NewSessionConnection(sessionwire.NewClientConnection(portsmocks.NewMockTransport(t)), client.SessionTarget{
				Intent:      cmd.intent,
				SessionName: cmd.name,
			})
			require.NoError(t, err)
			require.Equal(t, tt.want, connection.AttachRequest())
		})
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

func TestRunListReadsDownSessionsWithoutDaemon(t *testing.T) {
	stateRoot, runtimeRoot := t.TempDir(), t.TempDir()
	t.Setenv("XDG_STATE_HOME", stateRoot)
	t.Setenv("XDG_RUNTIME_DIR", runtimeRoot)

	p := newTestPersister(t, filepath.Join(stateRoot, "vev"))
	now := time.Now().UnixNano()
	if err := p.Save(persist.Record{Name: "stored", IncarnationID: domain.IncarnationID{1}, Cwd: t.TempDir(), CreatedAt: now, UpdatedAt: now}); err != nil {
		t.Fatalf("Save error = %v", err)
	}
	if err := p.Close(); err != nil {
		t.Fatalf("Close error = %v", err)
	}

	got := captureStdout(t, func() {
		if err := runList(context.Background(), command{kind: kindList}); err != nil {
			t.Fatalf("runList error = %v", err)
		}
	})
	for _, want := range []string{"stored", "down", "-"} {
		if !strings.Contains(got, want) {
			t.Fatalf("runList output %q missing %q", got, want)
		}
	}
}

func TestRunListUnreadableCatalogueProvidesResetGuidance(t *testing.T) {
	stateRoot, runtimeRoot := t.TempDir(), t.TempDir()
	stateDir := filepath.Join(stateRoot, "vev")
	runtimeDir := filepath.Join(runtimeRoot, "vev")
	t.Setenv("XDG_STATE_HOME", stateRoot)
	t.Setenv("XDG_RUNTIME_DIR", runtimeRoot)
	require.NoError(t, safedir.EnsurePrivate(stateDir))

	catalogue := persist.StorePath(stateDir)
	before := []byte("corrupt catalogue")
	require.NoError(t, os.WriteFile(catalogue, before, 0o600))

	var runErr error
	stdout := captureStdout(t, func() { runErr = Run([]string{"ls"}) })
	require.Empty(t, stdout)
	require.Error(t, runErr)
	require.ErrorContains(t, runErr, stateDir)
	require.ErrorContains(t, runErr, "rm -rf "+stateDir)
	require.NoFileExists(t, filepath.Join(runtimeDir, "daemon.sock"))
	after, err := os.ReadFile(catalogue)
	require.NoError(t, err)
	require.Equal(t, before, after)
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
	if err := p.Save(persist.Record{Name: "stored", IncarnationID: domain.IncarnationID{1}, Cwd: t.TempDir(), CreatedAt: now, UpdatedAt: now}); err != nil {
		t.Fatalf("Save error = %v", err)
	}
	if err := p.Close(); err != nil {
		t.Fatalf("Close error = %v", err)
	}
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

func TestRunAttachRejectsNestedVEVBeforeDial(t *testing.T) {
	called := false
	err := runAttachWithDeps(context.Background(), protocol.IntentEphemeral, "", "", "outer", nil, runAttachDeps{
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
	err := runAttachWithDeps(context.Background(), protocol.IntentNew, "scratch", "", "outer", nil, runAttachDeps{
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

	require.Equal(t, protocol.IntentNew, hello.Intent)
	require.Equal(t, "scratch", hello.Name)
	require.Equal(t, "xterm-direct", hello.TermEnv)
	require.Equal(t, "/tmp/work", hello.Cwd)
	require.True(t, hello.TrueColor)
	require.Equal(t, os.Environ(), hello.Env)
}

type namedDialer struct{ name string }

func (d namedDialer) Dial(context.Context) (ports.Transport, error) {
	return nil, fmt.Errorf("not used: %s", d.name)
}

func requireNamedClientDialer(t *testing.T, ctx context.Context, dialer ports.ClientDialer, name string) {
	t.Helper()
	_, err := dialer.Dial(ctx)
	require.EqualError(t, err, "not used: "+name)
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

			err := runAttachWithDeps(context.Background(), protocol.IntentAttach, "work", "remote.example", "", nil, runAttachDeps{
				remoteDialerFactory:     factory,
				selectedRemoteTransport: tt.selectedTransport,
				clipboard:               clip,
				runClient: func(ctx context.Context, deps client.Dependencies, request client.AttachRequest) error {
					requireNamedClientDialer(t, ctx, deps.Dialer, "remote")
					gotDialer = "remote"
					gotRemote = deps.Remote
					if !request.Remote || request.EnvironmentPolicy != protocol.EnvironmentPolicyClientOwned {
						t.Fatal("direct CLI remote attach must preserve client-owned environment policy")
					}
					gotClipboard = deps.Clipboard
					if request.Intent != protocol.IntentAttach || request.SessionName != "work" {
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

func TestRunAttachWithDepsDirectLegacyCallbackPreservesClientOwnership(t *testing.T) {
	factory := portsmocks.NewMockRemoteDialerFactory(t)
	factory.EXPECT().DialerForRemote("remote.example", "work", ports.RemoteTransportUDP, mock.Anything).Return(namedDialer{name: "remote-1"}, nil).Once()
	factory.EXPECT().DialerForRemote("selected.example", "picked", ports.RemoteTransportUDP, mock.Anything).Return(namedDialer{name: "remote-2"}, nil).Once()

	err := runAttachWithDeps(context.Background(), protocol.IntentAttach, "work", "remote.example", "", nil, runAttachDeps{
		remoteDialerFactory: factory,
		runClient: func(_ context.Context, deps client.Dependencies, request client.AttachRequest) error {
			require.True(t, request.Remote)
			require.Equal(t, protocol.EnvironmentPolicyClientOwned, request.EnvironmentPolicy)
			nextDialer, nextRequest, err := deps.AttachHandoff(protocol.AttachTarget{
				Endpoint: "selected.example", Session: "picked", Intent: protocol.IntentAttach,
				EnvironmentPolicy: protocol.EnvironmentPolicyClientOwned,
			})
			require.NoError(t, err)
			require.NotNil(t, nextDialer)
			require.Equal(t, protocol.EnvironmentPolicyClientOwned, nextRequest.EnvironmentPolicy)
			return nil
		},
	})
	require.NoError(t, err)
}

func TestRunAttachWithDepsRemotePickerHandoffReopensDirectConnection(t *testing.T) {
	factory := portsmocks.NewMockRemoteDialerFactory(t)
	factory.EXPECT().DialerForRemote("remote.example", "work", ports.RemoteTransportUDP, mock.Anything).Return(namedDialer{name: "remote-1"}, nil).Once()
	factory.EXPECT().DialerForRemote("selected.example", "picked", ports.RemoteTransportUDP, mock.Anything).Return(namedDialer{name: "remote-2"}, nil).Once()
	var calls int
	err := runAttachWithDeps(context.Background(), protocol.IntentAttach, "work", "remote.example", "", nil, runAttachDeps{
		remoteDialerFactory: factory,
		runClient: func(_ context.Context, deps client.Dependencies, request client.AttachRequest) error {
			calls++
			require.NotNil(t, deps.Dialer)
			require.True(t, request.Remote)
			require.Equal(t, protocol.EnvironmentPolicyClientOwned, request.EnvironmentPolicy, "direct CLI remote ownership must survive a handoff")
			if calls == 1 {
				return &client.AttachTargetError{Target: protocol.AttachTarget{Endpoint: "selected.example", Session: "picked", Intent: protocol.IntentAttach}}
			}
			return nil
		},
	})
	require.NoError(t, err)
	require.Equal(t, 2, calls, "a picker handoff must close the old connection and open a fresh direct connection")
}

func TestRunAttachWithDepsAllowsRevisitingRemoteHandoff(t *testing.T) {
	factory := portsmocks.NewMockRemoteDialerFactory(t)
	factory.EXPECT().DialerForRemote("remote.example", "work", ports.RemoteTransportUDP, mock.Anything).Return(namedDialer{name: "remote"}, nil).Once()
	factory.EXPECT().DialerForRemote("remote.example", "work", ports.RemoteTransportUDP, mock.Anything).Return(namedDialer{name: "remote-revisit"}, nil).Once()

	err := runAttachWithDeps(context.Background(), protocol.IntentAttach, "work", "remote.example", "", nil, runAttachDeps{
		remoteDialerFactory: factory,
		runClient: func(_ context.Context, deps client.Dependencies, _ client.AttachRequest) error {
			dialer, request, err := deps.AttachHandoff(protocol.AttachTarget{
				Endpoint: "remote.example", Session: "work", Intent: protocol.IntentAttach,
			})
			require.NoError(t, err)
			require.NotNil(t, dialer)
			require.Equal(t, "work", request.SessionName)
			return nil
		},
	})
	require.NoError(t, err)
}

func TestRunAttachWithDepsBoundsRepeatedAttachTargetHandoffs(t *testing.T) {
	factory := portsmocks.NewMockRemoteDialerFactory(t)
	factory.EXPECT().DialerForRemote("remote.example", "work", ports.RemoteTransportUDP, mock.Anything).Return(namedDialer{name: "remote"}, nil).Times(maxAttachTargetHandoffs + 1)

	calls := 0
	err := runAttachWithDeps(context.Background(), protocol.IntentAttach, "work", "remote.example", "", nil, runAttachDeps{
		remoteDialerFactory: factory,
		runClient: func(_ context.Context, _ client.Dependencies, _ client.AttachRequest) error {
			calls++
			return &client.AttachTargetError{Target: protocol.AttachTarget{Endpoint: "remote.example", Session: "work", Intent: protocol.IntentAttach}}
		},
	})
	require.ErrorContains(t, err, "attach handoff exceeded maximum")
	require.Equal(t, maxAttachTargetHandoffs+1, calls)
}

func TestRunAttachWithDepsLocalPickerHandoffAttachesSelectedRemote(t *testing.T) {
	factory := portsmocks.NewMockRemoteDialerFactory(t)
	factory.EXPECT().DialerForRemote("selected.example", "picked", ports.RemoteTransportUDP, mock.Anything).Return(namedDialer{name: "remote"}, nil).Once()

	localCalls, remoteCalls := 0, 0
	err := runAttachWithDeps(context.Background(), protocol.IntentAttach, "work", "", "", nil, runAttachDeps{
		localDialer:         func() ports.Dialer { return namedDialer{name: "local"} },
		remoteDialerFactory: factory,
		runClient: func(_ context.Context, deps client.Dependencies, request client.AttachRequest) error {
			if deps.Remote {
				remoteCalls++
				require.Equal(t, client.AttachRequest{Intent: protocol.IntentAttach, SessionName: "picked", Remote: true, Origin: protocol.RouteOriginDiscovery, OriginKey: "selected.example", HostLabel: "selected.example", EnvironmentPolicy: protocol.EnvironmentPolicyDaemonOwned}, request)
				return nil
			}
			localCalls++
			require.Equal(t, client.AttachRequest{Intent: protocol.IntentAttach, SessionName: "work", Origin: protocol.RouteOriginLocal, OriginKey: "local"}, request)
			return &client.AttachTargetError{Target: protocol.AttachTarget{Endpoint: "selected.example", Session: "picked", Intent: protocol.IntentAttach}}
		},
	})
	require.NoError(t, err)
	require.Equal(t, 1, localCalls, "the failed local picker attach must not retry locally")
	require.Equal(t, 1, remoteCalls, "the selected target must be dialed and attached exactly once")
}

func TestRunAttachWithDepsRejectsInvalidHandoffBeforeDialing(t *testing.T) {
	tests := []struct {
		name   string
		target protocol.AttachTarget
	}{
		{name: "invalid host", target: protocol.AttachTarget{Endpoint: "remote host", Session: "work", Intent: protocol.IntentAttach}},
		{name: "invalid session", target: protocol.AttachTarget{Endpoint: "remote.example", Session: "bad name", Intent: protocol.IntentAttach}},
		{name: "tokenless resume", target: protocol.AttachTarget{Endpoint: "remote.example", Session: "work", Intent: protocol.IntentResume}},
		{
			name: "tokenless resume with remote target",
			target: protocol.AttachTarget{
				Endpoint: "remote.example", Session: "work", Intent: protocol.IntentResume,
				RemoteTarget: &domain.RemoteSessionTarget{
					Endpoint: "remote.example", DisplayOrigin: "remote.example",
					LifecycleID: domain.SessionLifecycleID{1}, SessionName: "work", LiveTabID: "tab-1",
				},
				EnvironmentPolicy: protocol.EnvironmentPolicyDaemonOwned,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			factory := portsmocks.NewMockRemoteDialerFactory(t)
			factory.EXPECT().DialerForRemote("remote.example", "work", ports.RemoteTransportUDP, mock.Anything).Return(namedDialer{name: "remote"}, nil).Once()
			calls := 0
			err := runAttachWithDeps(context.Background(), protocol.IntentAttach, "work", "remote.example", "", nil, runAttachDeps{
				remoteDialerFactory: factory,
				runClient: func(_ context.Context, _ client.Dependencies, _ client.AttachRequest) error {
					calls++
					return &client.AttachTargetError{Target: tt.target}
				},
			})
			require.ErrorContains(t, err, "invalid remote attach handoff")
			require.Equal(t, 1, calls)
		})
	}
}

func TestRunAttachWithDepsRejectsInvalidRemoteTransportBeforeDialing(t *testing.T) {
	factory := portsmocks.NewMockRemoteDialerFactory(t)
	runClientCalled := false

	err := runAttachWithDeps(context.Background(), protocol.IntentAttach, "work", "remote.example", "", nil, runAttachDeps{
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

	err := runAttachWithDeps(context.Background(), protocol.IntentAttach, "work", "remote.example", "", nil, runAttachDeps{
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
	err := runAttachWithDeps(context.Background(), protocol.IntentEphemeral, "", "", "", nil, runAttachDeps{
		localDialer:         func() ports.Dialer { return namedDialer{name: "local"} },
		remoteDialerFactory: factory,
		clipboard:           &fakeClipboardReader{}, // must NOT reach runClient for a local attach
		runClient: func(ctx context.Context, deps client.Dependencies, request client.AttachRequest) error {
			requireNamedClientDialer(t, ctx, deps.Dialer, "local")
			gotDialer = "local"
			gotRemote = deps.Remote
			if request.Remote {
				t.Fatal("local carriage metadata must not be in the attach request")
			}
			gotClipboard = deps.Clipboard
			if request.Intent != protocol.IntentEphemeral || request.SessionName != "" {
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
		if err := runUDPBootstrap(context.Background()); err != nil {
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

	err := runUDPBootstrap(context.Background())
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

	err := runUDPBootstrap(context.Background())
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

type appCatalogCacheStub struct {
	loads int
}

func (s *appCatalogCacheStub) Load() ([]ports.RemoteCatalogCacheEntry, error) {
	s.loads++
	return nil, nil
}

func (s *appCatalogCacheStub) Store([]ports.RemoteCatalogCacheEntry) error { return nil }

func TestRemoteDiscoveryDaemonOptionWiresProductionPorts(t *testing.T) {
	oldHostStore := newRemoteHostStore
	oldCatalogCache := newRemoteCatalogCache
	oldCatalogClient := newRemoteCatalogClient
	oldDialerFactory := newRemoteDialerFactoryWithRuntimeObserver
	t.Cleanup(func() {
		newRemoteHostStore = oldHostStore
		newRemoteCatalogCache = oldCatalogCache
		newRemoteCatalogClient = oldCatalogClient
		newRemoteDialerFactoryWithRuntimeObserver = oldDialerFactory
	})

	stateDir := t.TempDir()
	cache := &appCatalogCacheStub{}
	var hostPath, cachePath string
	newRemoteHostStore = func(path string) ports.RemoteHostStore {
		hostPath = path
		return nil
	}
	newRemoteCatalogCache = func(path string) ports.RemoteCatalogCache {
		cachePath = path
		return cache
	}
	newRemoteCatalogClient = func() ports.RemoteCatalogClient { return nil }
	newRemoteDialerFactoryWithRuntimeObserver = func(ports.SerializedRuntimeObserver) ports.RemoteDialerFactory { return nil }

	option, err := remoteDiscoveryDaemonOption(stateDir, nil, "stdio")
	require.NoError(t, err)
	_ = daemon.New(nil, nil, slog.New(slog.NewTextHandler(io.Discard, nil)), option)
	require.Equal(t, remoteadapter.HostStorePath(stateDir), hostPath)
	require.Equal(t, remoteadapter.CatalogCachePath(stateDir), cachePath)
	require.Equal(t, 1, cache.loads, "daemon startup must load the cache once")
}

func TestRemoteDiscoveryDaemonOptionRejectsInvalidTransport(t *testing.T) {
	_, err := remoteDiscoveryDaemonOption(t.TempDir(), nil, "serial")
	require.EqualError(t, err, `vev: invalid remote transport "serial" (want "udp" or "stdio")`)
}
