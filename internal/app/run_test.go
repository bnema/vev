package app

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/bnema/vev/internal/adapters/snapshot"
	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/persist"
	"github.com/bnema/vev/internal/ports"
	portsmocks "github.com/bnema/vev/internal/ports/mocks"
	"github.com/bnema/vev/internal/usecase/client"
	"github.com/bnema/vev/internal/usecase/confirm"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

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

func TestPrintSessionsShowsStoppedState(t *testing.T) {
	var out bytes.Buffer
	printSessions(&out, []ports.SessionInfo{
		{Name: "main", Tabs: 2, Attached: true},
		{Name: "old", Stopped: true},
	})
	got := out.String()
	for _, want := range []string{"NAME", "STATE", "main", "running", "2", "yes", "old", "stopped", "-"} {
		if !strings.Contains(got, want) {
			t.Fatalf("printSessions output %q missing %q", got, want)
		}
	}
}

func TestRunListReadsStoppedSessionsWithoutDaemon(t *testing.T) {
	stateRoot, runtimeRoot := t.TempDir(), t.TempDir()
	t.Setenv("XDG_STATE_HOME", stateRoot)
	t.Setenv("XDG_RUNTIME_DIR", runtimeRoot)

	p, err := persist.Open(filepath.Join(stateRoot, "vev"))
	if err != nil {
		t.Fatalf("persist.Open error = %v", err)
	}
	now := time.Now().UnixNano()
	if err := p.Save(persist.Record{Name: "stored", Cwd: t.TempDir(), CreatedAt: now, UpdatedAt: now}); err != nil {
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

	p, err := persist.Open(filepath.Join(stateRoot, "vev"))
	if err != nil {
		t.Fatalf("persist.Open error = %v", err)
	}
	now := time.Now().UnixNano()
	if err := p.Save(persist.Record{Name: "stored", Cwd: t.TempDir(), CreatedAt: now, UpdatedAt: now}); err != nil {
		t.Fatalf("Save error = %v", err)
	}
	if err := p.Close(); err != nil {
		t.Fatalf("Close error = %v", err)
	}
	snapshots := snapshot.NewStore(filepath.Join(stateRoot, "vev", "snapshots"))
	if err := snapshots.Write("stored", []byte("snapshot bytes")); err != nil {
		t.Fatalf("snapshot Write error = %v", err)
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
	blobs, err := snapshots.Load()
	if err != nil {
		t.Fatalf("snapshot Load error = %v", err)
	}
	if len(blobs) != 0 {
		t.Fatalf("snapshots after kill = %#v, want none", blobs)
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
		attachLocal: func(context.Context, uint8, string, *slog.Logger) error {
			called = true
			return nil
		},
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
		attachLocal: func(context.Context, uint8, string, *slog.Logger) error {
			t.Fatal("nested new should not attach to the session")
			return nil
		},
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
		{name: "default remote mode is stdio", selectedTransport: "", wantMode: ports.RemoteTransportStdio},
		{name: "explicit udp mode", selectedTransport: "udp", wantMode: ports.RemoteTransportUDP},
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
				runClient: func(_ context.Context, d ports.Dialer, _ ports.Terminal, _ ports.Clock, intent uint8, name string, remote bool, clipboard ports.ClipboardReader, _ *slog.Logger) error {
					nd, ok := d.(namedDialer)
					if !ok {
						t.Fatalf("dialer type = %T, want namedDialer", d)
					}
					gotDialer = nd.name
					gotRemote = remote
					gotClipboard = clipboard
					if intent != ports.IntentAttach || name != "work" {
						t.Fatalf("intent/name = %d/%q, want attach/work", intent, name)
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
		runClient: func(context.Context, ports.Dialer, ports.Terminal, ports.Clock, uint8, string, bool, ports.ClipboardReader, *slog.Logger) error {
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
	factory.EXPECT().DialerForRemote("remote.example", "work", ports.RemoteTransportStdio, mock.Anything).Return(nil, factoryErr)
	runClientCalled := false

	err := runAttachWithDeps(context.Background(), ports.IntentAttach, "work", "remote.example", "", nil, runAttachDeps{
		remoteDialerFactory: factory,
		runClient: func(context.Context, ports.Dialer, ports.Terminal, ports.Clock, uint8, string, bool, ports.ClipboardReader, *slog.Logger) error {
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
		runClient: func(_ context.Context, d ports.Dialer, _ ports.Terminal, _ ports.Clock, intent uint8, name string, remote bool, clipboard ports.ClipboardReader, _ *slog.Logger) error {
			nd, ok := d.(namedDialer)
			if !ok {
				t.Fatalf("dialer type = %T, want namedDialer", d)
			}
			gotDialer = nd.name
			gotRemote = remote
			gotClipboard = clipboard
			if intent != ports.IntentEphemeral || name != "" {
				t.Fatalf("intent/name = %d/%q, want ephemeral/empty", intent, name)
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
