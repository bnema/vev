//go:build linux

package app

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/adapters/brokerconfig"
	"github.com/bnema/vev/internal/adapters/clock"
	"github.com/bnema/vev/internal/adapters/daemonmux"
	"github.com/bnema/vev/internal/adapters/ipc"
	"github.com/bnema/vev/internal/adapters/pty"
	"github.com/bnema/vev/internal/adapters/uiterm"
	"github.com/bnema/vev/internal/adapters/webterm"
	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/usecase/client"
	"github.com/bnema/vev/internal/usecase/daemon"
	"github.com/bnema/vev/pkg/safedir"
)

type offlineClientFixture struct {
	root        string
	socket      string
	streams     <-chan struct{}
	cancel      context.CancelFunc
	prodRuntime string
	prodState   string
}

func startOfflineClientFixture(t *testing.T) offlineClientFixture {
	t.Helper()
	root, prodRuntime, prodState := isolateSandboxEnv(t)
	require.NoError(t, os.MkdirAll(root, 0o700))
	route := filepath.Join(root, "local-mux.sock")
	policy := brokerTestPolicy()
	writeSandboxConfig(t, root, map[string]any{
		"marker": brokerconfig.Marker, "registrations": []any{},
		"local": map[string]any{
			"identity": brokerLocalTestIdentity, "displayOrigin": "local", "route": route,
			"policy": map[string]any{"protocolVersion": policy.ProtocolVersion, "catalogSchemaVersion": policy.CatalogSchemaVersion, "environmentPolicy": "client-owned", "transport": policy.Transport, "trust": policy.Trust, "launch": policy.Launch, "isolation": policy.Isolation},
		},
	})
	layout, err := offlineLayout(root)
	require.NoError(t, err)
	for _, dir := range []string{layout.Root, layout.Runtime, layout.State, layout.Log} {
		require.NoError(t, safedir.EnsurePrivate(dir))
	}
	require.NoError(t, layout.VerifyCreated())

	rawListener, err := ipc.ListenMux(route)
	require.NoError(t, err)
	aggregate := daemonmux.NewAggregateListener()
	binding, err := daemonmux.NewServerBinding(brokerLocalTestIdentity, ports.BrokerDaemonIncarnation{1}, policy)
	require.NoError(t, err)
	mux, err := daemonmux.NewServerSupervisor(aggregate, binding, daemonmux.DefaultMuxCeilings(), 8)
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		for {
			raw, acceptErr := rawListener.Accept()
			if acceptErr != nil {
				return
			}
			go func() { _ = mux.Adopt(ctx, raw) }()
		}
	}()
	streamSeen := make(chan struct{}, 16)
	counting := &countingServerListener{ServerListener: aggregate, seen: streamSeen}
	d := daemon.New(pty.NewFactory(), clock.New(), discardLog(), daemon.WithShell("/bin/sh", []string{"-c", "sleep 0.1; printf offline-ready; trap : TERM; while :; do read line || exit; done"}))
	go func() { _ = d.Serve(ctx, counting) }()

	// The client picker and registry must share wall-clock freshness semantics:
	// a manual clock rooted at Unix zero would correctly make the observation
	// stale to the real client clock before initial navigation resolves.
	deps, ready := testBrokerServeDeps(clock.New())
	brokerDone := runSandbox(ctx, brokerServeOptions{offlineRoot: root, idleGrace: time.Hour}, deps)
	var socket string
	select {
	case socket = <-ready:
	case serveErr := <-brokerDone:
		t.Fatalf("offline broker exited before readiness: %v", serveErr)
	case <-time.After(brokerTestWait):
		t.Fatal("offline broker did not become ready")
	}
	t.Cleanup(func() {
		cancel()
		_ = rawListener.Close()
		_ = mux.Close()
		_ = aggregate.Close()
		select {
		case <-brokerDone:
		case <-time.After(brokerTestWait):
			t.Error("offline broker did not stop")
		}
		requireProductionUntouched(t, prodRuntime, prodState)
	})
	return offlineClientFixture{root: root, socket: socket, streams: streamSeen, cancel: cancel, prodRuntime: prodRuntime, prodState: prodState}
}

type countingServerListener struct {
	ports.ServerListener
	seen chan<- struct{}
}

func (l *countingServerListener) Accept() (ports.ServerConnection, error) {
	connection, err := l.ServerListener.Accept()
	if err == nil {
		select {
		case l.seen <- struct{}{}:
		default:
		}
	}
	return connection, err
}

func awaitLogicalStream(t *testing.T, streams <-chan struct{}) {
	t.Helper()
	select {
	case <-streams:
	case <-time.After(brokerTestWait):
		t.Fatal("client did not reach a session through a broker logical stream")
	}
}

func offlineSnapshotText(snapshot ports.UISnapshot) string {
	var text strings.Builder
	for _, cell := range snapshot.Cells {
		if !cell.Continuation {
			text.WriteString(cell.Text)
		}
	}
	return text.String()
}

func awaitUITerminalMarker(t *testing.T, terminal *uiterm.Terminal, marker string) {
	t.Helper()
	deadline := time.NewTimer(brokerTestWait)
	defer deadline.Stop()
	for {
		if snapshot, err := terminal.Snapshot(); err == nil && strings.Contains(offlineSnapshotText(snapshot), marker) {
			return
		}
		select {
		case <-terminal.Changes():
		case <-deadline.C:
			t.Fatalf("terminal never rendered session marker %q", marker)
		}
	}
}

func TestParseBrokerClientArgsAndProductionDispatchIsolation(t *testing.T) {
	parsed, err := parseArgs([]string{brokerClientCommand, "--offline-root", "/tmp/offline", "--harness", offlineClientUIDriver})
	require.NoError(t, err)
	require.Equal(t, kindBrokerClient, parsed.kind)
	require.Equal(t, connectivityLocalOnly, mustConnectivityOwner(t, "kindBrokerClient"))

	ordinary := []struct {
		args []string
		kind cmdKind
	}{{nil, kindAttach}, {[]string{"list"}, kindList}, {[]string{"--ui-driver"}, kindUIDriver}, {[]string{"--web-daemon"}, kindWebDaemon}}
	for _, test := range ordinary {
		command, parseErr := parseArgs(test.args)
		require.NoError(t, parseErr)
		require.Equal(t, test.kind, command.kind)
		require.NotEqual(t, kindBrokerClient, command.kind)
	}

	rejections := []struct {
		name string
		args []string
	}{
		{name: "missing root", args: []string{brokerClientCommand}},
		{name: "missing root value", args: []string{brokerClientCommand, "--offline-root"}},
		{name: "duplicate root", args: []string{brokerClientCommand, "--offline-root", "/tmp/a", "--offline-root", "/tmp/b"}},
		{name: "unknown flag", args: []string{brokerClientCommand, "--offline-root", "/tmp/a", "--unknown"}},
		{name: "positional", args: []string{brokerClientCommand, "--offline-root", "/tmp/a", "extra"}},
		{name: "bad harness", args: []string{brokerClientCommand, "--offline-root", "/tmp/a", "--harness", "other"}},
	}
	for _, test := range rejections {
		t.Run(test.name, func(t *testing.T) {
			_, parseErr := parseArgs(test.args)
			require.Error(t, parseErr)
		})
	}
}

func mustConnectivityOwner(t *testing.T, kind string) connectivityOwner {
	t.Helper()
	owner, ok := connectivityOwnerFor(kind)
	require.True(t, ok)
	return owner
}

func TestOfflineClientTerminalReachesSessionThroughBrokerIPC(t *testing.T) {
	fixture := startOfflineClientFixture(t)
	ctx, cancel := context.WithCancel(t.Context())
	terminal, err := uiterm.New(ctx, domain.Geometry{Size: domain.Size{Cols: 80, Rows: 24}}, "")
	require.NoError(t, err)
	done := make(chan error, 1)
	failures := make(chan error, 1)
	go func() {
		done <- runOfflineClient(ctx, fixture.socket, terminal, discardLog(), nil, func(err error) {
			select {
			case failures <- err:
			default:
			}
		})
	}()
	select {
	case <-fixture.streams:
	case err := <-failures:
		t.Fatalf("offline client stream open failed: %v (%#v)", err, err)
	case <-time.After(brokerTestWait):
		t.Fatal("client did not reach a session through a broker logical stream")
	}
	awaitUITerminalMarker(t, terminal, "offline-ready")
	cancel()
	select {
	case err := <-done:
		require.NoError(t, ignoreContextCancellation(err))
	case <-time.After(brokerTestWait):
		t.Fatal("offline terminal client did not stop")
	}
}

func TestOfflineClientUIDriverReachesSessionThroughBrokerIPC(t *testing.T) {
	fixture := startOfflineClientFixture(t)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	terminal, err := uiterm.New(ctx, domain.Geometry{Size: domain.Size{Cols: 80, Rows: 24}}, "")
	require.NoError(t, err)
	ui := client.NewUI(terminal, clock.New())
	serverEnd, clientEnd := io.Pipe()
	writerReader, writerEnd := io.Pipe()
	stream := &stdioStream{reader: serverEnd, writer: writerEnd}
	done := make(chan error, 1)
	go func() { done <- runOfflineUIDriver(ctx, fixture.socket, terminal, ui, discardLog(), stream) }()
	awaitLogicalStream(t, fixture.streams)
	decoder := json.NewDecoder(writerReader)
	readyResult := make(chan map[string]any, 1)
	readyError := make(chan error, 1)
	go func() {
		var ready map[string]any
		if err := decoder.Decode(&ready); err != nil {
			readyError <- err
			return
		}
		readyResult <- ready
	}()
	select {
	case ready := <-readyResult:
		result, ok := ready["result"].(map[string]any)
		require.True(t, ok)
		require.Equal(t, false, result["control"], "autonomous supervisor has no UI action binding")
		require.Equal(t, string(ports.UIStatusReconnecting), result["status"], "reconnecting is the honest discovery status until autonomous UI binding exists")
		require.NotZero(t, result["generation"], "ready carries the real supervisor generation")
	case err := <-readyError:
		t.Fatalf("decode UI-driver ready: %v", err)
	case <-time.After(brokerTestWait):
		t.Fatal("offline UI driver did not publish ready")
	}
	awaitUITerminalMarker(t, terminal, "offline-ready")
	_ = clientEnd.Close()
	_ = writerReader.Close()
	cancel()
	select {
	case <-done:
	case <-time.After(brokerTestWait):
		t.Fatal("offline UI driver did not stop")
	}
}

func TestOfflineClientBrowserReachesSessionThroughBrokerIPC(t *testing.T) {
	fixture := startOfflineClientFixture(t)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	token := strings.Repeat("b", 43)
	handler, err := webterm.NewServer(ctx, webterm.Settings{}, token, func(runCtx context.Context, terminal *webterm.Terminal) error {
		return runOfflineClient(runCtx, fixture.socket, terminal, discardLog(), nil, nil)
	})
	require.NoError(t, err)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { r.Host = webterm.Address; handler.ServeHTTP(w, r) }))
	defer server.Close()
	headers := http.Header{"Origin": {webterm.Origin}, "Cookie": {"vev-web-session=" + token}}
	connection, _, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(server.URL, "http")+"/ws", &websocket.DialOptions{HTTPHeader: headers})
	require.NoError(t, err)
	defer connection.CloseNow()
	connection.SetReadLimit(64 << 20)
	awaitLogicalStream(t, fixture.streams)
	readCtx, stopRead := context.WithTimeout(ctx, brokerTestWait)
	defer stopRead()
	// This proves authenticated browser transport, real broker logical-stream
	// admission, and live browser frames. It does not prove post-Hello session
	// output: webterm may publish its transactional full-screen snapshot before
	// the fixture marker and does not guarantee another frame containing it.
	_, frame, err := connection.Read(readCtx)
	require.NoError(t, err)
	require.NotEmpty(t, frame, "browser emitted no frame after broker logical-stream admission")
	require.NoError(t, connection.CloseNow())
	waited := make(chan struct{})
	go func() {
		handler.Wait()
		close(waited)
	}()
	select {
	case <-waited:
	case <-time.After(brokerTestWait):
		t.Fatal("browser handler did not stop")
	}
}
