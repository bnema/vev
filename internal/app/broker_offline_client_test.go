//go:build linux

package app

import (
	"context"
	"crypto/rand"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/adapters/brokerconfig"
	"github.com/bnema/vev/internal/adapters/brokeripc"
	"github.com/bnema/vev/internal/adapters/clock"
	"github.com/bnema/vev/internal/adapters/daemonmux"
	"github.com/bnema/vev/internal/adapters/ipc"
	"github.com/bnema/vev/internal/adapters/pty"
	"github.com/bnema/vev/internal/adapters/snapshot"
	"github.com/bnema/vev/internal/adapters/uiterm"
	"github.com/bnema/vev/internal/adapters/webterm"
	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/persist"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/usecase/daemon"
	"github.com/bnema/vev/internal/usecase/recovery"
	"github.com/bnema/vev/pkg/safedir"
)

type offlineClientFixture struct {
	root          string
	socket        string
	streams       <-chan struct{}
	physical      <-chan struct{}
	physicalCount func() int
	policy        ports.BrokerPolicy
	losePhysical  func()
	cancel        context.CancelFunc
	prodRuntime   string
	prodState     string
	// catalogue is the daemon's durable session catalogue when the fixture was
	// started with persistence; nil otherwise.
	catalogue *persist.Persister
}

func startOfflineClientFixture(t *testing.T) offlineClientFixture {
	t.Helper()
	// The default sandbox shell prints one marker and then parks on read, so a
	// client proves it reached a session without depending on shell echo of
	// injected input.
	return startOfflineClientFixtureWithShell(t, "/bin/sh", []string{"-c", "sleep 0.1; printf offline-ready; trap : TERM; while :; do read line || exit; done"})
}

// startOfflineClientFixtureWithShell starts the same private broker sandbox with
// an explicitly supplied daemon shell, so a browser test can drive real session
// content and real session actions (echo and command output) through the
// ordinary client path instead of a parked shell.
func startOfflineClientFixtureWithShell(t *testing.T, shell string, shellArgs []string) offlineClientFixture {
	t.Helper()
	return startPersistentOfflineClientFixture(t, shell, shellArgs, false)
}

// startPersistentOfflineClientFixture starts the same private broker sandbox but
// additionally binds the daemon to real session persistence under the sandbox
// state directory. It exists so a test can prove which lifecycle state the
// daemon actually persists: a named session appears in the durable catalogue,
// while an ephemeral one is never written.
func startPersistentOfflineClientFixture(t *testing.T, shell string, shellArgs []string, persistEnabled bool) offlineClientFixture {
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
	physicalSeen := make(chan struct{}, 8)
	var physicalMu sync.Mutex
	var physicalConnections []io.Closer
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
			physicalMu.Lock()
			physicalConnections = append(physicalConnections, raw)
			physicalMu.Unlock()
			select {
			case physicalSeen <- struct{}{}:
			default:
			}
			go func() { _ = mux.Adopt(ctx, raw) }()
		}
	}()
	streamSeen := make(chan struct{}, 16)
	counting := &countingServerListener{ServerListener: aggregate, seen: streamSeen}
	daemonOpts := []daemon.Option{daemon.WithShell(shell, shellArgs)}
	var fixtureCatalogue *persist.Persister
	if persistEnabled {
		stateDir := filepath.Join(root, "state")
		opened, err := persist.OpenOrCreate(stateDir)
		require.NoError(t, err)
		repository := snapshot.NewRepository(filepath.Join(stateDir, "snapshots"))
		coordinator := recovery.NewCoordinator(opened.Catalogue, repository, rand.Reader)
		daemonOpts = append(daemonOpts,
			daemon.WithCatalogue(opened.Catalogue, opened.Records),
			daemon.WithSnapshotRepository(repository),
			daemon.WithRecoveryCoordinator(coordinator),
		)
		fixtureCatalogue = opened.Catalogue
	}
	d := daemon.New(pty.NewFactory(), clock.New(), discardLog(), daemonOpts...)
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
	// The sandbox broker becomes socket-ready before its first local daemon
	// observation is committed. Product clients start from a committed local
	// authority; wait for the fixture to provide the same contract so one-shot
	// initial navigation is not consumed against an artificial empty catalogue.
	probe, err := brokeripc.NewConnector(socket, brokeripc.Config{}).Connect(ctx)
	require.NoError(t, err)
	subscription, err := probe.Subscribe()
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		for _, observation := range probe.Snapshot().Daemons {
			if observation.Local {
				return true
			}
		}
		select {
		case <-subscription.Changed():
		default:
		}
		return false
	}, brokerTestWait, 5*time.Millisecond, "offline broker never committed its local daemon authority")
	subscription.Close()
	require.NoError(t, probe.Close())
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
	losePhysical := func() {
		physicalMu.Lock()
		connections := append([]io.Closer(nil), physicalConnections...)
		physicalMu.Unlock()
		for _, connection := range connections {
			_ = connection.Close()
		}
	}
	physicalCount := func() int {
		physicalMu.Lock()
		defer physicalMu.Unlock()
		return len(physicalConnections)
	}
	return offlineClientFixture{
		root: root, socket: socket, streams: streamSeen, physical: physicalSeen,
		physicalCount: physicalCount, policy: policy, losePhysical: losePhysical,
		cancel: cancel, prodRuntime: prodRuntime, prodState: prodState,
		catalogue: fixtureCatalogue,
	}
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

// TestOfflineEphemeralSessionIsNotPersistedViaBroker proves the daemon-owned
// broker fixture persists a named session but never an ephemeral one: the
// durable catalogue holds exactly the named record after both lifecycles, so
// the ephemeral session survives only while its daemon does.
func TestOfflineEphemeralSessionIsNotPersistedViaBroker(t *testing.T) {
	fixture := startPersistentOfflineClientFixture(t, "/bin/sh",
		[]string{"-c", "sleep 0.1; printf ephemeral-ready; trap : TERM; while :; do read line || exit; done"}, true)
	require.NotNil(t, fixture.catalogue, "the persistent fixture must expose its durable catalogue")
	ctx, cancel := context.WithTimeout(context.Background(), brokerTestWait)
	defer cancel()

	service, err := brokeripc.NewConnector(fixture.socket, brokeripc.Config{}).Connect(ctx)
	require.NoError(t, err)
	defer service.Close()

	create := func(name string, admission ports.BrokerStreamAdmission, intent uint8) {
		t.Helper()
		streamID, err := service.NextStreamID()
		require.NoError(t, err)
		request := ports.BrokerOpenStreamRequest{
			Purpose: ports.BrokerStreamAttachment, Stream: streamID, Local: true,
			Policy: fixture.policy, Admission: admission, Name: name,
			StartMode: ports.BrokerDaemonStartIfNeeded,
		}
		stream, err := service.OpenStream(ctx, request)
		require.NoError(t, err)
		require.NoError(t, stream.SendClient(protocol.Hello{
			Version: protocol.Version, Intent: intent, Name: name,
			Size: domain.Size{Cols: 80, Rows: 24}, EnvironmentPolicy: protocol.EnvironmentPolicyClientOwned,
		}))
		receiveOfflineWelcome(t, stream)
		require.NoError(t, stream.Close())
	}

	create("persisted", ports.BrokerAdmissionCreateNamed, protocol.IntentNew)
	create("", ports.BrokerAdmissionCreateEphemeral, protocol.IntentEphemeral)

	// The daemon persists on its own cadence; the durable catalogue must settle
	// to exactly the named record and never gain the ephemeral one.
	require.Eventually(t, func() bool {
		records, err := fixture.catalogue.Records()
		require.NoError(t, err)
		return len(records) == 1 && records[0].Name == "persisted"
	}, brokerTestWait, brokerTestPollTick, "the durable catalogue must hold exactly the named session")

	records, err := fixture.catalogue.Records()
	require.NoError(t, err)
	for _, record := range records {
		require.NotEmpty(t, record.Name, "an ephemeral session must never be persisted")
	}
}

func TestOfflineClientTerminalReachesSessionThroughBrokerIPC(t *testing.T) {
	fixture := startOfflineClientFixture(t)
	ctx, cancel := context.WithCancel(t.Context())
	terminal, err := uiterm.New(ctx, domain.Geometry{Size: domain.Size{Cols: 80, Rows: 24}}, "")
	require.NoError(t, err)
	done := make(chan error, 1)
	go func() {
		done <- runOfflineClient(ctx, fixture.socket, terminal, nil, discardLog(), nil, nil)
	}()
	awaitLogicalStream(t, fixture.streams)
	awaitUITerminalMarker(t, terminal, "offline-ready")
	cancel()
	select {
	case err := <-done:
		require.NoError(t, ignoreContextCancellation(err))
	case <-time.After(brokerTestWait):
		t.Fatal("offline terminal client did not stop")
	}
}

// The sandbox UI-driver harness is exercised by
// TestUIDriverSandboxHarnessUsesItsOwnConnector, TestUIDriverSandboxHarnessPickerOpensNothing,
// TestUIDriverClientKeepsSharedDaemonOnEOF, and the shared
// runUIDriverClient tests in ui_driver_client_test.go; the harness is the same
// runUIDriverClient call production uses, so it is not re-tested here.

func TestOfflineBrokerSharedTransportKeepsAttachmentsIndependent(t *testing.T) {
	fixture := startOfflineClientFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), brokerTestWait)
	defer cancel()

	service, err := brokeripc.NewConnector(fixture.socket, brokeripc.Config{}).Connect(ctx)
	require.NoError(t, err)
	defer service.Close()
	connections := make([]ports.BrokerLogicalConnection, 3)
	welcomes := make([]protocol.Welcome, 3)
	for i := range connections {
		streamID, err := service.NextStreamID()
		require.NoError(t, err)
		request := ports.BrokerOpenStreamRequest{Purpose: ports.BrokerStreamAttachment, Stream: streamID, Local: true, Policy: fixture.policy, StartMode: ports.BrokerDaemonStartIfNeeded}
		if i == 0 {
			request.Admission, request.Name = ports.BrokerAdmissionCreateNamed, "shared"
		} else {
			require.NotNil(t, welcomes[0].CommittedIdentity)
			request.Admission, request.Target = ports.BrokerAdmissionExact, welcomes[0].CommittedIdentity.Target
		}
		connection, err := service.OpenStream(ctx, request)
		require.NoError(t, err)
		connections[i] = connection
		hello := protocol.Hello{Version: protocol.Version, ClientID: [16]byte{9}, Size: domain.Size{Cols: 80 + i*10, Rows: 24 + i}}
		if i == 0 {
			hello.Intent, hello.Name = protocol.IntentNew, "shared"
		} else {
			hello.Intent, hello.Name, hello.ExactTarget = protocol.IntentAttach, "shared", &request.Target
		}
		require.NoError(t, connection.SendClient(hello))
		welcomes[i] = receiveOfflineWelcome(t, connection)
	}
	for range connections {
		awaitLogicalStream(t, fixture.streams)
	}
	// The fixture also performs one observation dial. The three attachment
	// streams must add only one pooled carriage, never one carriage per client.
	for range 2 {
		select {
		case <-fixture.physical:
		case <-time.After(brokerTestWait):
			t.Fatal("expected observation and pooled attachment transports")
		}
	}
	baselinePhysical := fixture.physicalCount()
	t.Cleanup(func() {
		require.Equal(t, baselinePhysical, fixture.physicalCount(), "logical attachments opened another daemonmux carriage")
	})

	for i, welcome := range welcomes {
		require.Equal(t, "shared", welcome.SessionName)
		require.NotZero(t, welcome.ResumeToken, "attachment %d has no independent resume token", i)
		for j := range i {
			require.NotEqual(t, welcomes[j].ResumeToken, welcome.ResumeToken, "same client process identity collapsed attachments %d and %d", j, i)
		}
	}
	// Resize remains attachment session traffic: each logical stream reports its
	// own window after distinct claims rather than a broker-coalesced peer size.
	sizes := make([]domain.Size, len(connections))
	for i, connection := range connections {
		sizes[i] = domain.Size{Cols: 101 + i*11, Rows: 31 + i}
		require.NoError(t, connection.SendClient(protocol.Resize{Size: sizes[i]}))
	}
	for i, connection := range connections {
		require.Equal(t, sizes[i], receiveOfflineOutputSize(t, connection, sizes[i]))
	}
	require.NoError(t, connections[0].SendClient(protocol.CommandRequest{Version: protocol.Version, RequestID: 6, Attached: true, Slug: "next-tab"}))
	require.True(t, receiveOfflineCommandResult(t, connections[0], 6).Outcome == protocol.CommandSucceeded)
	require.Equal(t, sizes[0], receiveOfflineOutputSize(t, connections[0], sizes[0]))
	for _, connection := range connections {
		require.NoError(t, connection.SendClient(protocol.CommandRequest{Version: protocol.Version, RequestID: 7, Attached: true, Slug: "next-tab"}))
		result := receiveOfflineCommandResult(t, connection, 7)
		require.True(t, result.Outcome == protocol.CommandSucceeded, result.Text)
	}

	require.NoError(t, connections[0].SendClient(protocol.Detach{}))
	require.NoError(t, receiveOfflineEOF(t, connections[0]), "detach must end its logical stream with orderly EOF")
	for i := 1; i < len(connections); i++ {
		requestID := uint64(20 + i)
		require.NoError(t, connections[i].SendClient(protocol.CommandRequest{Version: protocol.Version, RequestID: requestID, Attached: true, Slug: "next-tab"}))
		require.True(t, receiveOfflineCommandResult(t, connections[i], requestID).Outcome == protocol.CommandSucceeded)
	}
	for i := 1; i < len(connections); i++ {
		require.NoError(t, connections[i].Close())
	}
}

func TestOfflineBrokerPhysicalLossSettlesEverySharedStream(t *testing.T) {
	fixture := startOfflineClientFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), brokerTestWait)
	defer cancel()
	service, err := brokeripc.NewConnector(fixture.socket, brokeripc.Config{}).Connect(ctx)
	require.NoError(t, err)
	defer service.Close()

	var target protocol.ExactSessionTarget
	connections := make([]ports.BrokerLogicalConnection, 3)
	for i := range connections {
		streamID, err := service.NextStreamID()
		require.NoError(t, err)
		request := ports.BrokerOpenStreamRequest{Purpose: ports.BrokerStreamAttachment, Stream: streamID, Local: true, Policy: fixture.policy, StartMode: ports.BrokerDaemonStartIfNeeded}
		if i == 0 {
			request.Admission, request.Name = ports.BrokerAdmissionCreateNamed, "loss"
		} else {
			request.Admission, request.Target = ports.BrokerAdmissionExact, target
		}
		connection, err := service.OpenStream(ctx, request)
		require.NoError(t, err)
		connections[i] = connection
		hello := protocol.Hello{Version: protocol.Version, ClientID: [16]byte{3}, Size: domain.Size{Cols: 80 + i, Rows: 24}, Intent: protocol.IntentAttach, Name: "loss", ExactTarget: &target}
		if i == 0 {
			hello.Intent, hello.ExactTarget = protocol.IntentNew, nil
		}
		require.NoError(t, connection.SendClient(hello))
		welcome := receiveOfflineWelcome(t, connection)
		if i == 0 {
			require.NotNil(t, welcome.CommittedIdentity)
			target = welcome.CommittedIdentity.Target
		}
	}
	fixture.losePhysical()
	for i, connection := range connections {
		select {
		case <-connection.Done():
			// Closing the accepted raw carriage is an orderly transport loss at
			// this seam; the required invariant is that every stream settles.
		case <-time.After(brokerTestWait):
			t.Fatalf("stream %d survived physical transport loss", i)
		}
	}
}

func receiveOfflineWelcome(t *testing.T, connection ports.BrokerLogicalConnection) protocol.Welcome {
	t.Helper()
	message := receiveOfflineMessage(t, connection, "Welcome", func(message protocol.ServerMessage) bool {
		_, ok := message.(protocol.Welcome)
		return ok
	})
	return message.(protocol.Welcome)
}

func receiveOfflineOutputSize(t *testing.T, connection ports.BrokerLogicalConnection, expected domain.Size) domain.Size {
	t.Helper()
	message := receiveOfflineMessage(t, connection, "Output size", func(message protocol.ServerMessage) bool {
		output, ok := message.(protocol.Output)
		return ok && output.Size == expected
	})
	return message.(protocol.Output).Size
}

func receiveOfflineCommandResult(t *testing.T, connection ports.BrokerLogicalConnection, requestID uint64) protocol.CommandResult {
	t.Helper()
	message := receiveOfflineMessage(t, connection, "CommandResult", func(message protocol.ServerMessage) bool {
		result, ok := message.(protocol.CommandResult)
		return ok && result.RequestID == requestID
	})
	return message.(protocol.CommandResult)
}

func receiveOfflineEOF(t *testing.T, connection ports.BrokerLogicalConnection) error {
	t.Helper()
	result := make(chan error, 1)
	go func() {
		for {
			_, err := connection.ReceiveServer()
			if err != nil {
				result <- err
				return
			}
		}
	}()
	select {
	case err := <-result:
		if errors.Is(err, io.EOF) {
			return nil
		}
		return err
	case <-time.After(brokerTestWait):
		return errors.New("timed out waiting for orderly EOF")
	}
}

func receiveOfflineMessage(t *testing.T, connection ports.BrokerLogicalConnection, want string, match func(protocol.ServerMessage) bool) protocol.ServerMessage {
	t.Helper()
	type result struct {
		message protocol.ServerMessage
		err     error
	}
	resultCh := make(chan result, 1)
	go func() {
		for {
			message, err := connection.ReceiveServer()
			if err != nil || match(message) {
				resultCh <- result{message: message, err: err}
				return
			}
		}
	}()
	select {
	case received := <-resultCh:
		require.NoError(t, received.err, "waiting for %s", want)
		return received.message
	case <-connection.Done():
		t.Fatalf("connection settled while waiting for %s: %v", want, connection.Err())
	case <-time.After(brokerTestWait):
		_ = connection.Close()
		t.Fatalf("timed out waiting for %s", want)
	}
	return nil
}

func TestOfflineClientBrowserReachesSessionThroughBrokerIPC(t *testing.T) {
	fixture := startOfflineClientFixture(t)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	token := strings.Repeat("b", 43)
	handler, err := webterm.NewServer(ctx, webterm.Settings{}, token, func(runCtx context.Context, terminal *webterm.Terminal) error {
		return runOfflineClient(runCtx, fixture.socket, terminal, nil, discardLog(), nil, nil)
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
