package app

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
	"unicode"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/adapters/brokerconfig"
	"github.com/bnema/vev/internal/adapters/brokeripc"
	"github.com/bnema/vev/internal/adapters/quic"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol"
)

// Real-composition coverage for the P3.4 slice E remote mux helpers.
//
// The SSH carriage is exercised with an owned test subprocess: a fake `ssh`
// on PATH re-executes this test binary through TestMain's broker-helper
// interception, so the real helper code runs with real pipes and a real unix
// daemonmux fixture, and the exact ssh argv is captured for inspection. The
// QUIC carriage runs a real loopback authenticated QUIC server, so a successful
// round trip proves pin, token, nonce, and daemonmux handshake all compose.

const (
	fakeSSHArgsEnv  = "VEV_TEST_SSH_ARGS"
	fakeSSHReadyEnv = "VEV_TEST_SSH_READY"
	fakeSSHPIDEnv   = "VEV_TEST_SSH_PID"
	fakeSSHBinEnv   = "VEV_TEST_SSH_BIN"
	fakeSSHModeEnv  = "VEV_TEST_SSH_MODE"
	fakeSSHRootEnv  = "VEV_TEST_SSH_REMOTE_ROOT"
)

// fakeSSHExecHelper is an owned test ssh: it records its exact argv and then
// re-executes this test binary as the named hidden mux helper against an
// isolated remote sandbox. Its stdin/stdout remain the carriage pipes.
const fakeSSHExecHelper = `#!/bin/sh
printf '%s\n' "$@" > "$` + fakeSSHArgsEnv + `"
exec "$` + fakeSSHBinEnv + `" "$` + fakeSSHModeEnv + `" --offline-root "$` + fakeSSHRootEnv + `"
`

// fakeSSHEmitReadiness prints one controlled readiness line and exits, so a
// test can drive the broker-side pinned QUIC dial without a real remote host.
const fakeSSHEmitReadiness = `#!/bin/sh
printf '%s\n' "$@" > "$` + fakeSSHArgsEnv + `"
printf '%s\n' "$` + fakeSSHReadyEnv + `"
exit 0
`

// fakeSSHSleep blocks without producing readiness, so a test can prove setup
// cancellation reaps the bootstrap child.
const fakeSSHSleep = `#!/bin/sh
printf '%s\n' "$@" > "$` + fakeSSHArgsEnv + `"
printf '%s\n' "$$" > "$` + fakeSSHPIDEnv + `"
sleep 30
`

// fakeSSHFloodStderr floods stderr with text carrying a terminal escape and a
// control byte and never prints readiness, so a test can prove the broker-side
// diagnostic sink is read only after the os/exec copy goroutine has been killed
// and joined.
const fakeSSHFloodStderr = `#!/bin/sh
printf '%s\n' "$@" > "$` + fakeSSHArgsEnv + `"
printf '%s\n' "$$" > "$` + fakeSSHPIDEnv + `"
i=0
while true; do
	printf 'flood-%s\033[31mred\033[0m\001\002-noise\n' "$i" >&2
	i=$((i+1))
	sleep 0.01
done
`

// fakeSSHStallNeverReady stalls without printing readiness and without exiting,
// so a test can prove the independent broker-side setup timeout interrupts a
// bootstrap that the caller never cancels. It execs the blocking sleep so the
// reaped process owns the captured stderr pipe and no grandchild lingers.
const fakeSSHStallNeverReady = `#!/bin/sh
printf '%s\n' "$@" > "$` + fakeSSHArgsEnv + `"
printf '%s\n' "$$" > "$` + fakeSSHPIDEnv + `"
exec sleep 30
`

// installFakeSSH puts an owned `ssh` executable first on PATH and returns the
// file its argv is recorded to.
func installFakeSSH(t *testing.T, script string) string {
	t.Helper()
	binDir := t.TempDir()
	require.NoError(t, os.Chmod(binDir, 0o755))
	argsFile := filepath.Join(t.TempDir(), "ssh-args")
	require.NoError(t, os.WriteFile(filepath.Join(binDir, "ssh"), []byte(script), 0o755))
	t.Setenv("PATH", binDir+string(os.PathListSeparator)+os.Getenv("PATH"))
	t.Setenv(fakeSSHArgsEnv, argsFile)
	t.Setenv(fakeSSHBinEnv, os.Args[0])
	return argsFile
}

// muxTestEnv isolates production XDG roots and records every detached mux
// helper so no test leaks a process.
type muxTestEnv struct {
	t                      *testing.T
	prodRuntime, prodState string
	recordDir              string
}

func newMuxTestEnv(t *testing.T) *muxTestEnv {
	t.Helper()
	_, prodRuntime, prodState := isolateSandboxEnv(t)
	recordDir := filepath.Join(shortTempDir(t, "vevm"), "r")
	require.NoError(t, os.MkdirAll(recordDir, 0o700))
	t.Setenv(brokerHelperEnv, "1")
	t.Setenv(brokerHelperRecordDirEnv, recordDir)
	env := &muxTestEnv{t: t, prodRuntime: prodRuntime, prodState: prodState, recordDir: recordDir}
	t.Cleanup(env.cleanup)
	return env
}

// cleanup kills every recorded mux helper process.
func (e *muxTestEnv) cleanup() {
	for _, role := range []string{brokerMuxStdioCommand, brokerMuxQUICBootstrapCommand, brokerMuxQUICProxyCommand} {
		raw, err := os.ReadFile(filepath.Join(e.recordDir, role+".pids"))
		if err != nil {
			continue
		}
		for _, field := range strings.Fields(string(raw)) {
			pid, convErr := strconv.Atoi(field)
			if convErr != nil {
				continue
			}
			if err := syscall.Kill(pid, syscall.SIGKILL); err != nil && !errors.Is(err, syscall.ESRCH) {
				continue
			}
			_ = waitForProcessExit(pid, 5*time.Second)
		}
	}
}

// writeMuxRoot writes one marker-valid sandbox root carrying a single
// registration with the supplied route object.
func writeMuxRoot(t *testing.T, route any) string {
	t.Helper()
	root := filepath.Join(shortTempDir(t, "vevx"), "s")
	require.NoError(t, os.MkdirAll(root, 0o700))
	writeSandboxConfig(t, root, sandboxRegistrationDocument(route))
	return root
}

// loadTestRoute loads one sandbox configuration and returns the route its
// resolver publishes, so a test can inspect a parsed route without the
// unexported brokerconfig fields.
func loadTestRoute(t *testing.T, route any) brokerconfig.Route {
	t.Helper()
	_, parsed := loadTestRouteAndConfig(t, route)
	return parsed
}

// loadTestRouteAndConfig loads one sandbox configuration and returns both the
// immutable configuration and the route its resolver publishes, so a test can
// dial the route the way the broker does: with the whole resolved target.
func loadTestRouteAndConfig(t *testing.T, route any) (*brokerconfig.Config, brokerconfig.Route) {
	t.Helper()
	root := writeMuxRoot(t, route)
	layout, err := offlineLayout(root)
	require.NoError(t, err)
	config, err := brokerconfig.Load(layout)
	require.NoError(t, err)
	resolved, err := config.Resolver().ResolveDialTarget(context.Background(), ports.BrokerOpenStreamRequest{
		Purpose:      ports.BrokerStreamControl,
		Stream:       1,
		Endpoint:     brokerTestEndpoint,
		Registration: brokerTestRegistration(),
		Policy:       brokerTestPolicy(),
		StartMode:    ports.BrokerDaemonStartIfNeeded,
	})
	require.NoError(t, err)
	parsed, ok := config.RouteByAddress(resolved.Address)
	require.True(t, ok)
	return config, parsed
}

// testMuxDialTarget resolves the exact dial target the broker publishes for one
// provisioned route under one start authorization, so a test dials the route
// with the same whole target the pool does.
func testMuxDialTarget(t *testing.T, config *brokerconfig.Config, route brokerconfig.Route, mode ports.BrokerDaemonStartMode) ports.BrokerDialTarget {
	t.Helper()
	resolved, err := config.Resolver().ResolveDialTarget(context.Background(), ports.BrokerOpenStreamRequest{
		Purpose:      ports.BrokerStreamControl,
		Stream:       1,
		Endpoint:     brokerTestEndpoint,
		Registration: brokerTestRegistration(),
		Policy:       brokerTestPolicy(),
		StartMode:    mode,
	})
	require.NoError(t, err)
	require.Equal(t, route.Address(), resolved.Address, "the test dials the provisioned route")
	return resolved
}

// loadTestRouteTarget loads one sandbox configuration and returns both the
// parsed route and the exact resolved dial target the broker would publish for
// it, so a test can call dialBrokerRoute exactly as the pool does.
func loadTestRouteTarget(t *testing.T, route any) (brokerconfig.Route, ports.BrokerDialTarget) {
	t.Helper()
	config, parsed := loadTestRouteAndConfig(t, route)
	return parsed, testMuxDialTarget(t, config, parsed, ports.BrokerDaemonStartIfNeeded)
}

// TestParseBrokerMuxArgs pins the strict hidden-helper argument contract.
func TestParseBrokerMuxArgs(t *testing.T) {
	tests := []struct {
		name    string
		args    []string
		wantErr string
		want    brokerMuxOptions
	}{
		{name: "valid", args: []string{"--offline-root", "/srv/sandbox"}, want: brokerMuxOptions{offlineRoot: "/srv/sandbox", startMode: ports.BrokerDaemonExistingOnly}},
		{name: "explicit existing-only", args: []string{"--offline-root", "/srv/sandbox", brokerDaemonStartArg, "existing-only"}, want: brokerMuxOptions{offlineRoot: "/srv/sandbox", startMode: ports.BrokerDaemonExistingOnly}},
		{name: "explicit if-needed", args: []string{"--offline-root", "/srv/sandbox", brokerDaemonStartArg, "if-needed"}, want: brokerMuxOptions{offlineRoot: "/srv/sandbox", startMode: ports.BrokerDaemonStartIfNeeded}},
		{name: "missing root", args: nil, wantErr: "requires --offline-root"},
		{name: "empty root", args: []string{"--offline-root", ""}, wantErr: "requires a path"},
		{name: "duplicate root", args: []string{"--offline-root", "/a", "--offline-root", "/b"}, wantErr: "duplicate"},
		{name: "unknown flag", args: []string{"--idle-grace", "1m"}, wantErr: "unknown flag"},
		{name: "positional", args: []string{"--offline-root", "/a", "extra"}, wantErr: "positional"},
		{name: "missing daemon-start value", args: []string{"--offline-root", "/a", brokerDaemonStartArg}, wantErr: "requires a value"},
		{name: "duplicate daemon-start", args: []string{"--offline-root", "/a", brokerDaemonStartArg, "if-needed", brokerDaemonStartArg, "existing-only"}, wantErr: "duplicate"},
		{name: "unknown daemon-start", args: []string{"--offline-root", "/a", brokerDaemonStartArg, "always"}, wantErr: "unknown daemon start mode"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			command, err := parseBrokerMuxArgs(brokerMuxStdioCommand, kindBrokerMuxStdio, tt.args)
			if tt.wantErr != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tt.wantErr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.want, command.brokerMux)
		})
	}
}

// TestBrokerMuxHelpersHiddenFromPublicHelp proves the three helpers parse but
// stay out of public help and never alter ordinary commands.
func TestBrokerMuxHelpersHiddenFromPublicHelp(t *testing.T) {
	for _, tt := range []struct {
		name string
		kind cmdKind
	}{
		{brokerMuxStdioCommand, kindBrokerMuxStdio},
		{brokerMuxQUICBootstrapCommand, kindBrokerMuxQUICBootstrap},
		{brokerMuxQUICProxyCommand, kindBrokerMuxQUICProxy},
	} {
		require.NotContains(t, usageText, tt.name)
		command, err := parseArgs([]string{tt.name, "--offline-root", "/srv/sandbox"})
		require.NoError(t, err)
		require.Equal(t, tt.kind, command.kind)
		require.Equal(t, "/srv/sandbox", command.brokerMux.offlineRoot)
	}
	// Ordinary commands are untouched by the new hidden entries.
	attach, err := parseArgs(nil)
	require.NoError(t, err)
	require.Equal(t, kindAttach, attach.kind)
	list, err := parseArgs([]string{"ls"})
	require.NoError(t, err)
	require.Equal(t, kindList, list.kind)
}

// TestSSHMuxCommandSpecPreservesTrustAndNoPTY proves the ssh argv keeps `-T`,
// the option terminator, and the quoted remote argv, splices only the explicit
// trust inputs, and never disables host-key verification.
func TestSSHMuxCommandSpecPreservesTrustAndNoPTY(t *testing.T) {
	isolateSandboxEnv(t)
	route := loadTestRoute(t, map[string]any{
		"kind":   "ssh-stdio",
		"target": "user@host:2222",
		"argv":   []any{"vev", brokerMuxStdioCommand, "--offline-root", "/srv/remote"},
		"trust":  map[string]any{"knownHostsFile": "/etc/ssh/known_hosts", "connectTimeout": "30s"},
	})
	spec, err := sshMuxCommandSpec(route, ports.BrokerDaemonStartIfNeeded)
	require.NoError(t, err)
	require.Equal(t, "ssh", spec.Path)
	require.Equal(t, "-T", spec.Args[0])
	require.Contains(t, spec.Args, "--")
	require.Contains(t, spec.Args, "UserKnownHostsFile=/etc/ssh/known_hosts")
	require.Contains(t, spec.Args, "ConnectTimeout=30")
	joined := strings.Join(spec.Args, " ")
	require.Contains(t, joined, "-- user@host:2222 ")
	require.Contains(t, joined, brokerMuxStdioCommand)
	require.Contains(t, joined, "'"+brokerDaemonStartArg+"' 'if-needed'")
	require.NotContains(t, joined, "StrictHostKeyChecking")
	require.NotContains(t, joined, "BatchMode")
	require.NotContains(t, joined, "ProxyCommand")
	// The option terminator protects the target, and the -T option comes first.
	require.Less(t, indexOf(spec.Args, "-T"), indexOf(spec.Args, "--"))
	for _, arg := range spec.Args {
		require.NotEqual(t, "no", strings.ToLower(arg))
	}

	// A route without trust inputs reuses the mux command construction
	// unchanged (`-T -- target remote`).
	plain := loadTestRoute(t, map[string]any{
		"kind":   "ssh-quic",
		"target": "host",
		"argv":   []any{"vev", brokerMuxQUICBootstrapCommand, "--offline-root", "/srv/remote"},
	})
	plainSpec, err := sshMuxCommandSpec(plain, ports.BrokerDaemonExistingOnly)
	require.NoError(t, err)
	require.Equal(t, []string{"-T", "--"}, append([]string(nil), plainSpec.Args[:2]...))
	require.NotContains(t, plainSpec.Args, "-o")
	require.Contains(t, strings.Join(plainSpec.Args, " "), "'"+brokerDaemonStartArg+"' 'existing-only'")
}

// TestSSHMuxCommandSpecRefusesUnknownStartMode proves a route never carries an
// unreadable start authorization: an unknown mode is refused instead of
// defaulting the helper to a permissive one.
func TestSSHMuxCommandSpecRefusesUnknownStartMode(t *testing.T) {
	isolateSandboxEnv(t)
	route := loadTestRoute(t, map[string]any{
		"kind":   "ssh-stdio",
		"target": "host",
		"argv":   []any{"vev", brokerMuxStdioCommand, "--offline-root", "/srv/remote"},
	})
	_, err := sshMuxCommandSpec(route, ports.BrokerDaemonStartMode(0))
	require.Error(t, err)
}

// TestSSHMuxCommandSpecTargetQuotesSpacesInsideArgv proves a remote argv word
// with a space stays one quoted word and never becomes a second option.
func TestSSHMuxCommandSpecTargetQuotesSpacesInsideArgv(t *testing.T) {
	isolateSandboxEnv(t)
	route := loadTestRoute(t, map[string]any{
		"kind":   "ssh-stdio",
		"target": "host",
		"argv":   []any{"vev", "_broker-mux-stdio", "--offline-root", "/srv/with space"},
	})
	spec, err := sshMuxCommandSpec(route, ports.BrokerDaemonExistingOnly)
	require.NoError(t, err)
	// The whole remote command stays one quoted argv word, so the space-bearing
	// path is still one word and never becomes a second option.
	require.Contains(t, spec.Args[len(spec.Args)-1], `'/srv/with space'`)
	require.Contains(t, spec.Args[len(spec.Args)-1], "'"+brokerDaemonStartArg+"' 'existing-only'")
}

// TestBrokerQUICPeerAddr pins the host-independent address composition.
func TestBrokerQUICPeerAddr(t *testing.T) {
	tests := []struct {
		target  string
		port    int
		want    string
		wantErr bool
	}{
		{target: "user@host", port: 2222, want: "host:2222"},
		{target: "user@host:22", port: 2222, want: "host:2222"},
		{target: "host", port: 1, want: "host:1"},
		{target: "user@[::1]", port: 7, want: "[::1]:7"},
		{target: "host", port: 0, wantErr: true},
		{target: "host", port: 65536, wantErr: true},
		{target: "user@", port: 7, wantErr: true},
	}
	for _, tt := range tests {
		got, err := brokerQUICPeerAddr(tt.target, tt.port)
		if tt.wantErr {
			require.Error(t, err)
			continue
		}
		require.NoError(t, err)
		require.Equal(t, tt.want, got)
	}
}

// TestBrokerMuxConnectorRejectsUnknownAddress proves the transport selector
// refuses any address the immutable configuration did not produce.
func TestBrokerMuxConnectorRejectsUnknownAddress(t *testing.T) {
	isolateSandboxEnv(t)
	root := writeMuxRoot(t, map[string]any{"kind": "unix", "path": "/tmp/unused-mux.sock"})
	layout, err := offlineLayout(root)
	require.NoError(t, err)
	config, err := brokerconfig.Load(layout)
	require.NoError(t, err)
	connector, err := brokerMuxConnector(config, slog.New(slog.NewTextHandler(io.Discard, nil)))
	require.NoError(t, err)

	_, err = connector.Connect(context.Background(), ports.BrokerDialTarget{Fence: ports.BrokerEndpointFence{Local: true}, Policy: brokerTestPolicy(), Address: "offline-route-00000000000000000000000000000000", StartMode: ports.BrokerDaemonStartIfNeeded, ExpectedIdentity: ports.BrokerExpectedIdentity{Identity: brokerTestIdentity, Bound: true}})
	require.Error(t, err)
	var typed ports.BrokerError
	require.ErrorAs(t, err, &typed)
	require.Equal(t, ports.BrokerErrorUnavailable, typed.Code)
}

// TestBrokerServeSSHStdioRouteRoundTrip drives the full broker path over an SSH
// stdio route: the broker dials the fake ssh child, the hidden helper bridges
// its stdio to a real unix daemonmux fixture, and a typed Ping returns a Pong.
func TestBrokerServeSSHStdioRouteRoundTrip(t *testing.T) {
	env := newMuxTestEnv(t)
	policy := brokerTestPolicy()
	fixture := newBrokerMuxFixture(t, policy)
	remoteRoot := writeMuxRoot(t, map[string]any{"kind": "unix", "path": fixture.route})
	argsFile := installFakeSSH(t, fakeSSHExecHelper)
	t.Setenv(fakeSSHModeEnv, brokerMuxStdioCommand)
	t.Setenv(fakeSSHRootEnv, remoteRoot)
	localRoot := writeMuxRoot(t, map[string]any{
		"kind":   "ssh-stdio",
		"target": "user@muxhost:2222",
		"argv":   []any{"vev", brokerMuxStdioCommand, "--offline-root", remoteRoot},
		"trust":  map[string]any{"knownHostsFile": "/etc/ssh/known_hosts", "connectTimeout": "5s"},
	})

	socketPath, stop := startMuxBrokerServe(t, localRoot)
	defer stop()
	requireBrokerPing(t, socketPath)
	require.Positive(t, fixture.streams.Load())

	// The exact ssh argv was built by BuildCommandForMux plus the trust inputs.
	argv := readFileString(t, argsFile)
	require.Contains(t, argv, "-T\n")
	require.Contains(t, argv, "--\nuser@muxhost:2222\n")
	require.Contains(t, argv, brokerMuxStdioCommand)
	require.Contains(t, argv, "UserKnownHostsFile=/etc/ssh/known_hosts")
	require.Contains(t, argv, "ConnectTimeout=5")
	require.NotContains(t, argv, "StrictHostKeyChecking")
	require.NotContains(t, argv, "BatchMode")

	stop()
	env.requireProductionUntouched(t)
}

// TestBrokerServeSSHQUICRouteRoundTrip drives the full broker path over an
// SSH-authenticated QUIC route with a real loopback QUIC server and a real
// one-time credential.
func TestBrokerServeSSHQUICRouteRoundTrip(t *testing.T) {
	env := newMuxTestEnv(t)
	policy := brokerTestPolicy()
	fixture := newBrokerMuxFixture(t, policy)
	remoteRoot := writeMuxRoot(t, map[string]any{"kind": "unix", "path": fixture.route})
	installFakeSSH(t, fakeSSHExecHelper)
	t.Setenv(fakeSSHModeEnv, brokerMuxQUICBootstrapCommand)
	t.Setenv(fakeSSHRootEnv, remoteRoot)
	localRoot := writeMuxRoot(t, map[string]any{
		"kind":   "ssh-quic",
		"target": "127.0.0.1",
		"argv":   []any{"vev", brokerMuxQUICBootstrapCommand, "--offline-root", remoteRoot},
	})

	socketPath, stop := startMuxBrokerServe(t, localRoot)
	defer stop()
	requireBrokerPing(t, socketPath)
	require.Positive(t, fixture.streams.Load())

	stop()
	env.requireProductionUntouched(t)
}

// TestBrokerMuxQUICRejectsBadPinWithoutLeakingSecret proves the pinned dial
// fails closed on a wrong certificate pin and that the one-time credential
// never appears in the returned error.
func TestBrokerMuxQUICRejectsBadPinWithoutLeakingSecret(t *testing.T) {
	isolateSandboxEnv(t)
	installFakeSSH(t, fakeSSHEmitReadiness)

	server, readiness, err := quic.NewServer()
	require.NoError(t, err)
	defer func() { _ = server.Close() }()
	// Flip the pin: the TLS handshake must fail before any token is accepted.
	tampered := readiness
	tampered.Fingerprint = strings.Repeat("00", 32)
	t.Setenv(fakeSSHReadyEnv, string(mustJSON(t, tampered)))

	route, target := loadTestRouteTarget(t, map[string]any{
		"kind":   "ssh-quic",
		"target": "127.0.0.1",
		"argv":   []any{"vev", brokerMuxQUICBootstrapCommand, "--offline-root", "/srv/remote"},
	})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_, err = dialBrokerRoute(ctx, route, target, slog.New(slog.NewTextHandler(io.Discard, nil)))
	require.Error(t, err)
	requireBrokerErrorHasNoReadiness(t, err, readiness)
}

// TestBrokerMuxQUICRejectsExpiredReadiness proves client-side parsing refuses
// an implausibly old credential before dialing and never echoes its secret.
func TestBrokerMuxQUICRejectsExpiredReadiness(t *testing.T) {
	isolateSandboxEnv(t)
	installFakeSSH(t, fakeSSHEmitReadiness)

	token := base64.StdEncoding.EncodeToString([]byte(strings.Repeat("A", 32)))
	nonce := base64.StdEncoding.EncodeToString([]byte(strings.Repeat("B", 16)))
	readiness := quic.Readiness{
		Version:     1,
		Port:        2222,
		Token:       token,
		Nonce:       nonce,
		Fingerprint: strings.Repeat("00", 32),
		ExpiresAt:   time.Now().Add(-time.Hour).Unix(),
	}
	t.Setenv(fakeSSHReadyEnv, string(mustJSON(t, readiness)))

	route, target := loadTestRouteTarget(t, map[string]any{
		"kind":   "ssh-quic",
		"target": "127.0.0.1",
		"argv":   []any{"vev", brokerMuxQUICBootstrapCommand, "--offline-root", "/srv/remote"},
	})
	_, err := dialBrokerRoute(context.Background(), route, target, slog.New(slog.NewTextHandler(io.Discard, nil)))
	require.Error(t, err)
	require.NotContains(t, err.Error(), token)
	require.NotContains(t, err.Error(), nonce)
}

// TestBrokerMuxDialCancellationReapsBootstrap proves one setup context bounds
// the bootstrap wait, a cancellation returns promptly, and the bootstrap child
// is killed and reaped rather than leaked.
func TestBrokerMuxDialCancellationReapsBootstrap(t *testing.T) {
	isolateSandboxEnv(t)
	installFakeSSH(t, fakeSSHSleep)
	pidFile := filepath.Join(t.TempDir(), "ssh.pid")
	t.Setenv(fakeSSHPIDEnv, pidFile)

	route, target := loadTestRouteTarget(t, map[string]any{
		"kind":   "ssh-quic",
		"target": "127.0.0.1",
		"argv":   []any{"vev", brokerMuxQUICBootstrapCommand, "--offline-root", "/srv/remote"},
	})
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err := dialBrokerRoute(ctx, route, target, slog.New(slog.NewTextHandler(io.Discard, nil)))
	require.Error(t, err)
	require.Less(t, time.Since(start), 3*time.Second, "cancellation must return promptly")

	pid := readPID(t, pidFile)
	require.NoError(t, waitForProcessExit(pid, 3*time.Second), "the bootstrap child must be reaped")
}

// TestBrokerMuxDialFloodedStderrIsReapedAndSanitized is the regression for the
// stderr DiagnosticSink race: a bootstrap that floods stderr and never prints
// readiness must be killed and reaped before the captured sink is read, and the
// diagnostic that reaches the log must stay bounded and control-byte-free. Run
// under -race with -count>1, this fails if the os/exec copy goroutine is ever
// observed while it is still writing the sink.
func TestBrokerMuxDialFloodedStderrIsReapedAndSanitized(t *testing.T) {
	isolateSandboxEnv(t)
	installFakeSSH(t, fakeSSHFloodStderr)
	pidFile := filepath.Join(t.TempDir(), "ssh.pid")
	t.Setenv(fakeSSHPIDEnv, pidFile)

	route, target := loadTestRouteTarget(t, map[string]any{
		"kind":   "ssh-quic",
		"target": "127.0.0.1",
		"argv":   []any{"vev", brokerMuxQUICBootstrapCommand, "--offline-root", "/srv/remote"},
	})

	var logs bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&logs, nil))
	ctx, cancel := context.WithTimeout(context.Background(), 400*time.Millisecond)
	defer cancel()
	_, err := dialBrokerRoute(ctx, route, target, logger)
	require.Error(t, err)
	require.NotContains(t, err.Error(), "flood", "bootstrap stderr must never reach the error")

	// Reaping is observable: the flooding bootstrap is gone, not leaked.
	pid := readPID(t, pidFile)
	require.NoError(t, waitForProcessExit(pid, 3*time.Second), "the flooding bootstrap must be reaped")

	record := decodeBootstrapDiagnostic(t, logs.Bytes())
	// Each remote physical bootstrap logs exactly one broker_remote_dial; the
	// warm-reuse harness asserts its absence.
	require.Equal(t, 1, bytes.Count(logs.Bytes(), []byte(`"msg":"broker_remote_dial"`)))
	require.NotEmpty(t, record.Diagnostic)
	require.LessOrEqual(t, len(record.Diagnostic), 512, "the captured diagnostic must stay bounded")
	for _, r := range record.Diagnostic {
		require.False(t, unicode.IsControl(r) && r != '\t' && r != '\n', "control byte %q leaked into the diagnostic", r)
	}
}

// TestBrokerMuxDialStalledBootstrapUsesSetupTimeout proves the independent
// broker-side setup timeout bounds the readiness wait: with no caller deadline a
// helper that stalls without printing readiness is still interrupted within the
// bound, and its child is killed and reaped. It is deterministic because the
// effective bound is narrowed to a test value instead of the production one.
func TestBrokerMuxDialStalledBootstrapUsesSetupTimeout(t *testing.T) {
	isolateSandboxEnv(t)
	installFakeSSH(t, fakeSSHStallNeverReady)
	pidFile := filepath.Join(t.TempDir(), "ssh.pid")
	t.Setenv(fakeSSHPIDEnv, pidFile)

	restore := brokerMuxBootstrapReadyTimeout
	brokerMuxBootstrapReadyTimeout = 250 * time.Millisecond
	t.Cleanup(func() { brokerMuxBootstrapReadyTimeout = restore })

	route, target := loadTestRouteTarget(t, map[string]any{
		"kind":   "ssh-quic",
		"target": "127.0.0.1",
		"argv":   []any{"vev", brokerMuxQUICBootstrapCommand, "--offline-root", "/srv/remote"},
	})
	start := time.Now()
	_, err := dialBrokerRoute(context.Background(), route, target, slog.New(slog.NewTextHandler(io.Discard, nil)))
	elapsed := time.Since(start)
	require.Error(t, err)
	require.Contains(t, err.Error(), "read bootstrap readiness")
	require.GreaterOrEqual(t, elapsed, 200*time.Millisecond, "the independent bound must not return early")
	require.Less(t, elapsed, 5*time.Second, "the independent bound must interrupt a stalled bootstrap")

	pid := readPID(t, pidFile)
	require.NoError(t, waitForProcessExit(pid, 3*time.Second), "the stalled bootstrap must be reaped")
}

// TestBrokerServeUnixRouteRoundTrip proves the transport selector also handles
// the explicit unix route variant, not just the legacy path string.
func TestBrokerServeUnixRouteRoundTrip(t *testing.T) {
	env := newMuxTestEnv(t)
	fixture := newBrokerMuxFixture(t, brokerTestPolicy())
	localRoot := writeMuxRoot(t, map[string]any{"kind": "unix", "path": fixture.route})

	socketPath, stop := startMuxBrokerServe(t, localRoot)
	defer stop()
	requireBrokerPing(t, socketPath)
	require.Positive(t, fixture.streams.Load())

	stop()
	env.requireProductionUntouched(t)
}

// startMuxBrokerServe starts one offline broker over localRoot and returns its
// socket path plus an idempotent stop.
func startMuxBrokerServe(t *testing.T, localRoot string) (string, func()) {
	t.Helper()
	deps, ready := testBrokerServeDeps(newSandboxClock())
	ctx, cancel := context.WithCancel(context.Background())
	done := runSandbox(ctx, brokerServeOptions{offlineRoot: localRoot}, deps)
	socketPath := awaitSandboxReady(t, ready, done)
	var once bool
	stop := func() {
		if once {
			return
		}
		once = true
		awaitSandboxCancel(t, cancel, done)
		requireSocketRemoved(t, socketPath)
	}
	return socketPath, stop
}

// requireBrokerPing opens one control stream and requires one Pong.
func requireBrokerPing(t *testing.T, socketPath string) {
	t.Helper()
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
		Policy:       brokerTestPolicy(),
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
		t.Fatal("typed broker -> mux round trip timed out")
	}
}

// requireBrokerErrorHasNoReadiness asserts no readiness secret or stderr leaks
// into a returned error.
func requireBrokerErrorHasNoReadiness(t *testing.T, err error, readiness quic.Readiness) {
	t.Helper()
	message := err.Error()
	require.NotContains(t, message, readiness.Token)
	require.NotContains(t, message, readiness.Nonce)
	require.NotContains(t, message, readiness.Fingerprint)
	require.NotContains(t, message, "token")
	require.NotContains(t, message, "nonce")
}

// requireProductionUntouched asserts no production runtime or state entry was
// created by the composition or its helpers.
func (e *muxTestEnv) requireProductionUntouched(t *testing.T) {
	t.Helper()
	requireProductionUntouched(t, e.prodRuntime, e.prodState)
	entries, err := os.ReadDir(e.prodRuntime)
	require.NoError(t, err)
	for _, entry := range entries {
		require.NotContains(t, entry.Name(), "daemon.sock", "helpers must never start an ordinary daemon")
	}
}

func mustJSON(t *testing.T, value any) []byte {
	t.Helper()
	raw, err := json.Marshal(value)
	require.NoError(t, err)
	return raw
}

func readFileString(t *testing.T, path string) string {
	t.Helper()
	raw, err := os.ReadFile(path)
	require.NoError(t, err)
	return string(raw)
}

func readPID(t *testing.T, path string) int {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for {
		raw, err := os.ReadFile(path)
		if err == nil {
			pid, convErr := strconv.Atoi(strings.TrimSpace(string(raw)))
			require.NoError(t, convErr)
			return pid
		}
		if !time.Now().Before(deadline) {
			t.Fatalf("read pid file %s: %v", path, err)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func indexOf(values []string, want string) int {
	for i, value := range values {
		if value == want {
			return i
		}
	}
	return -1
}

// bootstrapDiagnostic is one decoded broker_mux_bootstrap_stderr log record.
type bootstrapDiagnostic struct {
	Message    string `json:"msg"`
	Diagnostic string `json:"diagnostic"`
}

// decodeBootstrapDiagnostic decodes the single sanitized diagnostic the broker
// logs for a failed bootstrap. Other records (the broker_remote_dial line) are
// skipped; exactly one diagnostic must be present.
func decodeBootstrapDiagnostic(t *testing.T, raw []byte) bootstrapDiagnostic {
	t.Helper()
	var found []bootstrapDiagnostic
	for _, line := range bytes.Split(bytes.TrimSpace(raw), []byte("\n")) {
		var record bootstrapDiagnostic
		require.NoError(t, json.Unmarshal(line, &record))
		if record.Message == "broker_mux_bootstrap_stderr" {
			found = append(found, record)
		}
	}
	require.Len(t, found, 1)
	return found[0]
}
