package app

// Real-subprocess coverage for the hidden `_broker-ready` probe.
//
// These tests re-execute the test binary through the production dispatch path
// (TestMain intercepts the hidden broker commands when brokerHelperEnv is set),
// so argument parsing, endpoint classification, the real broker IPC setup, the
// readiness evaluation, the JSON document, and the process exit code are all
// exercised exactly as an operator would observe them. A real production broker serve
// composes the broker over isolated XDG directories, and the probe reads it
// over a real AF_UNIX carriage.

import (
	"encoding/json"
	"errors"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/adapters/brokerconfig"
	"github.com/bnema/vev/internal/adapters/brokeripc"
)

// readySubprocessSandbox isolates real production XDG directories and records every
// spawned broker helper, exactly like the status subprocess sandbox.
type readySubprocessSandbox struct {
	t         *testing.T
	layout    brokerconfig.Layout
	recordDir string
	broker    *exec.Cmd
}

// newReadySubprocessSandbox provisions one empty production config and arranges
// cleanup of every recorded broker process.
func newReadySubprocessSandbox(t *testing.T) *readySubprocessSandbox {
	t.Helper()
	layout := emptyProductionBrokerLayout(t, "3s")
	recordDir := filepath.Join(shortTempDir(t, "vevw"), "r")
	require.NoError(t, os.MkdirAll(recordDir, 0o700))
	t.Setenv(brokerHelperEnv, "1")
	t.Setenv(brokerHelperRecordDirEnv, recordDir)
	sandbox := &readySubprocessSandbox{t: t, layout: layout, recordDir: recordDir}
	t.Cleanup(sandbox.cleanup)
	return sandbox
}

// socketPath is the broker endpoint inside the sandbox runtime directory.
func (s *readySubprocessSandbox) socketPath() string {
	return brokeripc.SocketPath(s.layout.Runtime)
}

// ready runs `_broker-ready` for this sandbox with the supplied extra flags and
// returns its stdout, stderr, and exit code.
func (s *readySubprocessSandbox) ready(timeout string, extra ...string) (string, string, int) {
	s.t.Helper()
	args := append([]string{brokerReadyCommand, "--timeout", timeout}, extra...)
	return runBrokerCommand(s.t, args...)
}

// startBroker runs one foreground `_broker-serve` helper in the background.
func (s *readySubprocessSandbox) startBroker() {
	s.t.Helper()
	command := exec.Command(os.Args[0], productionBrokerServeCommand)
	command.Env = os.Environ()
	command.Stdout = os.Stdout
	command.Stderr = os.Stderr
	require.NoError(s.t, command.Start())
	s.broker = command
}

// waitForBrokerSocket waits until the broker has bound its endpoint.
func (s *readySubprocessSandbox) waitForBrokerSocket(timeout time.Duration) bool {
	s.t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if info, err := os.Lstat(s.socketPath()); err == nil && info.Mode()&os.ModeSocket != 0 {
			return true
		}
		time.Sleep(10 * time.Millisecond)
	}
	return false
}

// cleanup terminates the background broker and every recorded serve process so
// no test leaks an orphan.
func (s *readySubprocessSandbox) cleanup() {
	if s.broker != nil && s.broker.Process != nil {
		_ = s.broker.Process.Signal(syscall.SIGTERM)
		done := make(chan struct{})
		go func() { _ = s.broker.Wait(); close(done) }()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			_ = s.broker.Process.Kill()
			<-done
		}
	}
	for _, pid := range recordedPIDsIn(s.t, s.recordDir, productionBrokerServeCommand) {
		if err := syscall.Kill(pid, syscall.SIGKILL); err != nil && !errors.Is(err, syscall.ESRCH) {
			continue
		}
		_ = waitForProcessExit(pid, 10*time.Second)
	}
}

// readyDoc is the bounded readiness report the probe prints.
type readyDoc struct {
	Schema  string   `json:"schema"`
	Status  string   `json:"status"`
	Require string   `json:"require"`
	Reason  string   `json:"reason"`
	Pending []string `json:"pending"`
	Epoch   string   `json:"epoch"`
}

// decodeReadyReport decodes exactly one bounded JSON report from stdout.
func decodeReadyReport(t *testing.T, raw string) readyDoc {
	t.Helper()
	trimmed := strings.TrimSpace(raw)
	require.Equal(t, 1, strings.Count(trimmed, "\n")+1, "the probe prints exactly one report line")
	var report readyDoc
	require.NoError(t, json.Unmarshal([]byte(trimmed), &report))
	require.Equal(t, "vev.broker-ready/v1", report.Schema)
	return report
}

// TestBrokerReadySubprocessReadsARealBrokerAuthority proves the positive path
// end to end: a real `_broker-serve` over an isolated root publishes its local
// daemon entry, `--require local-authority` is satisfied with exit zero and the
// broker epoch, and `--require local-catalogue` stays pending because the
// provisioned local route is absent, so it is never falsely reported ready.
func TestBrokerReadySubprocessReadsARealBrokerAuthority(t *testing.T) {
	sandbox := newReadySubprocessSandbox(t)
	// One provisioned local binding whose daemon is not started: the broker
	// publishes its local entry as derived authority, so `local-authority` is
	// observable while the catalogue is not.

	sandbox.startBroker()
	require.True(t, sandbox.waitForBrokerSocket(15*time.Second), "the broker must bind its endpoint")

	stdout, stderr, code := sandbox.ready("5s", "--require", "local-authority")
	require.Equal(t, 0, code, "stderr: %s", stderr)
	authority := decodeReadyReport(t, stdout)
	require.Equal(t, "ready", authority.Status)
	require.Equal(t, "authority", authority.Reason)
	require.Equal(t, []string{}, authority.Pending)
	require.Equal(t, "local-authority", authority.Require)
	require.NotEqual(t, "0", authority.Epoch, "a live broker reports its own epoch")
	require.NotEmpty(t, strings.TrimSpace(stdout))

	stdout, stderr, code = sandbox.ready("5s", "--require", "local-catalogue")
	require.Equal(t, 3, code, "stderr: %s", stderr)
	catalogue := decodeReadyReport(t, stdout)
	require.Equal(t, "timeout", catalogue.Status)
	require.Equal(t, []string{"local-catalogue"}, catalogue.Pending)
	require.NotEqual(t, "ready", catalogue.Status, "an unobserved catalogue is never reported ready")

}

// TestBrokerReadySubprocessStaleSocketIsNeverReady proves a socket file left by
// a dead owner is retried into the deadline: the probe reports timeout, never a
// false ready, and never leaves a second socket behind.
func TestBrokerReadySubprocessStaleSocketIsNeverReady(t *testing.T) {
	sandbox := newReadySubprocessSandbox(t)
	require.NoError(t, os.MkdirAll(sandbox.layout.Runtime, 0o700))
	bindStaleUnixSocket(t, sandbox.socketPath())

	start := time.Now()
	stdout, _, code := sandbox.ready("500ms", "--require", "local-authority")
	require.Equal(t, 3, code)
	report := decodeReadyReport(t, stdout)
	require.Equal(t, "timeout", report.Status)
	require.Equal(t, "deadline", report.Reason)
	require.Less(t, time.Since(start), 10*time.Second, "the probe is bounded by its own timeout")

	// A stale socket is recoverable absence, so nothing was started over it: the
	// path is still the same dead socket file and no helper process ran.
	info, err := os.Lstat(sandbox.socketPath())
	require.NoError(t, err)
	require.NotZero(t, info.Mode()&os.ModeSocket)
	require.Empty(t, recordedPIDsIn(t, sandbox.recordDir, productionBrokerServeCommand))
}

// TestBrokerReadySubprocessBlockedHandshakeIsNeverReady proves a live endpoint
// that never answers the broker preamble is never reported ready: the probe
// stays inside its own bound and reports timeout.
func TestBrokerReadySubprocessBlockedHandshakeIsNeverReady(t *testing.T) {
	sandbox := newReadySubprocessSandbox(t)
	require.NoError(t, os.MkdirAll(sandbox.layout.Runtime, 0o700))
	// A listener that accepts and never answers: the broker preamble stalls.
	_ = bindUnixSocket(t, sandbox.socketPath())

	start := time.Now()
	stdout, _, code := sandbox.ready("700ms", "--require", "local-authority")
	require.Equal(t, 3, code)
	report := decodeReadyReport(t, stdout)
	require.Equal(t, "timeout", report.Status)
	require.NotEqual(t, "ready", report.Status, "a stalled handshake is never a false ready")
	require.Less(t, time.Since(start), 10*time.Second)

	// A stalled-but-live endpoint is never respawned over.
	require.Empty(t, recordedPIDsIn(t, sandbox.recordDir, productionBrokerServeCommand))
}

// TestBrokerReadySubprocessNonSocketPathIsTerminal proves a path that exists but
// is not a Unix socket is a terminal security failure (exit 4) rather than a
// retryable absence, for both a regular file and a directory.
func TestBrokerReadySubprocessNonSocketPathIsTerminal(t *testing.T) {
	tests := []struct {
		name  string
		place func(t *testing.T, socketPath string)
	}{
		{
			name: "regular file",
			place: func(t *testing.T, socketPath string) {
				require.NoError(t, os.WriteFile(socketPath, []byte("not a socket"), 0o600))
			},
		},
		{
			name: "directory",
			place: func(t *testing.T, socketPath string) {
				require.NoError(t, os.MkdirAll(socketPath, 0o700))
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sandbox := newReadySubprocessSandbox(t)
			require.NoError(t, os.MkdirAll(sandbox.layout.Runtime, 0o700))
			tt.place(t, sandbox.socketPath())

			stdout, stderr, code := sandbox.ready("500ms", "--require", "local-authority")
			require.Equal(t, 4, code, "stderr: %s", stderr)
			report := decodeReadyReport(t, stdout)
			require.Equal(t, "terminal", report.Status)
			require.Equal(t, "security", report.Reason)
			require.Empty(t, recordedPIDsIn(t, sandbox.recordDir, productionBrokerServeCommand))
		})
	}
}

// TestBrokerReadySubprocessPermissionRefusalIsTerminal proves a path this
// process may not inspect is a terminal security failure with exit 4, and never
// a timeout or an absence that would invite a spawn.
func TestBrokerReadySubprocessPermissionRefusalIsTerminal(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("an unreadable directory does not deny root")
	}
	sandbox := newReadySubprocessSandbox(t)
	require.NoError(t, os.MkdirAll(sandbox.layout.Runtime, 0o700))
	require.NoError(t, os.Chmod(sandbox.layout.Runtime, 0o000))
	t.Cleanup(func() { _ = os.Chmod(sandbox.layout.Runtime, 0o700) })

	stdout, stderr, code := sandbox.ready("500ms", "--require", "local-authority")
	require.Equal(t, 4, code, "stderr: %s", stderr)
	report := decodeReadyReport(t, stdout)
	require.Equal(t, "terminal", report.Status)
	require.Equal(t, "security", report.Reason)
	require.Empty(t, recordedPIDsIn(t, sandbox.recordDir, productionBrokerServeCommand))
}

// TestBrokerReadySubprocessAbsentSocketTimesOut proves a missing endpoint is
// retried into the deadline and reported as a timeout with no broker spawned.
func TestBrokerReadySubprocessAbsentSocketTimesOut(t *testing.T) {
	sandbox := newReadySubprocessSandbox(t)
	_, statErr := os.Lstat(sandbox.socketPath())
	require.ErrorIs(t, statErr, os.ErrNotExist)

	stdout, _, code := sandbox.ready("400ms", "--require", "local-authority")
	require.Equal(t, 3, code)
	report := decodeReadyReport(t, stdout)
	require.Equal(t, "timeout", report.Status)
	require.Equal(t, "deadline", report.Reason)
	require.Empty(t, recordedPIDsIn(t, sandbox.recordDir, productionBrokerServeCommand), "the readiness probe never ensures or spawns")
}

// TestBrokerReadySubprocessNonBrokerEndpointIsTerminal proves a live endpoint
// that answers the broker setup with foreign framing is a terminal protocol
// failure (exit 4) rather than a retryable absence, so a socket that is not the
// expected broker is never waited out as if it were merely slow.
func TestBrokerReadySubprocessNonBrokerEndpointIsTerminal(t *testing.T) {
	sandbox := newReadySubprocessSandbox(t)
	require.NoError(t, os.MkdirAll(sandbox.layout.Runtime, 0o700))
	listener := bindUnixSocket(t, sandbox.socketPath())
	go answerWithForeignBytes(listener)

	stdout, stderr, code := sandbox.ready("700ms", "--require", "local-authority")
	require.Equal(t, 4, code, "stderr: %s", stderr)
	report := decodeReadyReport(t, stdout)
	require.Equal(t, "terminal", report.Status)
	require.Equal(t, "protocol", report.Reason)
	require.Empty(t, recordedPIDsIn(t, sandbox.recordDir, productionBrokerServeCommand))
}

// answerWithForeignBytes serves one connection per accept with a bounded
// non-framed answer, standing in for a Unix socket that is not a broker.
func answerWithForeignBytes(listener net.Listener) {
	for {
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		go func() {
			defer func() { _ = conn.Close() }()
			buf := make([]byte, 4096)
			_ = conn.SetReadDeadline(time.Now().Add(3 * time.Second))
			_, _ = conn.Read(buf)
			_, _ = conn.Write([]byte("this endpoint is not a vev broker at all........."))
		}()
	}
}

// TestBrokerReadySubprocessArgumentRefusalIsUsage proves the production dispatch
// path refuses an invalid invocation with the usage exit code and prints no
// report at all.
func TestBrokerReadySubprocessArgumentRefusalIsUsage(t *testing.T) {
	sandbox := newReadySubprocessSandbox(t)

	stdout, stderr, code := sandbox.ready("1s", "--require", "remote-catalogue")
	require.Equal(t, 2, code, "stderr: %s", stderr)
	require.Empty(t, strings.TrimSpace(stdout), "an argument refusal prints no report")
	require.NotEmpty(t, strings.TrimSpace(stderr))
}
