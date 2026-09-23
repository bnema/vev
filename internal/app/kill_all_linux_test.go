//go:build linux

package app

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/adapters/lifecycle"
)

func TestWaitProcessExit(t *testing.T) {
	tests := []struct {
		name    string
		signal  bool
		wantErr bool
	}{
		{name: "terminated process is reported gone", signal: true},
		{name: "live process times out", signal: false, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cmd := exec.Command("sleep", "30")
			require.NoError(t, cmd.Start())
			// Reap the child so it never lingers as a zombie that kill(pid, 0)
			// would still report as present.
			reaped := make(chan struct{})
			go func() { _ = cmd.Wait(); close(reaped) }()
			t.Cleanup(func() {
				_ = cmd.Process.Kill()
				<-reaped
			})
			if tt.signal {
				require.NoError(t, syscall.Kill(cmd.Process.Pid, syscall.SIGTERM))
			}
			err := waitProcessExit(context.Background(), cmd.Process.Pid, 300*time.Millisecond)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
		})
	}
}

func TestProcessRole(t *testing.T) {
	tests := []struct {
		name    string
		cmdline string
		want    string
	}{
		{name: "bare client", cmdline: "vev\x00", want: "client"},
		{name: "attach client", cmdline: "vev\x00attach\x00work\x00", want: "client"},
		{name: "daemon", cmdline: "/proc/self/exe\x00--daemon\x00", want: "daemon"},
		{name: "broker", cmdline: "/proc/self/exe\x00_broker-production-serve\x00", want: "broker"},
		{name: "hidden helper", cmdline: "/proc/self/exe\x00_broker-mux-quic-proxy\x00", want: "helper"},
		{name: "web daemon", cmdline: "vev\x00--web-daemon\x00", want: "helper"},
		{name: "web server", cmdline: "/proc/self/exe\x00--web-serve\x00", want: "helper"},
		{name: "daemon launcher", cmdline: "/proc/self/exe\x00--daemon-launcher\x00", want: "helper"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, processRole([]byte(tt.cmdline)))
		})
	}
}

// TestFindVevProcesses runs against a fake /proc: only this user's processes
// of the installed binary (also once replaced on disk) in the same runtime
// directory are found, never self.
func TestFindVevProcesses(t *testing.T) {
	root := t.TempDir()
	const exe = "/opt/bin/vev"
	const runtime = "XDG_RUNTIME_DIR=/run/user/1000"
	uid := os.Getuid()
	add := func(pid, target, cmdline, environ string) {
		dir := filepath.Join(root, pid)
		require.NoError(t, os.Mkdir(dir, 0o755))
		require.NoError(t, os.Symlink(target, filepath.Join(dir, "exe")))
		require.NoError(t, os.WriteFile(filepath.Join(dir, "cmdline"), []byte(cmdline), 0o644))
		require.NoError(t, os.WriteFile(filepath.Join(dir, "environ"), []byte(environ), 0o644))
	}
	add("10", exe, "vev\x00", "HOME=/h\x00"+runtime+"\x00")
	add("11", exe+" (deleted)", "/proc/self/exe\x00--daemon\x00", runtime+"\x00")
	add("12", "/usr/bin/emacs", "emacs\x00--daemon\x00", runtime+"\x00")
	add("13", "/tmp/vev-test-bin", "/tmp/vev-test-bin\x00--daemon\x00", runtime+"\x00")
	add("14", exe, "vev\x00", runtime+"\x00") // self
	add("15", exe, "/proc/self/exe\x00--daemon\x00", "XDG_RUNTIME_DIR=/repo/.dev/test/runtime\x00")
	require.NoError(t, os.Mkdir(filepath.Join(root, "self"), 0o755))

	scope := killAllScope{procRoot: root, exe: exe, uid: uid, self: 14, runtimeDir: "/run/user/1000"}
	got, err := findVevProcesses(scope)
	require.NoError(t, err)
	require.ElementsMatch(t, []vevProcess{{pid: 10, role: "client"}, {pid: 11, role: "daemon"}}, got)

	scope.runtimeDir = "/repo/.dev/test/runtime"
	dev, err := findVevProcesses(scope)
	require.NoError(t, err)
	require.Equal(t, []vevProcess{{pid: 15, role: "daemon"}}, dev, "a VEV_ENV run only sees its own environment")

	scope.uid = uid + 1
	none, err := findVevProcesses(scope)
	require.NoError(t, err)
	require.Empty(t, none, "another user's processes are never matched")
}

// startIgnoringTerm starts a shell that ignores SIGTERM and reports once the
// trap is installed.
func startIgnoringTerm(t *testing.T) (*exec.Cmd, <-chan *os.ProcessState) {
	t.Helper()
	cmd := exec.Command("sh", "-c", "trap '' TERM; echo ready; while :; do sleep 0.05; done")
	stdout, err := cmd.StdoutPipe()
	require.NoError(t, err)
	require.NoError(t, cmd.Start())
	line := make([]byte, 6)
	_, err = io.ReadFull(stdout, line)
	require.NoError(t, err)
	done := make(chan *os.ProcessState, 1)
	go func() { _ = cmd.Wait(); done <- cmd.ProcessState }()
	t.Cleanup(func() { _ = cmd.Process.Kill() })
	return cmd, done
}

func TestStopWaveEscalatesToSigkillAndKeepsGoing(t *testing.T) {
	stubborn, done := startIgnoringTerm(t)
	polite := exec.Command("sleep", "30")
	require.NoError(t, polite.Start())
	go func() { _ = polite.Wait() }()

	var report killAllReport
	stopWave(context.Background(), []vevProcess{{pid: stubborn.Process.Pid, role: "daemon"}, {pid: polite.Process.Pid, role: "client"}}, 100*time.Millisecond, &report)

	state := <-done
	status, ok := state.Sys().(syscall.WaitStatus)
	require.True(t, ok)
	require.Equal(t, syscall.SIGKILL, status.Signal(), "a process ignoring SIGTERM is killed")
	require.Empty(t, report.errs)
	require.Len(t, report.stopped, 2, "the rest of the wave still stops")
	require.Equal(t, []vevProcess{{pid: stubborn.Process.Pid, role: "daemon"}}, report.forced)
}

func TestRemoveDeadSockets(t *testing.T) {
	dir, err := os.MkdirTemp("", "vevk")
	require.NoError(t, err)
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	require.NoError(t, os.Chmod(dir, 0o700))
	require.NoError(t, os.Mkdir(filepath.Join(dir, "broker"), 0o700))

	live, err := net.Listen("unix", filepath.Join(dir, "daemon.sock"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = live.Close() })
	dead, err := net.Listen("unix", filepath.Join(dir, "daemonmux.sock"))
	require.NoError(t, err)
	dead.(*net.UnixListener).SetUnlinkOnClose(false)
	require.NoError(t, dead.Close())
	notSocket := filepath.Join(dir, "broker", "broker.sock")
	require.NoError(t, os.WriteFile(notSocket, nil, 0o600))

	removed := removeDeadSockets(dir)
	require.Equal(t, []string{filepath.Join(dir, "daemonmux.sock")}, removed)
	require.FileExists(t, filepath.Join(dir, "daemon.sock"), "a live socket is kept")
	require.FileExists(t, notSocket, "a regular file is never removed")
}

func TestRemoveDeadSocketsSkipsBusyLifecycle(t *testing.T) {
	dir, err := os.MkdirTemp("", "vevk")
	require.NoError(t, err)
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	require.NoError(t, os.Chmod(dir, 0o700))
	dead, err := net.Listen("unix", filepath.Join(dir, "daemon.sock"))
	require.NoError(t, err)
	dead.(*net.UnixListener).SetUnlinkOnClose(false)
	require.NoError(t, dead.Close())

	owner, err := lifecycle.TryAcquire(dir)
	require.NoError(t, err)
	t.Cleanup(func() { _ = owner.Release() })

	require.Empty(t, removeDeadSockets(dir), "a starting owner keeps its socket")
}

func TestKillAllRefusesInsideVev(t *testing.T) {
	t.Setenv("VEV", "1")
	var out bytes.Buffer
	err := runKillAll(context.Background(), &out)
	require.ErrorContains(t, err, "outside vev")
	require.Empty(t, out.String())
}

func TestPrintKillAllReport(t *testing.T) {
	tests := []struct {
		name   string
		report killAllReport
		want   string
	}{
		{name: "nothing running", want: "nothing to stop: vev was not running\n"},
		{
			name:   "clean stop",
			report: killAllReport{stopped: []vevProcess{{pid: 7, role: "broker"}}, removed: []string{"/run/x/daemon.sock"}},
			want:   "stopped broker (pid 7)\nremoved stale socket /run/x/daemon.sock\nvev is fully stopped; named sessions come back on the next start, numbered sessions are closed\n",
		},
		{
			name:   "forced daemon warns",
			report: killAllReport{stopped: []vevProcess{{pid: 9, role: "daemon"}}, forced: []vevProcess{{pid: 9, role: "daemon"}}},
			want:   "stopped daemon (pid 9)\nwarning: the daemon (pid 9) had to be force-killed; recent output in named sessions may be lost\nvev is fully stopped; named sessions come back on the next start, numbered sessions are closed\n",
		},
		{
			name:   "failure never claims a full stop",
			report: killAllReport{errs: []error{errors.New("stuck")}},
			want:   "",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var out bytes.Buffer
			printKillAllReport(&out, tt.report)
			require.Equal(t, tt.want, out.String())
		})
	}
}

// TestWaitProcessExitTreatsZombieAsGone pins that an exited child its parent
// has not reaped yet counts as stopped.
func TestWaitProcessExitTreatsZombieAsGone(t *testing.T) {
	cmd := exec.Command("sh", "-c", "exit 0")
	require.NoError(t, cmd.Start())
	t.Cleanup(func() { _ = cmd.Wait() }) // reap only after the check
	require.NoError(t, waitProcessExit(context.Background(), cmd.Process.Pid, 2*time.Second))
}
