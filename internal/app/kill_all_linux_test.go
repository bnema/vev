//go:build linux

package app

import (
	"bytes"
	"context"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
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
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, processRole([]byte(tt.cmdline)))
		})
	}
}

// TestFindVevProcesses runs against a fake /proc: only this user's processes
// of the installed binary (also once replaced on disk) are found, never self.
func TestFindVevProcesses(t *testing.T) {
	root := t.TempDir()
	const exe = "/opt/bin/vev"
	uid := os.Getuid()
	add := func(pid, target, cmdline string) {
		dir := filepath.Join(root, pid)
		require.NoError(t, os.Mkdir(dir, 0o755))
		require.NoError(t, os.Symlink(target, filepath.Join(dir, "exe")))
		require.NoError(t, os.WriteFile(filepath.Join(dir, "cmdline"), []byte(cmdline), 0o644))
	}
	add("10", exe, "vev\x00")
	add("11", exe+" (deleted)", "/proc/self/exe\x00--daemon\x00")
	add("12", "/usr/bin/emacs", "emacs\x00--daemon\x00")
	add("13", "/tmp/vev-test-bin", "/tmp/vev-test-bin\x00--daemon\x00")
	add("14", exe, "vev\x00") // self
	require.NoError(t, os.Mkdir(filepath.Join(root, "self"), 0o755))

	got, err := findVevProcesses(root, exe, uid, 14)
	require.NoError(t, err)
	require.ElementsMatch(t, []vevProcess{{pid: 10, role: "client"}, {pid: 11, role: "daemon"}}, got)

	none, err := findVevProcesses(root, exe, uid+1, 14)
	require.NoError(t, err)
	require.Empty(t, none, "another user's processes are never matched")
}

func TestStopProcessesEscalatesToSigkill(t *testing.T) {
	// A shell that ignores SIGTERM only stops on SIGKILL.
	cmd := exec.Command("sh", "-c", "trap '' TERM; while :; do sleep 0.05; done")
	require.NoError(t, cmd.Start())
	reaped := make(chan struct{})
	go func() { _ = cmd.Wait(); close(reaped) }()
	t.Cleanup(func() { _ = cmd.Process.Kill(); <-reaped })
	time.Sleep(50 * time.Millisecond) // let the trap install

	require.NoError(t, stopProcesses(context.Background(), []vevProcess{{pid: cmd.Process.Pid, role: "daemon"}}, 100*time.Millisecond))
	<-reaped
}

func TestRemoveDeadSockets(t *testing.T) {
	dir, err := os.MkdirTemp("", "vevk")
	require.NoError(t, err)
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	require.NoError(t, os.Mkdir(filepath.Join(dir, "broker"), 0o700))

	live, err := net.Listen("unix", filepath.Join(dir, "daemon.sock"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = live.Close() })
	dead, err := net.Listen("unix", filepath.Join(dir, "daemonmux.sock"))
	require.NoError(t, err)
	dead.(*net.UnixListener).SetUnlinkOnClose(false)
	require.NoError(t, dead.Close())

	removed := removeDeadSockets(dir)
	require.Equal(t, []string{filepath.Join(dir, "daemonmux.sock")}, removed)
	require.FileExists(t, filepath.Join(dir, "daemon.sock"), "a live socket is kept")
}

func TestKillAllRefusesInsideVev(t *testing.T) {
	t.Setenv("VEV", "1")
	var out bytes.Buffer
	err := runKillAll(context.Background(), &out)
	require.ErrorContains(t, err, "outside vev")
	require.Empty(t, out.String())
}

func TestPrintKillAllReport(t *testing.T) {
	var empty bytes.Buffer
	printKillAllReport(&empty, nil, nil)
	require.Equal(t, "nothing to stop: vev was not running\n", empty.String())

	var out bytes.Buffer
	printKillAllReport(&out, []vevProcess{{pid: 7, role: "broker"}}, []string{"/run/x/daemon.sock"})
	require.Equal(t, "stopped broker (pid 7)\nremoved stale socket /run/x/daemon.sock\nvev is fully stopped; named sessions come back on the next start\n", out.String())
}

// TestWaitProcessExitTreatsZombieAsGone pins that an exited child its parent
// has not reaped yet counts as stopped.
func TestWaitProcessExitTreatsZombieAsGone(t *testing.T) {
	cmd := exec.Command("sh", "-c", "exit 0")
	require.NoError(t, cmd.Start())
	t.Cleanup(func() { _ = cmd.Wait() }) // reap only after the check
	require.NoError(t, waitProcessExit(context.Background(), cmd.Process.Pid, 2*time.Second))
}
