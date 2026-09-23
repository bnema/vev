//go:build linux

package app

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/bnema/vev/internal/adapters/brokeripc"
	"github.com/bnema/vev/internal/adapters/ipc"
)

// killAllStopTimeout bounds how long each wave waits after SIGTERM before it
// escalates to SIGKILL.
const killAllStopTimeout = 5 * time.Second

// vevProcess is one running process of the installed vev binary.
type vevProcess struct {
	pid  int
	role string // "client", "broker", "daemon", or "helper"
}

// runKillAll stops every vev process of this user that runs the installed
// binary, including builds an upgrade has replaced on disk, then removes the
// sockets they left behind. It needs no broker, so it also works when the
// running broker is from another build. Named sessions are checkpointed by the
// daemon on SIGTERM and come back on the next start.
func runKillAll(ctx context.Context, out io.Writer) error {
	if os.Getenv("VEV") != "" {
		return errors.New("vev: `kill --all` stops the vev you are running in; run it from a terminal outside vev")
	}
	exe, err := os.Executable()
	if err != nil {
		return fmt.Errorf("vev: resolve executable: %w", err)
	}
	procs, err := findVevProcesses("/proc", installedPath(exe), os.Getuid(), os.Getpid())
	if err != nil {
		return err
	}
	// Clients first, so none of them can start a new broker or daemon from
	// its old binary while the rest stop.
	var clients, services []vevProcess
	for _, p := range procs {
		if p.role == "client" {
			clients = append(clients, p)
		} else {
			services = append(services, p)
		}
	}
	for _, wave := range [][]vevProcess{clients, services} {
		if err := stopProcesses(ctx, wave, killAllStopTimeout); err != nil {
			return err
		}
	}
	removed := removeDeadSockets(ipc.SocketDir())
	printKillAllReport(out, append(clients, services...), removed)
	return nil
}

// installedPath is the on-disk path an executable was started from, without
// the " (deleted)" suffix the kernel adds once an upgrade replaced it.
func installedPath(exe string) string {
	return strings.TrimSuffix(exe, " (deleted)")
}

// findVevProcesses lists processes owned by uid whose executable is exe, or
// was exe before an upgrade replaced it. self is never included.
func findVevProcesses(procRoot, exe string, uid, self int) ([]vevProcess, error) {
	entries, err := os.ReadDir(procRoot)
	if err != nil {
		return nil, fmt.Errorf("vev: list processes: %w", err)
	}
	var procs []vevProcess
	for _, entry := range entries {
		pid, err := strconv.Atoi(entry.Name())
		if err != nil || pid == self {
			continue
		}
		dir := filepath.Join(procRoot, entry.Name())
		info, err := os.Stat(dir)
		if err != nil {
			continue // the process exited meanwhile
		}
		if stat, ok := info.Sys().(*syscall.Stat_t); !ok || int(stat.Uid) != uid {
			continue
		}
		target, err := os.Readlink(filepath.Join(dir, "exe"))
		if err != nil || installedPath(target) != exe {
			continue
		}
		cmdline, err := os.ReadFile(filepath.Join(dir, "cmdline"))
		if err != nil {
			continue
		}
		procs = append(procs, vevProcess{pid: pid, role: processRole(cmdline)})
	}
	return procs, nil
}

// processRole names a vev process from its NUL-separated command line.
func processRole(cmdline []byte) string {
	args := strings.Split(strings.TrimRight(string(cmdline), "\x00"), "\x00")
	if len(args) < 2 {
		return "client"
	}
	switch args[1] {
	case "--daemon":
		return "daemon"
	case productionBrokerServeCommand:
		return "broker"
	}
	if strings.HasPrefix(args[1], "_") || args[1] == "--web-daemon" {
		return "helper"
	}
	return "client"
}

// stopProcesses sends SIGTERM to every process, waits up to timeout for all of
// them to exit, then sends SIGKILL to the ones still running.
func stopProcesses(ctx context.Context, procs []vevProcess, timeout time.Duration) error {
	for _, p := range procs {
		_ = syscall.Kill(p.pid, syscall.SIGTERM)
	}
	deadline := time.Now().Add(timeout)
	for _, p := range procs {
		wait := max(time.Until(deadline), 0)
		if err := waitProcessExit(ctx, p.pid, wait); err == nil {
			continue
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		_ = syscall.Kill(p.pid, syscall.SIGKILL)
		if err := waitProcessExit(ctx, p.pid, time.Second); err != nil {
			return fmt.Errorf("vev: %s (pid %d) did not stop: %w", p.role, p.pid, err)
		}
	}
	return nil
}

// removeDeadSockets deletes vev sockets nobody listens on any more, so the
// next start never trips over a leftover endpoint. A live socket is kept.
func removeDeadSockets(dir string) []string {
	var removed []string
	for _, path := range []string{
		filepath.Join(dir, "daemon.sock"),
		filepath.Join(dir, "daemonmux.sock"),
		brokeripc.SocketPath(filepath.Join(dir, "broker")),
	} {
		conn, err := net.DialTimeout("unix", path, 200*time.Millisecond)
		if err == nil {
			_ = conn.Close()
			continue
		}
		if !errors.Is(err, syscall.ECONNREFUSED) {
			continue
		}
		if os.Remove(path) == nil {
			removed = append(removed, path)
		}
	}
	return removed
}

func printKillAllReport(out io.Writer, procs []vevProcess, removed []string) {
	if len(procs) == 0 && len(removed) == 0 {
		_, _ = fmt.Fprintln(out, "nothing to stop: vev was not running")
		return
	}
	for _, p := range procs {
		_, _ = fmt.Fprintf(out, "stopped %s (pid %d)\n", p.role, p.pid)
	}
	for _, path := range removed {
		_, _ = fmt.Fprintf(out, "removed stale socket %s\n", path)
	}
	_, _ = fmt.Fprintln(out, "vev is fully stopped; named sessions come back on the next start")
}

// waitProcessExit polls until pid no longer exists or is a zombie: a dead
// process whose parent (for example a shell) has not reaped it yet.
func waitProcessExit(ctx context.Context, pid int, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		if err := syscall.Kill(pid, 0); errors.Is(err, syscall.ESRCH) || isZombie(pid) {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("still running after %s: %w", timeout, ctx.Err())
		case <-ticker.C:
		}
	}
}

// isZombie reports whether /proc says pid has exited but is not reaped yet.
func isZombie(pid int) bool {
	stat, err := os.ReadFile(filepath.Join("/proc", strconv.Itoa(pid), "stat"))
	if err != nil {
		return false
	}
	// The state follows the parenthesised command name, which may hold spaces.
	end := bytes.LastIndexByte(stat, ')')
	return end >= 0 && end+2 < len(stat) && stat[end+2] == 'Z'
}
