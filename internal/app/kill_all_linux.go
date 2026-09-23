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
	"github.com/bnema/vev/internal/adapters/lifecycle"
	"github.com/bnema/vev/internal/platform"
)

const (
	// killAllStopTimeout bounds how long each wave waits after SIGTERM
	// before it escalates to SIGKILL.
	killAllStopTimeout = 5 * time.Second
	// killAllRounds caps how many scan-and-stop rounds run, so a process that
	// a stopping one respawned is caught by the next scan.
	killAllRounds = 3
)

// vevProcess is one running process of the installed vev binary.
type vevProcess struct {
	pid  int
	role string // "client", "broker", "daemon", or "helper"
}

// killAllScope selects which processes `kill --all` may stop: this user's
// processes of the installed binary that use the same vev socket directory,
// so VEV_ENV and XDG-sandboxed runs never touch each other.
type killAllScope struct {
	procRoot  string
	exe       string
	uid       int
	self      int
	socketDir string // ipc.SocketDir of the caller
}

// killAllReport is what one `kill --all` did.
type killAllReport struct {
	stopped []vevProcess
	forced  []vevProcess // needed SIGKILL
	removed []string
	errs    []error
}

// runKillAll stops every vev process of this environment, including builds an
// upgrade replaced on disk, then removes the sockets they left behind. It needs
// no broker, so it also works when the running broker is from another build.
// The daemon checkpoints named sessions on SIGTERM; numbered sessions close.
func runKillAll(ctx context.Context, out io.Writer) error {
	if os.Getenv("VEV") != "" {
		return errors.New("vev: `kill --all` stops the vev you are running in; run it from a terminal outside vev")
	}
	exe, err := os.Executable()
	if err != nil {
		return fmt.Errorf("vev: resolve executable: %w", err)
	}
	scope := killAllScope{
		procRoot:  "/proc",
		exe:       installedPath(exe),
		uid:       os.Getuid(),
		self:      os.Getpid(),
		socketDir: ipc.SocketDir(),
	}
	report := stopEverything(ctx, scope, killAllStopTimeout)
	printKillAllReport(out, report)
	return errors.Join(report.errs...)
}

// stopEverything runs scan-and-stop rounds until a scan finds nothing, then
// removes dead sockets. A process that already failed to die is not retried.
func stopEverything(ctx context.Context, scope killAllScope, timeout time.Duration) killAllReport {
	var report killAllReport
	failed := map[int]bool{}
	scan := func() ([]vevProcess, error) {
		procs, err := findVevProcesses(scope)
		live := procs[:0]
		for _, p := range procs {
			if !failed[p.pid] {
				live = append(live, p)
			}
		}
		return live, err
	}
	for round := 0; ; round++ {
		procs, err := scan()
		if err != nil {
			report.errs = append(report.errs, err)
			return report
		}
		if len(procs) == 0 {
			if len(failed) == 0 {
				report.removed = removeDeadSockets(scope.socketDir)
			}
			return report
		}
		if round == killAllRounds {
			report.errs = append(report.errs, fmt.Errorf("vev: %d vev processes kept starting again; run `vev kill --all` once more", len(procs)))
			return report
		}
		// Clients first, so none of them can start a broker or daemon from
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
			for _, p := range stopWave(ctx, wave, timeout, &report) {
				failed[p.pid] = true
			}
		}
		if ctx.Err() != nil {
			report.errs = append(report.errs, ctx.Err())
			return report
		}
	}
}

// installedPath is the on-disk path an executable was started from, without
// the " (deleted)" suffix the kernel adds once an upgrade replaced it.
func installedPath(exe string) string {
	return strings.TrimSuffix(exe, " (deleted)")
}

// findVevProcesses lists the processes in scope. The caller never matches
// itself.
func findVevProcesses(scope killAllScope) ([]vevProcess, error) {
	entries, err := os.ReadDir(scope.procRoot)
	if err != nil {
		return nil, fmt.Errorf("vev: list processes: %w", err)
	}
	var procs []vevProcess
	for _, entry := range entries {
		pid, err := strconv.Atoi(entry.Name())
		if err != nil || pid == scope.self {
			continue
		}
		dir := filepath.Join(scope.procRoot, entry.Name())
		info, err := os.Stat(dir)
		if err != nil {
			continue // the process exited meanwhile
		}
		if stat, ok := info.Sys().(*syscall.Stat_t); !ok || int(stat.Uid) != scope.uid {
			continue
		}
		target, err := os.Readlink(filepath.Join(dir, "exe"))
		if err != nil || installedPath(target) != scope.exe {
			continue
		}
		if processSocketDir(dir, scope.uid) != scope.socketDir {
			continue // another VEV_ENV or sandbox
		}
		cmdline, err := os.ReadFile(filepath.Join(dir, "cmdline"))
		if err != nil {
			continue
		}
		procs = append(procs, vevProcess{pid: pid, role: processRole(cmdline)})
	}
	return procs, nil
}

// processSocketDir resolves the vev socket directory of the process in
// procDir from its initial environment and working directory, the same way
// vev itself does after VEV_ENV activation. It returns "" when unreadable.
func processSocketDir(procDir string, uid int) string {
	environ, err := os.ReadFile(filepath.Join(procDir, "environ"))
	if err != nil {
		return ""
	}
	env := map[string]string{}
	for _, entry := range strings.Split(string(environ), "\x00") {
		if key, value, ok := strings.Cut(entry, "="); ok {
			env[key] = value
		}
	}
	cwd, _ := os.Readlink(filepath.Join(procDir, "cwd"))
	runtime := platform.RuntimeDirFor(func(key string) string { return env[key] }, cwd)
	return ipc.SocketDirFor(runtime, uid)
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
	case "--daemon-launcher", "--web-daemon", "--web-serve", "--ui-driver":
		return "helper"
	}
	if strings.HasPrefix(args[1], "_") {
		return "helper"
	}
	return "client"
}

// stopWave sends SIGTERM to every process, waits up to timeout for all of them
// to exit, then sends SIGKILL to the ones still running. One stuck process
// never stops the rest of the wave; the ones that survive are returned.
func stopWave(ctx context.Context, procs []vevProcess, timeout time.Duration, report *killAllReport) (survivors []vevProcess) {
	for _, p := range procs {
		_ = syscall.Kill(p.pid, syscall.SIGTERM)
	}
	deadline := time.Now().Add(timeout)
	for _, p := range procs {
		if waitProcessExit(ctx, p.pid, max(time.Until(deadline), 0)) == nil {
			report.stopped = append(report.stopped, p)
			continue
		}
		_ = syscall.Kill(p.pid, syscall.SIGKILL)
		if err := waitProcessExit(context.WithoutCancel(ctx), p.pid, time.Second); err != nil {
			report.errs = append(report.errs, fmt.Errorf("vev: %s (pid %d) did not stop: %w", p.role, p.pid, err))
			survivors = append(survivors, p)
			continue
		}
		report.stopped = append(report.stopped, p)
		report.forced = append(report.forced, p)
	}
	return survivors
}

// removeDeadSockets deletes vev sockets nobody listens on any more, so the
// next start never trips over a leftover endpoint. It holds each lifecycle
// lock while it checks, so a daemon or broker that is starting keeps its
// socket, and it never removes anything that is not a socket.
func removeDeadSockets(dir string) []string {
	var removed []string
	for _, runtime := range []struct{ lockDir, socket string }{
		{dir, filepath.Join(dir, "daemon.sock")},
		{dir, filepath.Join(dir, "daemonmux.sock")},
		{filepath.Join(dir, "broker"), brokeripc.SocketPath(filepath.Join(dir, "broker"))},
	} {
		info, err := os.Lstat(runtime.socket)
		if err != nil || info.Mode().Type() != os.ModeSocket {
			continue
		}
		owner, err := lifecycle.TryAcquire(runtime.lockDir)
		if err != nil {
			continue // busy: a process is starting or still owns it
		}
		conn, err := net.DialTimeout("unix", runtime.socket, 200*time.Millisecond)
		if err == nil {
			_ = conn.Close()
		} else if errors.Is(err, syscall.ECONNREFUSED) && os.Remove(runtime.socket) == nil {
			removed = append(removed, runtime.socket)
		}
		_ = owner.Release()
	}
	return removed
}

func printKillAllReport(out io.Writer, report killAllReport) {
	if len(report.stopped) == 0 && len(report.removed) == 0 && len(report.errs) == 0 {
		_, _ = fmt.Fprintln(out, "nothing to stop: vev was not running")
		return
	}
	for _, p := range report.stopped {
		_, _ = fmt.Fprintf(out, "stopped %s (pid %d)\n", p.role, p.pid)
	}
	for _, path := range report.removed {
		_, _ = fmt.Fprintf(out, "removed stale socket %s\n", path)
	}
	for _, p := range report.forced {
		if p.role == "daemon" {
			_, _ = fmt.Fprintf(out, "warning: the daemon (pid %d) had to be force-killed; recent output in named sessions may be lost\n", p.pid)
		}
	}
	if len(report.errs) > 0 {
		return
	}
	_, _ = fmt.Fprintln(out, "vev is fully stopped; named sessions come back on the next start, numbered sessions are closed")
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
