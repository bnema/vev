//go:build darwin

package app

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"

	"github.com/bnema/vev/internal/adapters/lifecycle"
)

// platformDaemonProcessFinder verifies ownership on macOS with lsof: the
// candidate must be a same-user process running a vev daemon with
// lifecycle.lock open. macOS has no /proc, and lsof ships with the base system;
// when it is unavailable the fallback refuses rather than guessing from a
// process name. An empty (legacy) lock file is irrelevant because ownership is
// proven by the open descriptor, not by file contents.
type platformDaemonProcessFinder struct{}

// forceStopCommandTimeout bounds the lsof and ps subprocesses so a stuck
// process table cannot hang an interactive kill.
const forceStopCommandTimeout = 5 * time.Second

func (platformDaemonProcessFinder) Find(ctx context.Context, runtimeDir string) (daemonProcess, bool, error) {
	lockPath := lifecycle.Path(runtimeDir)
	if _, err := os.Stat(lockPath); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return daemonProcess{}, false, nil
		}
		return daemonProcess{}, false, fmt.Errorf("inspecting lifecycle lock: %w", err)
	}
	lsof, err := lsofPath()
	if err != nil {
		return daemonProcess{}, false, err
	}
	lsofCtx, cancel := context.WithTimeout(ctx, forceStopCommandTimeout)
	defer cancel()
	out, err := exec.CommandContext(lsofCtx, lsof, "-a", "-u", strconv.Itoa(os.Geteuid()), "-t", "--", lockPath).Output()
	if err != nil {
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) && exitErr.ExitCode() == 1 {
			// lsof exits 1 when no process matches.
			return daemonProcess{}, false, nil
		}
		return daemonProcess{}, false, fmt.Errorf("listing holders of %s: %w", lockPath, err)
	}
	for _, line := range strings.Split(strings.TrimSpace(string(out)), "\n") {
		pid, convErr := strconv.Atoi(strings.TrimSpace(line))
		if convErr != nil || pid <= 0 {
			continue
		}
		process, ok, infoErr := darwinProcessInfo(ctx, pid)
		if infoErr != nil {
			return daemonProcess{}, false, infoErr
		}
		if ok {
			return process, true, nil
		}
	}
	return daemonProcess{}, false, nil
}

func lsofPath() (string, error) {
	if path, err := exec.LookPath("lsof"); err == nil {
		return path, nil
	}
	// GUI applications often omit /usr/sbin from PATH.
	const systemLsof = "/usr/sbin/lsof"
	if _, err := os.Stat(systemLsof); err == nil {
		return systemLsof, nil
	}
	return "", errors.New("vev: locating lsof: not found; cannot verify daemon ownership")
}

// darwinProcessInfo returns the verified daemon identity for pid, or ok=false
// when the process vanished or is not a vev daemon.
func darwinProcessInfo(ctx context.Context, pid int) (daemonProcess, bool, error) {
	cmdCtx, cancel := context.WithTimeout(ctx, forceStopCommandTimeout)
	defer cancel()
	out, err := exec.CommandContext(cmdCtx, "ps", "-p", strconv.Itoa(pid), "-o", "uid=", "-o", "lstart=", "-o", "command=").Output()
	if err != nil {
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			return daemonProcess{}, false, nil // process vanished between lsof and ps
		}
		return daemonProcess{}, false, fmt.Errorf("inspecting process %d: %w", pid, err)
	}
	// uid and the five lstart fields precede the command.
	fields := strings.Fields(string(out))
	if len(fields) < 7 {
		return daemonProcess{}, false, nil
	}
	uid, convErr := strconv.Atoi(fields[0])
	if convErr != nil || uid != os.Geteuid() {
		return daemonProcess{}, false, nil
	}
	command := strings.Join(fields[6:], " ")
	if !hasDaemonArgument(strings.Fields(command)) {
		return daemonProcess{}, false, nil
	}
	return daemonProcess{PID: pid, Command: command, Start: strings.Join(fields[1:6], " ")}, true, nil
}
