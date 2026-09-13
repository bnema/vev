//go:build linux

package app

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"

	"github.com/bnema/vev/internal/adapters/lifecycle"
)

// platformDaemonProcessFinder verifies ownership through /proc: the candidate
// must be a same-user process whose open file descriptor table references
// lifecycle.lock. An empty (legacy) lock file is irrelevant because ownership
// is proven by the held descriptor, not by file contents.
type platformDaemonProcessFinder struct{}

func (platformDaemonProcessFinder) Find(ctx context.Context, runtimeDir string) (daemonProcess, bool, error) {
	lockInfo, err := os.Stat(lifecycle.Path(runtimeDir))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return daemonProcess{}, false, nil
		}
		return daemonProcess{}, false, fmt.Errorf("inspecting lifecycle lock: %w", err)
	}
	entries, err := os.ReadDir("/proc")
	if err != nil {
		return daemonProcess{}, false, fmt.Errorf("scanning process table: %w", err)
	}
	euid := os.Geteuid()
	for _, entry := range entries {
		if err := ctx.Err(); err != nil {
			return daemonProcess{}, false, err
		}
		pid, convErr := strconv.Atoi(entry.Name())
		if convErr != nil || pid <= 0 {
			continue
		}
		uid, err := procUID(pid)
		if err != nil || uid != euid {
			continue
		}
		holds, err := procHoldsFile(pid, lockInfo)
		if err != nil || !holds {
			continue
		}
		args, err := procArgs(pid)
		if err != nil || !hasDaemonArgument(args) {
			continue
		}
		start, err := procStartMarker(pid)
		if err != nil {
			continue
		}
		return daemonProcess{PID: pid, Command: strings.Join(args, " "), Start: start}, true, nil
	}
	return daemonProcess{}, false, nil
}

func procDir(pid int) string {
	return filepath.Join("/proc", strconv.Itoa(pid))
}

// procUID reports the owning user of a process directory. Under hidepid it may
// fail, in which case the candidate is skipped.
func procUID(pid int) (int, error) {
	info, err := os.Stat(procDir(pid))
	if err != nil {
		return -1, err
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return -1, errors.New("unexpected stat type for process directory")
	}
	return int(stat.Uid), nil
}

// procHoldsFile reports whether pid has an open descriptor referencing the same
// file as lockInfo, which is the ownership proof for lifecycle.lock.
func procHoldsFile(pid int, lockInfo os.FileInfo) (bool, error) {
	fds, err := os.ReadDir(filepath.Join(procDir(pid), "fd"))
	if err != nil {
		return false, err
	}
	for _, fd := range fds {
		info, err := os.Stat(filepath.Join(procDir(pid), "fd", fd.Name()))
		if err != nil {
			continue
		}
		if os.SameFile(info, lockInfo) {
			return true, nil
		}
	}
	return false, nil
}

func procArgs(pid int) ([]string, error) {
	raw, err := os.ReadFile(filepath.Join(procDir(pid), "cmdline"))
	if err != nil {
		return nil, err
	}
	args := strings.Split(string(raw), "\x00")
	if len(args) > 0 && args[len(args)-1] == "" {
		args = args[:len(args)-1]
	}
	return args, nil
}

// procStartMarker returns the kernel start time of pid. It changes when a PID is
// reused by another process, whatever the new process is called.
func procStartMarker(pid int) (string, error) {
	raw, err := os.ReadFile(filepath.Join(procDir(pid), "stat"))
	if err != nil {
		return "", err
	}
	end := strings.LastIndexByte(string(raw), ')')
	if end < 0 {
		return "", errors.New("malformed process stat")
	}
	fields := strings.Fields(string(raw[end+1:]))
	// After the command name, field 22 of /proc/PID/stat (starttime) is at
	// index 22-3 = 19 because the first listed field is the state.
	const startTimeIndex = 19
	if len(fields) <= startTimeIndex {
		return "", errors.New("malformed process stat")
	}
	return fields[startTimeIndex], nil
}
