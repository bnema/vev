package platform

import (
	"fmt"
	"os"
	"strings"
)

// ProcessCwd returns the current working directory for a Linux process.
func ProcessCwd(pid int) (string, error) {
	return processCwd("/proc", pid)
}

func processCwd(root string, pid int) (string, error) {
	return os.Readlink(fmt.Sprintf("%s/%d/cwd", root, pid))
}

// ProcessComm returns the command name from /proc/<pid>/comm.
func ProcessComm(pid int) (string, error) {
	return processComm("/proc", pid)
}

func processComm(root string, pid int) (string, error) {
	if pid <= 0 {
		return "", fmt.Errorf("process comm: invalid pid %d", pid)
	}
	data, err := os.ReadFile(fmt.Sprintf("%s/%d/comm", root, pid))
	if err != nil {
		return "", fmt.Errorf("process comm: read pid %d: %w", pid, err)
	}
	comm := strings.TrimSpace(string(data))
	if comm == "" {
		return "", fmt.Errorf("process comm: empty comm for pid %d", pid)
	}
	return comm, nil
}
