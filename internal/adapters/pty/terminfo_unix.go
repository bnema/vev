//go:build linux || darwin

package pty

import (
	"context"
	"os"
	"os/exec"
	"strings"
	"time"
)

const terminfoProbeTimeout = time.Second

// paneEnvironment selects a terminal description on the PTY host. Resolve the
// helper using the daemon's PATH, but look up terminfo using the child's search
// environment and directory. Never mutate the session's environment snapshot.
func paneEnvironment(ctx context.Context, env []string, dir string) []string {
	if env == nil {
		env = os.Environ()
	}
	term := "xterm-256color"
	probeCtx, cancel := context.WithTimeout(ctx, terminfoProbeTimeout)
	defer cancel()
	cmd := exec.CommandContext(probeCtx, "infocmp", "-x", "xterm-direct")
	cmd.Env = env
	cmd.Dir = dir
	cmd.WaitDelay = terminfoProbeTimeout
	if cmd.Run() == nil {
		term = "xterm-direct"
	}

	out := make([]string, 0, len(env)+1)
	for _, entry := range env {
		if !strings.HasPrefix(entry, "TERM=") {
			out = append(out, entry)
		}
	}
	return append(out, "TERM="+term)
}
