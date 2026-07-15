package ports

import (
	"context"
	"time"
)

// CommandSpec describes a bounded shell command execution request.
type CommandSpec struct {
	Command     string
	Env         []string
	Timeout     time.Duration
	StdoutLimit int
}

// ShellCommandRunner runs shell commands and returns bounded stdout bytes.
type ShellCommandRunner interface {
	Run(ctx context.Context, spec CommandSpec) ([]byte, error)
}
