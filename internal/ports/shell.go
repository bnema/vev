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
	StderrLimit int
}

// ShellCommandResult carries bounded output and the process exit status.
// ExitCode is 0 when the command ran to completion successfully, the process
// exit code when it ran and failed, and -1 when the command could not be
// started or was terminated by a signal.
type ShellCommandResult struct {
	Stdout   []byte
	Stderr   []byte
	ExitCode int
}

// ShellCommandRunner runs shell commands and returns bounded output.
type ShellCommandRunner interface {
	Run(ctx context.Context, spec CommandSpec) (ShellCommandResult, error)
}
