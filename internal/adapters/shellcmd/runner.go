package shellcmd

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os/exec"
	"syscall"

	"github.com/bnema/vev/internal/ports"
)

// Runner executes bounded shell commands for bar scripts and similar usecases.
type Runner struct{}

// New returns a production shell command runner.
func New() Runner { return Runner{} }

// Run executes spec.Command through sh -c, capturing bounded stdout and stderr.
func (Runner) Run(ctx context.Context, spec ports.CommandSpec) (ports.ShellCommandResult, error) {
	cmd := exec.CommandContext(ctx, "sh", "-c", spec.Command)
	cmd.Env = spec.Env
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.WaitDelay = spec.Timeout
	cmd.Cancel = func() error {
		if cmd.Process == nil {
			return nil
		}
		return syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
	}
	var stdout, stderr boundedBuffer
	stdout.limit = spec.StdoutLimit
	stderr.limit = spec.StderrLimit
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	err := cmd.Run()
	res := ports.ShellCommandResult{Stdout: stdout.Bytes(), Stderr: stderr.Bytes()}
	if err != nil {
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			res.ExitCode = exitErr.ExitCode()
		} else {
			res.ExitCode = -1
		}
		return res, err
	}
	return res, nil
}

type boundedBuffer struct {
	buf   bytes.Buffer
	limit int
}

func (b *boundedBuffer) Write(p []byte) (int, error) {
	if b.limit <= 0 {
		return len(p), nil
	}
	remaining := b.limit - b.buf.Len()
	if remaining > 0 {
		if len(p) < remaining {
			remaining = len(p)
		}
		_, _ = b.buf.Write(p[:remaining])
	}
	return len(p), nil
}

func (b *boundedBuffer) Bytes() []byte { return b.buf.Bytes() }

var _ ports.ShellCommandRunner = Runner{}
var _ io.Writer = (*boundedBuffer)(nil)
