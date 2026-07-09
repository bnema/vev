package shellcmd

import (
	"bytes"
	"context"
	"io"
	"os/exec"
	"syscall"

	"github.com/bnema/vev/internal/ports"
)

// Runner executes bounded shell commands for bar scripts and similar usecases.
type Runner struct{}

// New returns a production shell command runner.
func New() Runner { return Runner{} }

// Run executes spec.Command through sh -c, capturing bounded stdout and discarding stderr.
func (Runner) Run(ctx context.Context, spec ports.CommandSpec) ([]byte, error) {
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
	var stdout boundedBuffer
	stdout.limit = spec.StdoutLimit
	cmd.Stdout = &stdout
	cmd.Stderr = io.Discard
	if err := cmd.Run(); err != nil {
		return stdout.Bytes(), err
	}
	return stdout.Bytes(), nil
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
