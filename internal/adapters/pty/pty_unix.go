//go:build linux || darwin

package pty

// The Unix adapter drives /dev/ptmx directly and delegates slave preparation
// to rawterm rather than depending on a third-party pty package, keeping the
// dependency surface at the standard library alone.

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"sync"
	"syscall"
	"time"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/platform"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/pkg/rawterm"
)

// killGracePeriod is how long Close waits for a signalled child to exit before
// escalating from SIGHUP to SIGKILL.
const killGracePeriod = 2 * time.Second

// Factory implements ports.PTYFactory on supported Unix platforms.
type Factory struct{}

// NewFactory returns a Unix PTY factory.
func NewFactory() *Factory { return &Factory{} }

// Open spawns command with args attached to a freshly allocated pseudo-terminal
// and returns the master side as a ports.PTY. env is passed to the child
// verbatim (nil means inherit the current process environment); the caller
// decides TERM and friends. sz sets the terminal window size before the child
// starts, so the child observes the correct dimensions on its first query.
func (Factory) Open(ctx context.Context, command string, args []string, env []string, dir string, sz domain.Size) (ports.PTY, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	masterFd, err := syscall.Open("/dev/ptmx", syscall.O_RDWR|syscall.O_NOCTTY|syscall.O_CLOEXEC, 0)
	if err != nil {
		return nil, fmt.Errorf("pty: open /dev/ptmx: %w", err)
	}

	// Own the master fd until Start succeeds; close it on every error path.
	ok := false
	defer func() {
		if !ok {
			_ = syscall.Close(masterFd)
		}
	}()

	slave, err := rawterm.PreparePty(masterFd)
	if err != nil {
		return nil, fmt.Errorf("pty: prepare pty: %w", err)
	}
	// The child dups the slave into its std fds and holds its own copies, so the
	// parent always drops the slave once Start has run (or on any error).
	defer func() { _ = slave.Close() }()

	// Set the initial window size on the master before Start so the child's very
	// first size query already reflects sz.
	if sz.Valid() {
		if err := setWinsize(masterFd, sz); err != nil {
			return nil, fmt.Errorf("pty: initial TIOCSWINSZ: %w", err)
		}
	}

	cmd := exec.CommandContext(ctx, command, args...)
	cmd.Env = env
	cmd.Dir = platform.DirOrHome(dir)
	cmd.Stdin = slave
	cmd.Stdout = slave
	cmd.Stderr = slave
	cmd.SysProcAttr = &syscall.SysProcAttr{
		// New session (child becomes session and process-group leader) and make
		// the slave its controlling terminal. Ctty is the child-side descriptor
		// index; the slave is wired to fd 0 above, which is also the default.
		Setsid:  true,
		Setctty: true,
		Ctty:    0,
	}
	// CommandContext's default cancellation kills only the direct child. The
	// child leads a process group, so cancellation must target that group too.
	cmd.Cancel = func() error {
		if cmd.Process == nil {
			return os.ErrProcessDone
		}
		if err := signalProcessGroup(cmd.Process.Pid, syscall.SIGKILL); err != nil {
			if errors.Is(err, syscall.ESRCH) {
				return os.ErrProcessDone
			}
			return err
		}
		return nil
	}

	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("pty: start %q: %w", command, err)
	}
	ok = true

	return &unixPTY{
		master: os.NewFile(uintptr(masterFd), "pty-master"),
		cmd:    cmd,
	}, nil
}

// unixPTY is a running child attached to the master end of a pty.
type unixPTY struct {
	master *os.File
	cmd    *exec.Cmd

	closeOnce sync.Once
	closeErr  error
}

var _ ports.PTY = (*unixPTY)(nil)

// Read returns child output read from the master.
//
// When the child (the last process holding the slave open) exits, the kernel
// makes reads of the master fail with EIO. Callers treat "child gone" as a
// normal end of stream, so EIO is mapped to io.EOF here.
func (p *unixPTY) Read(b []byte) (int, error) {
	n, err := p.master.Read(b)
	if err != nil && errors.Is(err, syscall.EIO) {
		return n, io.EOF
	}
	return n, err
}

// Write sends input bytes to the child via the master.
func (p *unixPTY) Write(b []byte) (int, error) {
	return p.master.Write(b)
}

// Resize updates the terminal window size, which also delivers SIGWINCH to the
// child's foreground process group.
//
// The ioctl goes through SyscallConn rather than os.File.Fd(): Fd() would flip
// the master to blocking mode and detach it from the runtime poller, turning
// every subsequent Read into a thread-parking syscall and breaking the
// Close-unblocks-Read behavior. Control also pins the fd for the duration and
// fails cleanly (os.ErrClosed) after Close.
func (p *unixPTY) Resize(sz domain.Size) error {
	return p.ResizeGeometry(domain.Geometry{Size: sz})
}

// ResizeGeometry updates the complete terminal cell and pixel geometry.
func (p *unixPTY) ResizeGeometry(geometry domain.Geometry) error {
	geometry = geometry.NormalizePixels()
	rc, err := p.master.SyscallConn()
	if err != nil {
		return fmt.Errorf("pty: resize: %w", err)
	}
	var ioctlErr error
	if err := rc.Control(func(fd uintptr) {
		ioctlErr = setGeometry(int(fd), geometry)
	}); err != nil {
		return fmt.Errorf("pty: resize: %w", err)
	}
	return ioctlErr
}

// Pid reports the child process id.
func (p *unixPTY) Pid() int {
	return p.cmd.Process.Pid
}

// ForegroundPgid reports the process group currently in the foreground for the
// pty's controlling terminal.
func (p *unixPTY) ForegroundPgid() (int, error) {
	rc, err := p.master.SyscallConn()
	if err != nil {
		return 0, fmt.Errorf("pty: foreground pgid: %w", err)
	}
	var pgid int
	var ioctlErr error
	if err := rc.Control(func(fd uintptr) {
		pgid, ioctlErr = rawterm.ForegroundProcessGroup(int(fd))
	}); err != nil {
		return 0, fmt.Errorf("pty: foreground pgid: %w", err)
	}
	if ioctlErr != nil {
		return 0, fmt.Errorf("pty: foreground pgid: %w", ioctlErr)
	}
	return pgid, nil
}

// Close terminates the child and releases the master. It signals the child's
// process group with SIGHUP, waits up to killGracePeriod for it to exit, then
// escalates to SIGKILL. The child is reaped (via cmd.Wait) to avoid a zombie,
// and the master fd is closed last so any blocked Read unblocks. Close is
// idempotent: subsequent calls are no-ops and return the first result.
func (p *unixPTY) Close() error {
	p.closeOnce.Do(func() {
		pid := p.cmd.Process.Pid

		// Ignore errors: the child may already be gone.
		_ = signalProcessGroup(pid, syscall.SIGHUP)

		// Reap the child (exit status is intentionally discarded; the port has no
		// Wait — "child gone" surfaces to callers as io.EOF on Read).
		reaped := make(chan struct{})
		go func() {
			_ = p.cmd.Wait()
			close(reaped)
		}()

		select {
		case <-reaped:
		case <-time.After(killGracePeriod):
			_ = signalProcessGroup(pid, syscall.SIGKILL)
			<-reaped
		}

		p.closeErr = p.master.Close()
	})
	return p.closeErr
}

// signalProcessGroup signals the process group led by pid. Open creates each
// child as its own session and process-group leader, so a negative pid reaches
// both the command and every descendant that remains in its process group.
func signalProcessGroup(pid int, signal syscall.Signal) error {
	return syscall.Kill(-pid, signal)
}

// setWinsize applies sz to the terminal referenced by fd via TIOCSWINSZ.
func setWinsize(fd int, sz domain.Size) error {
	return setGeometry(fd, domain.Geometry{Size: sz})
}

// setGeometry applies cell dimensions and any pixel dimensions reported by the
// geometry authority. Zero pixel dimensions deliberately preserve the kernel's
// explicit unknown value instead of fabricating a cell-derived size.
func setGeometry(fd int, geometry domain.Geometry) error {
	return rawterm.SetWinsizeFull(fd, rawterm.Winsize{
		Col:    uint16(geometry.Cols),
		Row:    uint16(geometry.Rows),
		Xpixel: uint16(geometry.PixelWidth),
		Ypixel: uint16(geometry.PixelHeight),
	})
}
