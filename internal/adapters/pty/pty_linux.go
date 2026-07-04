//go:build linux

package pty

// The Linux adapter drives /dev/ptmx directly (open master, TIOCGPTN to learn
// the slave index, TIOCSPTLCK to unlock, then open /dev/pts/<N>) rather than
// depending on a third-party pty package, keeping the dependency surface at
// x/sys/unix plus the standard library.

import (
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
	"golang.org/x/sys/unix"
)

// killGracePeriod is how long Close waits for a signalled child to exit before
// escalating from SIGHUP to SIGKILL.
const killGracePeriod = 2 * time.Second

// Factory implements ports.PTYFactory for Linux.
type Factory struct{}

// NewFactory returns a Linux PTY factory.
func NewFactory() *Factory { return &Factory{} }

// Open spawns command with args attached to a freshly allocated pseudo-terminal
// and returns the master side as a ports.PTY. env is passed to the child
// verbatim (nil means inherit the current process environment); the caller
// decides TERM and friends. sz sets the terminal window size before the child
// starts, so the child observes the correct dimensions on its first query.
func (Factory) Open(command string, args []string, env []string, dir string, sz domain.Size) (ports.PTY, error) {
	masterFd, err := unix.Open("/dev/ptmx", unix.O_RDWR|unix.O_NOCTTY|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, fmt.Errorf("pty: open /dev/ptmx: %w", err)
	}

	// Own the master fd until Start succeeds; close it on every error path.
	ok := false
	defer func() {
		if !ok {
			_ = unix.Close(masterFd)
		}
	}()

	ptn, err := unix.IoctlGetInt(masterFd, unix.TIOCGPTN)
	if err != nil {
		return nil, fmt.Errorf("pty: TIOCGPTN: %w", err)
	}

	// Unlock the slave so it can be opened.
	if err := unix.IoctlSetPointerInt(masterFd, unix.TIOCSPTLCK, 0); err != nil {
		return nil, fmt.Errorf("pty: TIOCSPTLCK: %w", err)
	}

	slaveName := fmt.Sprintf("/dev/pts/%d", ptn)
	slave, err := os.OpenFile(slaveName, os.O_RDWR|unix.O_NOCTTY, 0)
	if err != nil {
		return nil, fmt.Errorf("pty: open %s: %w", slaveName, err)
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

	cmd := exec.Command(command, args...)
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

	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("pty: start %q: %w", command, err)
	}
	ok = true

	return &linuxPTY{
		master: os.NewFile(uintptr(masterFd), "pty-master"),
		cmd:    cmd,
	}, nil
}

// linuxPTY is a running child attached to the master end of a pty.
type linuxPTY struct {
	master *os.File
	cmd    *exec.Cmd

	closeOnce sync.Once
	closeErr  error
}

var _ ports.PTY = (*linuxPTY)(nil)

// Read returns child output read from the master.
//
// When the child (the last process holding the slave open) exits, the kernel
// makes reads of the master fail with EIO. Callers treat "child gone" as a
// normal end of stream, so EIO is mapped to io.EOF here.
func (p *linuxPTY) Read(b []byte) (int, error) {
	n, err := p.master.Read(b)
	if err != nil && errors.Is(err, unix.EIO) {
		return n, io.EOF
	}
	return n, err
}

// Write sends input bytes to the child via the master.
func (p *linuxPTY) Write(b []byte) (int, error) {
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
func (p *linuxPTY) Resize(sz domain.Size) error {
	rc, err := p.master.SyscallConn()
	if err != nil {
		return fmt.Errorf("pty: resize: %w", err)
	}
	var ioctlErr error
	if err := rc.Control(func(fd uintptr) {
		ioctlErr = setWinsize(int(fd), sz)
	}); err != nil {
		return fmt.Errorf("pty: resize: %w", err)
	}
	return ioctlErr
}

// Pid reports the child process id.
func (p *linuxPTY) Pid() int {
	return p.cmd.Process.Pid
}

// ForegroundPgid reports the process group currently in the foreground for the
// pty's controlling terminal.
func (p *linuxPTY) ForegroundPgid() (int, error) {
	rc, err := p.master.SyscallConn()
	if err != nil {
		return 0, fmt.Errorf("pty: foreground pgid: %w", err)
	}
	var pgid int
	var ioctlErr error
	if err := rc.Control(func(fd uintptr) {
		pgid, ioctlErr = unix.IoctlGetInt(int(fd), unix.TIOCGPGRP)
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
func (p *linuxPTY) Close() error {
	p.closeOnce.Do(func() {
		pid := p.cmd.Process.Pid

		// Negative pid targets the whole process group (the child is the group
		// leader thanks to Setsid). Ignore errors: the child may already be gone.
		_ = unix.Kill(-pid, unix.SIGHUP)

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
			_ = unix.Kill(-pid, unix.SIGKILL)
			<-reaped
		}

		p.closeErr = p.master.Close()
	})
	return p.closeErr
}

// setWinsize applies sz to the terminal referenced by fd via TIOCSWINSZ.
func setWinsize(fd int, sz domain.Size) error {
	return unix.IoctlSetWinsize(fd, unix.TIOCSWINSZ, &unix.Winsize{
		Row: uint16(sz.Rows),
		Col: uint16(sz.Cols),
	})
}
