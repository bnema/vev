// Package linuxterm provides the small set of terminal-control primitives
// vev needs (raw mode, window size, pty allocation) directly on top of the
// frozen stdlib syscall package, so the module does not depend on
// golang.org/x/term or golang.org/x/sys. vev is Linux-only, and the Linux
// TIOC*/TCGETS ioctl numbers and Termios layout syscall exposes are stable.
package linuxterm

import (
	"fmt"
	"syscall"
	"unsafe"
)

// State is an opaque snapshot of a terminal's termios settings, captured by
// MakeRaw and consumed by Restore.
type State struct {
	termios syscall.Termios
}

// winsize mirrors the kernel's struct winsize for TIOCGWINSZ/TIOCSWINSZ.
type winsize struct {
	Row    uint16
	Col    uint16
	Xpixel uint16
	Ypixel uint16
}

// ioctl issues a SYS_IOCTL syscall against fd with the given request and
// argument pointer, returning a Go error on failure. The pointer-to-uintptr
// conversion must happen inside the Syscall argument list (unsafe.Pointer
// rule 4), so arg stays a live pointer until the call.
func ioctl(fd int, req uintptr, arg unsafe.Pointer) error {
	_, _, errno := syscall.Syscall(syscall.SYS_IOCTL, uintptr(fd), req, uintptr(arg))
	if errno != 0 {
		return errno
	}
	return nil
}

// IsTerminal reports whether fd refers to a terminal device.
func IsTerminal(fd int) bool {
	var t syscall.Termios
	return ioctl(fd, syscall.TCGETS, unsafe.Pointer(&t)) == nil
}

// MakeRaw puts the terminal referenced by fd into raw mode and returns its
// pre-modification state, so a caller can later restore it. The flag
// changes applied match golang.org/x/term's MakeRaw semantics exactly.
func MakeRaw(fd int) (*State, error) {
	var oldState State
	if err := ioctl(fd, syscall.TCGETS, unsafe.Pointer(&oldState.termios)); err != nil {
		return nil, fmt.Errorf("linuxterm: get termios: %w", err)
	}

	raw := oldState.termios
	raw.Iflag &^= syscall.IGNBRK | syscall.BRKINT | syscall.PARMRK | syscall.ISTRIP | syscall.INLCR | syscall.IGNCR | syscall.ICRNL | syscall.IXON
	raw.Oflag &^= syscall.OPOST
	raw.Lflag &^= syscall.ECHO | syscall.ECHONL | syscall.ICANON | syscall.ISIG | syscall.IEXTEN
	raw.Cflag &^= syscall.CSIZE | syscall.PARENB
	raw.Cflag |= syscall.CS8
	raw.Cc[syscall.VMIN] = 1
	raw.Cc[syscall.VTIME] = 0

	if err := ioctl(fd, syscall.TCSETS, unsafe.Pointer(&raw)); err != nil {
		return nil, fmt.Errorf("linuxterm: set termios: %w", err)
	}

	return &oldState, nil
}

// Restore applies a previously captured State back to the terminal
// referenced by fd.
func Restore(fd int, s *State) error {
	if err := ioctl(fd, syscall.TCSETS, unsafe.Pointer(&s.termios)); err != nil {
		return fmt.Errorf("linuxterm: set termios: %w", err)
	}
	return nil
}

// GetSize returns the terminal's current column and row count.
func GetSize(fd int) (cols, rows int, err error) {
	var ws winsize
	if err := ioctl(fd, syscall.TIOCGWINSZ, unsafe.Pointer(&ws)); err != nil {
		return 0, 0, fmt.Errorf("linuxterm: get winsize: %w", err)
	}
	return int(ws.Col), int(ws.Row), nil
}

// SetWinsize sets the terminal's column and row count.
func SetWinsize(fd int, cols, rows uint16) error {
	ws := winsize{Row: rows, Col: cols}
	if err := ioctl(fd, syscall.TIOCSWINSZ, unsafe.Pointer(&ws)); err != nil {
		return fmt.Errorf("linuxterm: set winsize: %w", err)
	}
	return nil
}

// PtsNumber returns the pty slave number for the master referenced by
// masterFD (TIOCGPTN).
func PtsNumber(masterFD int) (int, error) {
	var n int32
	if err := ioctl(masterFD, syscall.TIOCGPTN, unsafe.Pointer(&n)); err != nil {
		return 0, fmt.Errorf("linuxterm: get pts number: %w", err)
	}
	return int(n), nil
}

// UnlockPt unlocks the pty slave associated with masterFD so it can be
// opened (TIOCSPTLCK).
func UnlockPt(masterFD int) error {
	var unlock int32
	if err := ioctl(masterFD, syscall.TIOCSPTLCK, unsafe.Pointer(&unlock)); err != nil {
		return fmt.Errorf("linuxterm: unlock pty: %w", err)
	}
	return nil
}

// ForegroundProcessGroup returns the process group id currently in the
// foreground for the terminal referenced by fd (TIOCGPGRP).
func ForegroundProcessGroup(fd int) (int, error) {
	var pgid int32
	if err := ioctl(fd, syscall.TIOCGPGRP, unsafe.Pointer(&pgid)); err != nil {
		return 0, fmt.Errorf("linuxterm: get foreground process group: %w", err)
	}
	return int(pgid), nil
}
