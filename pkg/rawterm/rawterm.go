//go:build linux || darwin

// Package rawterm provides the small set of terminal-control primitives vev
// needs directly on top of the frozen stdlib syscall package. The common raw
// mode and terminal-size operations use platform-specific termios requests.
package rawterm

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

// Winsize mirrors the kernel's struct winsize for TIOCGWINSZ/TIOCSWINSZ.
// Zero pixel dimensions mean the controlling terminal did not report them.
type Winsize struct {
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
	return ioctl(fd, reqGetTermios, unsafe.Pointer(&t)) == nil
}

// MakeRaw puts the terminal referenced by fd into raw mode and returns its
// pre-modification state, so a caller can later restore it. The flag
// changes applied match golang.org/x/term's MakeRaw semantics exactly.
func MakeRaw(fd int) (*State, error) {
	var oldState State
	if err := ioctl(fd, reqGetTermios, unsafe.Pointer(&oldState.termios)); err != nil {
		return nil, fmt.Errorf("rawterm: get termios: %w", err)
	}

	raw := oldState.termios
	raw.Iflag &^= syscall.IGNBRK | syscall.BRKINT | syscall.PARMRK | syscall.ISTRIP | syscall.INLCR | syscall.IGNCR | syscall.ICRNL | syscall.IXON
	raw.Oflag &^= syscall.OPOST
	raw.Lflag &^= syscall.ECHO | syscall.ECHONL | syscall.ICANON | syscall.ISIG | syscall.IEXTEN
	raw.Cflag &^= syscall.CSIZE | syscall.PARENB
	raw.Cflag |= syscall.CS8
	raw.Cc[syscall.VMIN] = 1
	raw.Cc[syscall.VTIME] = 0

	if err := ioctl(fd, reqSetTermios, unsafe.Pointer(&raw)); err != nil {
		return nil, fmt.Errorf("rawterm: set termios: %w", err)
	}

	return &oldState, nil
}

// Restore applies a previously captured State back to the terminal
// referenced by fd.
func Restore(fd int, s *State) error {
	if err := ioctl(fd, reqSetTermios, unsafe.Pointer(&s.termios)); err != nil {
		return fmt.Errorf("rawterm: set termios: %w", err)
	}
	return nil
}

// GetSize returns the terminal's current column and row count.
func GetSize(fd int) (cols, rows int, err error) {
	ws, err := GetWinsize(fd)
	if err != nil {
		return 0, 0, err
	}
	return int(ws.Col), int(ws.Row), nil
}

// GetWinsize returns the terminal's cell and optional pixel dimensions.
func GetWinsize(fd int) (Winsize, error) {
	var ws Winsize
	if err := ioctl(fd, syscall.TIOCGWINSZ, unsafe.Pointer(&ws)); err != nil {
		return Winsize{}, fmt.Errorf("rawterm: get winsize: %w", err)
	}
	return ws, nil
}

// SetWinsize sets the terminal's column and row count.
func SetWinsize(fd int, cols, rows uint16) error {
	return SetWinsizeFull(fd, Winsize{Row: rows, Col: cols})
}

// SetWinsizeFull sets the terminal's cell and optional pixel dimensions.
func SetWinsizeFull(fd int, ws Winsize) error {
	if err := ioctl(fd, syscall.TIOCSWINSZ, unsafe.Pointer(&ws)); err != nil {
		return fmt.Errorf("rawterm: set winsize: %w", err)
	}
	return nil
}

// ForegroundProcessGroup returns the process group id currently in the
// foreground for the terminal referenced by fd (TIOCGPGRP).
func ForegroundProcessGroup(fd int) (int, error) {
	var pgid int32
	if err := ioctl(fd, syscall.TIOCGPGRP, unsafe.Pointer(&pgid)); err != nil {
		return 0, fmt.Errorf("rawterm: get foreground process group: %w", err)
	}
	return int(pgid), nil
}
