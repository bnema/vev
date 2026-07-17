//go:build darwin

package rawterm

import (
	"bytes"
	"fmt"
	"os"
	"syscall"
	"unsafe"
)

const (
	// TIOCGETA and TIOCSETA are Darwin's termios ioctl requests. They differ
	// from the Linux TCGETS and TCSETS requests despite operating on the same
	// syscall.Termios type.
	reqGetTermios = syscall.TIOCGETA
	reqSetTermios = syscall.TIOCSETA
)

// PreparePty grants and unlocks the slave associated with masterFD, obtains
// its name from the kernel, and opens it. Darwin requires this exact sequence:
// TIOCPTYGRANT before TIOCPTYUNLK before TIOCPTYGNAME.
func PreparePty(masterFD int) (*os.File, error) {
	if err := ioctl(masterFD, syscall.TIOCPTYGRANT, nil); err != nil {
		return nil, fmt.Errorf("rawterm: grant pty: %w", err)
	}
	if err := ioctl(masterFD, syscall.TIOCPTYUNLK, nil); err != nil {
		return nil, fmt.Errorf("rawterm: unlock pty: %w", err)
	}

	var slaveName [128]byte
	if err := ioctl(masterFD, syscall.TIOCPTYGNAME, unsafe.Pointer(&slaveName[0])); err != nil {
		return nil, fmt.Errorf("rawterm: get pty name: %w", err)
	}
	nul := bytes.IndexByte(slaveName[:], 0)
	if nul < 0 {
		return nil, fmt.Errorf("rawterm: get pty name: unterminated slave name")
	}
	if nul == 0 {
		return nil, fmt.Errorf("rawterm: get pty name: empty slave name")
	}

	name := string(slaveName[:nul])
	slave, err := os.OpenFile(name, os.O_RDWR|syscall.O_NOCTTY, 0)
	if err != nil {
		return nil, fmt.Errorf("rawterm: open %s: %w", name, err)
	}
	return slave, nil
}
