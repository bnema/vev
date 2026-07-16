//go:build linux

package rawterm

import (
	"fmt"
	"os"
	"syscall"
	"unsafe"
)

const (
	reqGetTermios = syscall.TCGETS
	reqSetTermios = syscall.TCSETS
)

// PtsNumber returns the pty slave number for the master referenced by
// masterFD (TIOCGPTN).
func PtsNumber(masterFD int) (int, error) {
	var n int32
	if err := ioctl(masterFD, syscall.TIOCGPTN, unsafe.Pointer(&n)); err != nil {
		return 0, fmt.Errorf("rawterm: get pts number: %w", err)
	}
	return int(n), nil
}

// UnlockPt unlocks the pty slave associated with masterFD so it can be opened
// (TIOCSPTLCK).
func UnlockPt(masterFD int) error {
	var unlock int32
	if err := ioctl(masterFD, syscall.TIOCSPTLCK, unsafe.Pointer(&unlock)); err != nil {
		return fmt.Errorf("rawterm: unlock pty: %w", err)
	}
	return nil
}

// PreparePty unlocks the slave associated with masterFD and opens it.
func PreparePty(masterFD int) (*os.File, error) {
	n, err := PtsNumber(masterFD)
	if err != nil {
		return nil, err
	}
	if err := UnlockPt(masterFD); err != nil {
		return nil, err
	}
	slaveName := fmt.Sprintf("/dev/pts/%d", n)
	slave, err := os.OpenFile(slaveName, os.O_RDWR|syscall.O_NOCTTY, 0)
	if err != nil {
		return nil, fmt.Errorf("rawterm: open %s: %w", slaveName, err)
	}
	return slave, nil
}
