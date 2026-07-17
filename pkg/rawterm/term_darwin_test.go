//go:build darwin

package rawterm

import (
	"os"
	"syscall"
	"testing"
	"unsafe"
)

// openPTYPair opens a real Darwin PTY master/slave pair through the package
// under test, exercising the grant, unlock, name, and open protocol.
func openPTYPair(t *testing.T) (master, slave *os.File) {
	t.Helper()

	masterFD, err := syscall.Open("/dev/ptmx", syscall.O_RDWR|syscall.O_NOCTTY|syscall.O_CLOEXEC, 0)
	if err != nil {
		t.Skipf("open /dev/ptmx: %v", err)
	}
	master = os.NewFile(uintptr(masterFD), "pty-master")

	slave, err = PreparePty(masterFD)
	if err != nil {
		_ = master.Close()
		t.Fatalf("PreparePty: %v", err)
	}

	return master, slave
}

func TestPreparePtyOpensSlave(t *testing.T) {
	master, slave := openPTYPair(t)
	defer func() { _ = master.Close() }()
	defer func() { _ = slave.Close() }()

	if !IsTerminal(int(slave.Fd())) {
		t.Error("PreparePty slave is not a terminal")
	}
}

func TestMakeRawRestore(t *testing.T) {
	master, slave := openPTYPair(t)
	defer func() { _ = master.Close() }()
	defer func() { _ = slave.Close() }()

	fd := int(slave.Fd())
	old, err := MakeRaw(fd)
	if err != nil {
		t.Fatalf("MakeRaw: %v", err)
	}
	if old == nil {
		t.Fatal("MakeRaw returned nil state")
	}

	var raw syscall.Termios
	if err := ioctl(fd, reqGetTermios, unsafe.Pointer(&raw)); err != nil {
		t.Fatalf("get termios after MakeRaw: %v", err)
	}
	if raw.Lflag&(syscall.ECHO|syscall.ICANON) != 0 {
		t.Errorf("Lflag after MakeRaw = %#x, ECHO and ICANON must be clear", raw.Lflag)
	}
	if raw.Cc[syscall.VMIN] != 1 || raw.Cc[syscall.VTIME] != 0 {
		t.Errorf("Cc[VMIN, VTIME] = [%d, %d], want [1, 0]", raw.Cc[syscall.VMIN], raw.Cc[syscall.VTIME])
	}

	if err := Restore(fd, old); err != nil {
		t.Fatalf("Restore: %v", err)
	}
	var restored syscall.Termios
	if err := ioctl(fd, reqGetTermios, unsafe.Pointer(&restored)); err != nil {
		t.Fatalf("get termios after Restore: %v", err)
	}
	if restored != old.termios {
		t.Error("Restore did not restore the original termios settings")
	}
}

func TestSetWinsizeGetSizeRoundTrip(t *testing.T) {
	master, slave := openPTYPair(t)
	defer func() { _ = master.Close() }()
	defer func() { _ = slave.Close() }()

	tests := []struct {
		name string
		cols uint16
		rows uint16
	}{
		{name: "typical", cols: 80, rows: 24},
		{name: "wide", cols: 220, rows: 60},
		{name: "minimal", cols: 1, rows: 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := SetWinsize(int(slave.Fd()), tt.cols, tt.rows); err != nil {
				t.Fatalf("SetWinsize: %v", err)
			}
			cols, rows, err := GetSize(int(slave.Fd()))
			if err != nil {
				t.Fatalf("GetSize: %v", err)
			}
			if cols != int(tt.cols) || rows != int(tt.rows) {
				t.Errorf("GetSize = (%d, %d), want (%d, %d)", cols, rows, tt.cols, tt.rows)
			}
		})
	}
}
