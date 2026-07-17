//go:build linux

package rawterm

import (
	"errors"
	"os"
	"syscall"
	"testing"
	"unsafe"
)

// openPTYPair opens a real Linux PTY master/slave pair directly via
// /dev/ptmx using the package under test, so these tests double as an
// integration check of PreparePty against the kernel.
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

func TestIsTerminal(t *testing.T) {
	master, slave := openPTYPair(t)
	defer func() { _ = master.Close() }()
	defer func() { _ = slave.Close() }()

	if !IsTerminal(int(slave.Fd())) {
		t.Errorf("IsTerminal(pty slave) = false, want true")
	}

	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("os.Pipe: %v", err)
	}
	defer func() { _ = r.Close() }()
	defer func() { _ = w.Close() }()

	if IsTerminal(int(r.Fd())) {
		t.Errorf("IsTerminal(pipe) = true, want false")
	}
}

func TestSetWinsizeGetSizeRoundtrip(t *testing.T) {
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
			gotCols, gotRows, err := GetSize(int(slave.Fd()))
			if err != nil {
				t.Fatalf("GetSize: %v", err)
			}
			if gotCols != int(tt.cols) || gotRows != int(tt.rows) {
				t.Fatalf("GetSize = (%d, %d), want (%d, %d)", gotCols, gotRows, tt.cols, tt.rows)
			}
		})
	}
}

func TestMakeRawRestore(t *testing.T) {
	master, slave := openPTYPair(t)
	defer func() { _ = master.Close() }()
	defer func() { _ = slave.Close() }()

	fd := int(slave.Fd())

	var original syscall.Termios
	if err := ioctl(fd, reqGetTermios, unsafe.Pointer(&original)); err != nil {
		t.Fatalf("get termios before MakeRaw: %v", err)
	}

	old, err := MakeRaw(fd)
	if err != nil {
		t.Fatalf("MakeRaw: %v", err)
	}
	if old == nil {
		t.Fatalf("MakeRaw returned nil state")
	}

	var current syscall.Termios
	if err := ioctl(fd, reqGetTermios, unsafe.Pointer(&current)); err != nil {
		t.Fatalf("get termios after MakeRaw: %v", err)
	}
	if current.Lflag&syscall.ECHO != 0 {
		t.Errorf("ECHO still set after MakeRaw")
	}
	if current.Lflag&syscall.ICANON != 0 {
		t.Errorf("ICANON still set after MakeRaw")
	}
	if current.Cc[syscall.VMIN] != 1 {
		t.Errorf("Cc[VMIN] = %d, want 1", current.Cc[syscall.VMIN])
	}

	if err := Restore(fd, old); err != nil {
		t.Fatalf("Restore: %v", err)
	}

	var restored syscall.Termios
	if err := ioctl(fd, reqGetTermios, unsafe.Pointer(&restored)); err != nil {
		t.Fatalf("get termios after Restore: %v", err)
	}
	if restored != original {
		t.Errorf("termios after Restore = %#v, want original %#v", restored, original)
	}
}

func TestForegroundProcessGroup(t *testing.T) {
	master, slave := openPTYPair(t)
	defer func() { _ = master.Close() }()
	defer func() { _ = slave.Close() }()

	// No session has been established on this pty (no process ever made it
	// a controlling terminal via Setsid+Setctty), so the kernel legitimately
	// has no foreground process group to report and returns ENOTTY. That
	// still exercises the ioctl path; only an unexpected error is a failure.
	if _, err := ForegroundProcessGroup(int(slave.Fd())); err != nil && !errors.Is(err, syscall.ENOTTY) {
		t.Fatalf("ForegroundProcessGroup: %v", err)
	}
}
