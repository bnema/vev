//go:build linux

package pty

import (
	"os"
	"syscall"
	"testing"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/pkg/rawterm"
)

func TestSetGeometryPreservesPixelDimensions(t *testing.T) {
	masterFD, err := syscall.Open("/dev/ptmx", syscall.O_RDWR|syscall.O_NOCTTY|syscall.O_CLOEXEC, 0)
	if err != nil {
		t.Skipf("open /dev/ptmx: %v", err)
	}
	master := os.NewFile(uintptr(masterFD), "pty-master")
	defer master.Close()
	slave, err := rawterm.PreparePty(masterFD)
	if err != nil {
		t.Fatalf("PreparePty: %v", err)
	}
	defer slave.Close()

	geometry := domain.Geometry{Size: domain.Size{Cols: 80, Rows: 24}, PixelWidth: 640, PixelHeight: 384}
	if err := setGeometry(masterFD, geometry); err != nil {
		t.Fatalf("setGeometry: %v", err)
	}
	got, err := rawterm.GetWinsize(int(slave.Fd()))
	if err != nil {
		t.Fatalf("GetWinsize: %v", err)
	}
	want := rawterm.Winsize{Col: 80, Row: 24, Xpixel: 640, Ypixel: 384}
	if got != want {
		t.Fatalf("winsize = %+v, want %+v", got, want)
	}
}
