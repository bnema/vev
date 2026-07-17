package ipc

import (
	"errors"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/bnema/vev/internal/ports"
)

func TestListenAcceptRoundTrip(t *testing.T) {
	dir := shortSocketDir(t, "vev")

	ln, err := Listen(dir)
	if err != nil {
		t.Fatalf("Listen() error = %v", err)
	}
	defer func() { _ = ln.Close() }()

	wantPath := filepath.Join(dir, "daemon.sock")
	if ln.Addr() != wantPath {
		t.Fatalf("Addr() = %q, want %q", ln.Addr(), wantPath)
	}

	fi, err := os.Stat(wantPath)
	if err != nil {
		t.Fatalf("stat socket: %v", err)
	}
	if perm := fi.Mode().Perm(); perm != 0o600 {
		t.Fatalf("socket perm = %o, want 0600", perm)
	}

	errCh := make(chan error, 1)
	go func() {
		conn, err := net.Dial("unix", wantPath)
		if err != nil {
			errCh <- err
			return
		}
		defer func() { _ = conn.Close() }()
		client := NewTransport(conn)
		errCh <- client.Send(ports.Frame{Type: ports.MsgPing})
	}()

	transport, err := ln.Accept()
	if err != nil {
		t.Fatalf("Accept() error = %v", err)
	}
	defer func() { _ = transport.Close() }()

	got, err := transport.Recv()
	if err != nil {
		t.Fatalf("Recv() error = %v", err)
	}
	if got.Type != ports.MsgPing {
		t.Fatalf("Recv() type = %v, want MsgPing", got.Type)
	}
	if err := <-errCh; err != nil {
		t.Fatalf("client dial/send error = %v", err)
	}
}

func TestListenMkdirsSocketDir(t *testing.T) {
	dir := shortSocketDir(t, "nested", "socketdir")

	ln, err := Listen(dir)
	if err != nil {
		t.Fatalf("Listen() error = %v", err)
	}
	defer func() { _ = ln.Close() }()

	fi, err := os.Stat(dir)
	if err != nil {
		t.Fatalf("stat dir: %v", err)
	}
	if !fi.IsDir() {
		t.Fatalf("%s is not a directory", dir)
	}
	if perm := fi.Mode().Perm(); perm != 0o700 {
		t.Fatalf("dir perm = %o, want 0700", perm)
	}
}

func TestListenRejectsHostileSocketDir(t *testing.T) {
	dir := shortSocketDir(t, "socketdir")
	if err := os.MkdirAll(dir, 0o777); err != nil {
		t.Fatalf("mkdir hostile socket dir: %v", err)
	}
	if err := os.Chmod(dir, 0o777); err != nil {
		t.Fatalf("chmod hostile socket dir: %v", err)
	}

	ln, err := Listen(dir)
	if err == nil {
		_ = ln.Close()
		t.Fatal("Listen() error = nil, want hostile directory rejection")
	}
	if !strings.Contains(err.Error(), dir) {
		t.Fatalf("Listen() error = %q, want mention of %q", err, dir)
	}
}

func TestListenStaleSocketCleanedUp(t *testing.T) {
	dir := shortSocketDir(t, "vev")
	if err := os.Mkdir(dir, 0o700); err != nil {
		t.Fatalf("mkdir safe socket dir: %v", err)
	}
	sockPath := filepath.Join(dir, "daemon.sock")

	// Create a socket file with nobody listening behind it: bind, then
	// close without unlinking, to simulate a daemon that died without
	// cleaning up after itself.
	addr, err := net.ResolveUnixAddr("unix", sockPath)
	if err != nil {
		t.Fatalf("ResolveUnixAddr() error = %v", err)
	}
	dead, err := net.ListenUnix("unix", addr)
	if err != nil {
		t.Fatalf("ListenUnix() error = %v", err)
	}
	dead.SetUnlinkOnClose(false)
	if err := dead.Close(); err != nil {
		t.Fatalf("closing dead listener: %v", err)
	}
	if _, err := os.Stat(sockPath); err != nil {
		t.Fatalf("expected stale socket file to exist: %v", err)
	}

	// A real Listen() should detect the dead peer and recover.
	ln, err := Listen(dir)
	if err != nil {
		t.Fatalf("Listen() over stale socket error = %v, want success", err)
	}
	defer func() { _ = ln.Close() }()
}

func TestListenLiveDaemonRejected(t *testing.T) {
	dir := shortSocketDir(t, "vev")

	first, err := Listen(dir)
	if err != nil {
		t.Fatalf("first Listen() error = %v", err)
	}
	defer func() { _ = first.Close() }()

	_, err = Listen(dir)
	if !errors.Is(err, ErrDaemonRunning) {
		t.Fatalf("second Listen() error = %v, want ErrDaemonRunning", err)
	}
}

func TestSocketDirUsesXDGRuntimeDir(t *testing.T) {
	t.Setenv("XDG_RUNTIME_DIR", "/tmp/example-xdg")

	got := SocketDir()
	want := "/tmp/example-xdg/vev"
	if got != want {
		t.Fatalf("SocketDir() = %q, want %q", got, want)
	}
}

func TestSocketDirFallsBackWithoutXDGRuntimeDir(t *testing.T) {
	t.Setenv("XDG_RUNTIME_DIR", "")

	got := SocketDir()
	uid := os.Getuid()

	runUser := filepath.Join("/run/user", strconv.Itoa(uid))
	var want string
	if fi, err := os.Stat(runUser); err == nil && fi.IsDir() {
		want = filepath.Join(runUser, "vev")
	} else {
		want = "/tmp/vev-" + strconv.Itoa(uid)
	}

	if got != want {
		t.Fatalf("SocketDir() = %q, want %q", got, want)
	}
}
