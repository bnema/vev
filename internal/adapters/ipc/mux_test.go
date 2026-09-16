package ipc

import (
	"context"
	"errors"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/bnema/vev/internal/protocol/wire"
)

// muxTestPath returns a short, owner-only parent directory and one mux.sock
// path inside it, so a caller-supplied mux carriage path is exercised at the
// real AF_UNIX pathname limit rather than a long temporary path.
func muxTestPath(t *testing.T) string {
	t.Helper()
	return filepath.Join(shortSocketDir(t, "vev"), "mux.sock")
}

// mustListenMux binds one mux carriage at path and closes it at test end.
func mustListenMux(t *testing.T, path string) MuxListener {
	t.Helper()
	listener, err := ListenMux(path)
	if err != nil {
		t.Fatalf("ListenMux(%q): %v", path, err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	return listener
}

// dialMuxAsync dials one mux carriage and delivers the outcome.
func dialMuxAsync(t *testing.T, ctx context.Context, path string) <-chan struct {
	transport wire.BoundedTransport
	err       error
} {
	t.Helper()
	out := make(chan struct {
		transport wire.BoundedTransport
		err       error
	}, 1)
	go func() {
		transport, err := DialMuxContext(ctx, path)
		out <- struct {
			transport wire.BoundedTransport
			err       error
		}{transport: transport, err: err}
	}()
	return out
}

func awaitMuxDial(t *testing.T, out <-chan struct {
	transport wire.BoundedTransport
	err       error
}) wire.BoundedTransport {
	t.Helper()
	select {
	case result := <-out:
		if result.err != nil {
			t.Fatalf("DialMuxContext: %v", result.err)
		}
		if result.transport == nil {
			t.Fatal("DialMuxContext returned a nil transport")
		}
		t.Cleanup(func() { _ = result.transport.Close() })
		return result.transport
	case <-time.After(5 * time.Second):
		t.Fatal("DialMuxContext did not return")
		return nil
	}
}

// TestListenMuxCreatesOwnerOnlyParentAndSocket proves listener setup creates the
// caller's parent directory owner-only (0700) and tightens the bound socket to
// 0600, so the private carriage is never exposed through filesystem modes.
func TestListenMuxCreatesOwnerOnlyParentAndSocket(t *testing.T) {
	path := muxTestPath(t)
	parent := filepath.Dir(path)

	listener := mustListenMux(t, path)
	if listener.Addr() != path {
		t.Fatalf("Addr() = %q, want %q", listener.Addr(), path)
	}

	dirInfo, err := os.Stat(parent)
	if err != nil {
		t.Fatalf("stat parent: %v", err)
	}
	if perm := dirInfo.Mode().Perm(); perm != 0o700 {
		t.Fatalf("parent perm = %o, want 0700", perm)
	}

	sockInfo, err := os.Lstat(path)
	if err != nil {
		t.Fatalf("stat socket: %v", err)
	}
	if sockInfo.Mode()&os.ModeSocket == 0 {
		t.Fatalf("%s is not a socket: mode %v", path, sockInfo.Mode())
	}
	if perm := sockInfo.Mode().Perm(); perm != 0o600 {
		t.Fatalf("socket perm = %o, want 0600", perm)
	}
}

// TestListenMuxSameUserRoundTrip proves a real AF_UNIX mux carriage carries
// bounded framed envelopes both ways after both ends verify the peer is the
// current user.
func TestListenMuxSameUserRoundTrip(t *testing.T) {
	path := muxTestPath(t)
	listener := mustListenMux(t, path)

	accepted := make(chan struct {
		transport wire.BoundedTransport
		err       error
	}, 1)
	go func() {
		transport, err := listener.Accept()
		accepted <- struct {
			transport wire.BoundedTransport
			err       error
		}{transport: transport, err: err}
	}()

	client := awaitMuxDial(t, dialMuxAsync(t, context.Background(), path))

	serverResult := <-accepted
	if serverResult.err != nil {
		t.Fatalf("Accept: %v", serverResult.err)
	}
	server := serverResult.transport
	t.Cleanup(func() { _ = server.Close() })

	request := wire.Envelope{Payload: []byte("mux request stays exact")}
	if err := client.Send(request); err != nil {
		t.Fatalf("client Send: %v", err)
	}
	got, err := server.RecvBounded(uint64(len(request.Payload)))
	if err != nil {
		t.Fatalf("server RecvBounded: %v", err)
	}
	if string(got.Payload) != string(request.Payload) {
		t.Fatalf("server RecvBounded payload = %q, want %q", got.Payload, request.Payload)
	}

	reply := wire.Envelope{Payload: []byte("mux reply stays exact")}
	if err := server.Send(reply); err != nil {
		t.Fatalf("server Send: %v", err)
	}
	gotReply, err := client.RecvBounded(uint64(len(reply.Payload)))
	if err != nil {
		t.Fatalf("client RecvBounded: %v", err)
	}
	if string(gotReply.Payload) != string(reply.Payload) {
		t.Fatalf("client RecvBounded payload = %q, want %q", gotReply.Payload, reply.Payload)
	}
}

// TestListenMuxRejectsUnverifiedAccept proves the accept side verifies peer
// credentials before handing out a transport: an injected refusal is observable
// as ErrMuxPeerRejected and the connection is closed.
func TestListenMuxRejectsUnverifiedAccept(t *testing.T) {
	path := muxTestPath(t)
	reject := func(*net.UnixConn) error { return ErrMuxPeerRejected }
	listener, err := listenMux(path, reject)
	if err != nil {
		t.Fatalf("listenMux: %v", err)
	}
	t.Cleanup(func() { _ = listener.Close() })

	dialOutcome := dialMuxAsync(t, context.Background(), path)

	transport, err := listener.Accept()
	if transport != nil {
		_ = transport.Close()
		t.Fatal("Accept returned a transport for an unverified peer")
	}
	if !errors.Is(err, ErrMuxPeerRejected) {
		t.Fatalf("Accept error = %v, want ErrMuxPeerRejected", err)
	}

	result := <-dialOutcome
	if result.transport != nil {
		_ = result.transport.Close()
	}
}

// TestDialMuxRejectsUnverifiedPeer proves the dial side verifies peer
// credentials before handing out a transport: an injected refusal fails the
// dial with ErrMuxPeerRejected and no transport escapes.
func TestDialMuxRejectsUnverifiedPeer(t *testing.T) {
	path := muxTestPath(t)
	permissive := func(*net.UnixConn) error { return nil }
	listener, err := listenMux(path, permissive)
	if err != nil {
		t.Fatalf("listenMux: %v", err)
	}
	t.Cleanup(func() { _ = listener.Close() })

	accepted := make(chan wire.BoundedTransport, 1)
	go func() {
		transport, acceptErr := listener.Accept()
		if acceptErr == nil {
			accepted <- transport
			return
		}
		accepted <- nil
	}()

	reject := func(*net.UnixConn) error { return ErrMuxPeerRejected }
	transport, err := dialMux(context.Background(), path, reject)
	if transport != nil {
		_ = transport.Close()
		t.Fatal("dialMux returned a transport for an unverified peer")
	}
	if !errors.Is(err, ErrMuxPeerRejected) {
		t.Fatalf("dialMux error = %v, want ErrMuxPeerRejected", err)
	}

	select {
	case server := <-accepted:
		if server != nil {
			_ = server.Close()
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Accept did not return")
	}
}

// createStaleMuxSocket leaves a socket file at path with nobody listening
// behind it, simulating an owner that died without unlinking its socket.
func createStaleMuxSocket(t *testing.T, path string) os.FileInfo {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatalf("mkdir parent: %v", err)
	}
	addr, err := net.ResolveUnixAddr("unix", path)
	if err != nil {
		t.Fatalf("ResolveUnixAddr: %v", err)
	}
	dead, err := net.ListenUnix("unix", addr)
	if err != nil {
		t.Fatalf("ListenUnix: %v", err)
	}
	dead.SetUnlinkOnClose(false)
	if err := dead.Close(); err != nil {
		t.Fatalf("closing dead listener: %v", err)
	}
	info, err := os.Lstat(path)
	if err != nil {
		t.Fatalf("expected stale socket file: %v", err)
	}
	return info
}

// TestListenMuxStaleSocketRecovered proves a socket file left by a dead owner is
// recovered race-safely and the carriage binds and serves.
func TestListenMuxStaleSocketRecovered(t *testing.T) {
	path := muxTestPath(t)
	stale := createStaleMuxSocket(t, path)

	mustListenMux(t, path)

	recovered, err := os.Lstat(path)
	if err != nil {
		t.Fatalf("stat recovered socket: %v", err)
	}
	if os.SameFile(stale, recovered) {
		t.Fatal("stale socket inode was not replaced by the recovered listener")
	}

	// The recovered carriage serves a same-user connection.
	_ = awaitMuxDial(t, dialMuxAsync(t, context.Background(), path))
}

// TestListenMuxStaleRecoveryRacesToOneWinner proves the stale recovery is
// race-safe: concurrent listeners racing over one stale socket produce exactly
// one winner, and every loser observes the winner's live socket instead of
// unlinking it.
func TestListenMuxStaleRecoveryRacesToOneWinner(t *testing.T) {
	path := muxTestPath(t)
	createStaleMuxSocket(t, path)

	const contenders = 8
	type outcome struct {
		listener MuxListener
		err      error
	}
	results := make(chan outcome, contenders)
	var wg sync.WaitGroup
	for i := 0; i < contenders; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			listener, err := ListenMux(path)
			results <- outcome{listener: listener, err: err}
		}()
	}
	wg.Wait()
	close(results)

	successes, live := 0, 0
	for result := range results {
		switch {
		case result.err == nil:
			successes++
			t.Cleanup(func() { _ = result.listener.Close() })
		case errors.Is(result.err, ErrDaemonRunning):
			live++
		default:
			t.Fatalf("unexpected race outcome: %v", result.err)
		}
	}
	if successes != 1 {
		t.Fatalf("winners = %d, want exactly 1", successes)
	}
	if live != contenders-1 {
		t.Fatalf("losers observing a live owner = %d, want %d", live, contenders-1)
	}
}

// TestListenMuxRefusesLiveEndpoint proves a second listener on a live mux path
// is refused rather than clobbering it.
func TestListenMuxRefusesLiveEndpoint(t *testing.T) {
	path := muxTestPath(t)
	mustListenMux(t, path)

	second, err := ListenMux(path)
	if second != nil {
		_ = second.Close()
	}
	if !errors.Is(err, ErrDaemonRunning) {
		t.Fatalf("second ListenMux error = %v, want ErrDaemonRunning", err)
	}
}

// TestListenMuxRefusesForeignPath proves a path that exists and is not a socket
// is refused without being removed: a regular file and a directory are left
// intact.
func TestListenMuxRefusesForeignPath(t *testing.T) {
	t.Run("regular file", func(t *testing.T) {
		path := muxTestPath(t)
		if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
			t.Fatalf("mkdir parent: %v", err)
		}
		const contents = "caller data"
		if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
			t.Fatalf("write file: %v", err)
		}

		listener, err := ListenMux(path)
		if listener != nil {
			_ = listener.Close()
		}
		if !errors.Is(err, ErrMuxForeignPath) {
			t.Fatalf("ListenMux error = %v, want ErrMuxForeignPath", err)
		}
		got, readErr := os.ReadFile(path)
		if readErr != nil {
			t.Fatalf("foreign file was removed: %v", readErr)
		}
		if string(got) != contents {
			t.Fatalf("foreign file contents = %q, want %q", got, contents)
		}
	})

	t.Run("directory", func(t *testing.T) {
		path := muxTestPath(t)
		if err := os.MkdirAll(path, 0o700); err != nil {
			t.Fatalf("mkdir foreign directory: %v", err)
		}
		listener, err := ListenMux(path)
		if listener != nil {
			_ = listener.Close()
		}
		if !errors.Is(err, ErrMuxForeignPath) {
			t.Fatalf("ListenMux error = %v, want ErrMuxForeignPath", err)
		}
		if info, statErr := os.Stat(path); statErr != nil || !info.IsDir() {
			t.Fatalf("foreign directory was removed or changed: info=%v err=%v", info, statErr)
		}
	})
}

// TestMuxPathValidation proves invalid caller paths are refused before any
// filesystem change.
func TestMuxPathValidation(t *testing.T) {
	tests := []struct {
		name string
		path string
	}{
		{name: "empty", path: ""},
		{name: "relative", path: "mux.sock"},
		{name: "unclean", path: "/tmp/a/../mux.sock"},
		{name: "root", path: "/"},
		{name: "too long", path: "/" + string(make([]byte, muxSocketPathMax))},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := ListenMux(tt.path); !errors.Is(err, ErrMuxPath) {
				t.Fatalf("ListenMux(%q) error = %v, want ErrMuxPath", tt.path, err)
			}
			if _, err := DialMuxContext(context.Background(), tt.path); !errors.Is(err, ErrMuxPath) {
				t.Fatalf("DialMuxContext(%q) error = %v, want ErrMuxPath", tt.path, err)
			}
		})
	}
}

// TestListenMuxCloseUnblocksAcceptAndCleansSocket proves Close unblocks a
// blocked Accept, unlinks the socket it created, refuses later dials, and is
// idempotent.
func TestListenMuxCloseUnblocksAcceptAndCleansSocket(t *testing.T) {
	path := muxTestPath(t)
	listener := mustListenMux(t, path)

	acceptErr := make(chan error, 1)
	go func() {
		transport, err := listener.Accept()
		if transport != nil {
			_ = transport.Close()
		}
		acceptErr <- err
	}()

	if err := listener.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	select {
	case err := <-acceptErr:
		if err == nil {
			t.Fatal("Accept returned nil error after Close")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Close did not unblock Accept")
	}

	if _, err := os.Lstat(path); !os.IsNotExist(err) {
		t.Fatalf("socket still present after Close: %v", err)
	}
	if transport, err := DialMuxContext(context.Background(), path); err == nil {
		_ = transport.Close()
		t.Fatal("DialMuxContext succeeded after Close")
	}
	if err := listener.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
}

// TestDialMuxCancellationAndCloseUnblocksIO proves a canceled dial fails
// promptly and that closing one end unblocks a blocked read on the other.
func TestDialMuxCancellationAndCloseUnblocksIO(t *testing.T) {
	t.Run("canceled context", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if transport, err := DialMuxContext(ctx, muxTestPath(t)); !errors.Is(err, context.Canceled) {
			if transport != nil {
				_ = transport.Close()
			}
			t.Fatalf("DialMuxContext error = %v, want context.Canceled", err)
		}
	})

	t.Run("close unblocks read", func(t *testing.T) {
		path := muxTestPath(t)
		listener := mustListenMux(t, path)

		accepted := make(chan wire.BoundedTransport, 1)
		go func() {
			transport, _ := listener.Accept()
			accepted <- transport
		}()
		client := awaitMuxDial(t, dialMuxAsync(t, context.Background(), path))
		server := <-accepted
		if server == nil {
			t.Fatal("Accept did not return a transport")
		}

		readErr := make(chan error, 1)
		go func() {
			_, err := client.Recv()
			readErr <- err
		}()
		if err := server.Close(); err != nil {
			t.Fatalf("server Close: %v", err)
		}
		select {
		case err := <-readErr:
			if err == nil {
				t.Fatal("blocked Recv returned nil error after peer Close")
			}
		case <-time.After(5 * time.Second):
			t.Fatal("peer Close did not unblock Recv")
		}
	})
}

// TestListenMuxConcurrentCloseAndDial proves a Close racing many dials neither
// panics nor hangs, and every dial result is closed by the test.
func TestListenMuxConcurrentCloseAndDial(t *testing.T) {
	path := muxTestPath(t)
	listener := mustListenMux(t, path)

	const dialers = 16
	results := make(chan wire.BoundedTransport, dialers)
	var wg sync.WaitGroup
	for i := 0; i < dialers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			transport, err := DialMuxContext(context.Background(), path)
			if err != nil {
				results <- nil
				return
			}
			results <- transport
		}()
	}
	if err := listener.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	wg.Wait()
	close(results)
	for transport := range results {
		if transport != nil {
			_ = transport.Close()
		}
	}
}
