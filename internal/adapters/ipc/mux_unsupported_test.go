//go:build !linux && !darwin

package ipc

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"
)

// TestMuxFailsClosedWithoutPeerCredentialCheck proves the private mux carriage
// fails closed on a platform or build without the Linux SO_PEERCRED same-user
// check: a bound listener refuses every peer instead of admitting an unverified
// one, and dialing never returns an unverified transport.
func TestMuxFailsClosedWithoutPeerCredentialCheck(t *testing.T) {
	path := filepath.Join(shortSocketDir(t, "vev"), "mux.sock")
	listener, err := ListenMux(path)
	if err != nil {
		t.Fatalf("ListenMux: %v", err)
	}
	defer func() { _ = listener.Close() }()

	type dialOutcome struct {
		transport interface{ Close() error }
		err       error
	}
	dialed := make(chan dialOutcome, 1)
	go func() {
		transport, dialErr := DialMuxContext(context.Background(), path)
		if transport == nil {
			dialed <- dialOutcome{err: dialErr}
			return
		}
		dialed <- dialOutcome{transport: transport, err: dialErr}
	}()

	transport, acceptErr := listener.Accept()
	if transport != nil {
		_ = transport.Close()
		t.Fatal("Accept returned an unverified transport")
	}
	if !errors.Is(acceptErr, ErrMuxUnsupported) {
		t.Fatalf("Accept error = %v, want ErrMuxUnsupported", acceptErr)
	}

	select {
	case outcome := <-dialed:
		if outcome.transport != nil {
			_ = outcome.transport.Close()
			t.Fatal("DialMuxContext returned an unverified transport")
		}
		if !errors.Is(outcome.err, ErrMuxUnsupported) {
			t.Fatalf("DialMuxContext error = %v, want ErrMuxUnsupported", outcome.err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("DialMuxContext did not return")
	}
}
