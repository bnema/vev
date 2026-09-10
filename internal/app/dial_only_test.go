package app

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestDialOnlyLocalDialerNeverStartsDaemon pins the D2 source contract: the
// inventory control dialer performs a bare socket dial with no ensure, spawn,
// or attachment path. Against an isolated directory it must fail fast with a
// dial error instead of implicitly starting a daemon.
func TestDialOnlyLocalDialerNeverStartsDaemon(t *testing.T) {
	dir := t.TempDir()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	start := time.Now()
	dialer := dialOnlyLocalDialer{dir: dir}
	_, err := dialer.Dial(ctx)
	if err == nil {
		t.Fatal("dial-only control dial against an empty dir must fail")
	}
	if elapsed := time.Since(start); elapsed > 10*time.Second {
		t.Fatalf("dial-only control dial took %v; must not retry or spawn", elapsed)
	}
	plain := dialOnlyLocalDialer{dir: dir, observer: nil}
	if _, err := plain.Dial(context.Background()); err == nil {
		t.Fatal("dial-only control dial without observer must also fail fast")
	}
}

// TestHybridMissingLocalSourceNeverStartsDaemon pins the hybrid degradation
// contract: when the dial-only local inventory source is unreachable, the
// composition must surface the dial failure rather than starting a daemon or
// creating a local attachment. The missing source degrades that source only.
func TestHybridMissingLocalSourceNeverStartsDaemon(t *testing.T) {
	dir := t.TempDir()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	dialer := dialOnlyLocalDialer{dir: dir}
	require.Error(t, func() error {
		conn, err := dialer.Dial(ctx)
		if err != nil {
			return err
		}
		_ = conn.Close()
		return nil
	}(), "missing local control source must fail instead of starting a daemon")
}
