package app

import (
	"context"
	"testing"
	"time"
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
