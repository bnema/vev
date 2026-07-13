package ipc

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/bnema/vev/internal/ports"
)

type dialTestObserver struct{}

func (dialTestObserver) ObserveRuntime(ports.RuntimeMark) {}

func TestDialContextCanceledOrExpired(t *testing.T) {
	tests := []struct {
		name string
		ctx  func() (context.Context, context.CancelFunc)
		want error
	}{
		{
			name: "canceled",
			ctx: func() (context.Context, context.CancelFunc) {
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				return ctx, func() {}
			},
			want: context.Canceled,
		},
		{
			name: "deadline exceeded",
			ctx: func() (context.Context, context.CancelFunc) {
				return context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
			},
			want: context.DeadlineExceeded,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := tt.ctx()
			defer cancel()

			transport, err := DialContext(ctx, t.TempDir())
			if transport != nil {
				t.Fatal("DialContext returned a transport for a completed context")
			}
			if !errors.Is(err, tt.want) {
				t.Fatalf("DialContext error = %v, want wrapping %v", err, tt.want)
			}
		})
	}
}

func TestDialContextPreservesOptions(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "vev")
	listener, err := Listen(dir)
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	defer func() { _ = listener.Close() }()

	observer := dialTestObserver{}
	transport, err := DialContext(context.Background(), dir, WithRuntimeObserver(observer))
	if err != nil {
		t.Fatalf("DialContext: %v", err)
	}
	defer func() { _ = transport.Close() }()

	got, ok := transport.(*unixTransport)
	if !ok {
		t.Fatalf("DialContext transport = %T, want *unixTransport", transport)
	}
	if got.observer != observer {
		t.Fatal("DialContext did not apply runtime observer option")
	}
}
