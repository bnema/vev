package app

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/adapters/lifecycle"
	"github.com/bnema/vev/pkg/safedir"
)

func TestWaitForTargetOrLifecycleNamesAHeldLock(t *testing.T) {
	cfg := backoffConfig{initial: time.Millisecond, max: 2 * time.Millisecond, total: 20 * time.Millisecond}
	refuse := func(context.Context) (struct{}, error) { return struct{}{}, errors.New("no carriage") }
	tests := []struct {
		name     string
		held     bool
		wantHeld bool
	}{
		{name: "lock held by a silent owner", held: true, wantHeld: true},
		{name: "free lock is acquired", held: false, wantHeld: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir() + "/runtime"
			require.NoError(t, safedir.EnsurePrivate(dir))
			if tt.held {
				owner, err := lifecycle.TryAcquire(dir)
				require.NoError(t, err)
				t.Cleanup(func() { _ = owner.Release() })
			}

			_, owner, err := waitForTargetOrLifecycle(context.Background(), dir, refuse, cfg)

			if tt.wantHeld {
				require.ErrorIs(t, err, ErrLifecycleHeldByOther)
				require.ErrorIs(t, err, ErrDaemonUnreachable)
				return
			}
			require.NoError(t, err)
			require.NoError(t, owner.Release())
		})
	}
}
