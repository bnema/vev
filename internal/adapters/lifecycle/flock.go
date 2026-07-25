// Package lifecycle provides exclusive process ownership of vev's durable
// state for the complete daemon lifetime.
package lifecycle

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"github.com/bnema/vev/pkg/safedir"
)

// ErrBusy reports that another process currently owns durable session state.
var ErrBusy = errors.New("durable session state is busy")

// Owner holds the lifecycle lock descriptor open until Release.
type Owner struct {
	file *os.File
	path string
	once sync.Once
	err  error
}

// Path returns the fixed lifecycle lock path in runtimeDir.
func Path(runtimeDir string) string {
	return filepath.Join(runtimeDir, "lifecycle.lock")
}

// TryAcquire attempts to take lifecycle ownership without waiting.
func TryAcquire(runtimeDir string) (*Owner, error) {
	if err := safedir.EnsurePrivate(runtimeDir); err != nil {
		return nil, fmt.Errorf("lifecycle: securing runtime directory: %w", err)
	}

	path := Path(runtimeDir)
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, fmt.Errorf("lifecycle: opening %s: %w", path, err)
	}
	if err := syscall.Flock(int(file.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		closeErr := file.Close()
		if errors.Is(err, syscall.EWOULDBLOCK) || errors.Is(err, syscall.EAGAIN) {
			return nil, errors.Join(ErrBusy, closeErr)
		}
		return nil, errors.Join(fmt.Errorf("lifecycle: locking %s: %w", path, err), closeErr)
	}
	return &Owner{file: file, path: path}, nil
}

// Acquire waits until lifecycle ownership is available or ctx is cancelled.
// Only lock contention is retried; filesystem and lock errors fail closed.
func Acquire(ctx context.Context, runtimeDir string, retry time.Duration) (*Owner, error) {
	if retry <= 0 {
		retry = time.Millisecond
	}
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		owner, err := TryAcquire(runtimeDir)
		if err == nil {
			return owner, nil
		}
		if !errors.Is(err, ErrBusy) {
			return nil, err
		}

		timer := time.NewTimer(retry)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		case <-timer.C:
		}
	}
}

// Release unlocks and closes the owner exactly once.
func (o *Owner) Release() error {
	if o == nil {
		return nil
	}
	o.once.Do(func() {
		if o.file == nil {
			return
		}
		unlockErr := syscall.Flock(int(o.file.Fd()), syscall.LOCK_UN)
		closeErr := o.file.Close()
		if unlockErr != nil {
			unlockErr = fmt.Errorf("lifecycle: unlocking %s: %w", o.path, unlockErr)
		}
		if closeErr != nil {
			closeErr = fmt.Errorf("lifecycle: closing %s: %w", o.path, closeErr)
		}
		o.err = errors.Join(unlockErr, closeErr)
	})
	return o.err
}
