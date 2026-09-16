//go:build unix

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
	file, err := openLockFile(path)
	if err != nil {
		return nil, err
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

// openLockFile opens the lock inode without following a symlink and refuses to
// trust anything that is not a regular, owner-only file.
//
// O_NOFOLLOW makes a symlinked lock path fail closed with ELOOP instead of
// flocking the link target, and the fstat check fails closed on a directory,
// FIFO, device, or a file with group/other access, so a path another user could
// plant can never become the lifecycle lock. Only group and other access is
// refused: an owner-only file is trusted regardless of the exact owner bit
// pattern, so a lock created under a restrictive umask (or normalized by a
// different tool) is never a permanent, unrecoverable startup failure. A newly
// created lock is additionally chmodded to 0600 so a permissive umask can never
// leave group/other bits behind. The failure is reported as an ordinary error,
// never ErrBusy.
func openLockFile(path string) (*os.File, error) {
	fd, err := syscall.Open(path, syscall.O_CREAT|syscall.O_EXCL|syscall.O_RDWR|syscall.O_CLOEXEC|syscall.O_NOFOLLOW, 0o600)
	created := err == nil
	if err != nil {
		if !errors.Is(err, syscall.EEXIST) {
			if errors.Is(err, syscall.ELOOP) {
				return nil, fmt.Errorf("lifecycle: %s is a symlink", path)
			}
			return nil, fmt.Errorf("lifecycle: opening %s: %w", path, err)
		}
		fd, err = syscall.Open(path, syscall.O_RDWR|syscall.O_CLOEXEC|syscall.O_NOFOLLOW, 0)
		if err != nil {
			if errors.Is(err, syscall.ELOOP) {
				return nil, fmt.Errorf("lifecycle: %s is a symlink", path)
			}
			return nil, fmt.Errorf("lifecycle: opening %s: %w", path, err)
		}
	}
	file := os.NewFile(uintptr(fd), path)
	if created {
		if err := syscall.Fchmod(fd, 0o600); err != nil {
			return nil, errors.Join(fmt.Errorf("lifecycle: chmod %s: %w", path, err), file.Close())
		}
	}
	info, err := file.Stat()
	if err != nil {
		return nil, errors.Join(fmt.Errorf("lifecycle: stat %s: %w", path, err), file.Close())
	}
	if !info.Mode().IsRegular() {
		return nil, errors.Join(fmt.Errorf("lifecycle: %s is not a regular file", path), file.Close())
	}
	if info.Mode().Perm()&0o077 != 0 {
		return nil, errors.Join(fmt.Errorf("lifecycle: %s has permissions %04o, want owner-only", path, info.Mode().Perm()), file.Close())
	}
	return file, nil
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
