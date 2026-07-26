package snapshot

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"syscall"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/pkg/safedir"
)

func (r *Repository) ensureSession(id domain.IncarnationID) error {
	sessionPath := r.sessionPath(id)
	for _, directory := range []struct {
		path  string
		phase string
	}{
		{r.dir, "repository"},
		{filepath.Join(r.dir, repositorySessionsDir), "sessions"},
		{sessionPath, "session"},
		{filepath.Join(sessionPath, repositoryObjectsDir), "objects"},
		{filepath.Join(sessionPath, repositoryGenerations), "generations"},
	} {
		if err := r.ensurePrivateDirectoryPhase(directory.path, directory.phase); err != nil {
			return fmt.Errorf("create snapshot repository directory: %w", err)
		}
	}
	return nil
}

func (r *Repository) ensurePrivateDirectory(dir string) error {
	return r.ensurePrivateDirectoryPhase(dir, "snapshot directory")
}

func (r *Repository) ensurePrivateDirectoryPhase(dir, phase string) (err error) {
	// The configured root is the trust boundary. Once it is private, every
	// descendant is created through a pinned parent descriptor rather than via
	// a path that an attacker can replace between checks.
	if filepath.Clean(dir) == filepath.Clean(r.dir) {
		_, statErr := os.Lstat(dir)
		created := errors.Is(statErr, os.ErrNotExist)
		if statErr != nil && !created {
			return statErr
		}
		if err := safedir.EnsurePrivate(dir); err != nil {
			return err
		}
		if created {
			if hook := r.hooks.syncDirectory; hook != nil {
				if err := hook(filepath.Dir(dir)); err != nil {
					return fmt.Errorf("%s parent directory sync: %w", phase, err)
				}
			}
			if err := syncDirectory(filepath.Dir(dir)); err != nil {
				return fmt.Errorf("%s parent directory sync: %w", phase, err)
			}
		}
		return nil
	}
	if err := r.ensurePrivateDirectoryPhase(filepath.Dir(dir), "snapshot directory"); err != nil {
		return err
	}
	rel, ok := r.repositoryRelative(dir)
	if !ok {
		return fmt.Errorf("snapshot path outside repository")
	}
	root, err := r.openRoot()
	if err != nil {
		return err
	}
	defer func() { joinCloseError(&err, "close snapshot root", r.closeRoot(root)) }()
	created := false
	mkdirErr := root.Mkdir(rel, 0o700)
	if mkdirErr == nil {
		created = true
	}
	if mkdirErr != nil && !errors.Is(mkdirErr, os.ErrExist) {
		return mkdirErr
	}
	fi, err := root.Lstat(rel)
	if err != nil {
		return err
	}
	if !fi.IsDir() {
		return fmt.Errorf("snapshot directory is not a directory")
	}
	if !privateDirectory(fi) {
		return fmt.Errorf("snapshot directory is not private")
	}
	if created {
		if err := r.syncDirectory(filepath.Dir(dir)); err != nil {
			return fmt.Errorf("%s parent directory sync: %w", phase, err)
		}
	}
	return nil
}

// writeImmutable publishes data with link(2), whose EEXIST behavior prevents a
// raced target from being overwritten. A competing target is accepted only
// after verifier reads it through the same secure descriptor path as Load.
func (r *Repository) writeImmutable(path string, data []byte, verifier func([]byte) error) error {
	dir := filepath.Dir(path)
	phase := "object shard"
	if filepath.Base(dir) == repositoryGenerations {
		phase = "generation"
	}
	if err := r.ensurePrivateDirectoryPhase(dir, phase); err != nil {
		return err
	}
	return r.withAtomicTemp(dir, data, func(tempPath string) (bool, error) {
		if err := r.installImmutable(tempPath, path); err != nil {
			if !errors.Is(err, os.ErrExist) {
				return true, err
			}
			existing, readErr := r.readBounded(path)
			if readErr != nil {
				return true, readErr
			}
			return true, verifier(existing)
		}
		// link retains the temporary name, so cleanup must remove and sync it.
		return true, nil
	})
}

// writeMutable is used only for HEAD, the sole authoritative pointer allowed
// to advance. The old or fully synced new HEAD is therefore always recoverable.
func (r *Repository) writeMutable(path string, data []byte) error {
	dir := filepath.Dir(path)
	if err := r.ensurePrivateDirectory(dir); err != nil {
		return err
	}
	return r.withAtomicTemp(dir, data, func(tempPath string) (bool, error) {
		if err := r.rename(tempPath, path); err != nil {
			return true, err
		}
		// rename consumed tempPath. Its directory sync persists both rename and
		// the absence of the temporary entry.
		if err := r.syncDirectory(dir); err != nil {
			return true, err
		}
		return false, nil
	})
}

// withAtomicTemp prepares a private, durable temporary file, invokes publish,
// then closes and removes the temporary entry when publish reports that its
// operation did not consume it. Cleanup errors are joined after the operation
// error in the same order as the former immutable and mutable write paths.
func (r *Repository) withAtomicTemp(dir string, data []byte, publish func(string) (removeTemp bool, err error)) error {
	temp, err := r.createTemp(dir)
	if err != nil {
		return err
	}
	tempPath := temp.Name()
	closed := false
	cleanup := func(cause error) error {
		if !closed {
			if err := r.closeFile(temp); err != nil {
				cause = errors.Join(cause, fmt.Errorf("close snapshot temporary file: %w", err))
			}
			closed = true
		}
		if err := r.remove(tempPath); err != nil && !errors.Is(err, os.ErrNotExist) {
			cause = errors.Join(cause, err)
		} else if err == nil {
			cause = errors.Join(cause, r.syncDirectory(dir))
		}
		return cause
	}
	if err := temp.Chmod(0o600); err != nil {
		return cleanup(err)
	}
	if err := r.writeFile(temp, data); err != nil {
		return cleanup(err)
	}
	if err := r.syncFile(temp); err != nil {
		return cleanup(err)
	}
	if err := r.closeFile(temp); err != nil {
		closed = true
		return cleanup(fmt.Errorf("close snapshot temporary file: %w", err))
	}
	closed = true
	removeTemp, err := publish(tempPath)
	if !removeTemp {
		return err
	}
	return cleanup(err)
}

func (r *Repository) createTemp(dir string) (*os.File, error) {
	if r.hooks.createTemp != nil {
		if err := r.hooks.createTemp(dir); err != nil {
			return nil, err
		}
	}
	return r.createTempAt(dir)
}
func (r *Repository) writeFile(f *os.File, data []byte) error {
	if r.hooks.writeTemp != nil {
		if err := r.hooks.writeTemp(f.Name()); err != nil {
			return err
		}
	}
	_, err := f.Write(data)
	return err
}
func (r *Repository) syncFile(f *os.File) error {
	if r.hooks.syncFile != nil {
		if err := r.hooks.syncFile(f.Name()); err != nil {
			return err
		}
	}
	return f.Sync()
}
func (r *Repository) closeFile(f *os.File) error {
	var injected error
	if r.hooks.closeFile != nil {
		injected = r.hooks.closeFile(f.Name())
	}
	return errors.Join(injected, f.Close())
}

// joinCloseError retains a primary operation failure and appends contextual
// close failures, including cleanup-only paths, without suppressing either.
func joinCloseError(primary *error, operation string, closeErr error) {
	if closeErr != nil {
		*primary = errors.Join(*primary, fmt.Errorf("%s: %w", operation, closeErr))
	}
}

func (r *Repository) installImmutable(oldPath, newPath string) (err error) {
	if r.hooks.installImmutable != nil {
		if err := r.hooks.installImmutable(newPath); err != nil {
			return err
		}
	}
	oldRel, ok := r.repositoryRelative(oldPath)
	if !ok {
		return fmt.Errorf("snapshot path outside repository")
	}
	newRel, ok := r.repositoryRelative(newPath)
	if !ok {
		return fmt.Errorf("snapshot path outside repository")
	}
	root, err := r.openRoot()
	if err != nil {
		return err
	}
	defer func() { joinCloseError(&err, "close snapshot root", r.closeRoot(root)) }()
	return root.Link(oldRel, newRel)
}
func (r *Repository) rename(oldPath, newPath string) (err error) {
	if r.hooks.rename != nil {
		if err := r.hooks.rename(newPath); err != nil {
			return err
		}
	}
	oldRel, ok := r.repositoryRelative(oldPath)
	if !ok {
		return fmt.Errorf("snapshot path outside repository")
	}
	newRel, ok := r.repositoryRelative(newPath)
	if !ok {
		return fmt.Errorf("snapshot path outside repository")
	}
	root, err := r.openRoot()
	if err != nil {
		return err
	}
	defer func() { joinCloseError(&err, "close snapshot root", r.closeRoot(root)) }()
	return root.Rename(oldRel, newRel)
}
func (r *Repository) remove(path string) (err error) {
	if r.hooks.remove != nil {
		if err := r.hooks.remove(path); err != nil {
			return err
		}
	}
	rel, ok := r.repositoryRelative(path)
	if !ok {
		return fmt.Errorf("snapshot path outside repository")
	}
	root, err := r.openRoot()
	if err != nil {
		return err
	}
	defer func() { joinCloseError(&err, "close snapshot root", r.closeRoot(root)) }()
	return root.Remove(rel)
}
func (r *Repository) syncDirectory(dir string) error {
	if r.hooks.syncDirectory != nil {
		if err := r.hooks.syncDirectory(dir); err != nil {
			return err
		}
	}
	return syncDirectory(dir)
}
func syncDirectory(dir string) error {
	f, err := os.Open(dir)
	if err != nil {
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	return f.Close()
}

func (r *Repository) readOptionalBounded(path string) ([]byte, bool, error) {
	data, err := r.readBounded(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, false, nil
	}
	return data, err == nil, err
}

// readBounded rejects a final symlink explicitly (rejectFinalSymlink) before
// opening the component under the confined root, validates the opened
// descriptor, then reads a hard bounded amount. It deliberately never trusts
// path metadata obtained before opening the descriptor.
func (r *Repository) readBounded(path string) (data []byte, err error) {
	root, err := r.openRoot()
	if err != nil {
		return nil, err
	}
	defer func() { joinCloseError(&err, "close snapshot root", r.closeRoot(root)) }()
	return r.readBoundedRoot(root, path)
}

func (r *Repository) readBoundedRoot(root *os.Root, path string) ([]byte, error) {
	rel, ok := r.repositoryRelative(path)
	if !ok {
		return nil, fmt.Errorf("snapshot path outside repository")
	}
	if err := rejectFinalSymlink(root, rel); err != nil {
		return nil, err
	}
	f, err := root.OpenFile(rel, syscall.O_RDONLY|syscall.O_NONBLOCK, 0)
	if err != nil {
		return nil, err
	}
	if hook := r.hooks.beforePayloadRead; hook != nil {
		hook(path)
	}
	return readBoundedFile(f)
}

// readBounded remains for focused descriptor-hardening tests. Repository code
// must use r.readBounded so every path stays confined to the pinned private root.
func readBounded(path string) ([]byte, error) {
	fd, err := syscall.Open(path, syscall.O_RDONLY|syscall.O_CLOEXEC|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
	if err != nil {
		return nil, err
	}
	f := os.NewFile(uintptr(fd), path)
	if f == nil {
		_ = syscall.Close(fd)
		return nil, fmt.Errorf("open snapshot file")
	}
	return readBoundedFile(f)
}

func readBoundedFile(f *os.File) ([]byte, error) {
	fd := int(f.Fd())
	var stat syscall.Stat_t
	if err := syscall.Fstat(fd, &stat); err != nil {
		_ = f.Close()
		return nil, err
	}
	if stat.Mode&syscall.S_IFMT != syscall.S_IFREG {
		_ = f.Close()
		return nil, fmt.Errorf("not a regular file")
	}
	if int(stat.Uid) != os.Geteuid() {
		_ = f.Close()
		return nil, fmt.Errorf("snapshot file is not owned by effective uid")
	}
	if stat.Mode&0o077 != 0 {
		_ = f.Close()
		return nil, fmt.Errorf("snapshot file has unsafe permissions")
	}
	if stat.Size < 0 || stat.Size > int64(maxRepositoryRead) {
		_ = f.Close()
		return nil, fmt.Errorf("snapshot file too large")
	}
	data, readErr := io.ReadAll(io.LimitReader(f, int64(maxRepositoryRead)+1))
	closeErr := f.Close()
	if readErr != nil {
		return nil, readErr
	}
	if closeErr != nil {
		return nil, closeErr
	}
	if len(data) > maxRepositoryRead {
		return nil, fmt.Errorf("snapshot file too large")
	}
	return data, nil
}
