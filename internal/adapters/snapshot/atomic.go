package snapshot

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"syscall"

	"github.com/bnema/vev/pkg/safedir"
)

func (r *Repository) ensureSession(key string) error {
	for _, directory := range []struct {
		path  string
		phase string
	}{
		{r.dir, "repository"},
		{filepath.Join(r.dir, repositorySessionsDir), "sessions"},
		{r.sessionPath(key), "session"},
		{filepath.Join(r.sessionPath(key), repositoryObjectsDir), "objects"},
		{filepath.Join(r.sessionPath(key), repositoryGenerations), "generations"},
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

func (r *Repository) ensurePrivateDirectoryPhase(dir, phase string) error {
	_, err := os.Lstat(dir)
	created := errors.Is(err, os.ErrNotExist)
	if err != nil && !created {
		return err
	}
	if created {
		parent := filepath.Dir(dir)
		if _, err := os.Lstat(parent); errors.Is(err, os.ErrNotExist) {
			if err := r.ensurePrivateDirectoryPhase(parent, "snapshot directory"); err != nil {
				return err
			}
		} else if err != nil {
			return err
		}
		if err := os.Mkdir(dir, 0o700); err != nil && !errors.Is(err, os.ErrExist) {
			return err
		}
	}
	if err := safedir.EnsurePrivate(dir); err != nil {
		return err
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
	temp, err := r.createTemp(dir)
	if err != nil {
		return err
	}
	tempPath := temp.Name()
	closed := false
	cleanup := func(cause error) error {
		if !closed {
			if err := r.closeFile(temp); err != nil {
				cause = errors.Join(cause, err)
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
		return cleanup(err)
	}
	closed = true
	if err := r.installImmutable(tempPath, path); err != nil {
		if errors.Is(err, os.ErrExist) {
			existing, readErr := readBounded(path)
			if readErr != nil {
				return cleanup(readErr)
			}
			if verifyErr := verifier(existing); verifyErr != nil {
				return cleanup(verifyErr)
			}
			return cleanup(nil)
		}
		return cleanup(err)
	}
	// Removing the linked temporary entry before the directory sync lets one
	// sync persist both the immutable install and its consumed temporary file.
	return cleanup(nil)
}

// writeMutable is used only for HEAD, the sole authoritative pointer allowed
// to advance. The old or fully synced new HEAD is therefore always recoverable.
func (r *Repository) writeMutable(path string, data []byte) error {
	dir := filepath.Dir(path)
	if err := r.ensurePrivateDirectory(dir); err != nil {
		return err
	}
	temp, err := r.createTemp(dir)
	if err != nil {
		return err
	}
	tempPath := temp.Name()
	closed := false
	cleanup := func(cause error) error {
		if !closed {
			if err := r.closeFile(temp); err != nil {
				cause = errors.Join(cause, err)
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
		return cleanup(err)
	}
	closed = true
	if err := r.rename(tempPath, path); err != nil {
		return cleanup(err)
	}
	if err := r.syncDirectory(dir); err != nil {
		return cleanup(err)
	}
	// rename consumed tempPath. Its directory sync above persists both rename
	// and the absence of the temporary entry.
	return nil
}

func (r *Repository) createTemp(dir string) (*os.File, error) {
	if r.hooks.createTemp != nil {
		if err := r.hooks.createTemp(dir); err != nil {
			return nil, err
		}
	}
	return os.CreateTemp(dir, ".tmp-")
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
	closeErr := f.Close()
	if injected != nil {
		return injected
	}
	return closeErr
}
func (r *Repository) installImmutable(oldPath, newPath string) error {
	if r.hooks.installImmutable != nil {
		if err := r.hooks.installImmutable(newPath); err != nil {
			return err
		}
	}
	return os.Link(oldPath, newPath)
}
func (r *Repository) rename(oldPath, newPath string) error {
	if r.hooks.rename != nil {
		if err := r.hooks.rename(newPath); err != nil {
			return err
		}
	}
	return os.Rename(oldPath, newPath)
}
func (r *Repository) remove(path string) error {
	if r.hooks.remove != nil {
		if err := r.hooks.remove(path); err != nil {
			return err
		}
	}
	return os.Remove(path)
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

func readOptionalBounded(path string) ([]byte, bool, error) {
	data, err := readBounded(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, false, nil
	}
	return data, err == nil, err
}

// readBounded opens the final component with O_NOFOLLOW, validates the opened
// descriptor, then reads a hard bounded amount. It deliberately never trusts
// path metadata obtained before opening the descriptor.
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

func equalBytes(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
