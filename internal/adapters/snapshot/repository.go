package snapshot

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/bnema/vev/internal/ports"
)

const (
	repositorySessionsDir = "sessions"
	repositoryObjectsDir  = "objects"
	repositoryGenerations = "generations"
	repositoryHead        = "HEAD"
	generationWidth       = 20
	maxRepositoryRead     = maxSnapshotFileSize
)

// Repository is the crash-safe, content-addressed session snapshot store.
// Store remains available separately for the one-way legacy bridge.
type Repository struct {
	dir    string
	legacy *Store
	locks  sync.Map // map[string]*sync.Mutex
	hooks  repositoryHooks
}

// repositoryHooks makes each persistence boundary fault-injectable. Hooks run
// immediately before their respective syscall and are intentionally package
// private so production callers cannot weaken repository guarantees.
type repositoryHooks struct {
	beforeBlobWrite     func(string) error
	beforeManifestWrite func(string) error
	beforeHeadWrite     func(string) error
	createTemp          func(string) error
	writeTemp           func(string) error
	syncFile            func(string) error
	closeFile           func(string) error
	installImmutable    func(string) error
	rename              func(string) error
	syncDirectory       func(string) error
	remove              func(string) error
}

var _ ports.SnapshotRepository = (*Repository)(nil)
var _ ports.LegacySnapshotSource = (*Repository)(nil)

// NewRepository creates a repository rooted at dir. It does not create files
// until the first publication, so merely constructing it is side-effect free.
func NewRepository(dir string) *Repository { return &Repository{dir: dir, legacy: NewStore(dir)} }

func (r *Repository) sessionLock(key string) *sync.Mutex {
	lock, _ := r.locks.LoadOrStore(key, &sync.Mutex{})
	return lock.(*sync.Mutex)
}

func (r *Repository) Delete(ctx context.Context, name string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	key := sessionKey(name)
	lock := r.sessionLock(key)
	lock.Lock()
	defer lock.Unlock()

	path := r.sessionPath(key)
	if _, err := os.Lstat(path); errors.Is(err, os.ErrNotExist) {
		return nil
	} else if err != nil {
		return fmt.Errorf("stat snapshot session %q: %w", key, safeFilesystemError(err))
	}
	if err := os.RemoveAll(path); err != nil {
		return fmt.Errorf("remove snapshot session %q: %w", key, safeFilesystemError(err))
	}
	if err := r.syncDirectory(filepath.Join(r.dir, repositorySessionsDir)); err != nil {
		return fmt.Errorf("sync deleted snapshot session directory %q: %w", key, safeFilesystemError(err))
	}
	return nil
}

// Maintain removes abandoned temporary and quarantine files. Immutable data is
// deliberately never garbage-collected here because older generations need it.
func (r *Repository) Maintain(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return filepath.WalkDir(r.dir, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				return nil
			}
			return fmt.Errorf("walk snapshot maintenance path %q: %w", maintenancePath(r.dir, path), safeFilesystemError(err))
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if entry.IsDir() {
			return nil
		}
		base := entry.Name()
		if !strings.HasPrefix(base, ".tmp-") && !strings.HasPrefix(base, ".quarantine-") {
			return nil
		}
		if err := r.remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("remove snapshot maintenance file %q: %w", maintenancePath(r.dir, path), safeFilesystemError(err))
		}
		if err := r.syncDirectory(filepath.Dir(path)); err != nil {
			return fmt.Errorf("sync snapshot maintenance directory for %q: %w", maintenancePath(r.dir, path), safeFilesystemError(err))
		}
		return nil
	})
}

// maintenancePath avoids exposing the repository root while retaining a
// path-safe, actionable location in errors.
func maintenancePath(root, path string) string {
	rel, err := filepath.Rel(root, path)
	if err != nil || rel == "." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) || rel == ".." {
		return filepath.Base(path)
	}
	return rel
}

type safePathError struct {
	op    string
	cause error
}

func (e safePathError) Error() string { return e.op + ": " + e.cause.Error() }
func (e safePathError) Unwrap() error { return e.cause }

// safeFilesystemError preserves error matching without including a filesystem
// path from an os.PathError in terminal-facing error text.
func safeFilesystemError(err error) error {
	var pathError *os.PathError
	if errors.As(err, &pathError) {
		return safePathError{op: pathError.Op, cause: pathError.Err}
	}
	return err
}

func (r *Repository) LoadLegacy(ctx context.Context) ([]ports.LegacySnapshot, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	blobs, err := r.legacy.Load()
	if err != nil {
		return nil, err
	}
	out := make([]ports.LegacySnapshot, len(blobs))
	for i, blob := range blobs {
		out[i] = ports.LegacySnapshot{Name: blob.Name, Data: append([]byte(nil), blob.Data...)}
	}
	return out, nil
}

func (r *Repository) DeleteLegacy(ctx context.Context, name string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return r.legacy.Delete(name)
}
