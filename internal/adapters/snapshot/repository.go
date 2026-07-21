package snapshot

import (
	"context"
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
