package snapshot

import (
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
	// Legacy import is deliberately smaller than the per-file parser ceiling:
	// importing old snapshots must not accumulate an unbounded startup payload.
	maxLegacySnapshotFiles = 64
	maxLegacySnapshotBytes = 8 << 20
)

// Repository is the crash-safe, content-addressed session snapshot store.
// Store remains available separately for the one-way legacy bridge.
type Repository struct {
	dir   string
	locks sync.Map // map[string]*sync.Mutex
	hooks repositoryHooks

	// storageEpochs is guarded by each session's lock. It invalidates a
	// maintenance mark whenever publication or deletion changes that session's
	// on-disk namespace.
	storageEpochs map[string]uint64

	// maintenanceMu owns continuation state for bounded maintenance. Keeping
	// directory handles on the repository makes successive calls advance rather
	// than repeatedly reading the first batch of a large directory.
	maintenanceMu       sync.Mutex
	maintenanceCursors  map[string]*maintenanceCursor
	maintenanceSessions map[string]*sessionMaintenance

	// pendingLegacySync records an unlink whose root-directory sync failed.
	// It is keyed by the deterministic legacy filename and shares a per-file
	// lock with DeleteLegacy so a retry cannot acknowledge a recreated file.
	pendingLegacySync sync.Map // map[string]struct{}
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
func NewRepository(dir string) *Repository {
	return &Repository{dir: dir, storageEpochs: make(map[string]uint64)}
}

// invalidateStorageEpoch must be called with sessionLock(key) held.
func (r *Repository) invalidateStorageEpoch(key string) {
	r.storageEpochs[key]++
}

// storageEpoch must be called with sessionLock(key) held.
func (r *Repository) storageEpoch(key string) uint64 { return r.storageEpochs[key] }

func (r *Repository) sessionLock(key string) *sync.Mutex {
	lock, _ := r.locks.LoadOrStore(key, &sync.Mutex{})
	return lock.(*sync.Mutex)
}
