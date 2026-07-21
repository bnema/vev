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
	locks sync.Map // map[string]*sessionMutex
	hooks repositoryHooks

	// storageEpochs is guarded by each session's lock. It invalidates a
	// maintenance mark whenever publication or deletion changes that session's
	// on-disk namespace.
	storageEpochs map[string]uint64

	// maintenanceMu owns bounded continuation metadata: seek cookies and
	// pending directory entries. Directory descriptors are opened and closed
	// for each maintenance call.
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
	beforeBlobWrite          func(string) error
	beforeManifestWrite      func(string) error
	beforeHeadWrite          func(string) error
	createTemp               func(string) error
	writeTemp                func(string) error
	syncFile                 func(string) error
	closeFile                func(string) error
	installImmutable         func(string) error
	rename                   func(string) error
	syncDirectory            func(string) error
	remove                   func(string) error
	openMaintenanceDirectory func(string) (maintenanceDirectory, error)
	beforeSessionLock        func(string)
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

// sessionMutex provides a test-only boundary immediately before a contested
// session mutex. Production hooks are nil, so normal locking is unchanged.
type sessionMutex struct {
	repository *Repository
	key        string
	mu         sync.Mutex
}

func (m *sessionMutex) Lock() {
	if hook := m.repository.hooks.beforeSessionLock; hook != nil {
		hook(m.key)
	}
	m.mu.Lock()
}

func (m *sessionMutex) Unlock() { m.mu.Unlock() }

func (r *Repository) sessionLock(key string) *sessionMutex {
	lock, _ := r.locks.LoadOrStore(key, &sessionMutex{repository: r, key: key})
	return lock.(*sessionMutex)
}
