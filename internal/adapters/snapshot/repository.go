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
	hooks repositoryHooks

	// sessionStateMu owns both maps. A caller retains a reference before it
	// waits on a session mutex, so an idle entry can never be removed while a
	// waiter still holds its old mutex. Epoch entries share that lifetime.
	sessionStateMu sync.Mutex
	locks          map[string]*sessionMutex
	storageEpochs  map[string]uint64
	nextEpoch      uint64

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
	beforeBlobWrite     func(string) error
	beforeManifestWrite func(string) error
	// Object hooks instrument the publication path in package tests. They keep
	// the steady-state cost of retained history observable without exposing a
	// production metrics surface.
	beforeObjectRead             func(string)
	beforeObjectHash             func([]byte)
	beforeObjectCopy             func([]byte)
	beforeHeadWrite              func(string) error
	beforePendingQuarantineCheck func(string)
	createTemp                   func(string) error
	writeTemp                    func(string) error
	syncFile                     func(string) error
	closeFile                    func(string) error
	installImmutable             func(string) error
	rename                       func(string) error
	syncDirectory                func(string) error
	remove                       func(string) error
	openMaintenanceDirectory     func(string) (maintenanceDirectory, error)
	openLegacyDirectory          func(string) (legacyDirectory, error)
	beforeSessionLock            func(string)
}

var _ ports.SnapshotRepository = (*Repository)(nil)
var _ ports.LegacySnapshotSource = (*Repository)(nil)

// NewRepository creates a repository rooted at dir. It does not create files
// until the first publication, so merely constructing it is side-effect free.
func NewRepository(dir string) *Repository {
	return &Repository{
		dir:           dir,
		locks:         make(map[string]*sessionMutex),
		storageEpochs: make(map[string]uint64),
	}
}

// invalidateStorageEpoch must be called with lockSession(key) held. The
// registry mutex makes accesses for different session keys race-free too.
func (r *Repository) invalidateStorageEpoch(key string) {
	r.sessionStateMu.Lock()
	r.nextEpoch++
	r.storageEpochs[key] = r.nextEpoch
	r.sessionStateMu.Unlock()
}

// storageEpoch must be called with lockSession(key) held.
func (r *Repository) storageEpoch(key string) uint64 {
	r.sessionStateMu.Lock()
	epoch := r.storageEpochs[key]
	r.sessionStateMu.Unlock()
	return epoch
}

// sessionMutex is retained by each waiter and each resumable maintenance
// state. That reference is what makes eviction safe: a new caller cannot lock
// a replacement mutex until all users of the prior mutex have left it.
type sessionMutex struct {
	key        string
	mu         sync.Mutex
	references int
}

func (r *Repository) lockSession(key string) *sessionMutex {
	r.sessionStateMu.Lock()
	lock := r.locks[key]
	if lock == nil {
		lock = &sessionMutex{key: key}
		r.locks[key] = lock
	}
	lock.references++
	r.sessionStateMu.Unlock()
	if hook := r.hooks.beforeSessionLock; hook != nil {
		hook(key)
	}
	lock.mu.Lock()
	return lock
}

func (r *Repository) unlockSession(lock *sessionMutex) {
	lock.mu.Unlock()
	r.releaseSessionReference(lock)
}

func (r *Repository) retainSessionState(key string) *sessionMutex {
	r.sessionStateMu.Lock()
	lock := r.locks[key]
	// sessionMaintenanceState is called while the active session lock is held.
	if lock == nil {
		panic("snapshot: retain missing session lock")
	}
	lock.references++
	r.sessionStateMu.Unlock()
	return lock
}

func (r *Repository) releaseSessionReference(lock *sessionMutex) {
	r.sessionStateMu.Lock()
	lock.references--
	if lock.references < 0 {
		r.sessionStateMu.Unlock()
		panic("snapshot: release of unretained session lock")
	}
	if lock.references == 0 {
		if r.locks[lock.key] != lock {
			r.sessionStateMu.Unlock()
			panic("snapshot: session lock registry mismatch")
		}
		delete(r.locks, lock.key)
		delete(r.storageEpochs, lock.key)
	}
	r.sessionStateMu.Unlock()
}

// sessionStateCounts is test evidence that idle per-session state is evicted.
func (r *Repository) sessionStateCounts() (locks, epochs int) {
	r.sessionStateMu.Lock()
	defer r.sessionStateMu.Unlock()
	return len(r.locks), len(r.storageEpochs)
}
