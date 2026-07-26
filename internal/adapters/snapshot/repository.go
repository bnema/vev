package snapshot

import (
	"log/slog"
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
type Repository struct {
	dir   string
	hooks repositoryHooks
	log   *slog.Logger

	// sessionStateMu owns both maps. A caller retains a reference before it
	// waits on a session mutex, so an idle entry can never be removed while a
	// waiter still holds its old mutex. Epoch entries share that lifetime.
	sessionStateMu sync.Mutex
	locks          map[string]*sessionMutex
	storageEpochs  map[string]uint64
	nextEpoch      uint64
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
	beforeObjectRead func(string)
	// beforePayloadRead observes every bounded repository payload read. It is
	// test-only accounting instrumentation and runs immediately before the read.
	beforePayloadRead func(string)
	beforeObjectHash  func([]byte)
	beforeObjectCopy  func([]byte)
	beforeHeadRead    func(string) error
	beforeHeadWrite   func(string) error
	createTemp        func(string) error
	writeTemp         func(string) error
	syncFile          func(string) error
	closeFile         func(string) error
	installImmutable  func(string) error
	rename            func(string) error
	syncDirectory     func(string) error
	remove            func(string) error
	// afterOpenRoot and closeRoot make descriptor race and close-error paths
	// deterministic in package tests. closeRoot never replaces the real close.
	afterOpenRoot       func()
	closeRoot           func() error
	beforeDirectoryRead func(string)
	beforeSessionLock   func(string)
}

var _ ports.SnapshotRepository = (*Repository)(nil)

// NewRepository creates a repository rooted at dir. It does not create files
// until the first publication, so merely constructing it is side-effect free.
func NewRepository(dir string, logs ...*slog.Logger) *Repository {
	log := slog.Default()
	if len(logs) > 0 && logs[0] != nil {
		log = logs[0]
	}
	return &Repository{
		dir:           dir,
		log:           log,
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
