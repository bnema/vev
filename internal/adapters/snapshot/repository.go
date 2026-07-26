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
	dir string
	log *slog.Logger

	// sessionStateMu owns locks. A caller retains a reference before it waits
	// on a session mutex, so an idle entry can never be removed while a waiter
	// still holds its old mutex.
	sessionStateMu sync.Mutex
	locks          map[string]*sessionMutex
}

var _ ports.SnapshotRepository = (*Repository)(nil)

// NewRepository creates a repository rooted at dir with the default logger. It
// does not create files until the first publication, so construction is side-effect free.
func NewRepository(dir string) *Repository {
	return NewRepositoryWithLogger(dir, slog.Default())
}

// NewRepositoryWithLogger creates a repository rooted at dir with log.
func NewRepositoryWithLogger(dir string, log *slog.Logger) *Repository {
	if log == nil {
		log = slog.Default()
	}
	return &Repository{
		dir:   dir,
		log:   log,
		locks: make(map[string]*sessionMutex),
	}
}

// sessionMutex is retained by each waiter. That reference makes eviction
// safe: a new caller cannot lock a replacement mutex until all users of the
// prior mutex have left it.
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
	lock.mu.Lock()
	return lock
}

func (r *Repository) unlockSession(lock *sessionMutex) {
	lock.mu.Unlock()
	r.releaseSessionReference(lock)
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
	}
	r.sessionStateMu.Unlock()
}
