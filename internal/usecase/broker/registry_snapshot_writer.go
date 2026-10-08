package broker

import (
	"log/slog"
	"reflect"
	"sync"
	"time"

	"github.com/bnema/vev/internal/ports"
)

// snapshotWriter serializes durable snapshot writes off the registry lock.
// Enqueue never blocks on store I/O and always coalesces to the newest
// revision; Store calls run on one goroutine, so the durable store never sees
// an out-of-order write. A failed write is logged and retried by the next
// publication, so a store fault never stalls publication. Close flushes the
// newest pending snapshot, stops the writer, and waits, so no write outlives
// shutdown.
type snapshotWriter struct {
	store ports.BrokerSnapshotStore
	log   *slog.Logger

	mu      sync.Mutex
	pending *ports.BrokerSnapshot
	written ports.BrokerRevision
	// last is the durable content of the newest successful write with its
	// revision and observation timestamps cleared. Only the writer goroutine
	// touches it.
	last    *ports.BrokerSnapshot
	started bool
	closed  bool
	wake    chan struct{}
	stop    chan struct{}
	done    chan struct{}
}

func newSnapshotWriter(store ports.BrokerSnapshotStore, log *slog.Logger) *snapshotWriter {
	return &snapshotWriter{
		store: store, log: log,
		wake: make(chan struct{}, 1), stop: make(chan struct{}), done: make(chan struct{}),
	}
}

// enqueue stages a snapshot for durable persistence. It coalesces to the
// newest revision, starts the writer on first use, and never blocks on the
// store or copies session payloads. Enqueue after Close is refused: nothing is
// written after shutdown.
func (w *snapshotWriter) enqueue(snapshot ports.BrokerSnapshot) {
	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		w.log.Debug("broker: durable snapshot write refused after shutdown", "revision", snapshot.Revision)
		return
	}
	if w.pending != nil {
		if snapshot.Revision <= w.pending.Revision {
			w.mu.Unlock()
			return
		}
	} else if snapshot.Revision <= w.written {
		w.mu.Unlock()
		return
	}
	// The publication is immutable after publication, so staging it is a value
	// copy; the defensive durable copy is rendered on the writer goroutine.
	w.pending = &snapshot
	if !w.started {
		w.started = true
		go w.run()
	}
	w.mu.Unlock()
	select {
	case w.wake <- struct{}{}:
	default:
	}
}

func (w *snapshotWriter) run() {
	defer close(w.done)
	for {
		select {
		case <-w.stop:
			w.flush()
			return
		case <-w.wake:
			w.flush()
		}
	}
}

// flush writes the newest staged snapshot, skipping revisions the store
// already holds.
func (w *snapshotWriter) flush() {
	for {
		w.mu.Lock()
		snapshot := w.pending
		written := w.written
		w.pending = nil
		w.mu.Unlock()
		if snapshot == nil {
			return
		}
		if snapshot.Revision <= written {
			continue
		}
		next := durable(*snapshot)
		key := durableContent(next)
		if w.last != nil && reflect.DeepEqual(*w.last, key) {
			// Only observation timestamps moved: the stored copy already
			// carries the same durable state, so skip the rewrite and fsync.
			// A restart re-observes every host before trusting freshness.
			continue
		}
		if err := w.store.Store(next); err != nil {
			w.last = nil
			w.log.Error("broker: durable snapshot write failed", "revision", snapshot.Revision, "err", err)
			continue
		}
		w.last = &key
		w.mu.Lock()
		if snapshot.Revision > w.written {
			w.written = snapshot.Revision
		}
		w.mu.Unlock()
	}
}

// durableContent returns the durable projection without the fields that change
// on every routine observation: the revision and the attempt, success, and
// due timestamps.
func durableContent(snapshot ports.BrokerSnapshot) ports.BrokerSnapshot {
	out := snapshot
	out.Revision = 0
	out.Daemons = make([]ports.BrokerDaemonObservation, len(snapshot.Daemons))
	for i, daemon := range snapshot.Daemons {
		daemon.LastAttempt = time.Time{}
		daemon.LastSuccess = time.Time{}
		daemon.NextDue = time.Time{}
		out.Daemons[i] = daemon
	}
	return out
}

// close flushes the newest staged snapshot, stops the writer, and waits for
// the drain to finish. It never returns before the in-flight write completes.
func (w *snapshotWriter) close() {
	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		return
	}
	w.closed = true
	started := w.started
	w.mu.Unlock()
	if !started {
		return
	}
	close(w.stop)
	<-w.done
}
