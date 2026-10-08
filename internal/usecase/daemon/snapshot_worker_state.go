package daemon

import (
	"context"
	"sync"
)

// snapshotWorker owns the state of the repository snapshot worker and of the
// other durable writers joined with it (maintenance and restoration), so
// WaitDurableWriters can read all three completion signals under one lock.
//
// Lock position: mu is a leaf with respect to Daemon.mu. No code path acquires
// Daemon.mu, a session lock, or a pane lock while holding it, and capture,
// queue, and worker code never takes Daemon.mu. noticeMu is likewise a leaf and
// is never held together with mu.
//
// The zero value is usable for tests that build &Daemon{}: nil maps are
// allocated lazily by the enqueue paths, and a nil jobs channel never becomes
// ready in a select.
type snapshotWorker struct {
	// jobs is the bounded regular capture queue. It is never closed: producers
	// can race shutdown without risking a send-on-closed panic.
	jobs chan *snapshotCapture
	// wake wakes the repository scheduler when a session becomes dirty
	// or an attempt completes. It is never closed and producers only send
	// non-blockingly.
	wake chan struct{}

	// mu guards every field below it.
	mu sync.Mutex
	// admitted contains every capture accepted by either worker queue,
	// including captures buffered in jobs.
	admitted  map[*snapshotCapture]struct{}
	id        uint64
	ctx       context.Context
	cancel    context.CancelFunc
	done      chan struct{}
	flush     chan struct{}
	finalWake chan struct{}
	// finalJobs coalesces terminal captures by session when the bounded
	// regular queue is full. It retains at most snapshotFinalQueueCapacity named
	// sessions, each with only its newest terminal state while the worker blocks.
	finalJobs  map[*session]*snapshotCapture
	finalOrder []*session
	closing    bool
	inFlight   *snapshotCapture

	// maintenanceWorkerCancel/Done and restoreWorkerDone are the other durable
	// writers' ownership signals. They share mu with the snapshot worker and are
	// joined together in WaitDurableWriters; never split them. Startup
	// restoration reconciles durable checkpoints, so it is a durable writer.
	maintenanceWorkerCancel context.CancelFunc
	maintenanceWorkerDone   chan struct{}
	restoreWorkerDone       chan struct{}

	// noticeMu guards the active global persistence failure signature. It is
	// separate from mu so notice routing cannot block a producer or a
	// repository worker.
	noticeMu               sync.Mutex
	activeFailureSignature string
}
