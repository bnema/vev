package broker

import (
	"sync"

	"github.com/bnema/vev/internal/ports"
)

// Snapshot returns a defensive immutable copy without I/O.
func (r *Registry) Snapshot() ports.BrokerSnapshot { return r.current.Load().Clone() }

// Subscribe returns a capacity-one notification stream. Slow consumers never
// block publication and always re-read the newest snapshot.
func (r *Registry) Subscribe() ports.BrokerSubscription {
	s := &subscription{changed: make(chan struct{}, 1)}
	s.close = func() {
		r.mu.Lock()
		delete(r.subs, s)
		r.mu.Unlock()
	}
	r.mu.Lock()
	r.subs[s] = struct{}{}
	r.mu.Unlock()
	s.offer()
	return s
}

// revisionExhaustedLocked reports whether this epoch has no revision left to
// publish, latching the exhausted state on first detection. Callers must hold
// r.mu.
func (r *Registry) revisionExhaustedLocked() bool {
	if r.revisionExhausted {
		return true
	}
	if r.revision != ^ports.BrokerRevision(0) {
		return false
	}
	r.revisionExhausted = true
	r.log.Error("broker: revision series exhausted; refusing further publication", "epoch", r.epoch)
	return true
}

func (r *Registry) publishLocked(persist bool) {
	// Fail closed on an exhausted series: a wrapped revision would be zero, and
	// a wrapped series would publish revisions below the newest one, either of
	// which silently freezes durable persistence. The last valid snapshot stays
	// published, nothing is stored, and no subscriber is woken for a publication
	// that did not happen.
	if r.revisionExhaustedLocked() {
		return
	}
	// Remote daemons are published in registration order. The broker's own
	// machine daemon, when a producer is configured, is the
	// prepended entry at index zero; without a producer the registry produces no
	// local observation and never invents one.
	daemons := make([]ports.BrokerDaemonObservation, 0, len(r.hosts)+1)
	if r.local != nil {
		daemons = append(daemons, r.localHost.Clone())
	}
	for _, endpoint := range r.order {
		host, ok := r.hosts[endpoint]
		if !ok {
			continue
		}
		daemons = append(daemons, host.Clone())
	}
	snapshot := ports.BrokerSnapshot{Epoch: r.epoch, Revision: r.revision + 1, Daemons: daemons, Removed: r.tombstonesLocked()}
	if err := snapshot.Validate(); err != nil {
		// Fail closed: a publication the wire refuses aborts every client
		// connection on subscribe, so an invalid projection is never published.
		// The last valid snapshot stays current, nothing is persisted, and the
		// revision series does not advance past a publication that never
		// happened.
		r.log.Error("broker: refused invalid snapshot publication", "revision", r.revision+1, "err", err)
		return
	}
	r.revision = snapshot.Revision
	r.current.Store(&snapshot)
	if persist {
		// enqueue only stages an immutable publication: rendering the durable
		// copy and every store call happen on the writer goroutine, never
		// under the registry lock and never on the probe schedule.
		r.store.enqueue(snapshot)
	}
	for sub := range r.subs {
		sub.offer()
	}
}

// durable returns the persistable copy of a publication, rendered by the
// writer goroutine with the shared rule (ports.BrokerSnapshot.DurableProjection).
// The registry may publish the process-local daemon, which is never durable,
// so it is dropped first; the store would refuse it otherwise. Restored
// observations regain policy from membership (see stampHostAuthority).
func durable(snapshot ports.BrokerSnapshot) ports.BrokerSnapshot {
	return snapshot.WithoutLocal().DurableProjection()
}

type subscription struct {
	changed chan struct{}
	once    sync.Once
	close   func()
}

func (s *subscription) Changed() <-chan struct{} { return s.changed }

func (s *subscription) Close() { s.once.Do(s.close) }

func (s *subscription) offer() {
	select {
	case s.changed <- struct{}{}:
	default:
	}
}
