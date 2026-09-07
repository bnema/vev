package ports

import (
	"time"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/protocol/catalogue"
)

// RemoteHostSnapshot is one ordered host projection from the remote directory.
// Sessions are cloned ordered catalogue values; the slice is never mutated
// after publication. Checking is independent of Availability: an in-flight
// observation preserves the last-known availability and inventory.
type RemoteHostSnapshot struct {
	Endpoint            string
	DisplayOrigin       string
	Rank                int
	Registration        domain.RemoteRegistration
	Availability        domain.RemoteAvailability
	Checking            bool
	LastAttempt         time.Time
	LastSuccess         time.Time
	NextDue             time.Time
	ConsecutiveFailures uint
	LastFailure         domain.RemoteFailure
	InventoryKnown      bool
	Sessions            []catalogue.RemoteCatalogSession
}

// Clone returns a defensive copy with an independent session slice.
func (s RemoteHostSnapshot) Clone() RemoteHostSnapshot {
	out := s
	out.Sessions = append([]catalogue.RemoteCatalogSession(nil), s.Sessions...)
	for i := range out.Sessions {
		out.Sessions[i].Tabs = append([]catalogue.RemoteCatalogTab(nil), out.Sessions[i].Tabs...)
	}
	return out
}

// RemoteDirectorySnapshot is an immutable, fully defensive publication of the
// remote directory. Nested slices are never mutated after publication.
type RemoteDirectorySnapshot struct {
	Revision      uint64
	Initialized   bool
	RegistryError error
	Hosts         []RemoteHostSnapshot
}

// Clone returns a defensive copy with independent host and session slices.
// RegistryError is shared: published errors are immutable values.
func (s RemoteDirectorySnapshot) Clone() RemoteDirectorySnapshot {
	out := s
	out.Hosts = make([]RemoteHostSnapshot, len(s.Hosts))
	for i, host := range s.Hosts {
		out.Hosts[i] = host.Clone()
	}
	return out
}

// Find returns the snapshot for an exact endpoint identity.
func (s RemoteDirectorySnapshot) Find(endpoint string) (RemoteHostSnapshot, bool) {
	for _, host := range s.Hosts {
		if host.Endpoint == endpoint {
			return host, true
		}
	}
	return RemoteHostSnapshot{}, false
}

// RemoteDirectorySubscription wakes one subscriber when a newer snapshot is
// available. Each subscriber owns a capacity-one channel and re-reads the
// newest snapshot on wake; channels are never shared between subscribers.
type RemoteDirectorySubscription interface {
	Changed() <-chan struct{}
	Close()
}

// RemoteDirectory is the read-only projection seam between the remote monitor
// and presentation. Snapshot and subscription access perform no I/O; request
// methods are non-blocking coalesced hints, never synchronous refreshes.
type RemoteDirectory interface {
	Snapshot() RemoteDirectorySnapshot
	Subscribe() RemoteDirectorySubscription
	RequestReconcile(endpoint string)
	RegistryChanged()
}
