package ports

import (
	"errors"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/protocol/catalogue"
)

// Durable broker store sentinels. Adapters wrap these so a caller can classify
// a store failure without importing the adapter: conflicting authority means
// refresh the token and retry, a locked store means another owner is live, and
// invalid state is resolved offline from the recovery record. None of them is
// ever retried blindly inside the store.
var (
	// ErrBrokerHostConflict reports that durable host authority changed under
	// the caller's expected revision, or that the supplied membership would
	// regress or re-trust an existing registration without a fresh identity.
	ErrBrokerHostConflict = errors.New("ports: broker durable host authority conflict")
	// ErrBrokerStoreLocked reports that another owner already holds the durable
	// store's lifetime lock.
	ErrBrokerStoreLocked = errors.New("ports: broker durable store is owned elsewhere")
	// ErrBrokerStoreInvalidState reports durable state that fails validation.
	// Such state is never silently replaced or rolled back by the store.
	ErrBrokerStoreInvalidState = errors.New("ports: broker durable store state is invalid")
)

// BrokerHostRecord is durable registration authority. Membership and policy
// are never inferred from an observation or from the broker environment.
type BrokerHostRecord struct {
	Registration domain.RemoteRegistration
	Pinned       bool
	Learned      bool
	Policy       BrokerPolicy
}

// BrokerHosts is an optimistic-concurrency token and the ordered authority.
// Revision is store-local and independent of broker publication epochs.
type BrokerHosts struct {
	Revision uint64
	Hosts    []BrokerHostRecord
}

func (h BrokerHosts) Validate() error {
	if h.Revision == 0 {
		return errors.New("ports: invalid broker host state")
	}
	return ValidateBrokerHostRecords(h.Hosts)
}

// ValidateBrokerHostRecords reports whether caller-supplied membership is
// internally valid, independently of any revision token: every registration
// and policy is valid, every record is anchored as pinned or learned, no
// endpoint repeats, and the set is within the host bound. A durable store
// applies this rule to caller input before its CAS, so malformed membership is
// a caller error rather than durable-state corruption.
func ValidateBrokerHostRecords(hosts []BrokerHostRecord) error {
	if len(hosts) > BrokerMaxHosts {
		return errors.New("ports: invalid broker host state")
	}
	seen := make(map[string]bool, len(hosts))
	for _, host := range hosts {
		if err := host.Registration.Validate(); err != nil {
			return err
		}
		if err := host.Policy.Validate(); err != nil {
			return err
		}
		if (!host.Pinned && !host.Learned) || seen[host.Registration.Endpoint] {
			return errors.New("ports: invalid broker membership")
		}
		seen[host.Registration.Endpoint] = true
	}
	return nil
}

// BrokerHostStore owns durable membership separately from advisory snapshots.
// ReplaceHosts compares expected with the current host revision, increments it,
// and atomically discards observations for removed/replaced registrations or
// changed policies. Callers supply fresh incarnations on remove/re-add.
// ReplaceHosts validates the supplied membership (ValidateBrokerHostRecords)
// before its compare-and-swap, so malformed caller input is reported as a
// caller error and never as invalid durable state. Close releases exclusive
// ownership; no operation is valid after Close.
type BrokerHostStore interface {
	BrokerSnapshotStore
	LoadHosts() (BrokerHosts, error)
	ReplaceHosts(expected uint64, hosts []BrokerHostRecord) error
	Close() error
}

// BrokerHostStore is a BrokerSnapshotStore: the exclusive owner of durable
// membership also owns the advisory snapshot, so the registry writes through
// one seam. This guard keeps that composition compile-checked.
var _ BrokerSnapshotStore = (BrokerHostStore)(nil)

// ValidateDurableHostProjection reports whether one host projection satisfies
// the semantic rules of durable broker state: a published availability and
// failure kind inside their closed ranges, and a session inventory the
// catalogue accepts for the host's exact incarnation and success time. The
// offline store and the registry both apply this rule, so a projection the
// registry publishes is never rejected (and therefore silently never
// persisted) by the store. Transient state (Checking and the live failure
// cause) is deliberately not part of the durable rule.
func ValidateDurableHostProjection(host RemoteHostSnapshot) error {
	if host.Availability < domain.RemoteAvailabilityUnknown || host.Availability > domain.RemoteAvailabilityInvalidResponse {
		return errors.New("ports: broker host availability is out of range")
	}
	if host.LastFailure.Kind > domain.RemoteFailureInvalidResponse {
		return errors.New("ports: broker host failure kind is out of range")
	}
	if len(host.Sessions) == 0 {
		return nil
	}
	return catalogue.ValidateRemoteCatalogCacheEntries([]catalogue.RemoteCatalogCacheEntry{{
		Host:        host.Endpoint,
		Incarnation: host.Registration.Incarnation,
		FetchedAt:   host.LastSuccess,
		Sessions:    host.Sessions,
	}})
}
