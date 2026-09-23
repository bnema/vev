package ports

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"unicode"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/protocol/catalogue"
)

// Durable broker store sentinels. Adapters wrap these so a caller can classify
// a store failure without importing the adapter: conflicting authority means
// refresh the token and retry, a locked store means another owner is live, and
// invalid state is resolved offline from the recovery record. None of them is
// ever retried blindly inside the store.
var (
	// ErrBrokerMembershipImmutable reports that a registry was not explicitly
	// configured to accept membership mutations.
	ErrBrokerMembershipImmutable = errors.New("ports: broker membership is immutable")
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

// BrokerStoreOutcomeUnknownError reports a write failure after the durable
// commit point. Callers must reopen and reload authority rather than retrying.
type BrokerStoreOutcomeUnknownError struct{ Err error }

func (e BrokerStoreOutcomeUnknownError) Error() string {
	if e.Err == nil {
		return "ports: broker store outcome unknown"
	}
	return "ports: broker store outcome unknown: " + e.Err.Error()
}
func (e BrokerStoreOutcomeUnknownError) Unwrap() error { return e.Err }

type BrokerRouteKind string

const (
	BrokerRouteUnix        BrokerRouteKind = "unix"
	BrokerRouteSSHStdio    BrokerRouteKind = "ssh-stdio"
	BrokerRouteSSHQUIC     BrokerRouteKind = "ssh-quic"
	BrokerMaxRouteArgv                     = 32
	BrokerMaxRouteArgBytes                 = 4 << 10
)

// BrokerRouteSpec is durable routing authority. Adapters derive all runtime
// trust, timeout, address and launch details from policy and this minimal route.
type BrokerRouteSpec struct {
	Kind   BrokerRouteKind
	Path   string
	Target string
	Argv   []string
}

func (r BrokerRouteSpec) Validate() error {
	switch r.Kind {
	case BrokerRouteUnix:
		if r.Path == "" || !filepath.IsAbs(r.Path) || filepath.Clean(r.Path) != r.Path || r.Target != "" || len(r.Argv) != 0 {
			return errors.New("ports: invalid unix broker route")
		}
	case BrokerRouteSSHStdio, BrokerRouteSSHQUIC:
		if r.Path != "" || domain.ValidateRemoteHostTarget(r.Target) != nil || len(r.Argv) == 0 || len(r.Argv) > BrokerMaxRouteArgv {
			return errors.New("ports: invalid ssh broker route")
		}
		for _, arg := range r.Argv {
			if arg == "" || len(arg) > BrokerMaxRouteArgBytes || strings.IndexFunc(arg, func(v rune) bool { return v == 0 || unicode.IsControl(v) }) >= 0 {
				return fmt.Errorf("ports: invalid ssh broker route argv")
			}
		}
	default:
		return errors.New("ports: unknown broker route kind")
	}
	return nil
}

func (r BrokerRouteSpec) Clone() BrokerRouteSpec {
	r.Argv = append([]string(nil), r.Argv...)
	return r
}

// BrokerRouteForTransport maps the closed legacy policy transport vocabulary
// to canonical remote routes.
func BrokerRouteForTransport(transport, endpoint string) (BrokerRouteSpec, error) {
	switch transport {
	case "ssh-stdio", "stdio":
		return BrokerRouteSpec{Kind: BrokerRouteSSHStdio, Target: endpoint, Argv: []string{"vev", "_broker-mux-stdio", "--production"}}, nil
	case "ssh-quic", "quic":
		return BrokerRouteSpec{Kind: BrokerRouteSSHQUIC, Target: endpoint, Argv: []string{"vev", "_broker-mux-quic-bootstrap", "--production"}}, nil
	default:
		return BrokerRouteSpec{}, fmt.Errorf("ports: unsupported broker transport %q", transport)
	}
}

func CloneBrokerHostRecords(hosts []BrokerHostRecord) []BrokerHostRecord {
	out := append([]BrokerHostRecord(nil), hosts...)
	for i := range out {
		out[i].Route = out[i].Route.Clone()
	}
	return out
}

// UpgradeBrokerHostRoutes upgrades records written before routes became
// explicit. It is intentionally closed over the approved remote vocabulary.
func UpgradeBrokerHostRoutes(hosts []BrokerHostRecord) ([]BrokerHostRecord, error) {
	out := CloneBrokerHostRecords(hosts)
	for i := range out {
		if out[i].Route.Kind != "" {
			continue
		}
		route, err := BrokerRouteForTransport(out[i].Policy.Transport, out[i].Registration.Endpoint)
		if err != nil {
			return nil, fmt.Errorf("ports: upgrade broker route for %q: %w", out[i].Registration.Endpoint, err)
		}
		out[i].Route = route
	}
	return out, nil
}

// BrokerHostRecord is durable registration authority. Membership and policy
// are never inferred from an observation or from the broker environment.
type BrokerHostRecord struct {
	Registration domain.RemoteRegistration
	Pinned       bool
	Learned      bool
	Policy       BrokerPolicy
	Route        BrokerRouteSpec
	// Identity is the durable authenticated daemon binding. Empty means the
	// registration has not completed first contact yet.
	Identity BrokerDaemonIdentity
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
		if err := host.Route.Validate(); err != nil {
			return err
		}
		if host.Identity != "" {
			if err := host.Identity.Validate(); err != nil {
				return err
			}
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
// BrokerHostAuthorityReader exposes committed durable routing authority.
// Returned records must be defensive deep copies.
type BrokerHostAuthorityReader interface {
	LookupHost(ctx context.Context, endpoint string) (BrokerHostRecord, bool, error)
}

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

// ValidateDurableHostProjection reports whether one daemon observation
// satisfies the rules the offline store and wire accept: a local observation
// is never durable, availability and failure kinds stay in their closed
// ranges, and a present session inventory is a valid catalogue cache entry
// against the exact registration incarnation. The offline store and the
// registry both apply this rule, so a projection the registry publishes is
// never rejected (and therefore silently never persisted) by the store.
// Transient state (Checking and the live failure cause) is deliberately not
// part of the durable rule.
func ValidateDurableHostProjection(obs BrokerDaemonObservation) error {
	if obs.Local {
		return errors.New("ports: local daemon observation is never durable")
	}
	if obs.Availability < domain.RemoteAvailabilityUnknown || obs.Availability > domain.RemoteAvailabilityNoDaemon {
		return errors.New("ports: broker host availability is out of range")
	}
	if obs.LastFailure.Kind > domain.RemoteFailureInvalidResponse {
		return errors.New("ports: broker host failure kind is out of range")
	}
	// Observed identity is part of the durable rule, not only of the live one: a
	// restored record with partial identity or without the protocol version its
	// identity was authenticated with would otherwise reach a publication the
	// wire refuses, taking down every client connection instead of failing
	// closed here.
	if err := validateObservedDaemonState(obs); err != nil {
		return err
	}
	if len(obs.Sessions) == 0 {
		return nil
	}
	return catalogue.ValidateRemoteCatalogCacheEntries([]catalogue.RemoteCatalogCacheEntry{{
		Host:        obs.Endpoint,
		Incarnation: obs.Registration.Incarnation,
		FetchedAt:   obs.LastSuccess,
		Sessions:    obs.Sessions,
	}})
}

// ValidateDurableObservation checks the shared remote-only durable observation shape.
func ValidateDurableObservation(daemon BrokerDaemonObservation) error {
	if daemon.Local {
		return errors.New("local daemon observation is never durable")
	}
	if err := domain.ValidateRemoteHostTarget(daemon.Endpoint); err != nil {
		return err
	}
	if err := daemon.Registration.Validate(); err != nil {
		return err
	}
	if daemon.Endpoint != daemon.Registration.Endpoint {
		return errors.New("observation endpoint does not match registration")
	}
	if len(daemon.Sessions) > 0 && !daemon.InventoryKnown {
		return errors.New("observation carries sessions without known inventory")
	}
	return ValidateDurableHostProjection(daemon)
}

// ValidateDurableSnapshot checks the shared durable snapshot shape.
func ValidateDurableSnapshot(snapshot BrokerSnapshot) error {
	if snapshot.Epoch == 0 {
		if snapshot.Revision != 0 || len(snapshot.Daemons) != 0 || len(snapshot.Removed) != 0 {
			return errors.New("invalid empty snapshot")
		}
		return nil
	}
	if snapshot.Revision == 0 {
		return errors.New("snapshot has no revision")
	}
	if len(snapshot.Daemons) > BrokerMaxDaemonsPerSnapshot {
		return errors.New("snapshot has too many daemons")
	}
	seen := make(map[string]struct{}, len(snapshot.Daemons))
	for _, daemon := range snapshot.Daemons {
		if err := ValidateDurableObservation(daemon); err != nil {
			return err
		}
		if _, duplicate := seen[daemon.Endpoint]; duplicate {
			return errors.New("snapshot has duplicate host")
		}
		seen[daemon.Endpoint] = struct{}{}
	}
	if len(snapshot.Removed) > BrokerMaxTombstones {
		return errors.New("snapshot has too many tombstones")
	}
	for _, tombstone := range snapshot.Removed {
		if err := tombstone.Validate(); err != nil {
			return err
		}
		if _, live := seen[tombstone.Endpoint]; live {
			return errors.New("snapshot carries a live host as tombstone")
		}
	}
	return nil
}
