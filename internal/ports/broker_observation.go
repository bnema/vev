package ports

import (
	"errors"
	"fmt"
	"time"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/protocol/catalogue"
)

// Broker-native daemon observation (Plan 001 P5.2a).
//
// BrokerDaemonObservation is the broker's own projection of one daemon, local
// or remote, and the only one: no daemon monitors another. It keeps
// configured authority separate from observed state:
//
//   - configured authority: Local flag, Endpoint, Registration, Policy, and
//     the presentation hints DisplayOrigin and Rank. The registry stamps
//     them from its own host records (or the local composition) and never
//     trusts them from a probe.
//   - observed identity: Identity and Incarnation are zero until a probe
//     actually authenticated the daemon. Unknown identity is never invented.
//   - observed capability: ProtocolVersion and Capabilities mirror what the
//     daemon reported. ProtocolVersion is the daemon's own version and may
//     differ from Policy.ProtocolVersion; that difference is exactly how an
//     incompatible daemon is observable. Zero means unobserved.
//   - observation state: Availability, Checking, timestamps, failure
//     counters, and the exact session catalogue. Availability zero is
//     invalid: an unobserved daemon carries RemoteAvailabilityUnknown.
//
// Observing a daemon never attaches to it and never starts a stopped one.

const (
	// BrokerMaxDaemonsPerSnapshot bounds one snapshot publication: the local
	// daemon plus every configured remote host.
	BrokerMaxDaemonsPerSnapshot = BrokerMaxHosts + 1
	// BrokerMaxDisplayOriginBytes bounds the presentation origin hint.
	BrokerMaxDisplayOriginBytes = 128
)

// BrokerDaemonObservation is one immutable daemon projection in one broker
// snapshot. Nested slices are never mutated after publication.
type BrokerDaemonObservation struct {
	// Local marks the broker's own machine daemon. A local observation has
	// no endpoint and no registration: the local daemon is not a configured
	// host. At most one local observation exists per snapshot.
	Local bool

	// Configured authority (registry-stamped, never probe-supplied).
	Endpoint      string // remote only; matches Registration.Endpoint
	DisplayOrigin string
	Rank          int
	Registration  domain.RemoteRegistration
	Policy        BrokerPolicy

	// Observed identity/incarnation (zero until observed).
	Identity    BrokerDaemonIdentity
	Incarnation BrokerDaemonIncarnation

	// Observed version/capability (zero until observed).
	ProtocolVersion uint16
	Capabilities    uint32

	// Availability, checking, failure, and freshness state.
	Availability        domain.RemoteAvailability
	Checking            bool
	LastAttempt         time.Time
	LastSuccess         time.Time
	NextDue             time.Time
	ConsecutiveFailures uint
	FailureEpisode      uint64
	LastFailure         domain.RemoteFailure
	InventoryKnown      bool

	// Exact session catalogue observed from the daemon.
	Sessions []catalogue.RemoteCatalogSession
}

// Validate enforces the closed projection rules.
func (o BrokerDaemonObservation) Validate() error {
	// Policy is required authority for every daemon, observed or not.
	if err := o.Policy.Validate(); err != nil {
		return fmt.Errorf("ports: broker daemon observation: %w", err)
	}
	if err := validateBrokerDisplayText(o.DisplayOrigin, BrokerMaxDisplayOriginBytes, "display origin"); err != nil {
		return fmt.Errorf("ports: broker daemon observation: %w", err)
	}
	if o.DisplayOrigin == "" {
		return errors.New("ports: broker daemon observation has no display origin")
	}
	if o.Local {
		if o.Endpoint != "" || o.Registration != (domain.RemoteRegistration{}) {
			return errors.New("ports: local daemon observation carries remote authority")
		}
	} else {
		if err := domain.ValidateRemoteHostTarget(o.Endpoint); err != nil {
			return fmt.Errorf("ports: broker daemon observation: %w", err)
		}
		if err := o.Registration.Validate(); err != nil {
			return fmt.Errorf("ports: broker daemon observation: %w", err)
		}
		if o.Endpoint != o.Registration.Endpoint {
			return errors.New("ports: broker daemon observation endpoint does not match registration")
		}
	}
	if err := validateObservedDaemonState(o); err != nil {
		return err
	}
	if o.Availability < domain.RemoteAvailabilityUnknown || o.Availability > domain.RemoteAvailabilityInvalidResponse {
		return errors.New("ports: broker daemon observation availability is out of range")
	}
	if o.LastFailure.Kind > domain.RemoteFailureInvalidResponse {
		return errors.New("ports: broker daemon observation failure kind is out of range")
	}
	if len(o.Sessions) > BrokerMaxSessionsPerHost {
		return errors.New("ports: broker daemon observation has too many sessions")
	}
	if len(o.Sessions) > 0 && !o.InventoryKnown {
		return errors.New("ports: broker daemon observation carries sessions without known inventory")
	}
	return nil
}

// validateObservedDaemonState applies the observed-state rules shared by a live
// publication and a durable projection. Observed identity is all-or-nothing and
// never invented: identity and incarnation appear together, both validate, and
// an observed daemon always carries the protocol version its identity was
// authenticated with. The offline store, a restored durable record, and the
// registry all apply this one rule, so a projection either layer accepts is
// never published as an observation the wire refuses.
func validateObservedDaemonState(observation BrokerDaemonObservation) error {
	observed := observation.Identity != ""
	if observed != !observation.Incarnation.IsZero() {
		return errors.New("ports: broker daemon observation has partial identity")
	}
	if !observed {
		return nil
	}
	if err := observation.Identity.Validate(); err != nil {
		return err
	}
	if err := observation.Incarnation.Validate(); err != nil {
		return err
	}
	if observation.ProtocolVersion == 0 {
		return errors.New("ports: observed daemon identity carries no protocol version")
	}
	return nil
}

// Clone returns a defensive copy with an independent session slice. Each
// session keeps the exact nil-ness of its tab list: an empty but present list
// is catalogue-valid and must not become absent through a copy.
func (o BrokerDaemonObservation) Clone() BrokerDaemonObservation {
	out := o
	out.Sessions = append([]catalogue.RemoteCatalogSession(nil), o.Sessions...)
	for i := range out.Sessions {
		if o.Sessions[i].Tabs == nil {
			continue
		}
		out.Sessions[i].Tabs = append(make([]catalogue.RemoteCatalogTab, 0, len(o.Sessions[i].Tabs)), o.Sessions[i].Tabs...)
	}
	return out
}
