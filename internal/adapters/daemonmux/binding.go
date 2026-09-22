package daemonmux

import (
	"errors"

	"github.com/bnema/vev/internal/ports"
)

// ErrInvalidBinding reports an invalid daemon authority. An authority must
// contain a valid identity, a non-zero incarnation, and a non-empty closed set
// of distinct, valid provisioned admissions.
var ErrInvalidBinding = errors.New("daemonmux: invalid server bindings")

// ServerPolicyAdmission is one provisioned carriage shape the daemon accepts:
// the exact connection policy for that shape and the locality it serves. A
// daemon serves its own local carriage and each provisioned remote transport, so
// one binding legitimately carries several entries; the origin makes the
// accepted locality explicit rather than inferring it from the peer's Open.
//
// The origin is per policy, not globally unique: two remote transport policies
// (for example quic and stdio) share the one remote locality, so admission
// resolves origin from the accepted policy entry rather than a global origin
// table.
type ServerPolicyAdmission struct {
	Policy ports.BrokerPolicy
	Origin ports.SessionConnectionOrigin
}

// Validate enforces the closed provisioned-entry contract: a valid exact policy
// and a known locality. A missing or invalid origin is refused, so an accepted
// entry can never inherit an implicit locality.
func (e ServerPolicyAdmission) Validate() error {
	if err := e.Policy.Validate(); err != nil {
		return err
	}
	return e.Origin.Validate()
}

// ServerBinding is an immutable daemon authority. The entry slice is cloned at
// construction and is never exposed, making the admitted set safe to share
// between concurrent handshakes.
type ServerBinding struct {
	identity    ports.BrokerDaemonIdentity
	incarnation ports.BrokerDaemonIncarnation
	entries     []ServerPolicyAdmission
}

// NewServerBindings constructs a closed admission authority from provisioned
// entries. An empty set, a duplicate policy, an invalid policy, and an unknown
// origin are rejected. Entries are cloned, so a later change to the caller's
// slice cannot weaken what the handshake already enforced.
func NewServerBindings(identity ports.BrokerDaemonIdentity, incarnation ports.BrokerDaemonIncarnation, entries []ServerPolicyAdmission) (ServerBinding, error) {
	binding := ServerBinding{identity: identity, incarnation: incarnation, entries: append([]ServerPolicyAdmission(nil), entries...)}
	if err := binding.Validate(); err != nil {
		return ServerBinding{}, err
	}
	return binding, nil
}

// NewServerBinding is retained as a source-compatible shorthand for fixtures
// that provision exactly one local policy. It is never used by production
// composition, which builds the daemon's whole closed set with NewServerBindings.
func NewServerBinding(identity ports.BrokerDaemonIdentity, incarnation ports.BrokerDaemonIncarnation, policy ports.BrokerPolicy) (ServerBinding, error) {
	return NewServerBindings(identity, incarnation, []ServerPolicyAdmission{{Policy: policy, Origin: ports.SessionOriginLocal}})
}

func (b ServerBinding) Identity() ports.BrokerDaemonIdentity       { return b.identity }
func (b ServerBinding) Incarnation() ports.BrokerDaemonIncarnation { return b.incarnation }

// Policy returns the sole entry's policy for legacy single-policy callers. It
// returns the zero value for a multi-entry authority; admission never uses it.
func (b ServerBinding) Policy() ports.BrokerPolicy {
	if len(b.entries) == 1 {
		return b.entries[0].Policy
	}
	return ports.BrokerPolicy{}
}

// Origin returns the sole entry's locality for legacy single-policy callers. It
// returns the unknown origin for a multi-entry authority.
func (b ServerBinding) Origin() ports.SessionConnectionOrigin {
	if len(b.entries) == 1 {
		return b.entries[0].Origin
	}
	return ports.SessionOriginUnknown
}

func (b ServerBinding) Validate() error {
	if b.identity.Validate() != nil || b.incarnation.Validate() != nil || len(b.entries) == 0 {
		return ErrInvalidBinding
	}
	for i, entry := range b.entries {
		if entry.Validate() != nil {
			return ErrInvalidBinding
		}
		for j := 0; j < i; j++ {
			// Duplicate policy - and therefore a duplicated entry - is refused:
			// one policy is one carriage shape, so a second entry naming the
			// same policy would make the accepted locality and policy ambiguous.
			if entry.Policy.Compatible(b.entries[j].Policy) {
				return ErrInvalidBinding
			}
		}
	}
	return nil
}

// Accepted returns the exact provisioned entry matching policy, carrying that
// entry's policy and origin. No merging, inheritance, echo, or fallback is
// performed: a request differing in one field is refused.
func (b ServerBinding) Accepted(policy ports.BrokerPolicy) (ServerPolicyAdmission, bool) {
	for _, provisioned := range b.entries {
		if provisioned.Policy.Compatible(policy) {
			return provisioned, true
		}
	}
	return ServerPolicyAdmission{}, false
}

func (b ServerBinding) Accepts(policy ports.BrokerPolicy) bool {
	_, ok := b.Accepted(policy)
	return ok
}
