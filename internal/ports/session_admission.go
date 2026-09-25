package ports

import (
	"errors"
	"fmt"
	"unicode/utf8"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/protocol"
)

// SessionConnectionOrigin is the closed carriage-origin taxonomy for one
// admitted session connection: which provisioned carriage shape accepted it.
// It is the locality bound an admission is validated against - a connection is
// either the broker's own local carriage or one provisioned remote carriage -
// and Unknown is never a validated admission.
type SessionConnectionOrigin uint8

const (
	// SessionOriginUnknown is the zero value: no provisioned origin was stamped.
	// It is refused by SessionAdmission.Validate and never produced by an
	// accepting side that provisioned a binding.
	SessionOriginUnknown SessionConnectionOrigin = iota
	// SessionOriginLocal is a carriage to the accepting side's own machine
	// daemon, which is not a configured remote host.
	SessionOriginLocal
	// SessionOriginRemote is a carriage provisioned for a configured remote
	// endpoint.
	SessionOriginRemote
)

func (o SessionConnectionOrigin) String() string {
	switch o {
	case SessionOriginUnknown:
		return "unknown"
	case SessionOriginLocal:
		return "local"
	case SessionOriginRemote:
		return "remote"
	default:
		return fmt.Sprintf("invalid(%d)", uint8(o))
	}
}

// Validate refuses the zero value: every validated admission names exactly one
// provisioned locality rather than inheriting an implicit one.
func (o SessionConnectionOrigin) Validate() error {
	switch o {
	case SessionOriginLocal, SessionOriginRemote:
		return nil
	default:
		return errors.New("ports: invalid session connection origin")
	}
}

// SessionAdmission is the closed admission metadata of one admitted session
// connection: the provisioned carriage shape that accepted it,
// the exact accepted policy, the declared stream purpose, and - for attachment
// purposes only - the closed admission variant, its validated creation name,
// its exact session target, and its bounded per-request environment.
//
// It is stamped by the accepting side from its own provisioned authority and
// the peer's validated Open, never echoed from an untrusted request, and is
// exposed to a daemon-consuming use case through SessionAdmissionProvider. It
// carries no wire form of its own: the mux Open already carries every field,
// and an admission is reconstructed at acceptance from that Open plus the
// provisioned origin.
type SessionAdmission struct {
	// Origin is the provisioned carriage shape that accepted this connection.
	// It is never Unknown on a validated admission.
	Origin SessionConnectionOrigin
	// Policy is the exact accepted physical policy, resolved from the
	// provisioned binding, never the peer's requested value.
	Policy BrokerPolicy
	// Purpose is the closed stream purpose the admitted stream declared.
	Purpose BrokerStreamPurpose
	// Admission is the closed attachment-admission variant. It is nonzero only
	// for BrokerStreamAttachment; control and observation carry none.
	Admission BrokerStreamAdmission
	// Name is the validated session name for BrokerAdmissionCreateNamed and
	// BrokerAdmissionAttachNamed and is empty for every other variant.
	Name string
	// Target is the exact session lifecycle target for BrokerAdmissionExact and
	// is zero for every other variant.
	Target protocol.ExactSessionTarget
	// Env is the bounded per-request session environment. It is empty for
	// control and observation and is never merged into a process environment.
	Env []string
}

// Validate enforces the closed admission contract: a known carriage origin, a
// valid policy, a closed purpose with its matching admission shape, and the
// per-request environment bounds. Unknown origin, an unset purpose, an
// attachment shape that contradicts its purpose, and any out-of-bound value are
// refused.
//
// Locality bound: an admission is accepted only for a known Local or Remote
// origin. A zero (Unknown) origin is never a validated admission, so a consumer
// can trust that an admission it is handed names exactly one carriage shape.
func (a SessionAdmission) Validate() error {
	if err := a.Origin.Validate(); err != nil {
		return err
	}
	if err := a.Policy.Validate(); err != nil {
		return err
	}
	switch a.Purpose {
	case BrokerStreamAttachment:
		if err := a.Admission.Validate(); err != nil {
			return err
		}
		switch a.Admission {
		case BrokerAdmissionExact:
			if err := a.Target.Validate(); err != nil {
				return err
			}
			if a.Name != "" {
				return errors.New("ports: exact admission carries a creation name")
			}
		case BrokerAdmissionCreateNamed, BrokerAdmissionAttachNamed:
			if a.Target != (protocol.ExactSessionTarget{}) {
				return errors.New("ports: named admission carries an exact target")
			}
			if err := domain.ValidateSessionName(a.Name); err != nil {
				return fmt.Errorf("ports: invalid creation session name: %w", err)
			}
		case BrokerAdmissionCreateEphemeral:
			if a.Target != (protocol.ExactSessionTarget{}) {
				return errors.New("ports: ephemeral creation carries an exact target")
			}
			if a.Name != "" {
				return errors.New("ports: ephemeral creation carries a session name")
			}
		}
	case BrokerStreamControl, BrokerStreamObservation:
		if a.Admission != 0 || a.Name != "" || a.Target != (protocol.ExactSessionTarget{}) || len(a.Env) != 0 {
			return errors.New("ports: control/observation carries attachment state")
		}
	default:
		return errors.New("ports: invalid session admission purpose")
	}
	if uint64(len(a.Env)) > BrokerMaxEnvEntries {
		return errors.New("ports: session admission has too many environment entries")
	}
	for _, entry := range a.Env {
		if len(entry) > BrokerMaxEnvEntryBytes {
			return errors.New("ports: session admission environment entry too long")
		}
		if !utf8.ValidString(entry) {
			return errors.New("ports: session admission environment is not valid UTF-8")
		}
	}
	return nil
}

// Clone returns a deep copy with an independent environment slice, so a
// consumer that mutates what it holds can never alter admitted authority or a
// sibling's view of it.
func (a SessionAdmission) Clone() SessionAdmission {
	out := a
	out.Env = append([]string(nil), a.Env...)
	return out
}

// SessionAdmissionProvider is the optional boundary-safe port a typed accepted
// server connection implements when the accepting side stamped admission
// metadata onto it. A daemon-consuming use case asserts it structurally; a
// connection built by a legacy constructor that carries no provisioned
// admission reports ok=false rather than a zero admission.
//
// The returned value is a defensive copy: mutating it never alters the
// connection's admitted authority, and concurrent readers each receive their
// own copy.
type SessionAdmissionProvider interface {
	SessionAdmission() (SessionAdmission, bool)
}
