package daemonmux

// Server binding (P3.2a).
//
// ServerBinding is the daemon's validated, immutable authority for one
// accepted daemonmux physical connection: the stable authenticated daemon
// identity used for pooling, this process incarnation used to detect a
// restart behind that identity, and the exact connection policy the daemon
// requires. It is constructed once, validates its own invariants, exposes
// only read accessors, and is never mutated, so a later change to a caller's
// local value cannot weaken what the server handshake already enforced.
//
// The server handshake compares a client's requested policy against this
// binding rather than echoing the request back: an accepted response carries
// the binding's identity, incarnation, and policy. Identity is stable across
// restarts; the incarnation is a freshly sampled nonzero 16-byte value.
//
// Authentication premise: the binding is only as trustworthy as the carrier
// that carried the handshake. The carriage must already be authenticated
// (for example an established QUIC channel with an exact peer pin) before a
// connection reaches this handshake, and the expected endpoint binding is
// authoritative. This package never authenticates a carrier and never treats
// an in-band identity claim as proof.

import (
	"errors"

	"github.com/bnema/vev/internal/ports"
)

// ErrInvalidBinding reports a daemon-side physical binding that is not
// authoritative: an invalid or empty authenticated daemon identity, a zero
// daemon incarnation, or an invalid connection policy. NewServerBinding
// refuses it once, at construction, so the handshake never re-validates an
// unusable binding.
var ErrInvalidBinding = errors.New("daemonmux: invalid server binding")

// ServerBinding is the daemon's validated, immutable physical binding for one
// accepted daemonmux connection. Its fields are unexported and reachable only
// through the Identity, Incarnation, and Policy accessors, so a ServerBinding
// value is immutable once constructed and safe to copy and share.
type ServerBinding struct {
	identity    ports.BrokerDaemonIdentity
	incarnation ports.BrokerDaemonIncarnation
	policy      ports.BrokerPolicy
}

// NewServerBinding validates and returns the daemon's immutable physical
// binding. It refuses an invalid authenticated identity, a zero incarnation,
// or an invalid policy with ErrInvalidBinding; the returned binding either
// validates completely or is never handed out.
func NewServerBinding(identity ports.BrokerDaemonIdentity, incarnation ports.BrokerDaemonIncarnation, policy ports.BrokerPolicy) (ServerBinding, error) {
	binding := ServerBinding{identity: identity, incarnation: incarnation, policy: policy}
	if err := binding.Validate(); err != nil {
		return ServerBinding{}, err
	}
	return binding, nil
}

// Identity returns the stable authenticated daemon identity the binding
// advertises and pools on.
func (b ServerBinding) Identity() ports.BrokerDaemonIdentity { return b.identity }

// Incarnation returns the nonzero process incarnation the binding advertises
// so a broker can detect a restart behind the stable identity.
func (b ServerBinding) Incarnation() ports.BrokerDaemonIncarnation { return b.incarnation }

// Policy returns the exact connection policy the binding requires.
func (b ServerBinding) Policy() ports.BrokerPolicy { return b.policy }

// Validate reports whether the binding is authoritative: a valid authenticated
// identity, a nonzero incarnation, and a valid policy.
func (b ServerBinding) Validate() error {
	if err := b.identity.Validate(); err != nil {
		return ErrInvalidBinding
	}
	if err := b.incarnation.Validate(); err != nil {
		return ErrInvalidBinding
	}
	if err := b.policy.Validate(); err != nil {
		return ErrInvalidBinding
	}
	return nil
}

// Accepts reports whether the requested policy exactly equals the binding's
// policy. Pooling requires exact equality across every field, so a request
// that differs in any field is refused rather than merged or inherited.
func (b ServerBinding) Accepts(policy ports.BrokerPolicy) bool {
	return b.policy.Compatible(policy)
}
