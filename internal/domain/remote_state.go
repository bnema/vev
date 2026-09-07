package domain

import (
	"fmt"
)

// RemoteAvailability is the monitor's last-known reachability for one
// registered endpoint. Checking is tracked separately and never replaces
// availability: an in-flight observation preserves the previous value.
type RemoteAvailability uint8

const (
	RemoteAvailabilityUnknown RemoteAvailability = iota + 1
	RemoteAvailabilityReachable
	RemoteAvailabilityUnreachable
	RemoteAvailabilityIncompatible
	RemoteAvailabilityAuthFailed
	RemoteAvailabilityInvalidResponse
)

// String returns the stable lowercase wire/display token for an availability.
func (a RemoteAvailability) String() string {
	switch a {
	case RemoteAvailabilityUnknown:
		return "unknown"
	case RemoteAvailabilityReachable:
		return "reachable"
	case RemoteAvailabilityUnreachable:
		return "unreachable"
	case RemoteAvailabilityIncompatible:
		return "incompatible"
	case RemoteAvailabilityAuthFailed:
		return "authentication_failed"
	case RemoteAvailabilityInvalidResponse:
		return "invalid_response"
	default:
		return "unknown"
	}
}

// RemoteFailureKind classifies why one remote observation failed. It mirrors
// the observation error classes without carrying raw stderr or endpoints.
type RemoteFailureKind uint8

const (
	RemoteFailureNone RemoteFailureKind = iota
	RemoteFailureTransport
	RemoteFailureTimeout
	RemoteFailureTrust
	RemoteFailureAuthentication
	RemoteFailureIncompatible
	RemoteFailureInvalidResponse
)

// String returns the stable lowercase token for a failure kind.
func (k RemoteFailureKind) String() string {
	switch k {
	case RemoteFailureNone:
		return "none"
	case RemoteFailureTransport:
		return "transport"
	case RemoteFailureTimeout:
		return "timeout"
	case RemoteFailureTrust:
		return "trust"
	case RemoteFailureAuthentication:
		return "authentication"
	case RemoteFailureIncompatible:
		return "incompatible"
	case RemoteFailureInvalidResponse:
		return "invalid_response"
	default:
		return "transport"
	}
}

// RemoteFailure is the typed last-failure value carried in snapshots. Err
// preserves the underlying cause for errors.Is/As callers; Kind is the
// sanitized classification safe for presentation and notice policy.
type RemoteFailure struct {
	Kind RemoteFailureKind
	Err  error
}

// Error reports the sanitized failure kind.
func (f RemoteFailure) Error() string { return "remote observation failed: " + f.Kind.String() }

// Unwrap exposes the underlying cause to errors.Is/As.
func (f RemoteFailure) Unwrap() error { return f.Err }

// RemoteGeneration is a per-registration fencing generation. It increments
// each time an endpoint is (re-)registered so late results bound to an older
// generation can never apply to a newer registration of the same endpoint.
type RemoteGeneration uint64

// RemoteRegistration is the exact configured identity of one monitored
// endpoint: the endpoint string plus a persistent random incarnation. Full
// removal followed by re-add creates a new incarnation, so equality on both
// fields fences ABA re-registration even when the endpoint string matches.
type RemoteRegistration struct {
	Endpoint    string
	Incarnation [16]byte
	Generation  RemoteGeneration
}

// NewRemoteRegistration creates a registration for a validated endpoint
// with an explicit incarnation. Randomness is the caller's concern:
// adapters sample fresh incarnations while domain stays deterministic.
func NewRemoteRegistration(endpoint string, incarnation [16]byte) (RemoteRegistration, error) {
	if err := ValidateRemoteHostTarget(endpoint); err != nil {
		return RemoteRegistration{}, err
	}
	return RemoteRegistration{Endpoint: endpoint, Incarnation: incarnation, Generation: 1}, nil
}

// IsZero reports whether the registration carries no identity. A zero
// incarnation never identifies a host; unbound legacy records stay zero.
func (r RemoteRegistration) IsZero() bool {
	return r.Endpoint == "" || r.Incarnation == [16]byte{}
}

// Equal compares exact configured identity: endpoint, incarnation and
// generation must all match.
func (r RemoteRegistration) Equal(other RemoteRegistration) bool {
	return r.Endpoint == other.Endpoint && r.Incarnation == other.Incarnation && r.Generation == other.Generation
}

// Validate rejects empty endpoints and zero incarnations.
func (r RemoteRegistration) Validate() error {
	if err := ValidateRemoteHostTarget(r.Endpoint); err != nil {
		return err
	}
	if r.Incarnation == [16]byte{} {
		return fmt.Errorf("remote registration for %q has no incarnation", r.Endpoint)
	}
	if r.Generation == 0 {
		return fmt.Errorf("remote registration for %q has no generation", r.Endpoint)
	}
	return nil
}

// Stable RemoteReason tokens shared by catalogue validation, picker
// presentation, and daemon projections. Centralizing them here keeps
// display/diagnostic labels consistent without magic strings.
const (
	RemoteReasonRefreshing      = "refreshing"
	RemoteReasonCatalogStale    = "catalog_stale"
	RemoteReasonHostUnreachable = "host_unreachable"
	RemoteReasonVersionMismatch = "version_mismatch"
	RemoteReasonSessionDown     = "session_down"
	RemoteReasonSessionBroken   = "session_broken"
	RemoteReasonSessionStopped  = "session_stopped"
	RemoteReasonIdentityChanged = "identity_changed"
	RemoteReasonNotFound        = "not_found"
	RemoteReasonTimeout         = "timeout"
	RemoteReasonMalformed       = "malformed"
	RemoteReasonAuthFailure     = "auth_failure"
)
