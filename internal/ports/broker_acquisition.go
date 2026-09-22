package ports

import (
	"context"
	"errors"
	"fmt"

	"github.com/bnema/vev/internal/domain"
)

// BrokerEndpointFence identifies the exact route authority used for one dial.
// Remote registrations carry their full incarnation/generation; local routes
// are explicit and never fabricate a registration.
type BrokerEndpointFence struct {
	Local        bool
	Registration domain.RemoteRegistration
}

func (f BrokerEndpointFence) Validate() error {
	if f.Local {
		if f.Registration != (domain.RemoteRegistration{}) {
			return errors.New("ports: local endpoint fence carries a registration")
		}
		return nil
	}
	return f.Registration.Validate()
}

// BrokerDaemonStartMode is the closed, explicit authorization a caller attaches
// to one acquisition: whether the transport may start the target daemon when it
// is not already reachable, or must observe only what is already running.
//
// Purpose alone cannot carry this: a control stream that lists or mutates a
// stopped daemon's persisted records is allowed to start it, while an
// observation and the explicit daemon-stop itself must never start it. The mode
// is an authorization, never an obligation: it can only narrow the configured
// policy, and a policy that does not permit launching still refuses.
type BrokerDaemonStartMode uint8

const (
	// BrokerDaemonExistingOnly authorizes only an already running daemon. A
	// transport that carries this mode must never start a daemon, a broker, or
	// any recursive helper process.
	BrokerDaemonExistingOnly BrokerDaemonStartMode = iota + 1
	// BrokerDaemonStartIfNeeded additionally authorizes starting the target
	// daemon when it is not running, subject to the resolved policy's launch
	// authority.
	BrokerDaemonStartIfNeeded
)

func (m BrokerDaemonStartMode) String() string {
	switch m {
	case BrokerDaemonExistingOnly:
		return "existing_only"
	case BrokerDaemonStartIfNeeded:
		return "start_if_needed"
	default:
		return fmt.Sprintf("invalid(%d)", uint8(m))
	}
}

// Validate refuses the zero value: every acquisition states its start authority
// explicitly rather than inheriting an implicit ability to start a daemon.
func (m BrokerDaemonStartMode) Validate() error {
	switch m {
	case BrokerDaemonExistingOnly, BrokerDaemonStartIfNeeded:
		return nil
	default:
		return errors.New("ports: invalid broker daemon start mode")
	}
}

// BrokerExpectedIdentity is an explicit optional durable identity binding.
// BrokerDaemonIdentity itself remains strict and is never valid when empty.
type BrokerExpectedIdentity struct {
	Identity BrokerDaemonIdentity
	Bound    bool
}

func (e BrokerExpectedIdentity) Validate() error {
	if !e.Bound {
		if e.Identity != "" {
			return errors.New("ports: unbound expected identity carries a value")
		}
		return nil
	}
	return e.Identity.Validate()
}

// BrokerDialTarget authorizes one bounded dial before authentication. Address
// selects the adapter route; ExpectedIdentity fences an existing durable
// binding but may be explicitly unbound for first contact. StartMode is carried
// over from the resolution request and must reach the transport before any
// connect-or-spawn decision.
type BrokerDialTarget struct {
	Fence            BrokerEndpointFence
	Address          string
	Policy           BrokerPolicy
	StartMode        BrokerDaemonStartMode
	ExpectedIdentity BrokerExpectedIdentity
}

func (t BrokerDialTarget) Validate() error {
	if err := t.Fence.Validate(); err != nil {
		return err
	}
	if err := t.StartMode.Validate(); err != nil {
		return err
	}
	if err := t.Policy.Validate(); err != nil {
		return err
	}
	if err := t.ExpectedIdentity.Validate(); err != nil {
		return err
	}
	return validateBrokerToken(t.Address, BrokerMaxAddressBytes, "dial target address")
}

// BrokerRouteAuthority resolves current membership and route authority. It
// performs no network I/O and never invents a daemon identity. Resolution
// propagates BrokerOpenStreamRequest.StartMode onto the returned dial target
// unchanged, so the resolved authority and the requested start mode are decided
// together and neither can be widened afterwards.
type BrokerRouteAuthority interface {
	ResolveDialTarget(context.Context, BrokerOpenStreamRequest) (BrokerDialTarget, error)
}

// BrokerIdentityBindingRequest asks the broker authority to adopt an identity
// learned from an authenticated carriage under the exact resolution fence.
type BrokerIdentityBindingRequest struct {
	Fence    BrokerEndpointFence
	Policy   BrokerPolicy
	Identity BrokerDaemonIdentity
}

func (r BrokerIdentityBindingRequest) Validate() error {
	if err := r.Fence.Validate(); err != nil {
		return err
	}
	if err := r.Policy.Validate(); err != nil {
		return err
	}
	return r.Identity.Validate()
}

// BrokerIdentityBinder commits first-contact identity authority before a
// physical connection may be published or carry a logical stream.
type BrokerIdentityBinder interface {
	BindAuthenticatedIdentity(context.Context, BrokerIdentityBindingRequest) (BrokerDaemonIdentity, error)
}
