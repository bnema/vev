// Broker endpoint connector and physical connection.
//
// EndpointConnector is the broker-side bridge between the transport-neutral
// ports.BrokerEndpointConnector contract and the daemonmux multiplexer. It owns
// the setup half of one pooled physical connection: dial one already
// authenticated raw framed carriage for the resolved endpoint, run the
// daemonmux physical preamble over the FramedCarrierBridge, negotiate the
// effective ceilings, and hand the live pump and logical connector to a
// PhysicalConnection.
//
// The dial function is injected, not chosen here: the connector never knows
// whether the carriage is IPC, QUIC, SSH stdio, or a test pipe, and it never
// authenticates it. The carriage must already be authenticated before this
// connector sees it; the resolved endpoint's identity and policy are the
// authoritative values the handshake verifies the daemon's response against.
//
// One caller-supplied setup context (or its deadline) bounds dial and handshake
// as a single unit: the same context goes to the dial function and to
// RunClientHandshake, and no timer is created here. A successful connection is
// deliberately detached from that setup context: the pump is started on a
// context that preserves the setup values but not its cancellation, so a caller
// that cancels or lets its setup deadline elapse after Connect returned never
// tears down a pooled physical connection. The connection's lifetime is owned
// by PhysicalConnection.Close.
//
// PhysicalConnection is the broker-side ports.BrokerPhysicalConnection over
// that live pump. Its Identity, Incarnation, and Policy are immutable values
// captured from the verified handshake; Done, Err, and FailureKind delegate to
// the pump's publish-once terminal authority, so a physical loss closes Done
// before every logical stream is terminalized; OpenStream exact-checks the
// request policy against the accepted physical policy and then defers to the
// LogicalConnector; Close tears the pump (and therefore the carrier) down
// exactly once.
package daemonmux

import (
	"context"
	"errors"
	"fmt"
	"os/exec"
	"sync"

	"github.com/bnema/vev/internal/adapters/sshstdio"
	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
)

// ErrConnectorConfig reports an endpoint connector constructed without a dial
// function or with an invalid ceiling advertisement, or a Connect call with an
// invalid resolved endpoint. It is a caller bug, not a peer refusal.
var ErrConnectorConfig = errors.New("daemonmux: invalid endpoint connector configuration")

// RawCarrierDialer establishes one already authenticated raw framed carriage
// for one resolved dial target. It returns a carriage the daemonmux
// FramedCarrierBridge can adapt; it returns promptly once ctx is done. The
// carriage's authentication and transport selection live entirely outside this
// package, so the connector carries no dial policy of its own.
//
// The whole target travels, not only its opaque address: the resolved start
// authorization, policy, and endpoint fence are exactly what a carriage owner
// needs to decide whether the transport may start the target daemon. They are
// passed as one value so a dial can never observe an address that belongs to a
// different authorization than the one the connector validated and will verify
// in the physical preamble.
type RawCarrierDialer func(ctx context.Context, target ports.BrokerDialTarget) (RawFramedTransport, error)

// EndpointConnector implements ports.BrokerEndpointConnector over daemonmux. It
// owns no carriage state itself: every Connect produces one independent
// PhysicalConnection that owns its own pump, carrier, and logical connector.
// It is safe for concurrent use.
type EndpointConnector struct {
	dial     RawCarrierDialer
	ceilings MuxCeilings
}

var _ ports.BrokerEndpointConnector = (*EndpointConnector)(nil)

// NewEndpointConnector returns a connector that dials carriage through dial and
// advertises ceilings in the daemonmux physical preamble. A nil dial or an
// invalid ceiling advertisement is refused with ErrConnectorConfig; the
// ceilings are validated and copied, so a later change to the caller's value
// never changes what a connection negotiates.
func NewEndpointConnector(dial RawCarrierDialer, ceilings MuxCeilings) (*EndpointConnector, error) {
	if dial == nil {
		return nil, ErrConnectorConfig
	}
	if err := ceilings.Validate(); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrConnectorConfig, err)
	}
	return &EndpointConnector{dial: dial, ceilings: ceilings}, nil
}

// Connect dials the resolved endpoint, completes the daemonmux physical
// preamble against the endpoint's authoritative identity and policy, and
// returns a live ports.BrokerPhysicalConnection. The resolved target is handed
// to the dial function whole, after validation, so the carriage owner sees the
// exact start authorization and fence the connector is about to verify. The
// caller's ctx (or its deadline) bounds dial and handshake together; every
// failure closes the carriage, so no half-open physical connection survives. A
// successful connection is detached from ctx: cancelling or expiring ctx
// afterwards never stops the pooled physical connection, which Close alone
// owns.
func (c *EndpointConnector) Connect(ctx context.Context, endpoint ports.BrokerDialTarget) (ports.BrokerPhysicalConnection, error) {
	if c == nil || c.dial == nil {
		return nil, ErrConnectorConfig
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := endpoint.Validate(); err != nil {
		return nil, ports.BrokerError{Code: ports.BrokerErrorIncompatible, Cause: err}
	}

	raw, err := c.dial(ctx, endpoint)
	if err != nil {
		if raw != nil {
			_ = raw.Close()
		}
		return nil, dialError(err)
	}
	if raw == nil {
		return nil, ports.BrokerError{Code: ports.BrokerErrorIncompatible, Cause: ErrCarrierState}
	}

	bridge, err := NewPreambleCarrier(raw)
	if err != nil {
		_ = raw.Close()
		return nil, ports.BrokerError{Code: ports.BrokerErrorIncompatible, Cause: err}
	}

	result, err := RunClientHandshake(ctx, bridge, endpoint, c.ceilings)
	if err != nil {
		// The handshake already closed a refused carriage; Close is idempotent.
		_ = bridge.Close()
		return nil, handshakeError(err)
	}
	// The endpoint stays authoritative: re-verify the accepted authority even
	// though the handshake already checked it, so a future handshake change can
	// never weaken what the connector publishes.
	if (endpoint.ExpectedIdentity.Bound && result.Identity != endpoint.ExpectedIdentity.Identity) || !result.Policy.Compatible(endpoint.Policy) || result.Incarnation.Validate() != nil {
		_ = bridge.Close()
		return nil, ports.BrokerError{Code: ports.BrokerErrorIncompatible}
	}
	if err := bridge.Negotiate(result.Ceilings); err != nil {
		_ = bridge.Close()
		return nil, ports.BrokerError{Code: ports.BrokerErrorIncompatible, Cause: err}
	}

	pump, err := NewPump(bridge, DirectionServer, result.Ceilings)
	if err != nil {
		_ = bridge.Close()
		return nil, ports.BrokerError{Code: ports.BrokerErrorIncompatible, Cause: err}
	}
	logical, err := NewLogicalConnector(pump)
	if err != nil {
		_ = pump.Close()
		return nil, ports.BrokerError{Code: ports.BrokerErrorIncompatible, Cause: err}
	}

	physical := &PhysicalConnection{
		pump:        pump,
		logical:     logical,
		identity:    result.Identity,
		incarnation: result.Incarnation,
		policy:      result.Policy,
	}
	// The setup context ends here. WithoutCancel keeps the setup values while
	// removing its cancellation and deadline, so Close - not the caller's setup
	// context - ends the pooled connection.
	pump.Start(context.WithoutCancel(ctx))
	return physical, nil
}

// dialError classifies one dial failure: cancellation and deadline stay
// recognizable for the pool's precedence, a validated broker error passes
// through, and anything else becomes an unavailable adapter failure with its
// cause retained locally.
func dialError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	if missingRemoteDaemon(err) {
		return ports.BrokerError{Code: ports.BrokerErrorNoDaemon, Cause: err}
	}
	var typed ports.BrokerError
	if errors.As(err, &typed) && typed.Validate() == nil {
		return err
	}
	return ports.BrokerError{Code: ports.BrokerErrorUnavailable, Cause: err}
}

// missingRemoteDaemon only accepts the reserved helper exit after SSH has
// started; SSH's own failures (including exit 255) remain unavailable.
func missingRemoteDaemon(err error) bool {
	if !errors.Is(err, sshstdio.ErrMuxSSHExit) {
		return false
	}
	var exit *exec.ExitError
	return errors.As(err, &exit) && exit.ExitCode() == sshstdio.MuxExitNoDaemon
}

// handshakeError classifies one handshake failure: cancellation and deadline
// stay recognizable, a peer refusal is a policy conflict, a local authority
// mismatch is an incompatibility, and anything else is unavailable.
func handshakeError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	if missingRemoteDaemon(err) {
		return ports.BrokerError{Code: ports.BrokerErrorNoDaemon, Cause: err}
	}
	var refusal *HandshakeRefusal
	if errors.As(err, &refusal) {
		return ports.BrokerError{Code: ports.BrokerErrorConflictingPolicy, Cause: err}
	}
	if errors.Is(err, ErrHandshakeRejected) {
		return ports.BrokerError{Code: ports.BrokerErrorIncompatible, Cause: err}
	}
	if errors.Is(err, ErrHandshakeConfig) {
		return ports.BrokerError{Code: ports.BrokerErrorIncompatible, Cause: err}
	}
	return ports.BrokerError{Code: ports.BrokerErrorUnavailable, Cause: err}
}

// PhysicalConnection is one pooled physical daemonmux connection: the live
// pump, its logical connector, and the immutable daemon authority the physical
// preamble verified. It implements ports.BrokerPhysicalConnection and is safe
// for concurrent use.
type PhysicalConnection struct {
	pump    *Pump
	logical *LogicalConnector

	identity    ports.BrokerDaemonIdentity
	incarnation ports.BrokerDaemonIncarnation
	policy      ports.BrokerPolicy

	closeOnce sync.Once
	closeErr  error
}

var _ ports.BrokerPhysicalConnection = (*PhysicalConnection)(nil)

// Done returns the pump's terminal channel: closed exactly once, before every
// logical stream of the connection is terminalized.
func (c *PhysicalConnection) Done() <-chan struct{} {
	if c == nil || c.pump == nil {
		return nil
	}
	return c.pump.Done()
}

// Err returns the pump's terminal cause: nil while open and for an orderly
// local Close.
func (c *PhysicalConnection) Err() error {
	if c == nil || c.pump == nil {
		return nil
	}
	return c.pump.Err()
}

// FailureKind returns the pump's terminal classification: RemoteFailureNone
// while open and for an orderly local Close.
func (c *PhysicalConnection) FailureKind() domain.RemoteFailureKind {
	if c == nil || c.pump == nil {
		return domain.RemoteFailureNone
	}
	return c.pump.FailureKind()
}

// Identity returns the authenticated daemon identity the handshake verified
// against the resolved endpoint. It is immutable for the connection's lifetime.
func (c *PhysicalConnection) Identity() ports.BrokerDaemonIdentity {
	if c == nil {
		return ""
	}
	return c.identity
}

// Incarnation returns the daemon process incarnation captured at handshake: a
// stable identity across restarts carries a fresh, nonzero incarnation.
func (c *PhysicalConnection) Incarnation() ports.BrokerDaemonIncarnation {
	if c == nil {
		return ports.BrokerDaemonIncarnation{}
	}
	return c.incarnation
}

// Policy returns the exact accepted connection policy this physical connection
// was pooled under. It is immutable for the connection's lifetime.
func (c *PhysicalConnection) Policy() ports.BrokerPolicy {
	if c == nil {
		return ports.BrokerPolicy{}
	}
	return c.policy
}

// OpenStream exact-checks one open request against the accepted physical
// policy and then opens one independently cancellable logical stream through
// the LogicalConnector. A policy that differs in any field is refused as a
// conflicting policy without touching the carriage, so a physical connection
// can never multiplex a stream under a policy it did not negotiate.
func (c *PhysicalConnection) OpenStream(ctx context.Context, request ports.BrokerOpenStreamRequest) (ports.BrokerEnvelopeStream, error) {
	if c == nil || c.logical == nil {
		return nil, ports.BrokerError{Code: ports.BrokerErrorIncompatible, Cause: ErrLogicalConfig}
	}
	if !request.Policy.Compatible(c.policy) {
		return nil, ports.BrokerError{Code: ports.BrokerErrorConflictingPolicy}
	}
	// Never hand back a typed nil: Open returns a concrete *LogicalConnection, so
	// returning it directly on the error path would wrap a nil pointer in a
	// non-nil interface, and a caller that checks the interface against nil would
	// then call a method on it.
	connection, err := c.logical.Open(ctx, request)
	if err != nil {
		return nil, err
	}
	if connection == nil {
		return nil, ports.BrokerError{Code: ports.BrokerErrorIncompatible, Cause: ports.BrokerAdmissionInvalid}
	}
	return connection, nil
}

// Close tears the physical connection down exactly once: it closes the pump,
// which cancels both carriage goroutines and closes the carrier through the
// adapter prompt-close contract. Every caller observes the same carrier close
// error. Close is concurrent-safe and prompt.
func (c *PhysicalConnection) Close() error {
	if c == nil {
		return nil
	}
	c.closeOnce.Do(func() {
		if c.pump != nil {
			c.closeErr = c.pump.Close()
		}
	})
	return c.closeErr
}
