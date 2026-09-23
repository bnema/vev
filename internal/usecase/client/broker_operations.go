package client

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol"
)

// Client control operations over independent broker streams (Plan 001 P7,
// worker 3).
//
// List, Command, Kill, KillAll, and StopDaemon are client use-case operations
// that borrow a BrokerService and open one typed, independently cancellable
// logical stream per attempt. The broker routes and admits; the daemon executes
// and answers. No CLI code touches a ClientConnection or a daemon dialer after
// cutover, and this object owns neither the service nor any attachment state:
// each call owns and closes exactly its own stream.
//
// The correlation contract is deliberately strict:
//
//   - One operation is one logical stream carrying one request and one reply;
//     streams are never pipelined, reused, or replayed.
//   - The exact epoch/connection/stream triplet names the attempt. On a
//     single-use stream the request ID is therefore the constant 1: the
//     correlation is per connection to a typed daemon, never global, so no RNG
//     and no fictitious deduplication key is introduced.
//   - Before the request reaches the wire, every refusal (invalid input, a
//     stale route, a failed open, an elapsed bound, an already-cancelled
//     caller) is a definite not-sent outcome wrapping
//     ErrBrokerOperationNotSent. Nothing was mutated.
//   - From the moment SendClient is entered, no failure proves non-delivery: a
//     send error, EOF, a lost broker, a timeout, a cancellation, or a reply
//     that is wrong-typed, wrongly correlated, or malformed all become a
//     synthetic, correctly correlated OutcomeUnknown with a nil error. The
//     caller inspects the outcome and never replays.
//   - A terminal, correctly correlated, well-formed reply is returned verbatim
//     with a nil error, including Failed and Unknown. A failure is never hidden
//     behind a plain context error and never converted into a nil result.
//
// Every operation shares one absolute bound of brokerOperationTimeout, driven
// by the injected ports.Clock/ports.Timer and never restarted, which covers
// acquisition (stream open) and the reply. Cancellation closes only the stream
// this call owns, so it unblocks Send/Receive without disturbing the borrowed
// service or any sibling stream. Timers and callbacks are disarmed and the
// receive goroutine is joined before return, so a late reply can never change
// the verdict.

// ErrBrokerOperationNotSent reports a control operation that definitely never
// reached the wire: validation, route authority, stream admission, or a
// pre-dispatch cancellation refused it before any message was sent. The cause
// stays available through errors.Is/As, and no mutation may have happened.
var ErrBrokerOperationNotSent = errors.New("client: broker operation not sent")

// errBrokerOperationLost reports a dispatched control request whose reply can
// no longer be trusted: a send error, a physical loss, EOF, a timeout, a
// cancellation, or a wrong/malformed reply. It is local-only and is folded into
// a synthetic OutcomeUnknown for mutating operations; List reports it directly.
var errBrokerOperationLost = errors.New("client: broker operation reply lost")

// brokerOperationRequestID is the RequestID carried on a single-use control
// stream. Correlation is by connection to a typed daemon, not globally, so a
// constant is exactly sufficient; it is never derived from an RNG.
const brokerOperationRequestID uint64 = 1

// brokerOperationTimeout bounds every control operation end to end, from stream
// acquisition through the terminal reply. It is the minimum with the caller's
// own context and is never restarted.
const brokerOperationTimeout = 10 * time.Second

// BrokerOperationRoute identifies the exact broker epoch and destination one
// control operation may address. A local destination is exactly Local=true with
// no registration; a remote destination carries the complete registration
// observed in a broker snapshot, never a bare hostname and never an invented
// identity.
type BrokerOperationRoute struct {
	Epoch       ports.BrokerEpoch
	Destination ports.BrokerEndpointFence
}

// Validate checks the closed route shape: a non-zero epoch and a valid
// destination fence. It performs no I/O and consults no snapshot.
func (r BrokerOperationRoute) Validate() error {
	if r.Epoch == 0 {
		return errors.New("vev: broker operation route carries no epoch")
	}
	if err := r.Destination.Validate(); err != nil {
		return fmt.Errorf("vev: broker operation route destination: %w", err)
	}
	return nil
}

// BrokerOperations executes client control operations over independent broker
// streams. It is concurrent-safe, holds no attachment or picker state, never
// closes the borrowed service, and exposes no way to open or retarget a
// session.
type BrokerOperations struct {
	service ports.BrokerService
	clock   ports.Clock
}

// NewBrokerOperations builds the executor over a borrowed broker service. It
// refuses a nil or typed-nil service or clock so no operation can panic on a
// missing dependency; the caller keeps ownership of the service lifetime.
func NewBrokerOperations(service ports.BrokerService, clock ports.Clock) (*BrokerOperations, error) {
	if supervisorNil(service) {
		return nil, errors.New("vev: broker operations require a broker service")
	}
	if supervisorNil(clock) {
		return nil, errors.New("vev: broker operations require a clock")
	}
	return &BrokerOperations{service: service, clock: clock}, nil
}

// List opens one control stream, sends one protocol.List, and returns exactly
// the daemon's protocol.Sessions. It never substitutes the broker cache for a
// fresh reading and never fabricates an empty result when the daemon is
// unreachable: a refused or lost listing is an ordinary error. A new explicit
// list is always safe, so the implementation never retries on its own.
func (o *BrokerOperations) List(ctx context.Context, route BrokerOperationRoute) ([]protocol.SessionInfo, error) {
	reply, err := o.exchange(ctx, route, ports.BrokerDaemonExistingOnly, protocol.List{})
	if err != nil {
		if errors.Is(err, errBrokerOperationLost) {
			return nil, fmt.Errorf("vev: reading session list: %w", err)
		}
		return nil, err
	}
	sessions, ok := reply.(protocol.Sessions)
	if !ok {
		logBrokerOperationCause("list reply", fmt.Errorf("unexpected reply %T", reply))
		return nil, fmt.Errorf("vev: unexpected reply %T to list", reply)
	}
	return sessions.Sessions, nil
}

// Command dispatches one control command on its own un-attached control
// stream. Version must match the negotiated protocol, RequestID must be zero at
// entry because this operation allocates it, and Attached and Self must be
// false: the composition normalizes --self into an explicit target before
// admission, so a Self request here has no usable context and is never sent.
// Args is copied at admission, so a caller can never mutate a dispatched
// request.
func (o *BrokerOperations) Command(ctx context.Context, route BrokerOperationRoute, request protocol.CommandRequest) (protocol.CommandResult, error) {
	if request.Version != protocol.Version {
		return protocol.CommandResult{}, brokerOperationNotSent(fmt.Errorf("command protocol version %d is not supported", request.Version))
	}
	if request.RequestID != 0 {
		return protocol.CommandResult{}, brokerOperationNotSent(errors.New("command request carries a request ID"))
	}
	if request.Attached {
		return protocol.CommandResult{}, brokerOperationNotSent(errors.New("command request is attached"))
	}
	if request.Self {
		return protocol.CommandResult{}, brokerOperationNotSent(errors.New("command request is not normalized to an explicit target"))
	}
	request.RequestID = brokerOperationRequestID
	request.Args = append([]string(nil), request.Args...)

	reply, err := o.exchange(ctx, route, ports.BrokerDaemonStartIfNeeded, request)
	if err != nil {
		if errors.Is(err, errBrokerOperationLost) {
			o.reconcile(route)
			return unknownBrokerCommandResult(brokerOperationRequestID, "command outcome unknown"), nil
		}
		return protocol.CommandResult{}, err
	}
	result, ok := reply.(protocol.CommandResult)
	if !ok || result.RequestID != brokerOperationRequestID || !result.Valid() {
		logBrokerOperationCause("command reply", fmt.Errorf("unexpected reply %T", reply))
		o.reconcile(route)
		return unknownBrokerCommandResult(brokerOperationRequestID, "command outcome unknown"), nil
	}
	if result.Outcome == protocol.CommandOutcomeUnknown {
		o.reconcile(route)
	}
	return result, nil
}

// Kill asks the daemon to terminate one named session on its own control
// stream. The name is validated session-name authority and addresses the
// session's current incarnation at execution time, exactly as the v58 Kill
// message carries; no lifecycle-exact guarantee is announced that the message
// cannot carry.
func (o *BrokerOperations) Kill(ctx context.Context, route BrokerOperationRoute, name string) (protocol.KillResult, error) {
	if err := domain.ValidateSessionName(name); err != nil {
		return protocol.KillResult{}, brokerOperationNotSent(fmt.Errorf("kill session name: %w", err))
	}
	return o.kill(ctx, route, ports.BrokerDaemonStartIfNeeded, protocol.Kill{
		RequestID: brokerOperationRequestID,
		Name:      name,
		Scope:     protocol.KillSession,
	})
}

// KillAll asks the daemon to purge every session on its own control stream
// with an empty name and the KillAll scope. It never stops the daemon: final
// session removal is not a shutdown.
func (o *BrokerOperations) KillAll(ctx context.Context, route BrokerOperationRoute) (protocol.KillResult, error) {
	return o.kill(ctx, route, ports.BrokerDaemonStartIfNeeded, protocol.Kill{
		RequestID: brokerOperationRequestID,
		Scope:     protocol.KillAll,
	})
}

// StopDaemon asks the daemon to shut down on its own control stream with an
// empty name and the KillDaemon scope, under the ExistingOnly start mode so the
// stop can never start the target it is stopping. A success is exactly one
// correlated KillSucceeded: a close or EOF is never a success, and a lost
// acknowledgement is OutcomeUnknown.
func (o *BrokerOperations) StopDaemon(ctx context.Context, route BrokerOperationRoute) (protocol.KillResult, error) {
	return o.kill(ctx, route, ports.BrokerDaemonExistingOnly, protocol.Kill{
		RequestID: brokerOperationRequestID,
		Scope:     protocol.KillDaemon,
	})
}

// kill runs one Kill-family operation and maps its reply through the shared
// correlation table: a lost or wrong/malformed reply becomes a synthetic
// OutcomeUnknown with a nil error, while a terminal reply is returned verbatim.
func (o *BrokerOperations) kill(ctx context.Context, route BrokerOperationRoute, mode ports.BrokerDaemonStartMode, request protocol.Kill) (protocol.KillResult, error) {
	reply, err := o.exchange(ctx, route, mode, request)
	if err != nil {
		if errors.Is(err, errBrokerOperationLost) {
			o.reconcile(route)
			return unknownBrokerKillResult(brokerOperationRequestID, "kill outcome unknown"), nil
		}
		return protocol.KillResult{}, err
	}
	result, ok := reply.(protocol.KillResult)
	if !ok || !validBrokerKillResult(result, brokerOperationRequestID) {
		logBrokerOperationCause("kill reply", fmt.Errorf("unexpected reply %T", reply))
		o.reconcile(route)
		return unknownBrokerKillResult(brokerOperationRequestID, "kill outcome unknown"), nil
	}
	if result.Outcome == protocol.KillOutcomeUnknown {
		o.reconcile(route)
	}
	return result, nil
}

// exchange owns the whole lifecycle of one attempt: route revalidation against
// the current snapshot, one allocated stream identity, one bounded open, one
// dispatch, and exactly one correlated reply.
//
// It reports a definite not-sent outcome (wrapping ErrBrokerOperationNotSent)
// for every refusal that happens before SendClient is entered, and
// errBrokerOperationLost once the request may have reached the wire. The
// caller decides how each class maps onto a result.
func (o *BrokerOperations) exchange(ctx context.Context, route BrokerOperationRoute, mode ports.BrokerDaemonStartMode, message protocol.ClientMessage) (protocol.ServerMessage, error) {
	if err := route.Validate(); err != nil {
		return nil, brokerOperationNotSent(err)
	}
	opCtx, _, finish := newBoundedContext(ctx, o.clock, brokerOperationTimeout)
	defer finish()
	if err := opCtx.Err(); err != nil {
		return nil, brokerOperationNotSent(err)
	}
	stream, err := o.openControlStream(opCtx, route, mode)
	if err != nil {
		return nil, brokerOperationNotSent(err)
	}
	// The stream is owned by exactly this attempt: every exit path closes it.
	// Closing only this stream never disturbs the borrowed service or a sibling
	// stream, and the BrokerLogicalConnection contract requires Close to unblock
	// Send/Receive, so cancellation needs no service shutdown.
	defer func() { _ = stream.Close() }()
	if err := opCtx.Err(); err != nil {
		return nil, brokerOperationNotSent(err)
	}
	stop := context.AfterFunc(opCtx, func() { _ = stream.Close() })
	defer stop()

	if err := stream.SendClient(message); err != nil {
		// The request may have been partially delivered, so a send error never
		// proves the daemon did not act.
		logBrokerOperationCause("dispatch", err)
		return nil, errBrokerOperationLost
	}
	replies := make(chan brokerOperationReply, 1)
	go func() {
		reply, recvErr := stream.ReceiveServer()
		replies <- brokerOperationReply{message: reply, err: recvErr}
	}()
	var reply brokerOperationReply
	select {
	case reply = <-replies:
		// The bound is authoritative: if it lapsed while the reply was being
		// read, the reply is late and can never upgrade the verdict back to a
		// terminal result. Only a reply observed strictly inside the bound is
		// trusted.
		if opCtx.Err() != nil {
			return nil, errBrokerOperationLost
		}
	case <-opCtx.Done():
		// The bound lapsed or the caller cancelled before a reply was observed:
		// close the stream to unblock the read, join the reader, and keep the
		// verdict indeterminate. The channel is buffered so the reader always
		// publishes, and a reply that arrives concurrently with the bound can
		// never upgrade this attempt back to a terminal result.
		_ = stream.Close()
		<-replies
		return nil, errBrokerOperationLost
	}
	if reply.err != nil {
		logBrokerOperationCause("reply", reply.err)
		return nil, errBrokerOperationLost
	}
	return reply.message, nil
}

// brokerOperationReply is one joined read result from a control stream.
type brokerOperationReply struct {
	message protocol.ServerMessage
	err     error
}

// openControlStream revalidates the route against the current snapshot, then
// allocates one stream identity and opens the exact control stream it names.
// Policy is copied from the snapshot's selected observation and the process
// environment is never read.
func (o *BrokerOperations) openControlStream(ctx context.Context, route BrokerOperationRoute, mode ports.BrokerDaemonStartMode) (ports.BrokerLogicalConnection, error) {
	snapshot := o.service.Snapshot()
	if err := snapshot.Validate(); err != nil {
		return nil, fmt.Errorf("broker catalogue is unavailable: %w", err)
	}
	if route.Epoch != snapshot.Epoch {
		return nil, fmt.Errorf("broker operation route epoch %d does not match the current epoch %d", route.Epoch, snapshot.Epoch)
	}
	observation, ok := brokerOperationAuthority(snapshot, route.Destination)
	if !ok {
		return nil, errors.New("broker operation destination is not in the current snapshot")
	}
	stream, err := o.service.NextStreamID()
	if err != nil {
		return nil, err
	}
	request := ports.BrokerOpenStreamRequest{
		Epoch:      route.Epoch,
		Purpose:    ports.BrokerStreamControl,
		Local:      route.Destination.Local,
		Connection: o.service.ConnectionID(),
		Stream:     stream,
		Policy:     observation.Policy,
		StartMode:  mode,
	}
	if !route.Destination.Local {
		request.Endpoint = observation.Endpoint
		request.Registration = observation.Registration
	}
	if err := request.Validate(); err != nil {
		return nil, err
	}
	return o.service.OpenStream(ctx, request)
}

// brokerOperationAuthority selects the exact observation one destination
// names: the local daemon for a local destination, or the observation whose
// complete registration (endpoint, incarnation, and generation) is exactly
// equal for a remote one. A missing or replaced registration is never resolved
// by name or by fallback.
func brokerOperationAuthority(snapshot ports.BrokerSnapshot, destination ports.BrokerEndpointFence) (ports.BrokerDaemonObservation, bool) {
	for i := range snapshot.Daemons {
		observation := snapshot.Daemons[i]
		if destination.Local {
			if observation.Local {
				return observation, true
			}
			continue
		}
		if observation.Local {
			continue
		}
		if observation.Registration.Equal(destination.Registration) {
			return observation, true
		}
	}
	return ports.BrokerDaemonObservation{}, false
}

// reconcile asks for one coalesced re-observation of the operation's
// destination after an indeterminate outcome. The hint is non-blocking and is
// never awaited: it cannot prove the mutation succeeded, and a broker loss may
// simply drop it, after which the client resynchronizes normally. The local
// destination is normalized to the empty endpoint in both implementations.
func (o *BrokerOperations) reconcile(route BrokerOperationRoute) {
	if route.Destination.Local {
		o.service.RequestReconcile("")
		return
	}
	o.service.RequestReconcile(route.Destination.Registration.Endpoint)
}

// brokerOperationNotSent wraps a definite pre-dispatch refusal with the shared
// sentinel while keeping the exact cause reachable.
func brokerOperationNotSent(cause error) error {
	if cause == nil {
		return ErrBrokerOperationNotSent
	}
	return fmt.Errorf("%w: %w", ErrBrokerOperationNotSent, cause)
}

// unknownBrokerCommandResult builds a correctly correlated synthetic Unknown
// command outcome: the expected request ID, code zero, bounded presentation
// text, and no output. The diagnostic cause is logged, never placed in the
// result text.
func unknownBrokerCommandResult(requestID uint64, text string) protocol.CommandResult {
	return protocol.CommandResult{
		RequestID: requestID,
		Outcome:   protocol.CommandOutcomeUnknown,
		Text:      text,
	}
}

// unknownBrokerKillResult builds a correctly correlated synthetic Unknown kill
// outcome: the expected request ID, code zero, bounded presentation text, and
// no failures.
func unknownBrokerKillResult(requestID uint64, text string) protocol.KillResult {
	return protocol.KillResult{
		RequestID: requestID,
		Outcome:   protocol.KillOutcomeUnknown,
		Text:      text,
	}
}

// validBrokerKillResult reports whether a decoded kill reply is well formed and
// correlated to the expected request. The daemon's success and failure shapes
// require a consistent code, while a daemon-reported Unknown may carry a
// shutdown code.
func validBrokerKillResult(result protocol.KillResult, requestID uint64) bool {
	if result.RequestID != requestID {
		return false
	}
	switch result.Outcome {
	case protocol.KillSucceeded:
		return result.Code == 0
	case protocol.KillFailed:
		return result.Code != 0
	case protocol.KillOutcomeUnknown:
		return true
	default:
		return false
	}
}

// logBrokerOperationCause records a control-operation diagnostic without ever
// copying it into presentation text.
func logBrokerOperationCause(stage string, err error) {
	if err == nil {
		return
	}
	slog.Default().Debug("broker control operation indeterminate", "stage", stage, "err", err)
}
