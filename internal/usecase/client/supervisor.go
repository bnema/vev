package client

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"reflect"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bnema/vev/internal/ports"
)

// Autonomous client supervisor (Plan 001 P5.1a, offline and unactivated).
//
// The supervisor is the attachment-free, terminal-owning owner of one
// autonomous client process. It enters raw mode once, before it ever reaches
// for the broker, and owns a single terminal input lifetime for the whole run.
// While the broker connection is down it renders the picker and keeps
// reconnecting; it never attaches a session and never interprets terminal
// input. Only the picker presentation and the broker-connectivity lifecycle are
// exercised by this slice: PresentConnecting and PresentAttached are defined so
// the attach slices can drive them later, but nothing here enters them.
//
// Connectivity is one attempt at a time. An attempt connects, subscribes, and
// waits for the first committed publication before it is Ready; readiness
// resets the equal-jitter exponential retry cadence (100ms doubling to a 2s
// cap). A transient connect, subscribe, or snapshot failure, and the loss of an
// established connection observed through BrokerService.Done/Err while no
// logical stream is held, both retire the old subscription and service, wait
// the cadence, and reconnect under a fresh generation. A failure that can never
// succeed by retrying (an incompatible carriage or a conflicting policy) is
// surfaced once and stops automatic reconnection; only the domain's terminal
// codes end the process.
//
// A connection attempt is joined before Run returns. Since the connector
// contract requires Connect to honor cancellation, a completion that arrives
// after the supervisor has begun terminating is still drained and its service
// and subscription are closed, so a late superseded success never leaks. Raw
// mode is restored exactly once on every exit path, terminal EOF and process
// cancellation included.
//
// Parent cancellation settles the run: an attempt that settles at the same
// instant as cancellation is retired instead of adopted, and the run ends with
// ctx.Err without publishing a retry cadence, rendering ConnectivityRetryWait,
// or notifying a connectivity failure.

// Supervisor retry cadence bounds. The exponential cap doubles from the initial
// interval to the maximum; the equal-jitter interval is then half the cap plus
// a uniform fraction of the other half.
const (
	supervisorRetryInitial = 100 * time.Millisecond
	supervisorRetryMax     = 2 * time.Second
)

// Presentation is what the autonomous client is presenting on the terminal.
type Presentation uint8

const (
	// PresentPicker shows the session picker. P5.1a exercises only this state.
	PresentPicker Presentation = iota + 1
	// PresentConnecting shows the attach transition. Reserved for the attach
	// slice; the P5.1a driver never enters it.
	PresentConnecting
	// PresentAttached shows a live attachment. Reserved for the attach slice.
	PresentAttached
	// PresentTerminating is the terminal state: raw mode is being restored and
	// the process is leaving.
	PresentTerminating
)

func (p Presentation) String() string {
	switch p {
	case PresentPicker:
		return "picker"
	case PresentConnecting:
		return "connecting"
	case PresentAttached:
		return "attached"
	case PresentTerminating:
		return "terminating"
	default:
		return "unknown"
	}
}

// Connectivity is the broker-connection lifecycle.
type Connectivity uint8

const (
	// ConnectivityDisconnected holds no broker connection: either before the
	// first attempt, after a non-retryable failure, or while terminating.
	ConnectivityDisconnected Connectivity = iota + 1
	// ConnectivityConnectingBroker is one in-flight connect/subscribe/first
	// publication attempt.
	ConnectivityConnectingBroker
	// ConnectivityReady holds an established connection with its first
	// publication committed.
	ConnectivityReady
	// ConnectivityRetryWait waits the retry cadence before the next attempt.
	ConnectivityRetryWait
)

func (c Connectivity) String() string {
	switch c {
	case ConnectivityDisconnected:
		return "disconnected"
	case ConnectivityConnectingBroker:
		return "connecting_broker"
	case ConnectivityReady:
		return "ready"
	case ConnectivityRetryWait:
		return "retry_wait"
	default:
		return "unknown"
	}
}

// State is the immutable projection of the supervisor.
type State struct {
	Presentation Presentation
	Connectivity Connectivity
	// Attempt counts consecutive broker-connection failures since the last
	// Ready. It is zero before the first attempt and after Ready, and drives
	// the equal-jitter exponential backoff.
	Attempt uint64
	// Generation identifies the current connection attempt. It increases before
	// every attempt, so a settled attempt carries the generation that produced
	// it. Run joins each attempt before it advances, so every completion today
	// carries the current generation: the comparison in adoptAttempt is a
	// defensive invariant for a future concurrent driver, not a path this
	// driver can currently reach.
	Generation uint64
	// Err is the most recent visible connectivity failure, nil while healthy
	// and before any failure.
	Err error
}

// supervisorEventKind is one input to the pure supervisor reducer.
type supervisorEventKind uint8

const (
	// supervisorBeginAttempt starts one connect/subscribe attempt.
	supervisorBeginAttempt supervisorEventKind = iota + 1
	// supervisorReady marks the first committed publication of the current
	// attempt.
	supervisorReady
	// supervisorTransientFailure is a connect, subscribe, or first-publication
	// failure that is worth retrying.
	supervisorTransientFailure
	// supervisorBrokerLoss is the loss of an established connection observed
	// through BrokerService.Done/Err while no logical stream is held.
	supervisorBrokerLoss
	// supervisorAttachBegin marks the admission of one committed attachment
	// stream: the picker released input and the supervisor is presenting the
	// connecting state until the initial publication is committed.
	supervisorAttachBegin
	// supervisorAttached marks a committed initial publication (Plan 001
	// P5.3b): the worker proved the full output frame was written, flushed,
	// and UI-committed. Welcome alone never produces it.
	supervisorAttached
	// supervisorAttachEnded returns a settled attachment to the picker without
	// restarting the process. event.err carries the typed failure, if any.
	supervisorAttachEnded
	// supervisorNonRetryable is a failure that retrying can never fix.
	supervisorNonRetryable
	// supervisorTerminal ends the process: terminal EOF, process cancellation,
	// or a terminal broker failure.
	supervisorTerminal
)

type supervisorEvent struct {
	kind supervisorEventKind
	err  error
}

// reduceSupervisor is the pure connectivity/presentation reducer. It is
// side-effect free so the state machine can be exhaustively table-tested; the
// driver applies it under a mutex and renders the result.
func reduceSupervisor(state State, event supervisorEvent) State {
	switch event.kind {
	case supervisorBeginAttempt:
		state.Presentation = PresentPicker
		state.Connectivity = ConnectivityConnectingBroker
		state.Generation++
		state.Err = nil
	case supervisorReady:
		state.Connectivity = ConnectivityReady
		state.Attempt = 0
		state.Err = nil
	case supervisorTransientFailure, supervisorBrokerLoss:
		state.Connectivity = ConnectivityRetryWait
		state.Attempt++
		state.Err = event.err
	case supervisorAttachBegin:
		// Connecting is the honest presentation until the committed initial
		// publication: the broker connection is still ready and the attempt
		// cadence is untouched.
		state.Presentation = PresentConnecting
		state.Err = nil
	case supervisorAttached:
		state.Presentation = PresentAttached
		state.Err = nil
	case supervisorAttachEnded:
		// A settled attachment always returns to the picker in the same
		// process, attached or not; a stream that fails after attachment must
		// never leave the attached presentation showing.
		state.Presentation = PresentPicker
		state.Err = event.err
	case supervisorNonRetryable:
		state.Connectivity = ConnectivityDisconnected
		state.Err = event.err
	case supervisorTerminal:
		state.Presentation = PresentTerminating
		state.Connectivity = ConnectivityDisconnected
		state.Err = event.err
	}
	return state
}

// SupervisorConfig supplies the autonomous client's dependencies. The
// connector, terminal, and clock are required; render, notify, and jitter are
// injectable seams with safe defaults. Render, notify, and jitter must not
// block: the supervisor drives them from its single run goroutine.
type SupervisorConfig struct {
	// Connector establishes the broker connection. Required.
	Connector ports.BrokerConnector
	// Terminal is the controlling terminal the supervisor owns. Required.
	Terminal ports.Terminal
	// Clock supplies the retry timers. Required; use ports.Clock, not the wall
	// clock.
	Clock ports.Clock
	// Render paints the current state. It is called once per visible
	// transition. Optional; the default renders nothing.
	Render func(State)
	// Notify surfaces a connectivity failure without leaving the picker.
	// Optional; the default notifies nothing.
	Notify func(State, error)
	// Jitter returns an equal-jitter fraction in [0,1). It is the only
	// randomness seam so tests can pin the retry cadence exactly. Optional; the
	// default is a process-local random source.
	Jitter func() float64
	// Picker is the client-owned picker (Plan 001 P5.2b). When set, the
	// supervisor folds every broker publication into it and hands it every
	// terminal read from the same single input lifetime used for EOF detection,
	// so the picker never starts a second reader. When nil the supervisor drops
	// terminal input exactly as P5.1a does. Optional.
	Picker pickerHost
	// AttachmentEnvironment is composition-owned process context copied into
	// each attachment Hello. The use case deliberately does not inspect the
	// process environment; omitted fields retain their protocol zero values.
	AttachmentEnvironment AttachmentEnvironment
}

// AttachmentEnvironment is the composition seam for client environment data
// carried by attachment Hello messages.
type AttachmentEnvironment struct {
	TermEnv   string
	Cwd       string
	TrueColor bool
}

// Supervisor owns one autonomous client process: raw mode, one terminal input
// lifetime, and the broker-connectivity lifecycle.
type Supervisor struct {
	cfg SupervisorConfig

	mu    sync.Mutex
	state State

	// attachments is the single foreground host every committed attachment
	// runs through (Plan 001 P5.3b). The supervisor owns it for the whole run
	// and retains close authority over each stream it admits.
	attachments *attachmentHost
	// nextStream is the strictly increasing logical stream ID allocator for
	// the current broker connection. Failed admitted opens consume their ID,
	// exactly as the pool contract requires.
	nextStream atomic.Uint64
	// nextAttachment identifies one attachment run within the connection
	// attempt generation.
	nextAttachment atomic.Uint64
	// clientID is the stable client identity carried in every Hello this
	// supervisor sends, across reconnects and attachments.
	clientID [16]byte
}

// NewSupervisor validates the required dependencies and returns a supervisor
// parked on the picker with no connection. It does not touch the terminal; Run
// enters raw mode.
func NewSupervisor(cfg SupervisorConfig) (*Supervisor, error) {
	if supervisorNil(cfg.Connector) {
		return nil, errors.New("vev: supervisor requires a broker connector")
	}
	if supervisorNil(cfg.Terminal) {
		return nil, errors.New("vev: supervisor requires a terminal")
	}
	if supervisorNil(cfg.Clock) {
		return nil, errors.New("vev: supervisor requires a clock")
	}
	if cfg.Render == nil {
		cfg.Render = func(State) {}
	}
	if supervisorNil(cfg.Picker) {
		// Normalize a typed-nil picker so Run's nil check and the input
		// lifetime's consumer check are both safe.
		cfg.Picker = nil
	}
	if cfg.Jitter == nil {
		cfg.Jitter = rand.Float64
	}
	supervisor := &Supervisor{
		cfg:      cfg,
		state:    State{Presentation: PresentPicker, Connectivity: ConnectivityDisconnected},
		clientID: newClientID(),
	}
	// The host is the supervisor's existing foreground grant. It owns no raw
	// mode and starts no reader: the supervisor keeps its one terminal input
	// lifetime, so the attachment path adds neither a second reader nor a
	// second writer. Terminal input pumping into a session and UI actions are
	// deliberately left to P5.4 (see the P5.1b integration note).
	supervisor.attachments = newAttachmentHost(attachmentHostConfig{
		Terminal:   cfg.Terminal,
		Clock:      cfg.Clock,
		OnAttached: func(AttachmentToken) { supervisor.transition(supervisorEvent{kind: supervisorAttached}) },
	})
	return supervisor, nil
}

// State returns the current immutable projection.
func (s *Supervisor) State() State {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.state
}

// Run owns the terminal for exactly its lifetime. It enters raw mode before it
// connects to the broker, starts the single terminal input reader, and then
// drives connect/subscribe/retry until the terminal reaches EOF, the process
// context is cancelled, or a terminal broker failure settles the process. Raw
// mode is restored exactly once on every exit path.
func (s *Supervisor) Run(ctx context.Context) (retErr error) {
	restore, err := s.cfg.Terminal.EnterRaw()
	if err != nil {
		return fmt.Errorf("vev: entering raw mode: %w", err)
	}
	if restore == nil {
		restore = func() error { return nil }
	}
	restoreTerminal := sync.OnceValue(restore)
	defer func() {
		if rerr := restoreTerminal(); rerr != nil && retErr == nil {
			retErr = fmt.Errorf("vev: restoring terminal: %w", rerr)
		}
	}()

	input := startTerminalInputLifetime(s.cfg.Terminal.In(), s.cfg.Picker)

	// The active connection is owned across loop iterations. retire is
	// idempotent so every exit path closes exactly the connection it adopted.
	var (
		service ports.BrokerService
		sub     ports.BrokerSubscription
	)
	retire := func() {
		if sub != nil {
			sub.Close()
			sub = nil
		}
		if service != nil {
			_ = service.Close()
			service = nil
		}
	}
	defer retire()

	for {
		// The generation is claimed before the connector runs, so the attempt is
		// always attributable to the state that started it and a completion can be
		// matched against the generation that produced it.
		s.transition(supervisorEvent{kind: supervisorBeginAttempt})
		generation := s.State().Generation
		attemptCtx, cancelAttempt := context.WithCancel(ctx)
		outcome := make(chan supervisorAttempt, 1)
		go func() { outcome <- s.connectAttempt(attemptCtx, generation) }()

		result, terminated, termErr := s.awaitAttempt(ctx, input, outcome)
		if terminated {
			// Connect must honor cancellation, so joining the attempt here both
			// retires a late superseded success and leaves no goroutine behind.
			cancelAttempt()
			late := <-outcome
			late.retire()
			s.transition(supervisorEvent{kind: supervisorTerminal, err: termErr})
			return termErr
		}
		cancelAttempt()
		adopted, cause := s.adoptAttempt(ctx, result)
		if !adopted {
			// The cancelled or superseded attempt is retired rather than adopted, so
			// it cannot drive the state of a newer attempt and a cancelled run never
			// publishes a retry cadence, renders ConnectivityRetryWait, or notifies a
			// connectivity failure for it.
			result.retire()
			if cause != nil {
				s.transition(supervisorEvent{kind: supervisorTerminal, err: cause})
				return cause
			}
			continue
		}

		if result.err != nil {
			if supervisorTerminalFailure(result.err) {
				s.transition(supervisorEvent{kind: supervisorTerminal, err: result.err})
				return result.err
			}
			if !supervisorRetryable(result.err) {
				// Retrying can never fix this. Return to the picker with the
				// visible error and stop reconnecting; the process still owns
				// the terminal until the user exits or it is cancelled.
				s.transition(supervisorEvent{kind: supervisorNonRetryable, err: result.err})
				s.notify(result.err)
				terminalErr := s.awaitTermination(ctx, input)
				s.transition(supervisorEvent{kind: supervisorTerminal, err: terminalErr})
				return terminalErr
			}
			if terminated, termErr := s.retryFailure(ctx, input, supervisorTransientFailure, result.err); terminated {
				return termErr
			}
			continue
		}

		service, sub = result.service, result.sub
		if s.cfg.Picker != nil {
			// Render the first committed publication immediately, before waiting
			// for the next one; the picker never shows a stale empty catalogue
			// while an established connection already has state.
			s.cfg.Picker.ApplySnapshot(service.Snapshot())
		}
		s.transition(supervisorEvent{kind: supervisorReady})

		// Ready phase: fold publications, admit committed attachments one at a
		// time, and return to the picker after each. A committed attachment
		// never replaces the broker connection; only a broker loss leaves this
		// loop, and cancellation or terminal EOF ends the run.
		var lossErr error
	ready:
		for {
			ready := s.awaitReady(ctx, input, service, sub)
			if ready.terminated {
				retire()
				s.transition(supervisorEvent{kind: supervisorTerminal, err: ready.termErr})
				return ready.termErr
			}
			if ready.commitKey == "" {
				lossErr = ready.lossErr
				break ready
			}
			if terminated, termErr := s.runCommittedAttachment(ctx, input, service, ready.commitKey); terminated {
				retire()
				s.transition(supervisorEvent{kind: supervisorTerminal, err: termErr})
				return termErr
			}
		}
		retire()
		if cause := ctx.Err(); cause != nil {
			// Parent cancellation wins over a loss that settled at the same instant,
			// exactly as it does at the attempt settle. The connection was already
			// released above, so the loss is simply never turned into a retry
			// cadence, a ConnectivityRetryWait render, or a notification for a run
			// that was already cancelled.
			s.transition(supervisorEvent{kind: supervisorTerminal, err: cause})
			return cause
		}
		if terminated, termErr := s.retryFailure(ctx, input, supervisorBrokerLoss, normalizeUnavailable(lossErr)); terminated {
			return termErr
		}
	}
}

// supervisorAttempt is the settled outcome of one connect/subscribe/first
// publication attempt. generation is the attempt's generation, claimed before
// the connector ran, so the completion is always attributable to the attempt
// that produced it (see adoptAttempt's generation fence).
type supervisorAttempt struct {
	generation uint64
	service    ports.BrokerService
	sub        ports.BrokerSubscription
	err        error
}

// retire closes whatever the attempt produced. It is safe on a partially built
// or empty attempt, and it treats a typed-nil service or subscription as absent
// rather than calling into it.
func (a supervisorAttempt) retire() {
	if !supervisorNil(a.sub) {
		a.sub.Close()
	}
	if !supervisorNil(a.service) {
		_ = a.service.Close()
	}
}

// adoptAttempt decides whether one settled attempt may be adopted, and reports the
// parent cancellation that must settle the run instead. The decision is made under
// the state lock, atomically with respect to the state it consults, so an attempt
// that settled after the parent cancelled the run is never adopted even though
// awaitAttempt's select may have taken its outcome over an already-ready
// ctx.Done. The generation fence is enforced at the same decision point: a
// superseded attempt is rejected too. It is a defensive invariant, not a
// currently reachable production path, because Run joins each attempt before it
// advances; it is kept so a future driver that admits more than one attempt at a
// time can never let a late completion drive the state of a newer attempt.
func (s *Supervisor) adoptAttempt(ctx context.Context, result supervisorAttempt) (adopted bool, cause error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if cause := ctx.Err(); cause != nil {
		return false, cause
	}
	if result.generation != s.state.Generation {
		return false, nil
	}
	return true, nil
}

// retryFailure records one retryable connectivity failure, surfaces it, and
// waits the cadence for the next attempt. It is the shared tail of the transient
// connect/subscribe failure and the established-connection loss paths, so both
// select the same observable transition, notification, and backoff.
func (s *Supervisor) retryFailure(ctx context.Context, input *terminalInputLifetime, kind supervisorEventKind, err error) (bool, error) {
	if cause := ctx.Err(); cause != nil {
		s.transition(supervisorEvent{kind: supervisorTerminal, err: cause})
		return true, cause
	}
	s.transition(supervisorEvent{kind: kind, err: err})
	s.notify(err)
	if terminated, termErr := s.waitBackoff(ctx, input); terminated {
		s.transition(supervisorEvent{kind: supervisorTerminal, err: termErr})
		return true, termErr
	}
	return false, nil
}

// connectAttempt connects once, subscribes, and waits for the first committed
// publication. It returns a live service and subscription only when the
// connection is Ready; every failure closes whatever it opened before
// returning, so a caller has nothing to clean up on the error path. Every
// connect, subscribe, or first-publication failure is stamped with generation
// and normalized to a typed unavailable failure so the run loop never observes
// an untyped cause.
func (s *Supervisor) connectAttempt(ctx context.Context, generation uint64) supervisorAttempt {
	service, err := s.cfg.Connector.Connect(ctx)
	if err != nil {
		return supervisorAttempt{generation: generation, err: normalizeUnavailable(err)}
	}
	if supervisorNil(service) {
		return supervisorAttempt{generation: generation, err: ports.BrokerError{
			Code: ports.BrokerErrorUnavailable,
			Text: "broker connector returned no service",
		}}
	}
	sub, err := service.Subscribe()
	if err != nil {
		_ = service.Close()
		return supervisorAttempt{generation: generation, err: normalizeUnavailable(err)}
	}
	if supervisorNil(sub) {
		// A service that reports no subscription cannot ever become Ready, so it
		// is a typed unavailable failure rather than a nil dereference on the
		// first Changed() call.
		_ = service.Close()
		return supervisorAttempt{generation: generation, err: ports.BrokerError{
			Code: ports.BrokerErrorUnavailable,
			Text: "broker service returned no subscription",
		}}
	}
	for {
		if service.Snapshot().Epoch != 0 {
			return supervisorAttempt{generation: generation, service: service, sub: sub}
		}
		select {
		case <-sub.Changed():
			continue
		case <-service.Done():
			cause := normalizeUnavailable(service.Err())
			sub.Close()
			_ = service.Close()
			return supervisorAttempt{generation: generation, err: cause}
		case <-ctx.Done():
			sub.Close()
			_ = service.Close()
			return supervisorAttempt{generation: generation, err: ctx.Err()}
		}
	}
}

// awaitAttempt waits for the in-flight attempt to settle, or for the run to be
// terminated first.
func (s *Supervisor) awaitAttempt(ctx context.Context, input *terminalInputLifetime, outcome <-chan supervisorAttempt) (supervisorAttempt, bool, error) {
	select {
	case result := <-outcome:
		return result, false, nil
	case <-ctx.Done():
		return supervisorAttempt{}, true, ctx.Err()
	case err := <-input.EOF():
		return supervisorAttempt{}, true, terminalReadCause(err)
	}
}

// readyOutcome is the result of one wait inside the established-connection
// phase: either the broker connection was lost, the run was terminated, or a
// committed picker selection is ready to be attached.
type readyOutcome struct {
	// commitKey is the exact catalogue key captured with a commit decision. A
	// non-empty value names one attachment to admit; the ready loop never
	// resolves anything else.
	commitKey string
	// lossErr is the broker service's stable terminal cause when the
	// established connection was lost.
	lossErr error
	// terminated reports that the run itself must end, with termErr carrying
	// the process cancellation or terminal-read cause.
	terminated bool
	termErr    error
}

// awaitReady waits for an established connection to be lost, for a committed
// picker selection, or for the run to be terminated first. Publications are
// folded into the picker as they arrive; a commit decision wakes the loop so
// the supervisor can admit exactly the row the user committed. It opens no
// stream and starts no terminal work itself.
func (s *Supervisor) awaitReady(ctx context.Context, input *terminalInputLifetime, service ports.BrokerService, sub ports.BrokerSubscription) readyOutcome {
	var changed <-chan struct{}
	var ops <-chan struct{}
	if s.cfg.Picker != nil {
		if !supervisorNil(sub) {
			changed = sub.Changed()
		}
		ops = s.cfg.Picker.OpsReady()
	}
	for {
		select {
		case <-service.Done():
			return readyOutcome{lossErr: service.Err()}
		case <-changed:
			s.cfg.Picker.ApplySnapshot(service.Snapshot())
		case <-ops:
			if key, ok := s.takeCommittedKey(); ok {
				return readyOutcome{commitKey: key}
			}
		case <-ctx.Done():
			return readyOutcome{terminated: true, termErr: ctx.Err()}
		case err := <-input.EOF():
			return readyOutcome{terminated: true, termErr: terminalReadCause(err)}
		}
	}
}

// takeCommittedKey drains exactly one committed presentation decision. Only a
// commit with a captured catalogue key admits an attachment; every other
// decision (cursor moves, search, kill, close) is consumed and dropped here,
// so picker input alone can never open, write, or retarget a session.
func (s *Supervisor) takeCommittedKey() (string, bool) {
	picker := s.cfg.Picker
	if picker == nil {
		return "", false
	}
	op, key := picker.TakeOp()
	if !op.commit || key == "" {
		return "", false
	}
	return key, true
}

// awaitTermination parks the supervisor on the picker after a non-retryable
// failure until the terminal ends or the process is cancelled. A clean terminal
// EOF returns nil; every other cause is reported.
func (s *Supervisor) awaitTermination(ctx context.Context, input *terminalInputLifetime) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-input.EOF():
		return terminalReadCause(err)
	}
}

// waitBackoff waits one equal-jitter retry interval for the current failure
// count, or settles on cancellation or terminal EOF. The timer is always
// stopped and drained, so no timer outlives the attempt transition.
func (s *Supervisor) waitBackoff(ctx context.Context, input *terminalInputLifetime) (bool, error) {
	delay := supervisorBackoffDelay(s.State().Attempt, s.cfg.Jitter)
	if delay <= 0 {
		return false, nil
	}
	timer := s.cfg.Clock.NewTimer(delay)
	defer stopSupervisorTimer(timer)
	select {
	case <-timer.C():
		return false, nil
	case <-ctx.Done():
		return true, ctx.Err()
	case err := <-input.EOF():
		return true, terminalReadCause(err)
	}
}

// supervisorBackoffDelay is the equal-jitter exponential interval for one
// failure count: the cap doubles from supervisorRetryInitial to
// supervisorRetryMax, and the interval is half the cap plus a uniform fraction
// of the other half, so attempts spread across [cap/2, cap). It is pure so the
// cadence can be tested exactly.
func supervisorBackoffDelay(attempt uint64, jitter func() float64) time.Duration {
	if attempt == 0 {
		return 0
	}
	limit := supervisorRetryInitial
	for i := uint64(1); i < attempt && limit < supervisorRetryMax; i++ {
		limit *= 2
	}
	if limit > supervisorRetryMax {
		limit = supervisorRetryMax
	}
	half := limit / 2
	fraction := jitter()
	if !(fraction >= 0) {
		// NaN is unordered: both fraction < 0 and fraction > 1 are false for
		// it, so an explicit non-negative test maps NaN and every negative value
		// to the minimum fraction. Without this, a NaN would reach the duration
		// conversion and yield an undefined interval instead of the documented
		// half-cap minimum.
		fraction = 0
	}
	if fraction > 1 {
		fraction = 1
	}
	return half + time.Duration(fraction*float64(half))
}

// terminalReadCause maps a terminal read end to the Run result: a clean EOF is
// an orderly exit (nil), any other read failure is reported.
func terminalReadCause(err error) error {
	if err == nil || errors.Is(err, io.EOF) {
		return nil
	}
	return err
}

// stopSupervisorTimer stops and drains a timer, supporting buffered timer
// adapters with pre-Go-1.23 reset semantics.
func stopSupervisorTimer(timer ports.Timer) {
	if timer == nil {
		return
	}
	if !timer.Stop() {
		select {
		case <-timer.C():
		default:
		}
	}
}

// supervisorTerminalFailure reports whether a broker failure must end the
// process. Only the domain's terminal codes do; every other failure returns the
// client to the picker.
func supervisorTerminalFailure(err error) bool {
	var typed ports.BrokerError
	return errors.As(err, &typed) && typed.Terminal()
}

// supervisorRetryable reports whether a failure is worth another attempt after
// backoff. An incompatible carriage or a conflicting policy can never succeed
// by retrying, so it is surfaced once and automatic reconnection stops; every
// other failure is treated as transient.
func supervisorRetryable(err error) bool {
	var typed ports.BrokerError
	if errors.As(err, &typed) {
		switch typed.Code {
		case ports.BrokerErrorIncompatible, ports.BrokerErrorConflictingPolicy:
			return false
		}
	}
	return true
}

// normalizeUnavailable presents one connect, subscribe, or connection-loss
// failure as a typed ports.BrokerError. A cause that is already a BrokerError is
// returned unchanged, so its own code (for example incompatible or conflicting
// policy) still selects the non-retryable path; every untyped failure, including
// a nil cause, becomes an unavailable BrokerError that preserves the original
// error as its local-only Cause.
func normalizeUnavailable(err error) error {
	if err == nil {
		return ports.BrokerError{Code: ports.BrokerErrorUnavailable, Text: "broker connection lost"}
	}
	var typed ports.BrokerError
	if errors.As(err, &typed) {
		return err
	}
	return ports.BrokerError{Code: ports.BrokerErrorUnavailable, Text: "broker connection lost", Cause: err}
}

// notify surfaces a connectivity failure through the optional notifier.
func (s *Supervisor) notify(err error) {
	if s.cfg.Notify == nil || err == nil {
		return
	}
	s.cfg.Notify(s.State(), err)
}

// transition applies one reducer event under the supervisor mutex and renders
// the resulting state outside it, so a renderer that reads State back never
// deadlocks.
func (s *Supervisor) transition(event supervisorEvent) {
	s.mu.Lock()
	s.state = reduceSupervisor(s.state, event)
	state := s.state
	s.mu.Unlock()
	s.cfg.Render(state)
}

// terminalInputLifetime owns the single read of the controlling terminal. It is
// started once and never replaced: a bare io.Reader cannot be interrupted, so
// the goroutine may outlive Run when stdin never reaches EOF, exactly like the
// attach client's input pump. It never closes caller-owned input. When a picker
// consumer is supplied (Plan 001 P5.2b) each read is handed to it from this one
// reader, so the picker never starts a second reader; without a consumer the
// bytes are discarded exactly as P5.1a does.
type terminalInputLifetime struct {
	eof      chan error
	once     sync.Once
	consumer pickerInputConsumer
}

// startTerminalInputLifetime starts the single terminal read. A nil reader is
// treated as an immediate orderly EOF. A nil consumer discards read bytes; a
// non-nil consumer owns them for the picker presentation.
func startTerminalInputLifetime(in io.Reader, consumer pickerInputConsumer) *terminalInputLifetime {
	lifetime := &terminalInputLifetime{eof: make(chan error, 1), consumer: consumer}
	if supervisorNil(in) {
		lifetime.finish(io.EOF)
		return lifetime
	}
	go func() {
		buffer := make([]byte, 4096)
		for {
			n, err := in.Read(buffer)
			if err != nil {
				lifetime.finish(err)
				return
			}
			if n == 0 {
				// A zero-byte, nil-error read is legal but cannot make
				// progress; keep reading so a well-behaved reader is never
				// mistaken for EOF.
				continue
			}
			if lifetime.consumer != nil {
				// The consumer may retain a partial escape or UTF-8 prefix, so it
				// gets its own copy rather than the reused read buffer.
				lifetime.consumer.ConsumeTerminalRead(append([]byte(nil), buffer[:n]...))
			}
		}
	}()
	return lifetime
}

// finish publishes the terminal cause exactly once.
func (l *terminalInputLifetime) finish(err error) {
	l.once.Do(func() { l.eof <- err })
}

// EOF is closed-result channel delivering the terminal read cause exactly once.
func (l *terminalInputLifetime) EOF() <-chan error { return l.eof }

// supervisorNil reports whether a dependency is nil in any shape. A typed nil
// satisfies a plain != nil comparison, so the guard would defer the failure to
// the first method call, which panics; the reflection check keeps it total.
func supervisorNil(dependency any) bool {
	if dependency == nil {
		return true
	}
	value := reflect.ValueOf(dependency)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Ptr, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}
