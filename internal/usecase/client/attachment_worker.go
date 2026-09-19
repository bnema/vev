package client

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
)

// Attachment worker mechanisms (Plan 001 P5.1b, offline and unactivated).
//
// This file extracts the pieces of the legacy attach attempt that the
// autonomous supervisor must own, without changing the legacy Runner and
// without wiring the supervisor into production. It reuses the existing
// terminal input pump (terminalInputPump) and foreground output gate
// (foregroundSendLease) rather than duplicating them.
//
// Ownership split:
//
//   - The supervisor owns the terminal: raw mode, the terminal writer, the
//     single input pump, the UI publication transaction, resize events, and the
//     process lifecycle. None of those move into a worker.
//   - One AttachmentWorker owns exactly one broker logical stream and the typed
//     session I/O on it. It cannot enter or restore raw mode, choose an
//     endpoint, restore a source route, retain an attachment in the cache, or
//     write the terminal directly.
//   - Every terminal-facing action a worker performs crosses the
//     supervisor-granted AttachmentForeground. The supervisor grants exactly
//     one foreground at a time; on cancellation or supersession it finalizes
//     that grant (closing Done and refusing output, UI, resize, and new input),
//     lets the woken worker Ack/Preserve the read it holds through a bounded
//     join, then revokes input authority before closing the owned stream.
//   - Every worker result carries the AttachmentToken (generation/attempt) the
//     supervisor granted, so a late result from a canceled or superseded
//     generation is discarded rather than applied.

// Integration requirement: P5.1a currently owns a rival terminal reader. Before
// activation it must use this same pump for picker and attachment consumers;
// never start both readers. Runner and supervisor wiring remain unchanged.
//
// UI actions are deliberately not part of this slice. AttachmentForeground has
// no action admission seam and attachmentHostConfig has no Actions field:
// binding UI.bindForeground and admitting actions requires the P5 supervisor's
// automation loop, which does not exist yet, so exposing it would publish a
// generation no component can drive. The transactional UI output path
// (ports.UIOutputTransaction, reached through Output) stays independent and
// usable. When the P5 supervisor owns input automation, it must bind and revoke
// UI actions at the same foreground boundaries this host finalizes.

// attachmentWorkerJoinTimeout bounds the supervisor's join of a cancelled
// worker during the final input window. A worker that honors neither
// cancellation nor a closed Done is abandoned rather than allowed to pin the
// supervisor; the supervisor then revokes the retained consumer and closes the
// stream anyway. It mirrors the attachment retirement join budget.
const attachmentWorkerJoinTimeout = 5 * time.Second

// errAttachmentForegroundRevoked reports a worker action refused because the
// supervisor revoked its input/output authority.
var errAttachmentForegroundRevoked = errors.New("client: attachment foreground authority revoked")

// AttachmentToken identifies one attachment worker run. Generation is the
// supervisor-assigned foreground generation. The host never allocates it while
// UI action binding is deferred, so the supervisor supplies it and the host
// stamps it onto the event and every publication; Attempt is the worker
// identity, not a UI generation. A zero Generation is never granted.
type AttachmentToken struct {
	Generation uint64
	Attempt    uint64
}

// IsZero reports whether the token was never assigned by a supervisor.
func (t AttachmentToken) IsZero() bool { return t.Generation == 0 }

// String renders the token for diagnostics.
func (t AttachmentToken) String() string {
	return fmt.Sprintf("gen=%d attempt=%d", t.Generation, t.Attempt)
}

// AttachmentEventKind classifies one worker's terminal outcome. Every kind
// returns the client to the picker: a worker event never terminates the process
// by itself. Only the supervisor's own state machine decides that.
type AttachmentEventKind uint8

const (
	// AttachmentEventEnded is an orderly end: the logical stream reached EOF or
	// the worker detached without failure.
	AttachmentEventEnded AttachmentEventKind = iota + 1
	// AttachmentEventLost is a transport or logical-stream loss. The logical
	// stream's own Done/Err carries the stable cause.
	AttachmentEventLost
	// AttachmentEventFailed is a typed protocol or application failure on the
	// stream, distinct from a carriage loss.
	AttachmentEventFailed
)

// String renders the event kind for diagnostics.
func (k AttachmentEventKind) String() string {
	switch k {
	case AttachmentEventEnded:
		return "ended"
	case AttachmentEventLost:
		return "lost"
	case AttachmentEventFailed:
		return "failed"
	default:
		return "unknown"
	}
}

// AttachmentEvent is the typed result of one worker run. Token is always the
// token the supervisor granted: the host stamps it, so a worker cannot report
// under another generation.
type AttachmentEvent struct {
	Token AttachmentToken
	Kind  AttachmentEventKind
	Err   error
}

// AttachmentInputEvent is one authorized terminal input delivery. Err carries a
// terminal read failure (for example io.EOF) alongside any final bytes.
type AttachmentInputEvent struct {
	Data []byte
	Err  error
}

// AttachmentForeground is the supervisor-granted handle through which a worker
// touches the terminal. It is the worker's only path to input, resize, output,
// UI publication, and the attached transition. Every method is refused once the
// supervisor revokes the grant, so a canceled or superseded generation cannot
// read input, resize, write output, publish UI, or mark itself attached.
type AttachmentForeground interface {
	// Token is the generation/attempt identity of this grant.
	Token() AttachmentToken
	// Stream is the supervisor-admitted logical stream this worker owns for its
	// lifetime. The supervisor retains close authority.
	Stream() ports.BrokerLogicalConnection
	// Input blocks until the next authorized terminal input delivery. It returns
	// false once authority is revoked or ctx ends.
	Input(ctx context.Context) (AttachmentInputEvent, bool)
	// AckInput commits the last Input delivery so the shared reader may accept
	// the next one. It decides that delivery: at most one of AckInput and
	// PreserveInput may decide a delivery, and a second decision for the same
	// delivery is rejected as a no-op. It is a no-op once authority is revoked.
	AckInput()
	// PreserveInput re-queues the undecided bytes of the last Input delivery so
	// the next authorized attempt can replay them exactly once. It decides that
	// delivery: a duplicate decision (a later AckInput or PreserveInput for the
	// same delivery, or a call with no outstanding delivery) is rejected as a
	// no-op instead of re-queuing different bytes. It stays authorized while the
	// supervisor finalizes this grant (after Done closes and before revocation),
	// so a cancelled worker can still save input; it is a no-op once authority is
	// revoked.
	PreserveInput(data []byte)
	// Resize blocks until the next authorized terminal resize. It returns false
	// once authority is revoked or ctx ends. A nil or closed resize source is
	// disabled, not an attachment end; Resize waits for cancellation in that case.
	Resize(ctx context.Context) (domain.Geometry, bool)
	// Output publishes context and writes and flushes bytes in one authorized
	// transaction. Missing UI or terminal returns ports.ErrUIUnavailable.
	Output(uiContext ports.UIContext, data []byte) error
	// MarkAttached records the attached transition. It is refused once authority
	// is revoked.
	MarkAttached() bool
	// Attached reports whether MarkAttached has succeeded.
	Attached() bool
	// Done is closed when the supervisor finalizes this grant. It stops new
	// input, resize, output, and UI publication and wakes the worker, while
	// AckInput and PreserveInput stay authorized for the read the worker already
	// holds until the supervisor revokes the grant.
	Done() <-chan struct{}
}

// AttachmentWorker is the contract for one broker logical stream's typed
// session I/O. Run owns the stream returned by Stream (via the foreground) and
// must honor ctx cancellation: the supervisor closes the stream after revoking
// authority, which must unblock receive/send. Run must never enter or restore
// raw mode, choose an endpoint, restore a source route, or touch the attachment
// cache; none of those authorities are reachable from its arguments.
type AttachmentWorker interface {
	Run(ctx context.Context, fg AttachmentForeground) AttachmentEvent
}

// attachmentAuthorityState is the explicit lifecycle of the single foreground
// slot. Finalization is a first-class state rather than a side effect of a
// closed Done channel: while finalizing, only Ack/Preserve remain authorized,
// every other action and a competing grant are refused.
type attachmentAuthorityState uint8

const (
	// authorityIdle has no foreground owner.
	authorityIdle attachmentAuthorityState = iota
	// authorityActive grants the foreground every AttachmentForeground action.
	authorityActive
	// authorityFinalizing retains the same foreground only for Ack/Preserve
	// while the supervisor joins a cancelled worker. New input, resize, output,
	// UI publication, MarkAttached, and a competing grant are all refused.
	authorityFinalizing
)

// attachmentAuthority owns the single supervisor foreground slot. It is safe for
// concurrent use: the supervisor finalizes and revokes from its own goroutine
// while a worker acts from its run goroutine.
type attachmentAuthority struct {
	mu      sync.Mutex
	current *attachmentForeground
	state   attachmentAuthorityState
}

// grant installs fg as the sole foreground in the active state. It fails while
// another grant is live or while the shared input pump already has a consumer,
// which is what keeps exactly one foreground owner. The host never allocates a
// UI generation here: UI action binding is deferred to the P5 supervisor.
func (a *attachmentAuthority) grant(fg *attachmentForeground) bool {
	if a == nil || fg == nil {
		return false
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.current != nil {
		return false
	}
	if fg.input != nil {
		var ok bool
		fg.consumer, ok = fg.input.tryClaim()
		if !ok {
			return false
		}
	}
	a.current = fg
	a.state = authorityActive
	return true
}

// beginFinalize moves fg into the finalizing state: the worker keeps the slot
// and its pump consumer for Ack/Preserve, but every other action is refused.
// It reports false when fg no longer holds the slot.
func (a *attachmentAuthority) beginFinalize(fg *attachmentForeground) bool {
	if a == nil || fg == nil {
		return false
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.current != fg {
		return false
	}
	a.state = authorityFinalizing
	return true
}

// revoke clears fg if it is still the foreground, releasing the pump consumer
// and any future UI binding, and reports whether it was. It runs after the
// bounded join. Input release and clearing the slot are atomic with respect to
// the next grant.
func (a *attachmentAuthority) revoke(fg *attachmentForeground) bool {
	if a == nil || fg == nil {
		return false
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.current != fg {
		return false
	}
	if fg.input != nil && fg.consumer != 0 {
		fg.input.revoke(fg.consumer)
	}
	a.current = nil
	a.state = authorityIdle
	return true
}

// foreground is the sole synchronized ownership accessor. It returns the
// owner in both the active and finalizing states.
func (a *attachmentAuthority) foreground() *attachmentForeground {
	if a == nil {
		return nil
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.current
}

// actionAuthorized reports whether fg may take a terminal-facing action now: it
// must hold the slot and the slot must not be finalizing.
func (a *attachmentAuthority) actionAuthorized(fg *attachmentForeground) bool {
	if a == nil || fg == nil {
		return false
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.current == fg && a.state == authorityActive
}

// act runs fn under the authority lock exactly when fg may still act, so an
// action or state transition (the attached marker) is atomic with respect to
// finalization and revocation.
func (a *attachmentAuthority) act(fg *attachmentForeground, fn func()) bool {
	if a == nil || fg == nil {
		return false
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.current != fg || a.state != authorityActive {
		return false
	}
	fn()
	return true
}

// retain runs fn under the authority lock exactly when fg still owns the slot,
// including while finalizing. It is the Ack/Preserve path: those only commit or
// re-queue input the worker already held, so a cancelled generation may still
// save it during the final input window. Every other action uses act or
// actionAuthorized.
func (a *attachmentAuthority) retain(fg *attachmentForeground, fn func()) bool {
	if a == nil || fg == nil {
		return false
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.current != fg {
		return false
	}
	fn()
	return true
}

// attachmentHostConfig supplies the supervisor-owned dependencies. Terminal,
// clock, and input belong to the supervisor; UI and OnAttached are optional
// seams. There is no Actions seam: UI action admission needs the P5
// supervisor's automation loop before it can be exposed (see the package
// integration requirement above).
type attachmentHostConfig struct {
	// Terminal is the controlling terminal the supervisor owns. Optional only
	// so a mechanism test can omit output entirely; production supplies it.
	Terminal ports.Terminal
	// Clock supplies the bounded-join timer. Use ports.Clock, not the wall
	// clock.
	Clock ports.Clock
	// Input is the supervisor-owned single terminal reader. The host claims one
	// consumer per foreground and revokes it after finalization. Optional.
	Input *terminalInputPump
	// UI is the supervisor-owned UI publication transaction. When nil it is
	// taken from Terminal when that implements ports.UIOutputTransaction.
	UI ports.UIOutputTransaction
	// OnAttached observes a successful MarkAttached. Optional.
	OnAttached func(AttachmentToken)
}

// attachmentHost is the supervisor-side owner of the terminal foreground. It
// grants one worker generation at a time input/output authority over a single
// admitted logical stream, joins that worker within a bound, and discards late
// results. It owns no raw mode and restores no terminal: those stay with the
// supervisor.
type attachmentHost struct {
	term      ports.Terminal
	clock     ports.Clock
	input     *terminalInputPump
	ui        ports.UIOutputTransaction
	resizes   <-chan domain.Geometry
	onAttach  func(AttachmentToken)
	authority attachmentAuthority
}

// newAttachmentHost builds the reusable foreground host. A nil clock falls back
// to the system clock; a nil terminal leaves output refused but still enforces
// authority.
func newAttachmentHost(cfg attachmentHostConfig) *attachmentHost {
	clock := cfg.Clock
	if supervisorNil(clock) {
		clock = systemClock{}
	}
	ui := cfg.UI
	resizes := (<-chan domain.Geometry)(nil)
	if !supervisorNil(cfg.Terminal) {
		if supervisorNil(ui) {
			ui, _ = cfg.Terminal.(ports.UIOutputTransaction)
		}
		resizes = cfg.Terminal.ResizeEvents()
	}
	return &attachmentHost{
		term:     cfg.Terminal,
		clock:    clock,
		input:    cfg.Input,
		ui:       ui,
		resizes:  resizes,
		onAttach: cfg.OnAttached,
	}
}

// Begin grants one generation's foreground and starts its worker on a
// supervisor-joined goroutine. It never closes or otherwise mutates stream, so
// on false the caller retains sole ownership of it. It returns false when:
//
//   - the host is nil;
//   - the token is zero (no supervisor generation was assigned);
//   - the worker or stream is nil, in any typed-nil shape;
//   - another foreground is already live, or is finalizing a previous grant;
//   - the shared input pump already has a consumer (a picker or rival
//     foreground owns input).
//
// The granted token, event token, and published UI context always use the
// caller-supplied Generation. The caller must Wait or Cancel the returned run.
func (h *attachmentHost) Begin(ctx context.Context, token AttachmentToken, worker AttachmentWorker, stream ports.BrokerLogicalConnection) (*attachmentRun, bool) {
	if h == nil || token.IsZero() || supervisorNil(worker) || supervisorNil(stream) {
		return nil, false
	}
	if ctx == nil {
		ctx = context.Background()
	}
	workerCtx, cancel := context.WithCancel(ctx)
	fg := h.newForeground(token, stream)
	if !h.authority.grant(fg) {
		cancel()
		return nil, false
	}
	run := &attachmentRun{
		host:    h,
		token:   token,
		stream:  stream,
		fg:      fg,
		cancel:  cancel,
		outcome: make(chan AttachmentEvent, 1),
		done:    make(chan struct{}),
		retired: make(chan struct{}),
	}
	go func() {
		defer close(run.done)
		event := worker.Run(workerCtx, fg)
		// The host stamps the granted token: a worker can never report under a
		// generation it was not granted, and the settlement check is exact.
		event.Token = token
		run.outcome <- event
	}()
	return run, true
}

// Run is Begin followed immediately by Wait, for callers that join in place.
// The second result is false when the run was superseded or cancelled, in which
// case the worker's late event is discarded.
func (h *attachmentHost) Run(ctx context.Context, token AttachmentToken, worker AttachmentWorker, stream ports.BrokerLogicalConnection) (AttachmentEvent, bool) {
	run, ok := h.Begin(ctx, token, worker, stream)
	if !ok {
		return AttachmentEvent{}, false
	}
	return run.Wait(ctx)
}

// newForeground allocates a candidate; grant claims input before publishing it.
func (h *attachmentHost) newForeground(token AttachmentToken, stream ports.BrokerLogicalConnection) *attachmentForeground {
	return &attachmentForeground{
		authority:  &h.authority,
		token:      token,
		stream:     stream,
		input:      h.input,
		lease:      newForegroundSendLease(),
		term:       h.term,
		ui:         h.ui,
		resizes:    h.resizes,
		done:       make(chan struct{}),
		decided:    true,
		onAttached: h.onAttach,
	}
}

// attachmentRun is one in-flight worker generation owned by the supervisor.
type attachmentRun struct {
	host   *attachmentHost
	token  AttachmentToken
	stream ports.BrokerLogicalConnection
	fg     *attachmentForeground
	cancel context.CancelFunc
	// outcome is capacity one so a worker that settles after the supervisor has
	// already moved on can never block on its own return.
	outcome chan AttachmentEvent
	// done closes when the worker goroutine has returned.
	done      chan struct{}
	joinOnce  sync.Once
	stateMu   sync.Mutex
	retired   chan struct{}
	isRetired bool
}

// Wait joins the run. It returns the worker's typed event with adopted=true
// only when the worker settled on its own and the supervisor had not cancelled;
// a run that was cancelled or superseded is joined and its late event discarded
// (adopted=false). The caller's ctx stops waiting for an outcome; retirement
// then uses the separate bounded worker-join budget.
func (r *attachmentRun) Wait(ctx context.Context) (AttachmentEvent, bool) {
	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case event := <-r.outcome:
		// Settlement and external retirement compete at one state boundary.
		adopted := r.retire() && ctx.Err() == nil && event.Token == r.token
		r.cleanup()
		if adopted {
			return event, true
		}
	case <-r.retired:
		r.cleanup()
	case <-ctx.Done():
		r.Cancel()
	}
	return AttachmentEvent{}, false
}

// retire is authoritative even when outcome and cancellation are both ready.
func (r *attachmentRun) retire() bool {
	r.stateMu.Lock()
	defer r.stateMu.Unlock()
	if r.isRetired {
		return false
	}
	r.isRetired = true
	close(r.retired)
	return true
}

// Cancel marks retirement, runs the deterministic final input window, and
// returns once the worker is joined or the join bound expires and the retained
// consumer and stream are released.
func (r *attachmentRun) Cancel() {
	r.retire()
	r.cleanup()
}

// cleanup is the ordered teardown. It refuses further output/UI/resize/new input
// and closes Done, wakes the worker, then joins within the bound while the pump
// consumer stays authorized for Ack/Preserve, so a worker woken by Done can
// still save the read it holds. Only after the join (or its timeout) does it
// revoke the consumer and foreground slot, then close the owned stream.
func (r *attachmentRun) cleanup() {
	r.joinOnce.Do(func() {
		r.fg.finalize()
		r.cancel()
		r.join()
		r.fg.finish()
		_ = r.stream.Close()
	})
}

// join waits for the worker goroutine within the shared join bound. It allocates
// a timer only when the worker has not already returned.
func (r *attachmentRun) join() {
	select {
	case <-r.done:
		return
	default:
	}
	timer := r.host.clock.NewTimer(attachmentWorkerJoinTimeout)
	if supervisorNil(timer) {
		timer = systemClock{}.NewTimer(attachmentWorkerJoinTimeout)
	}
	defer stopSupervisorTimer(timer)
	select {
	case <-r.done:
	case <-timer.C():
	}
}

// attachmentForeground is the supervisor-granted implementation of
// AttachmentForeground. It reuses the existing terminal input pump for input
// and the existing foreground send lease for output serialization.
type attachmentForeground struct {
	authority *attachmentAuthority
	token     AttachmentToken
	stream    ports.BrokerLogicalConnection
	input     *terminalInputPump
	consumer  uint64
	lease     *foregroundSendLease
	term      ports.Terminal
	ui        ports.UIOutputTransaction
	resizes   <-chan domain.Geometry
	done      chan struct{}
	// finalizedOnce and finishedOnce split the single teardown into the
	// deterministic final input window: finalize closes Done and stops output
	// first, finish revokes the retained consumer only after the join.
	finalizedOnce sync.Once
	finishedOnce  sync.Once
	// deliveryMu guards the at-most-once input decision. Input opens the window
	// by marking a freshly received delivery undecided; AckInput or PreserveInput
	// decides it at most once. decided starts true, so a decision with no
	// outstanding delivery is refused instead of silently committing nothing.
	deliveryMu sync.Mutex
	decided    bool
	attached   atomic.Bool
	onAttached func(AttachmentToken)
}

// Token is the generation/attempt identity of this grant.
func (f *attachmentForeground) Token() AttachmentToken { return f.token }

// Stream is the supervisor-admitted logical stream this worker owns.
func (f *attachmentForeground) Stream() ports.BrokerLogicalConnection { return f.stream }

// Done is closed when the supervisor starts finalizing this grant. It stops
// Input and Resize and wakes a worker so it can still Ack/Preserve the read it
// holds; the output gate is stopped at the same time.
func (f *attachmentForeground) Done() <-chan struct{} { return f.done }

// finalize opens the final input window exactly once: it moves the grant into
// the finalizing authority state, stops the output gate (waiting for any
// in-flight output transaction to end), and closes Done so Input/Resize refuse
// new work and the worker wakes. The pump consumer and foreground slot stay
// owned, so the worker may still Ack/Preserve the read it holds.
// Lock order is authority -> send lease; beginFinalize releases authority before
// waiting on the lease, so output never observes authority while it holds the
// lease.
func (f *attachmentForeground) finalize() {
	if f == nil {
		return
	}
	f.finalizedOnce.Do(func() {
		f.authority.beginFinalize(f)
		if f.lease != nil {
			f.lease.stop()
		}
		close(f.done)
	})
}

// finish revokes the retained consumer and clears the foreground slot after the
// bounded join. It runs before the stream is closed, so undecided input either
// reached the pump during finalization or is dropped with the revocation.
func (f *attachmentForeground) finish() {
	if f == nil {
		return
	}
	f.finishedOnce.Do(func() { f.authority.revoke(f) })
}

// beginDelivery opens the decision window for the delivery the worker just
// received: the next AckInput or PreserveInput decides it, and a further
// decision is refused until Input receives another delivery.
func (f *attachmentForeground) beginDelivery() {
	f.deliveryMu.Lock()
	f.decided = false
	f.deliveryMu.Unlock()
}

// decideDelivery reports whether this is the first decision for the current
// delivery and marks it decided. A second Ack/Preserve for the same delivery,
// or a decision with no outstanding delivery, is refused.
func (f *attachmentForeground) decideDelivery() bool {
	f.deliveryMu.Lock()
	defer f.deliveryMu.Unlock()
	if f.decided {
		return false
	}
	f.decided = true
	return true
}

// Input leases the next authorized terminal read. It waits on the pump's ready
// signal exactly like the attach scanner, so a shared single reader is never
// duplicated.
func (f *attachmentForeground) Input(ctx context.Context) (AttachmentInputEvent, bool) {
	if ctx == nil {
		ctx = context.Background()
	}
	for {
		if !f.authority.actionAuthorized(f) || f.input == nil {
			return AttachmentInputEvent{}, false
		}
		result, ok := f.input.take(ctx, f.consumer)
		if ok {
			f.beginDelivery()
			return AttachmentInputEvent{Data: result.data, Err: result.err}, true
		}
		if ctx.Err() != nil {
			return AttachmentInputEvent{}, false
		}
		select {
		case <-ctx.Done():
			return AttachmentInputEvent{}, false
		case <-f.done:
			return AttachmentInputEvent{}, false
		case <-f.input.ready:
		}
	}
}

// AckInput commits the last delivery so the shared reader may publish the next.
// It decides that delivery, so a duplicate Ack/Preserve for it is a no-op.
func (f *attachmentForeground) AckInput() {
	if f == nil || f.input == nil {
		return
	}
	if !f.decideDelivery() {
		return
	}
	f.authority.retain(f, func() { f.input.ack(f.consumer) })
}

// PreserveInput re-queues the undecided bytes of the last delivery for the next
// authorized attempt. It decides that delivery, so a duplicate Ack/Preserve for
// it is a no-op and can never re-queue different bytes. It stays authorized
// while the supervisor finalizes the grant.
func (f *attachmentForeground) PreserveInput(data []byte) {
	if f == nil || f.input == nil {
		return
	}
	if !f.decideDelivery() {
		return
	}
	f.authority.retain(f, func() {
		f.input.ack(f.consumer)
		f.input.preserveResidual(f.consumer, data)
	})
}

// Resize waits for the next authorized terminal resize.
func (f *attachmentForeground) Resize(ctx context.Context) (domain.Geometry, bool) {
	if ctx == nil {
		ctx = context.Background()
	}
	if !f.authority.actionAuthorized(f) {
		return domain.Geometry{}, false
	}
	resizes := f.resizes
	for {
		select {
		case geometry, ok := <-resizes:
			if !ok {
				resizes = nil
				continue
			} // uiterm has no resize source
			if !f.authority.actionAuthorized(f) {
				return domain.Geometry{}, false
			}
			return geometry, true
		case <-ctx.Done():
			return domain.Geometry{}, false
		case <-f.done:
			return domain.Geometry{}, false
		}
	}
}

// Output holds the same send lease across the entire UI observation boundary.
// UIOutputTransaction is an observation channel, not attachment commit
// authority: an unavailable or closed observation channel does not stop the
// terminal frame. Other publication failures abort. Successful terminal write,
// flush, and EndOutput(true) commit the frame; any failed or short write or
// failed flush leaves EndOutput(false).
func (f *attachmentForeground) Output(uiContext ports.UIContext, data []byte) error {
	if f == nil {
		return errAttachmentForegroundRevoked
	}
	var outputErr error
	ok := f.lease.send(func() bool {
		if !f.authority.actionAuthorized(f) {
			return false
		}
		if supervisorNil(f.ui) || supervisorNil(f.term) {
			outputErr = ports.ErrUIUnavailable
			return true
		}
		writer := f.term.Out()
		if supervisorNil(writer) {
			outputErr = ports.ErrUIUnavailable
			return true
		}
		uiContext.Generation = f.token.Generation
		f.ui.BeginOutput(uiContext)
		success := false
		defer func() { f.ui.EndOutput(success) }()
		// An unavailable capture means the optional UI observation channel is
		// disabled or closed, so the terminal frame still writes and flushes.
		// Any other publication error aborts before the frame is written.
		if err := f.ui.PublishContext(uiContext); err != nil && !errors.Is(err, ports.ErrUIUnavailable) {
			outputErr = err
			return true
		}
		var n int
		n, outputErr = writer.Write(data)
		if outputErr == nil && n != len(data) {
			outputErr = io.ErrShortWrite
		}
		if outputErr != nil {
			return true
		}
		outputErr = f.term.Flush()
		success = outputErr == nil
		return true
	})
	if !ok {
		return errAttachmentForegroundRevoked
	}
	return outputErr
}

// MarkAttached records the attached transition atomically with respect to
// finalization and revocation.
func (f *attachmentForeground) MarkAttached() bool {
	if f == nil {
		return false
	}
	set := f.authority.act(f, func() { f.attached.Store(true) })
	if !set {
		return false
	}
	if f.onAttached != nil {
		f.onAttached(f.token)
	}
	return true
}

// Attached reports whether MarkAttached has succeeded.
func (f *attachmentForeground) Attached() bool {
	return f != nil && f.attached.Load()
}
