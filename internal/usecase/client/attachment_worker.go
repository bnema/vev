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
	"github.com/bnema/vev/internal/protocol"
)

// Attachment worker mechanisms (Plan 001 P5.1b, offline and unactivated).
//
// This file defines the attachment mechanisms owned by the autonomous
// supervisor. It reuses the terminal input pump (terminalInputPump) and
// foreground output gate (foregroundSendLease) rather than duplicating them.
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

// Picker and attachment consumers use the same terminal input pump; they must
// never start rival readers.
//
// UI action admission is optional and reuses this host's single pump claim.
// Binding occurs after grant claims the pump and before the worker starts;
// release occurs when finalization starts, before the claim can return to the
// picker. The UI-generated generation is authoritative and is stamped onto the
// attachment token and every publication.

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
	Data     []byte
	Err      error
	actionID uint64
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
	// Resize blocks until the latest authorized terminal resize for this
	// attachment. It returns false once authority is revoked or ctx ends.
	Resize(ctx context.Context) (domain.Geometry, bool)
	// Lifecycle blocks for one explicit typed lifecycle action.
	Lifecycle(ctx context.Context) (AttachmentLifecycleActionKind, bool)
	// Output publishes context and writes and flushes bytes in one authorized
	// transaction. Missing UI or terminal returns ports.ErrUIUnavailable.
	Output(uiContext ports.UIContext, data []byte) error
	// PublishAttached publishes the committed attached presentation once the
	// first frame was written, flushed, and committed. It is fenced exactly like
	// Output and writes no terminal bytes: the screen it publishes is the frame
	// that already drained, so no second writer and no extra flush is needed.
	PublishAttached(uiContext ports.UIContext) error
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

func (a *attachmentAuthority) currentToken() AttachmentToken {
	if a == nil {
		return AttachmentToken{}
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.current == nil {
		return AttachmentToken{}
	}
	return a.current.token
}

// grant installs fg as the sole foreground in the active state. It fails while
// another grant is live or while the shared input pump already has a consumer,
// which is what keeps exactly one foreground owner.
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
// clock, and input belong to the supervisor; UI seams and OnAttached are
// optional.
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
	// Actions carries explicit lifecycle decisions for the current attachment.
	Actions <-chan AttachmentLifecycleAction
	// UI is the supervisor-owned UI publication transaction. When nil it is
	// taken from Terminal when that implements ports.UIOutputTransaction.
	UI ports.UIOutputTransaction
	// ActionUI optionally binds automation to the same claimed input consumer.
	ActionUI *UI
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
	actionUI  *UI
	resizes   <-chan domain.Geometry
	actions   <-chan AttachmentLifecycleAction
	onAttach  func(AttachmentToken)
	authority attachmentAuthority

	geometryMu         sync.Mutex
	geometry           domain.Geometry
	geometryValid      bool
	geometrySeq        uint64
	geometryUpdate     chan struct{}
	presentationUpdate chan struct{}
	geometryCancel     context.CancelFunc
	geometryDone       chan struct{}
	ownerBoundary      bool

	// navigationRequests carries a coalesced request from the attached worker
	// to compose the client's session picker over the live attachment (the
	// daemon's navigation PickerOffer). Only the supervisor consumes it.
	navigationRequests chan struct{}
	// internalActions carries supervisor-originated lifecycle decisions for the
	// current foreground, such as the detach that precedes an attachment swap.
	internalActions chan AttachmentLifecycleAction
	// routeDemand wakes the supervisor to publish routes when the committed
	// session changes; navigations carries the daemon's navigation requests
	// (Plan 003 C4/C5). Only the supervisor consumes them.
	routeDemand chan struct{}
	navigations chan protocol.ServerMessage
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
	var initialGeometry domain.Geometry
	var initialGeometryValid bool
	if !supervisorNil(cfg.Terminal) {
		if supervisorNil(ui) {
			ui, _ = cfg.Terminal.(ports.UIOutputTransaction)
		}
		resizes = cfg.Terminal.ResizeEvents()
		if geometry, err := cfg.Terminal.Geometry(); err == nil && geometry.Valid() {
			initialGeometry = geometry.NormalizePixels()
			initialGeometryValid = true
		}
	}
	return &attachmentHost{
		term:               cfg.Terminal,
		clock:              clock,
		input:              cfg.Input,
		ui:                 ui,
		actionUI:           cfg.ActionUI,
		resizes:            resizes,
		actions:            cfg.Actions,
		onAttach:           cfg.OnAttached,
		geometry:           initialGeometry,
		geometryValid:      initialGeometryValid,
		geometryUpdate:     make(chan struct{}, 1),
		presentationUpdate: make(chan struct{}, 1),
		navigationRequests: make(chan struct{}, 1),
		internalActions:    make(chan AttachmentLifecycleAction, 1),
		routeDemand:        make(chan struct{}, 1),
		navigations:        make(chan protocol.ServerMessage, 1),
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
// The granted token and event token always retain the caller-supplied
// supervisor generation. When action UI is configured, its independent
// generation is used only for UI publication and action fencing. The caller
// must Wait or Cancel the returned run.
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
	// A request or action left behind by a previous foreground never applies
	// to this one.
	h.drainForegroundSignals()
	if h.actionUI != nil {
		fg.uiGeneration = h.actionUI.bindForeground(workerCtx, fg.input, fg.consumer)
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
func (h *attachmentHost) setInput(input *terminalInputPump) {
	if h == nil {
		return
	}
	h.input = input
	h.ownerBoundary = true
}

func (h *attachmentHost) startGeometry(ctx context.Context) {
	if h == nil {
		return
	}
	if h.geometryDone != nil {
		select {
		case <-h.geometryDone:
			h.geometryDone = nil
			h.geometryCancel = nil
		default:
			return
		}
	}
	geometryCtx, cancel := context.WithCancel(ctx)
	h.geometryCancel = cancel
	h.geometryDone = make(chan struct{})
	go func() {
		defer close(h.geometryDone)
		for {
			select {
			case geometry, ok := <-h.resizes:
				if !ok {
					return
				}
				if !geometry.Valid() {
					continue
				}
				h.geometryMu.Lock()
				h.geometry = geometry.NormalizePixels()
				h.geometryValid = true
				h.geometrySeq++
				h.geometryMu.Unlock()
				select {
				case h.geometryUpdate <- struct{}{}:
				default:
				}
				// Presentation invalidation is deliberately independent from the
				// attachment geometry sequence/wakeup. The supervisor consumes this
				// coalesced signal and decides whether a picker presentation may be
				// repainted; this collector never renders directly.
				select {
				case h.presentationUpdate <- struct{}{}:
				default:
				}
			case <-geometryCtx.Done():
				return
			}
		}
	}()
}

func (h *attachmentHost) stopGeometry() {
	if h == nil || h.geometryCancel == nil {
		return
	}
	h.geometryCancel()
	<-h.geometryDone
	h.geometryCancel = nil
	h.geometryDone = nil
}

func (h *attachmentHost) latestGeometry() (domain.Geometry, bool) {
	if h == nil {
		return domain.Geometry{}, false
	}
	h.geometryMu.Lock()
	defer h.geometryMu.Unlock()
	return h.geometry, h.geometryValid
}

func (h *attachmentHost) geometryAfter(sequence uint64) (domain.Geometry, uint64, bool) {
	h.geometryMu.Lock()
	defer h.geometryMu.Unlock()
	return h.geometry, h.geometrySeq, h.geometryValid && h.geometrySeq > sequence
}

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
		actions:    h.actions,
		host:       h,
		done:       make(chan struct{}),
		decided:    true,
		onAttached: h.onAttach,
		repaint:    make(chan struct{}, 1),
		tabSelect:  make(chan domain.TabStableID, 1),
		routes:     make(chan protocol.RecentRouteSnapshot, 1),
		replies:    make(chan protocol.ClientMessage, attachmentReplyCapacity),
	}
}

// drainForegroundSignals drops any navigation request or internal lifecycle
// action that belonged to an earlier foreground.
func (h *attachmentHost) drainForegroundSignals() {
	for {
		select {
		case <-h.navigationRequests:
		case <-h.internalActions:
		case <-h.routeDemand:
		case <-h.navigations:
		default:
			return
		}
	}
}

// NavigationRequests wakes the supervisor when the attached worker asked for
// the client picker over the live attachment.
func (h *attachmentHost) NavigationRequests() <-chan struct{} {
	if h == nil {
		return nil
	}
	return h.navigationRequests
}

// beginNavigationOverlay composes the supervisor-owned picker over the current
// foreground: from its return, attachment output is applied and acknowledged
// but never written, and every authorized input delivery is handed to sink. It
// reports false when no active foreground can host the overlay.
func (h *attachmentHost) beginNavigationOverlay(sink pickerInputConsumer) bool {
	if h == nil || sink == nil {
		return false
	}
	fg := h.authority.foreground()
	if fg == nil {
		return false
	}
	return fg.setOverlay(attachmentOverlayNavigation, sink)
}

// endNavigationOverlay returns the terminal to the current foreground and asks
// its worker for an authoritative repaint over the picker box.
func (h *attachmentHost) endNavigationOverlay() {
	if h == nil {
		return
	}
	if fg := h.authority.foreground(); fg != nil {
		fg.clearOverlay(attachmentOverlayNavigation)
	}
}

// committedTarget reports the session the current foreground last committed
// output for, so the supervisor can tell a commit to the attached session from
// a commit elsewhere.
func (h *attachmentHost) committedTarget() (protocol.ExactSessionTarget, bool) {
	target, _, ok := h.committedView()
	return target, ok
}

// committedView reports the session and the tab the current foreground last
// committed output for.
func (h *attachmentHost) committedView() (protocol.ExactSessionTarget, domain.TabStableID, bool) {
	if h == nil {
		return protocol.ExactSessionTarget{}, "", false
	}
	fg := h.authority.foreground()
	if fg == nil {
		return protocol.ExactSessionTarget{}, "", false
	}
	fg.overlayMu.Lock()
	defer fg.overlayMu.Unlock()
	return fg.committed, fg.committedTab, fg.committedKnown
}

// committedTargetOrZero is committedTarget with an unknown target as zero.
func (h *attachmentHost) committedTargetOrZero() protocol.ExactSessionTarget {
	target, ok := h.committedTarget()
	if !ok {
		return protocol.ExactSessionTarget{}
	}
	return target
}

// requestTabSelection asks the current foreground's worker to show one tab of
// its attached session in place. The newest request replaces an unsent one.
func (h *attachmentHost) requestTabSelection(token AttachmentToken, tab domain.TabStableID) bool {
	if h == nil || tab == "" {
		return false
	}
	fg := h.authority.foreground()
	if fg == nil || fg.token != token {
		return false
	}
	for {
		select {
		case fg.tabSelect <- tab:
			return true
		default:
		}
		select {
		case <-fg.tabSelect:
		default:
		}
	}
}

// requestDetach asks the current foreground's worker to detach explicitly, so
// the daemon observes a clean Detach before the supervisor swaps attachments.
func (h *attachmentHost) requestDetach(token AttachmentToken) {
	if h == nil {
		return
	}
	select {
	case h.internalActions <- AttachmentLifecycleAction{Token: token, Kind: AttachmentDetachToPicker}:
	default:
	}
}

// attachmentOverlayKind names the client-composed overlay that owns the
// terminal over a live attachment.
type attachmentOverlayKind uint8

const (
	attachmentOverlayNone attachmentOverlayKind = iota
	// attachmentOverlayNavigation is the supervisor-owned session picker.
	attachmentOverlayNavigation
	// attachmentOverlayMove is the worker-owned move-destination picker the
	// serving daemon offered.
	attachmentOverlayMove
)

// attachmentOverlayForeground is the optional overlay seam of the real
// foreground. A worker reaches it by assertion so scripted foregrounds may
// omit it; without it, overlays are simply never presented.
type attachmentOverlayForeground interface {
	requestNavigationPicker()
	setOverlay(kind attachmentOverlayKind, sink pickerInputConsumer) bool
	clearOverlay(kind attachmentOverlayKind)
	overlayKind() attachmentOverlayKind
	divertInput(data []byte) bool
	overlayRepaint() <-chan struct{}
	overlayOutput(uiContext ports.UIContext, data []byte) error
	noteCommitted(target protocol.ExactSessionTarget, tab domain.TabStableID)
	tabSelections() <-chan domain.TabStableID
	routeSnapshots() <-chan protocol.RecentRouteSnapshot
	navigationReplies() <-chan protocol.ClientMessage
	requestNavigation(message protocol.ServerMessage)
}

var _ attachmentOverlayForeground = (*attachmentForeground)(nil)

// requestNavigationPicker records the daemon's navigation offer for the
// supervisor. The request is coalesced; the supervisor decides whether the
// current presentation can host the overlay.
func (f *attachmentForeground) requestNavigationPicker() {
	if f == nil || f.host == nil || !f.authority.actionAuthorized(f) {
		return
	}
	select {
	case f.host.navigationRequests <- struct{}{}:
	default:
	}
}

// setOverlay installs kind under the output lease, so no attachment frame is
// mid-write when the overlay takes the terminal. An occupied slot refuses a
// different overlay.
func (f *attachmentForeground) setOverlay(kind attachmentOverlayKind, sink pickerInputConsumer) bool {
	if f == nil || kind == attachmentOverlayNone {
		return false
	}
	installed := false
	f.lease.send(func() bool {
		if !f.authority.actionAuthorized(f) {
			return false
		}
		f.overlayMu.Lock()
		defer f.overlayMu.Unlock()
		if f.overlay != attachmentOverlayNone && f.overlay != kind {
			return true
		}
		f.overlay = kind
		f.overlaySink = sink
		installed = true
		return true
	})
	return installed
}

// clearOverlay releases kind under the output lease and asks the worker for an
// authoritative repaint: the terminal still shows the picker box and any
// attachment frames it suppressed were never written.
func (f *attachmentForeground) clearOverlay(kind attachmentOverlayKind) {
	if f == nil {
		return
	}
	cleared := false
	f.lease.send(func() bool {
		f.overlayMu.Lock()
		defer f.overlayMu.Unlock()
		if f.overlay != kind {
			return true
		}
		f.overlay = attachmentOverlayNone
		f.overlaySink = nil
		cleared = true
		return true
	})
	if !cleared {
		// The lease stops at finalization; the overlay dies with the grant.
		f.overlayMu.Lock()
		if f.overlay == kind {
			f.overlay = attachmentOverlayNone
			f.overlaySink = nil
		}
		f.overlayMu.Unlock()
		return
	}
	select {
	case f.repaint <- struct{}{}:
	default:
	}
}

func (f *attachmentForeground) overlayKind() attachmentOverlayKind {
	if f == nil {
		return attachmentOverlayNone
	}
	f.overlayMu.Lock()
	defer f.overlayMu.Unlock()
	return f.overlay
}

// divertInput hands one authorized delivery to the supervisor-owned overlay.
// It reports false when no navigation overlay owns input, so the worker keeps
// its session path.
func (f *attachmentForeground) divertInput(data []byte) bool {
	if f == nil {
		return false
	}
	f.overlayMu.Lock()
	sink := f.overlaySink
	kind := f.overlay
	f.overlayMu.Unlock()
	if kind != attachmentOverlayNavigation || sink == nil {
		return false
	}
	sink.ConsumeTerminalRead(data)
	return true
}

func (f *attachmentForeground) overlayRepaint() <-chan struct{} {
	if f == nil {
		return nil
	}
	return f.repaint
}

func (f *attachmentForeground) noteCommitted(target protocol.ExactSessionTarget, tab domain.TabStableID) {
	if f == nil {
		return
	}
	f.overlayMu.Lock()
	changed := !f.committedKnown || f.committed != target
	f.committed = target
	f.committedTab = tab
	f.committedKnown = true
	f.overlayMu.Unlock()
	if changed {
		f.demandRoutes()
	}
}

// tabSelections carries the supervisor's in-place tab switches to the worker.
func (f *attachmentForeground) tabSelections() <-chan domain.TabStableID {
	if f == nil {
		return nil
	}
	return f.tabSelect
}

// overlayOutput writes one overlay frame through the same lease and UI
// transaction as attachment output, bypassing only the overlay suppression.
func (f *attachmentForeground) overlayOutput(uiContext ports.UIContext, data []byte) error {
	return f.write(uiContext, data, true)
}

// attachmentQueryForeground is the optional terminal-query seam of the real
// foreground. Scripted foregrounds may omit it; the worker then skips palette
// detection but still strips terminal replies from input.
type attachmentQueryForeground interface {
	writeTerminalQuery(data []byte) error
}

var _ attachmentQueryForeground = (*attachmentForeground)(nil)

// writeTerminalQuery writes and flushes terminal query bytes (palette probes)
// under the output lease. They draw nothing, so neither an overlay nor the UI
// observation channel is involved.
func (f *attachmentForeground) writeTerminalQuery(data []byte) error {
	if f == nil {
		return errAttachmentForegroundRevoked
	}
	var err error
	ok := f.lease.send(func() bool {
		if !f.authority.actionAuthorized(f) {
			return false
		}
		if supervisorNil(f.term) || supervisorNil(f.term.Out()) {
			err = ports.ErrUIUnavailable
			return true
		}
		var n int
		n, err = f.term.Out().Write(data)
		if err == nil && n != len(data) {
			err = io.ErrShortWrite
		}
		if err == nil {
			err = f.term.Flush()
		}
		return true
	})
	if !ok {
		return errAttachmentForegroundRevoked
	}
	return err
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
	authority    *attachmentAuthority
	token        AttachmentToken
	stream       ports.BrokerLogicalConnection
	input        *terminalInputPump
	consumer     uint64
	lease        *foregroundSendLease
	term         ports.Terminal
	ui           ports.UIOutputTransaction
	uiGeneration uint64
	resizes      <-chan domain.Geometry
	actions      <-chan AttachmentLifecycleAction
	host         *attachmentHost
	done         chan struct{}
	// finalizedOnce and finishedOnce split the single teardown into the
	// deterministic final input window: finalize closes Done and stops output
	// first, finish revokes the retained consumer only after the join.
	finalizedOnce sync.Once
	finishedOnce  sync.Once
	// deliveryMu guards the at-most-once input decision. Input opens the window
	// by marking a freshly received delivery undecided; AckInput or PreserveInput
	// decides it at most once. decided starts true, so a decision with no
	// outstanding delivery is refused instead of silently committing nothing.
	deliveryMu  sync.Mutex
	decided     bool
	geometrySeq uint64
	attached    atomic.Bool
	onAttached  func(AttachmentToken)

	// overlayMu guards the overlay slot and the committed target. The slot is
	// changed only inside the output lease, so an overlay never takes the
	// terminal while an attachment frame is mid-write.
	overlayMu      sync.Mutex
	overlay        attachmentOverlayKind
	overlaySink    pickerInputConsumer
	committed      protocol.ExactSessionTarget
	committedTab   domain.TabStableID
	committedKnown bool
	// repaint wakes the worker to request an authoritative repaint once an
	// overlay released the terminal.
	repaint chan struct{}
	// tabSelect carries one pending in-place tab switch to the worker.
	tabSelect chan domain.TabStableID
	// routes carries the newest client route snapshot to the worker; replies
	// carries the supervisor's navigation failures (Plan 003 C4/C5).
	routes  chan protocol.RecentRouteSnapshot
	replies chan protocol.ClientMessage
}

// Token is the generation/attempt identity of this grant.
func (f *attachmentForeground) Token() AttachmentToken { return f.token }

func (f *attachmentForeground) actionableGeneration() uint64 {
	if f.uiGeneration != 0 {
		return f.uiGeneration
	}
	return f.token.Generation
}

func (f *attachmentForeground) uiPublished() {
	if f.uiGeneration == 0 || f.host == nil || f.host.actionUI == nil {
		return
	}
	f.host.actionUI.published(f.uiGeneration)
}

func (f *attachmentForeground) uiReceipt(receipt protocol.UIReceipt) {
	if f.uiGeneration != 0 && f.host != nil && f.host.actionUI != nil {
		f.host.actionUI.receipt(f.uiGeneration, receipt)
	}
}

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
		if f.uiGeneration != 0 && f.host != nil && f.host.actionUI != nil {
			f.host.actionUI.releaseForeground(f.uiGeneration)
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
	f.finishedOnce.Do(func() {
		// Autonomous supervisor foregrounds have a host-lifetime geometry
		// collector. At that owner-class boundary, no attachment byte may reach
		// the picker. Isolated legacy worker replacement remains lossless.
		if f.input != nil && f.host != nil && f.host.ownerBoundary {
			f.input.dropOwned(f.consumer)
		}
		f.authority.revoke(f)
	})
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
			return AttachmentInputEvent{Data: result.data, Err: result.err, actionID: result.actionID}, true
		}
		if ctx.Err() != nil {
			return AttachmentInputEvent{}, false
		}
		select {
		case <-ctx.Done():
			return AttachmentInputEvent{}, false
		case <-f.done:
			return AttachmentInputEvent{}, false
		case request := <-f.input.automation:
			f.input.mu.Lock()
			clean := f.input.consumer == f.consumer && request.consumer == f.consumer && f.input.pending == nil && len(f.input.residual) == 0 && f.input.delivering == 0
			f.input.mu.Unlock()
			clean = clean && request.ctx.Err() == nil && request.record.source == terminalInputAutomation && request.record.actionID != 0 && request.record.generation == f.uiGeneration && request.record.endBatch && len(request.record.data) > 0 && len(request.record.data) <= uiMaxInputBytes && validUIKeyBatch(request.record.data)
			if clean && request.owner != nil {
				clean = request.owner.accept(request.record.actionID, request.record.generation)
			}
			request.admitted <- clean
			if !clean {
				continue
			}
			request.dispatched <- true
			return AttachmentInputEvent{Data: append([]byte(nil), request.record.data...), actionID: request.record.actionID}, true
		case <-f.input.readyFor(f.consumer):
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
func (f *attachmentForeground) Lifecycle(ctx context.Context) (AttachmentLifecycleActionKind, bool) {
	if ctx == nil {
		ctx = context.Background()
	}
	for {
		if !f.authority.actionAuthorized(f) {
			return 0, false
		}
		var internal <-chan AttachmentLifecycleAction
		if f.host != nil {
			internal = f.host.internalActions
		}
		select {
		case action, ok := <-f.actions:
			if !ok || !f.authority.actionAuthorized(f) {
				return 0, false
			}
			if action.Token != f.token {
				continue
			}
			return action.Kind, action.Kind == AttachmentDetachToPicker || action.Kind == AttachmentDetachAndExit
		case action := <-internal:
			if action.Token != f.token || !f.authority.actionAuthorized(f) {
				continue
			}
			return action.Kind, action.Kind == AttachmentDetachToPicker || action.Kind == AttachmentDetachAndExit
		case <-ctx.Done():
			return 0, false
		case <-f.done:
			return 0, false
		}
	}
}

func (f *attachmentForeground) Resize(ctx context.Context) (domain.Geometry, bool) {
	if ctx == nil {
		ctx = context.Background()
	}
	if !f.authority.actionAuthorized(f) || f.host == nil {
		return domain.Geometry{}, false
	}
	if !f.host.ownerBoundary {
		resizes := f.resizes
		for {
			select {
			case geometry, ok := <-resizes:
				if !ok {
					resizes = nil
					continue
				}
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
	for {
		if geometry, next, ok := f.host.geometryAfter(f.geometrySeq); ok {
			f.geometrySeq = next
			if f.authority.actionAuthorized(f) {
				return geometry, true
			}
			return domain.Geometry{}, false
		}
		select {
		case <-ctx.Done():
			return domain.Geometry{}, false
		case <-f.done:
			return domain.Geometry{}, false
		case <-f.host.geometryUpdate:
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
	return f.write(uiContext, data, false)
}

// write is the shared output transaction. While an overlay owns the terminal an
// attachment frame (overlay=false) is accepted without being written or
// published: the caller still applies and acknowledges it, and the overlay's
// release requests an authoritative repaint. Overlay frames bypass only that
// suppression.
func (f *attachmentForeground) write(uiContext ports.UIContext, data []byte, overlay bool) error {
	if f == nil {
		return errAttachmentForegroundRevoked
	}
	var outputErr error
	ok := f.lease.send(func() bool {
		if !f.authority.actionAuthorized(f) {
			return false
		}
		if !overlay && f.overlayKind() != attachmentOverlayNone {
			return true
		}
		if supervisorNil(f.term) {
			outputErr = ports.ErrUIUnavailable
			return true
		}
		writer := f.term.Out()
		if supervisorNil(writer) {
			outputErr = ports.ErrUIUnavailable
			return true
		}
		uiContext = f.presentationContext(uiContext)
		hasUI := !supervisorNil(f.ui)
		if hasUI {
			f.ui.BeginOutput(uiContext)
		}
		success := false
		defer func() {
			if hasUI {
				f.ui.EndOutput(success)
				if success {
					f.uiPublished()
				}
			}
		}()
		// An unavailable capture means the optional UI observation channel is
		// disabled or closed, so the terminal frame still writes and flushes.
		// Any other publication error aborts before the frame is written.
		if hasUI {
			if err := f.ui.PublishContext(uiContext); err != nil && !errors.Is(err, ports.ErrUIUnavailable) {
				outputErr = err
				return true
			}
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

// presentationContext applies the published-context shape rule to one foreground
// transaction. Only an Attached transaction carries the real actionable
// generation and the committed session metadata; any other presentation carries
// the run's stable UI handle and its status with every other field zeroed. A
// pre-attach Connecting frame therefore never publishes an actionable
// generation or session identity, and no caller can fence an action against a
// generation the attachment has not committed.
func (f *attachmentForeground) presentationContext(identity ports.UIContext) ports.UIContext {
	handle := identity.AttachmentHandle
	if f.host != nil && f.host.actionUI != nil {
		handle = f.host.actionUI.Handle()
	}
	if identity.Status != ports.UIStatusAttached {
		return ports.UIContext{AttachmentHandle: handle, Status: identity.Status}
	}
	identity.Generation = f.actionableGeneration()
	identity.AttachmentHandle = handle
	return identity
}

// PublishAttached publishes the committed attached presentation. It takes the
// same send lease and the same authorization fence as Output, so it can never
// race a finalization or a revoked generation, and it writes no bytes: the
// attached presentation describes the frame that already drained. A closed UI
// observation channel is tolerated exactly as Output tolerates it, while any
// other publication failure is reported and leaves the run unattached.
func (f *attachmentForeground) PublishAttached(uiContext ports.UIContext) error {
	if f == nil {
		return errAttachmentForegroundRevoked
	}
	var outputErr error
	ok := f.lease.send(func() bool {
		if !f.authority.actionAuthorized(f) {
			return false
		}
		if supervisorNil(f.ui) {
			return true
		}
		uiContext = f.presentationContext(uiContext)
		f.ui.BeginOutput(uiContext)
		publishErr := f.ui.PublishContext(uiContext)
		if publishErr != nil && !errors.Is(publishErr, ports.ErrUIUnavailable) {
			outputErr = publishErr
			f.ui.EndOutput(false)
			return true
		}
		f.ui.EndOutput(true)
		f.uiPublished()
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
