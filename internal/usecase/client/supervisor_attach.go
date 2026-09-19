package client

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol"
)

// Committed attachment orchestration (Plan 001 P5.3b, offline and unactivated).
//
// After the picker resolves a committed selection to one exact
// ports.BrokerOpenStreamRequest, the supervisor opens that logical stream on
// the broker and hands the typed connection to the real attachment worker
// through its existing foreground grant. The supervisor still owns the
// terminal, raw mode, the single input lifetime, and the close authority over
// the stream; the picker never opens, writes, or retargets a session.
//
// One absolute deadline bounds the whole attachment: stream open, the session
// preamble, Hello/Welcome, and the committed initial publication. It is never
// restarted and never extended. When the admitted stream exposes an accepted
// absolute deadline (ports.HandshakeDeadlineProvider) the earlier of the two is
// kept, so queue time spent before the stream reached this client is consumed
// rather than repaid with a second budget; an already-elapsed deadline fails the
// attachment immediately. The deadline is driven by ports.Clock/ports.Timer,
// never the wall clock and never a sleep.

// attachmentTimeoutError is the one typed failure a deadline ends an attachment
// with. The cause stays local for errors.Is/As and is never display text.
func attachmentTimeoutError(cause error) error {
	return ports.BrokerError{Code: ports.BrokerErrorTimeout, Text: "attachment timed out", Cause: cause}
}

// attachmentDeadline owns the one absolute deadline of a whole attachment. It
// is safe for concurrent use: its timer goroutine expires it while the
// supervisor's run goroutine may adopt a later deadline after the stream opens.
type attachmentDeadline struct {
	clock    ports.Clock
	ctx      context.Context
	cancel   context.CancelFunc
	timedOut chan struct{}

	mu         sync.Mutex
	deadline   time.Time
	timer      ports.Timer
	stop       chan struct{}
	done       chan struct{}
	expired    atomic.Bool
	expireOnce sync.Once
	// finishOnce makes finish idempotent across the deferred teardown and any
	// early failure path.
	finishOnce sync.Once
}

// startAttachmentDeadline arms the pre-open budget of one attachment. The
// returned deadline already bounds the OpenStream call; adopt may fold the
// stream's own accepted absolute deadline into it, but the earlier deadline
// always stays in force.
func startAttachmentDeadline(parent context.Context, clock ports.Clock) *attachmentDeadline {
	if parent == nil {
		parent = context.Background()
	}
	if supervisorNil(clock) {
		clock = systemClock{}
	}
	ctx, cancel := context.WithCancel(parent)
	deadline := &attachmentDeadline{
		clock:    clock,
		ctx:      ctx,
		cancel:   cancel,
		timedOut: make(chan struct{}),
	}
	deadline.mu.Lock()
	deadline.deadline = clock.Now().Add(protocol.HandshakeTimeout)
	deadline.armLocked(protocol.HandshakeTimeout)
	deadline.mu.Unlock()
	return deadline
}

// armLocked starts a fresh timer for exactly duration. Callers hold d.mu and
// must have stopped any previous timer. It never derives duration from
// protocol.HandshakeTimeout itself, so folding in an absolute deadline can only
// shrink the remaining budget, never restart it.
func (d *attachmentDeadline) armLocked(duration time.Duration) {
	if duration <= 0 {
		d.expire()
		return
	}
	timer := d.clock.NewTimer(duration)
	if supervisorNil(timer) {
		timer = systemClock{}.NewTimer(duration)
	}
	stop := make(chan struct{})
	done := make(chan struct{})
	d.timer = timer
	d.stop = stop
	d.done = done
	go func() {
		defer close(done)
		select {
		case <-timer.C():
			d.expire()
		case <-stop:
		}
	}()
}

// stopTimerLocked stops and joins the current timer goroutine. Callers hold
// d.mu. It is a no-op when no timer is armed.
func (d *attachmentDeadline) stopTimerLocked() {
	if d.timer == nil {
		return
	}
	close(d.stop)
	<-d.done
	d.timer.Stop()
	d.timer, d.stop, d.done = nil, nil, nil
}

// expire closes the timeout signal and cancels the attachment context exactly
// once without taking d.mu. Timer retirement may join a goroutine that has
// already selected the timer channel, so expiry must never need that mutex.
func (d *attachmentDeadline) expire() {
	d.expireOnce.Do(func() {
		d.expired.Store(true)
		close(d.timedOut)
		d.cancel()
	})
}

// adopt folds the admitted stream's accepted absolute deadline into the one
// attachment deadline, keeping the EARLIER of the two. It reports
// errAttachmentDeadline when the effective deadline already elapsed (including
// during stream open), which fails the attachment instead of starting a second
// budget. A stream without the optional seam keeps the pre-open deadline
// unchanged.
//
// Adoption never extends the deadline. The accepted deadline is anchored
// before this client's attempt began, so it is normally later than the client's
// own budget once the stream is admitted; adopting it verbatim would repay the
// open stage with a second full budget and let the whole attachment outlive the
// one absolute deadline. Keeping the earlier value still consumes the time
// already spent before this client saw the stream, because that anchor is
// earlier.
func (d *attachmentDeadline) adopt(stream ports.BrokerLogicalConnection) error {
	provider, ok := stream.(ports.HandshakeDeadlineProvider)
	if !ok {
		return nil
	}
	accepted := provider.HandshakeDeadline()
	if accepted.IsZero() {
		return nil
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	d.stopTimerLocked()
	// Expiry is lock-free and may win while the old timer is being joined.
	// The post-join flag is authoritative; never re-arm a cancelled context.
	if d.expired.Load() {
		return errAttachmentDeadline
	}
	if accepted.Before(d.deadline) {
		d.deadline = accepted
	}
	remaining := d.deadline.Sub(d.clock.Now())
	if remaining <= 0 {
		d.expire()
		return errAttachmentDeadline
	}
	if d.expired.Load() {
		return errAttachmentDeadline
	}
	d.armLocked(remaining)
	return nil
}

// Context bounds every stage of the attachment (stream open, preamble,
// Hello/Welcome, initial publication) with the one absolute deadline.
func (d *attachmentDeadline) Context() context.Context { return d.ctx }

// TimedOut reports whether the absolute deadline fired.
func (d *attachmentDeadline) TimedOut() bool {
	select {
	case <-d.timedOut:
		return true
	default:
		return false
	}
}

// cause maps an open-stream failure to the typed timeout when the deadline
// fired, so an expired budget is never reported as an ordinary unavailability.
func (d *attachmentDeadline) cause(err error) error {
	if d.TimedOut() {
		return attachmentTimeoutError(errors.Join(errAttachmentDeadline, err))
	}
	return normalizeUnavailable(err)
}

// disarm stops the deadline without cancelling the worker context. It is safe
// to race with expiry and with the deferred finish.
func (d *attachmentDeadline) disarm() {
	if d == nil {
		return
	}
	d.mu.Lock()
	d.stopTimerLocked()
	d.mu.Unlock()
}

// finish stops the deadline timer and cancels the attachment context. It is
// idempotent and is the caller's deferred teardown for one attachment.
func (d *attachmentDeadline) finish() {
	if d == nil {
		return
	}
	d.finishOnce.Do(func() {
		d.mu.Lock()
		d.stopTimerLocked()
		d.mu.Unlock()
		d.cancel()
	})
}

// attachmentSettlement is one worker run's outcome as observed by the
// supervisor: whether the supervisor adopted the event or retired the run.
type attachmentSettlement struct {
	event   AttachmentEvent
	adopted bool
}

// resolveCommittedStreamRequest turns exactly one committed catalogue key into
// the exact broker stream request the user committed. The stream ID is
// allocated strictly increasing per connection and is consumed even when
// resolution refuses, matching the pool's never-reused ID contract.
func (s *Supervisor) resolveCommittedStreamRequest(service ports.BrokerService, key string) (ports.BrokerOpenStreamRequest, error) {
	base := pickerResolveBase{
		Connection: service.ConnectionID(),
		Stream:     ports.BrokerStreamID(s.nextStream.Add(1)),
	}
	return s.cfg.Picker.ResolveKey(key, base)
}

// newAttachmentWorker builds the real typed-session worker for one admitted
// request. The worker receives no endpoint, route, or raw-mode authority: it
// only drives the typed session protocol on the stream the supervisor opened.
func (s *Supervisor) newAttachmentWorker(request ports.BrokerOpenStreamRequest, beforeAttached func() error) (AttachmentWorker, error) {
	geometry, err := s.cfg.Terminal.Geometry()
	if err != nil {
		return nil, fmt.Errorf("vev: reading terminal geometry: %w", err)
	}
	environment := s.cfg.AttachmentEnvironment
	return newSessionAttachmentWorker(sessionAttachmentConfig{
		Request:        request,
		ClientID:       s.clientID,
		Geometry:       geometry,
		TermEnv:        environment.TermEnv,
		Cwd:            environment.Cwd,
		TrueColor:      environment.TrueColor,
		BeforeAttached: beforeAttached,
	}), nil
}

// runCommittedAttachment admits one committed selection end to end: resolve,
// open the exact stream under the one absolute deadline, run the real
// attachment worker through the supervisor's foreground grant, and return to
// the picker. It reports terminated=true only when the run itself must end
// (process cancellation or terminal EOF); every attachment outcome returns to
// the picker in the same process.
func (s *Supervisor) runCommittedAttachment(ctx context.Context, input *terminalInputLifetime, service ports.BrokerService, key string) (bool, error) {
	picker := s.cfg.Picker
	if picker == nil {
		return false, nil
	}
	// Present connecting before releasing picker input, so a slow resolve/open
	// never leaves a visible picker whose keystrokes are silently discarded.
	s.transition(supervisorEvent{kind: supervisorAttachBegin})
	// Release picker input for the whole attachment. Exactly one owner
	// consumes the shared terminal reader, so picker input can never reach a
	// session and the attachment path starts no second reader.
	picker.SetOwnsInput(false)
	defer picker.SetOwnsInput(true)

	request, err := s.resolveCommittedStreamRequest(service, key)
	if err != nil {
		// A refused selection is a typed presentation notice; it never opens a
		// stream or becomes a session write.
		s.reportAttachmentFailure(err)
		return false, nil
	}

	deadline := startAttachmentDeadline(ctx, s.cfg.Clock)
	defer deadline.finish()

	stream, err := service.OpenStream(deadline.Context(), request)
	if err != nil {
		s.reportAttachmentFailure(deadline.cause(err))
		return false, nil
	}
	if supervisorNil(stream) {
		s.reportAttachmentFailure(ports.BrokerError{Code: ports.BrokerErrorUnavailable, Text: "broker returned no stream"})
		return false, nil
	}
	if err := deadline.adopt(stream); err != nil {
		// The accepted absolute deadline already elapsed before this client
		// saw the stream. Fail the attachment instead of restarting a budget.
		_ = stream.Close()
		s.reportAttachmentFailure(attachmentTimeoutError(err))
		return false, nil
	}

	worker, err := s.newAttachmentWorker(request, func() error {
		deadline.disarm()
		if deadline.expired.Load() {
			return errAttachmentDeadline
		}
		return nil
	})
	if err != nil {
		_ = stream.Close()
		s.reportAttachmentFailure(err)
		return false, nil
	}

	token := AttachmentToken{Generation: s.State().Generation, Attempt: s.nextAttachment.Add(1)}
	run, ok := s.attachments.Begin(deadline.Context(), token, worker, stream)
	if !ok {
		// Another foreground owns the terminal, or the shared reader is
		// claimed. Begin never closes the stream, so the supervisor does.
		_ = stream.Close()
		s.transition(supervisorEvent{kind: supervisorAttachEnded})
		return false, nil
	}
	return s.settleAttachment(ctx, input, service, deadline, run)
}

// settleAttachment joins one admitted attachment while watching the parent
// context, terminal EOF, and broker-connection loss. The deadline is the only
// other stop: when it fires before the worker settled, the attachment ends as
// a typed timeout without claiming attachment.
func (s *Supervisor) settleAttachment(ctx context.Context, input *terminalInputLifetime, service ports.BrokerService, deadline *attachmentDeadline, run *attachmentRun) (bool, error) {
	settled := make(chan attachmentSettlement, 1)
	go func() {
		event, adopted := run.Wait(deadline.Context())
		settled <- attachmentSettlement{event: event, adopted: adopted}
	}()

	var result attachmentSettlement
	var terminated bool
	var termErr error
	select {
	case result = <-settled:
	case <-service.Done():
		// The broker connection is gone; the supervisor cancels the run and
		// lets the ready loop observe the loss and retire the connection.
		run.Cancel()
		result = <-settled
	case <-ctx.Done():
		run.Cancel()
		result = <-settled
		terminated, termErr = true, ctx.Err()
	case err := <-input.EOF():
		run.Cancel()
		result = <-settled
		terminated, termErr = true, terminalReadCause(err)
	}

	if terminated {
		return true, termErr
	}
	if deadline.TimedOut() && !result.adopted {
		timeout := attachmentTimeoutError(errAttachmentDeadline)
		s.transition(supervisorEvent{kind: supervisorAttachEnded, err: timeout})
		s.notifyAttachment(timeout)
		return false, nil
	}
	if result.adopted && (result.event.Kind == AttachmentEventLost || result.event.Kind == AttachmentEventFailed) {
		s.transition(supervisorEvent{kind: supervisorAttachEnded, err: result.event.Err})
		s.notifyAttachment(result.event.Err)
		return false, nil
	}
	// An orderly end, or a run the supervisor retired for a broker loss,
	// returns to the picker with no attachment failure reported. A stream that
	// failed after attachment was lost/failed above, so it can never leave the
	// attached presentation showing.
	s.transition(supervisorEvent{kind: supervisorAttachEnded})
	return false, nil
}

// reportAttachmentFailure returns to the picker with a typed, visible failure.
func (s *Supervisor) reportAttachmentFailure(err error) {
	s.transition(supervisorEvent{kind: supervisorAttachEnded, err: err})
	s.notifyAttachment(err)
}

// notifyAttachment publishes the typed failure to the optional observer.
func (s *Supervisor) notifyAttachment(err error) {
	if s.cfg.Notify == nil || err == nil {
		return
	}
	s.cfg.Notify(s.State(), err)
}
