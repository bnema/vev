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

// Committed attachment orchestration.
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
// allocated by the broker service — the single allocator on its own connection
// — and is consumed even when resolution refuses, matching the never-reused
// contract.
func (s *Supervisor) resolveInitialStreamRequest(service ports.BrokerService, snapshot ports.BrokerSnapshot, navigation InitialNavigation) (ports.BrokerOpenStreamRequest, error) {
	// The intent is resolved against the very snapshot the resolver saw, so the
	// authority the resolver translated its target into and the authority the
	// request is resolved from can never diverge.
	request, err := navigation.Resolve(snapshot.Clone())
	if err != nil {
		return ports.BrokerOpenStreamRequest{}, classifyInitialNavigationFailure(err)
	}
	stream, err := service.NextStreamID()
	if err != nil {
		return ports.BrokerOpenStreamRequest{}, err
	}
	// The gabarit deliberately leaves the connection and stream identity zero:
	// the supervisor, not the resolver and not the intention, owns them, and the
	// environment is never read from the broker process.
	request.Connection = service.ConnectionID()
	request.Stream = stream
	return request, nil
}

// classifyInitialNavigationFailure turns a resolver or validation failure into
// the bounded selection-unavailable notice a composition can display. A typed
// catalogue refusal keeps its own classification; anything else is reported as
// a bounded no-selection refusal rather than as a destination failure, because
// no destination was ever dialed.
func classifyInitialNavigationFailure(err error) error {
	var catalogueErr pickerCatalogueError
	if errors.As(err, &catalogueErr) {
		return err
	}
	return pickerCatalogueError{Code: pickerCatalogueNoSelection, Text: "initial navigation could not be resolved"}
}

func (s *Supervisor) resolveCommittedStreamRequest(service ports.BrokerService, key string) (ports.BrokerOpenStreamRequest, attachmentTab, error) {
	stream, err := service.NextStreamID()
	if err != nil {
		return ports.BrokerOpenStreamRequest{}, attachmentTab{}, err
	}
	base := pickerResolveBase{
		Connection: service.ConnectionID(),
		Stream:     stream,
	}
	if resolver, ok := s.cfg.Picker.(pickerTargetResolver); ok {
		return resolver.ResolveKeyTarget(key, base)
	}
	request, err := s.cfg.Picker.ResolveKey(key, base)
	return request, attachmentTab{}, err
}

// pickerTargetResolver is the optional tab-aware resolution of the real
// picker; a scripted picker may resolve sessions only.
type pickerTargetResolver interface {
	ResolveKeyTarget(key string, base pickerResolveBase) (ports.BrokerOpenStreamRequest, attachmentTab, error)
}

// pickerAttachmentTarget is one resolved attachment: the exact broker stream
// request and the tab its Hello selects.
type pickerAttachmentTarget struct {
	request ports.BrokerOpenStreamRequest
	tab     attachmentTab
}

// newAttachmentWorker builds the real typed-session worker for one admitted
// request. The worker receives no endpoint, route, or raw-mode authority: it
// only drives the typed session protocol on the stream the supervisor opened.
func (s *Supervisor) newAttachmentWorker(request ports.BrokerOpenStreamRequest, tab attachmentTab, beforeAttached func() error, sessionEnv SessionEnvironment) (AttachmentWorker, error) {
	geometry, ok := s.attachments.latestGeometry()
	if !ok {
		var err error
		geometry, err = s.cfg.Terminal.Geometry()
		if err != nil {
			return nil, fmt.Errorf("vev: reading terminal geometry: %w", err)
		}
	}
	environment := s.cfg.AttachmentEnvironment
	worker, err := newSessionAttachmentWorker(sessionAttachmentConfig{
		Request:            request,
		ClientID:           s.clientID,
		Geometry:           geometry,
		TermEnv:            environment.TermEnv,
		Cwd:                environment.Cwd,
		TrueColor:          environment.TrueColor,
		SessionEnvironment: sessionEnv,
		BeforeAttached:     beforeAttached,
		Clock:              s.cfg.Clock,
		Theme:              &s.theme,
		Clipboard:          s.cfg.Clipboard,
		Tab:                tab,
	})
	if err != nil {
		return nil, err
	}
	return worker, nil
}

// takeInitialNavigation consumes the one-shot intent exactly once. Consumption
// is recorded before the resolver runs and before any stream is opened, so a
// refusal, a cancellation, a timeout, or a reconnection can never re-arm it: a
// later ready cycle observes the intent already consumed. Before the first
// committed publication this method is never reached, so a reconnection that
// precedes readiness cannot consume the intent either.
//
// The resolver runs on a defensive copy of the committed publication and is
// consulted at most once for the whole run. A resolved picker is a success
// without an operation.
func (s *Supervisor) takeInitialNavigation(snapshot ports.BrokerSnapshot) (InitialNavigation, bool, error) {
	// The resolver is a translation of a CLI target into identity, so it is only
	// consulted against a snapshot that satisfies its own contract. A malformed
	// publication consumes the intent and reports a bounded refusal instead of
	// handing an unvalidated authority to the resolver or the request.
	if err := snapshot.Validate(); err != nil {
		return InitialNavigation{}, true, fmt.Errorf("vev: initial navigation snapshot: %w", err)
	}
	s.mu.Lock()
	if s.navigationConsumed {
		s.mu.Unlock()
		return InitialNavigation{}, false, nil
	}
	s.navigationConsumed = true
	navigation := s.navigation
	resolver := s.cfg.ResolveInitialNavigation
	s.mu.Unlock()

	if resolver != nil {
		resolved, err := resolver(snapshot.Clone())
		if errors.Is(err, ErrInitialNavigationNotObserved) {
			// Not a refusal: the target's daemon has not been observed yet.
			// The intent stays armed for a later publication.
			s.mu.Lock()
			s.navigationConsumed = false
			s.mu.Unlock()
			return InitialNavigation{}, false, err
		}
		if err != nil {
			return InitialNavigation{}, true, err
		}
		navigation = resolved
	}
	if navigation.Kind == InitialNavigationPicker {
		return InitialNavigation{}, true, nil
	}
	if err := navigation.Validate(); err != nil {
		return InitialNavigation{}, true, err
	}
	return navigation, true, nil
}

// ErrInitialNavigationNotObserved is returned by an InitialNavigationResolver
// whose decision depends on a daemon inventory the committed publication does
// not carry yet (for example attach-or-create before the host's first
// observation). The supervisor keeps the intent armed and resolves it again on
// later publications, bounded by initialNavigationObservationBudget.
var ErrInitialNavigationNotObserved = errors.New("vev: initial navigation target is not observed yet")

// InitialNavigationNotObserved is the resolver's ErrInitialNavigationNotObserved
// naming the configured endpoint whose fresh observation it needs; the
// supervisor asks the broker to reconcile that endpoint once per wait.
// Fallback, when it is not the picker, is the navigation to take if the budget
// expires before the endpoint is observed (a remote host whose observation
// fails can still serve a stream); the destination daemon stays the final
// authority over it.
type InitialNavigationNotObserved struct {
	Endpoint string
	Fallback InitialNavigation
}

func (e InitialNavigationNotObserved) Error() string {
	return ErrInitialNavigationNotObserved.Error() + ": " + e.Endpoint
}

func (e InitialNavigationNotObserved) Is(target error) bool {
	return target == ErrInitialNavigationNotObserved
}

// initialNavigationObservationBudget bounds how long an initial navigation
// waits for its target's first observation; it stays inside the 15-second
// handshake budget.
const initialNavigationObservationBudget = 10 * time.Second

// initialNavigationTake is one consumption attempt of the armed initial
// navigation against the committed publication that decided it.
type initialNavigationTake struct {
	snapshot   ports.BrokerSnapshot
	navigation InitialNavigation
	consumed   bool
	err        error
	terminated bool
	termErr    error
}

// awaitInitialNavigationObservation re-resolves the armed initial navigation
// on each committed publication until the resolver can decide, the budget
// expires (the resolver's fallback, or a bounded refusal without one), or the
// run ends. The returned snapshot is the publication the decision was made on,
// so the stream request resolves against the same authority.
func (s *Supervisor) awaitInitialNavigationObservation(ctx context.Context, input *terminalInputLifetime, service ports.BrokerService, pending InitialNavigationNotObserved) initialNavigationTake {
	timer := s.cfg.Clock.NewTimer(initialNavigationObservationBudget)
	defer timer.Stop()
	for {
		select {
		case <-s.brokerChanged():
			snapshot := service.Snapshot()
			navigation, consumed, err := s.takeInitialNavigation(snapshot)
			if errors.Is(err, ErrInitialNavigationNotObserved) {
				_ = errors.As(err, &pending)
				continue
			}
			return initialNavigationTake{snapshot: snapshot, navigation: navigation, consumed: consumed, err: err}
		case <-timer.C():
			s.mu.Lock()
			s.navigationConsumed = true
			s.mu.Unlock()
			if pending.Fallback.Kind != InitialNavigationPicker {
				return initialNavigationTake{snapshot: service.Snapshot(), navigation: pending.Fallback, consumed: true}
			}
			return initialNavigationTake{consumed: true, err: ErrInitialNavigationNotObserved}
		case <-service.Done():
			// The ready loop observes the loss; the intent stays armed for the
			// next adopted connection.
			return initialNavigationTake{}
		case <-ctx.Done():
			return initialNavigationTake{terminated: true, termErr: ctx.Err()}
		case err := <-input.EOF():
			return initialNavigationTake{terminated: true, termErr: terminalReadCause(err)}
		}
	}
}

// runCommittedAttachment admits one committed selection end to end: resolve,
// open the exact stream under the one absolute deadline, run the real
// attachment worker through the supervisor's foreground grant, and return to
// the picker. It reports terminated=true only when the run itself must end
// (process cancellation or terminal EOF); every attachment outcome returns to
// the picker in the same process.
func (s *Supervisor) runInitialNavigation(ctx context.Context, input *terminalInputLifetime, service ports.BrokerService) (bool, error) {
	// The intent is consumed against the first committed publication of the
	// adopted connection; the resolver and the request resolution share that one
	// read.
	snapshot := service.Snapshot()
	navigation, consumed, err := s.takeInitialNavigation(snapshot)
	if errors.Is(err, ErrInitialNavigationNotObserved) {
		var pending InitialNavigationNotObserved
		if errors.As(err, &pending) && pending.Endpoint != "" {
			service.RequestReconcile(pending.Endpoint)
		}
		take := s.awaitInitialNavigationObservation(ctx, input, service, pending)
		if take.terminated {
			return true, take.termErr
		}
		snapshot, navigation, consumed, err = take.snapshot, take.navigation, take.consumed, take.err
	}
	if !consumed {
		return false, nil
	}
	if err != nil {
		s.reportAttachmentFailure(classifyInitialNavigationFailure(err))
		return false, nil
	}
	if navigation.Kind == InitialNavigationPicker {
		// Picker is a success without an operation: no stream is opened, and the
		// intent stays consumed so a later reconnect cannot replay it.
		return false, nil
	}
	request, err := s.resolveInitialStreamRequest(service, snapshot, navigation)
	if err != nil {
		s.reportAttachmentFailure(err)
		return false, nil
	}
	// The picker was hidden behind Connecting while this navigation was
	// pending: keys typed then must not replay as a selection or a close
	// after the attachment returns to it.
	if s.cfg.Picker != nil {
		s.cfg.Picker.TakeOp()
	}
	s.pendingPickerKey = ""
	return s.runResolvedAttachment(ctx, input, service, pickerAttachmentTarget{request: request}, SessionEnvironmentLocalCLI)
}

func (s *Supervisor) runCommittedAttachment(ctx context.Context, input *terminalInputLifetime, service ports.BrokerService, key string) (bool, error) {
	if s.cfg.Picker == nil {
		return false, nil
	}
	request, tab, err := s.resolveCommittedStreamRequest(service, key)
	if err != nil {
		// A refused selection is a typed presentation notice; it never opens a
		// stream or becomes a session write.
		s.reportAttachmentFailure(err)
		return false, nil
	}
	return s.runResolvedAttachment(ctx, input, service, pickerAttachmentTarget{request: request, tab: tab}, SessionEnvironmentLocalPicker)
}

func (s *Supervisor) runResolvedAttachment(ctx context.Context, input *terminalInputLifetime, service ports.BrokerService, target pickerAttachmentTarget, localProvenance SessionEnvironmentProvenance) (bool, error) {
	picker := s.cfg.Picker
	if picker == nil {
		return false, nil
	}
	picker.SetOwnsInput(false)
	input.releasePicker()
	defer func() {
		// Whoever owned the terminal since the last picker frame, the next
		// frame must draw the whole box again.
		s.invalidatePickerPresentation()
		// Back on the plain picker there is no attachment to start on.
		s.setPickerCurrent(pickerCurrent{})
		// Publications folded into routes while attached never reached the
		// picker catalogue.
		picker.ApplySnapshot(service.Snapshot())
		picker.SetOwnsInput(true)
		input.acquirePicker()
	}()
	defer func() { s.resuming, s.resumeErr, s.resume = false, nil, nil }()
	resumes := 0
	for {
		s.resumeErr = nil
		terminated, termErr, swap := s.attachOnce(ctx, input, service, target, localProvenance)
		if terminated {
			return terminated, termErr
		}
		if swap == nil {
			resume := s.resume
			s.resume = nil
			switch {
			case resume != nil:
				// A live attachment was lost: each loss after a successful
				// attach starts a fresh resume budget.
				resumes = 0
			case s.resuming && s.resumeErr != nil:
				// A resume attempt hit a transport-class failure before
				// attaching (the route or destination is still down): try the
				// same target again.
				retry := target
				resume = &retry
			default:
				// No resume, or the attempt already presented a final outcome
				// (session gone, ended, refused).
				return false, nil
			}
			lastErr := s.resumeErr
			resumes++
			if resumes > maxAttachmentResumes {
				s.logger.Warn("client_attachment_resume_exhausted", "endpoint", resume.request.Endpoint, "session", resume.request.Target.SessionName, "attempts", resumes-1, "error", lastErr)
				s.resuming = false
				s.transition(supervisorEvent{kind: supervisorAttachEnded, err: lastErr})
				s.notifyLifecycle(LifecycleNoticeDestinationFailed)
				s.notifyAttachment(lastErr)
				return false, nil
			}
			s.resuming = true
			if terminated, termErr, brokerLost := s.waitResume(ctx, input, service, resumes); terminated || brokerLost {
				return terminated, termErr
			}
			stream, err := service.NextStreamID()
			if err != nil {
				s.resuming = false
				s.reportAttachmentFailure(err)
				return false, nil
			}
			resume.request.Stream = stream
			s.logger.Info("client_attachment_resume", "endpoint", resume.request.Endpoint, "session", resume.request.Target.SessionName, "attempt", resumes)
			target = *resume
			continue
		}
		resumes = 0
		s.resuming = false
		// A commit from the picker overlay to another target: the previous
		// attachment detached cleanly, and the swap goes through Connecting
		// exactly like any committed selection. The stream identity is
		// allocated now, not at commit: streams opened while the source was
		// live (route publications) may have moved the connection's
		// anti-replay window past an identity reserved earlier.
		stream, err := service.NextStreamID()
		if err != nil {
			s.reportAttachmentFailure(err)
			return false, nil
		}
		swap.request.Stream = stream
		target, localProvenance = *swap, SessionEnvironmentLocalPicker
	}
}

// attachOnce runs one resolved attachment end to end. A non-nil swap names the
// next target the picker overlay committed while this attachment was live.
func (s *Supervisor) attachOnce(ctx context.Context, input *terminalInputLifetime, service ports.BrokerService, target pickerAttachmentTarget, localProvenance SessionEnvironmentProvenance) (bool, error, *pickerAttachmentTarget) {
	terminated, termErr := s.attachResolved(ctx, input, service, target, localProvenance)
	swap := s.pendingSwap
	s.pendingSwap = nil
	if terminated {
		return terminated, termErr, nil
	}
	return false, nil, swap
}

func (s *Supervisor) attachResolved(ctx context.Context, input *terminalInputLifetime, service ports.BrokerService, target pickerAttachmentTarget, localProvenance SessionEnvironmentProvenance) (bool, error) {
	request := target.request
	s.transition(supervisorEvent{kind: supervisorAttachBegin})
	deadline := startAttachmentDeadline(ctx, s.cfg.Clock)
	defer deadline.finish()

	sessionEnv := s.cfg.SessionEnvironment.Clone()
	if request.Local {
		sessionEnv.Provenance = localProvenance
	} else {
		sessionEnv.Provenance = SessionEnvironmentRemote
		sessionEnv.Env = nil
		sessionEnv.Cwd = ""
	}
	if err := sessionEnv.Validate(); err != nil {
		s.reportAttachmentFailure(err)
		return false, nil
	}
	request.Env = append([]string(nil), sessionEnv.Env...)

	s.logger.Debug("client_stream_open_begin", "local", request.Local, "endpoint", request.Endpoint, "admission", request.Admission, "generation", s.State().Generation)
	stream, err := service.OpenStream(deadline.Context(), request)
	if err != nil {
		s.logger.Warn("client_stream_open_failed", "local", request.Local, "endpoint", request.Endpoint, "code", brokerErrorCode(err), "error", err, "cause", errors.Unwrap(err))
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

	worker, err := s.newAttachmentWorker(request, target.tab, func() error {
		deadline.disarm()
		if deadline.expired.Load() {
			return errAttachmentDeadline
		}
		return nil
	}, sessionEnv)
	if err != nil {
		_ = stream.Close()
		s.reportAttachmentFailure(err)
		return false, nil
	}

	token := AttachmentToken{Generation: s.State().Generation, Attempt: s.nextAttachment.Add(1)}
	s.logger.Debug("client_stream_opened", "local", request.Local, "endpoint", request.Endpoint, "generation", token.Generation, "attempt", token.Attempt)
	run, ok := s.attachments.Begin(deadline.Context(), token, worker, stream)
	if !ok {
		// Another foreground owns the terminal, or the shared reader is
		// claimed. Begin never closes the stream, so the supervisor does.
		_ = stream.Close()
		s.transition(supervisorEvent{kind: supervisorAttachEnded})
		return false, nil
	}
	return s.settleAttachment(ctx, input, service, deadline, run, request)
}

// settleAttachment joins one admitted attachment while watching the parent
// context, terminal EOF, and broker-connection loss. The deadline is the only
// other stop: when it fires before the worker settled, the attachment ends as
// a typed timeout without claiming attachment.
func (s *Supervisor) settleAttachment(ctx context.Context, input *terminalInputLifetime, service ports.BrokerService, deadline *attachmentDeadline, run *attachmentRun, request ports.BrokerOpenStreamRequest) (bool, error) {
	settled := make(chan attachmentSettlement, 1)
	go func() {
		event, adopted := run.Wait(deadline.Context())
		settled <- attachmentSettlement{event: event, adopted: adopted}
	}()

	overlay := &attachmentPickerOverlay{sup: s, run: run, service: service, request: request}
	defer overlay.drop()

	var result attachmentSettlement
	var brokerLost bool
	var terminated bool
	var termErr error
settlement:
	for {
		// Begin has admitted the foreground, which can write before MarkAttached
		// and before the initial publication. Keep presentation invalidation
		// buffered for the next picker state unless the picker overlay owns the
		// terminal; consuming it otherwise could overlay session output with a
		// second terminal writer.
		select {
		case result = <-settled:
			break settlement
		case <-s.attachments.NavigationRequests():
			overlay.enter()
		case <-s.brokerChanged():
			if overlay.active {
				overlay.applyPublication()
			}
			s.publishRoutes(service, run, request)
		case <-s.attachments.RouteDemands():
			s.publishRoutes(service, run, request)
		case message := <-s.attachments.Navigations():
			if !overlay.swapping {
				s.settleDaemonNavigation(service, overlay, message)
			}
		case <-overlay.previewChanged():
			overlay.publishPreview()
		case <-overlay.ops():
			overlay.takeOp()
		case consumed := <-s.attachments.OverlayActions():
			overlay.settleAction(consumed)
		case <-overlay.invalidation():
			overlay.resize()
		case outcome := <-s.kills.results():
			s.finishPickerKill(service, outcome)
		case <-service.Done():
			brokerLost = true
			// The broker connection is gone; the supervisor cancels the run and
			// lets the ready loop observe the loss and retire the connection.
			run.Cancel()
			result = <-settled
			break settlement
		case <-ctx.Done():
			run.Cancel()
			result = <-settled
			terminated, termErr = true, ctx.Err()
			break settlement
		case err := <-input.EOF():
			run.Cancel()
			result = <-settled
			terminated, termErr = true, terminalReadCause(err)
			break settlement
		}
	}

	// The attachment settled, so its foreground (and overlay slot) is gone.
	overlay.drop()
	if !terminated && !brokerLost {
		s.takeSettledNavigation(service, run.token, request)
	}
	if terminated {
		s.pendingSwap = nil
		return true, termErr
	}
	if s.pendingSwap != nil {
		if !brokerLost {
			// The overlay committed another target and this attachment
			// detached for it: go straight to Connecting for the swap, never
			// through the plain picker.
			return false, nil
		}
		s.pendingSwap = nil
	}
	if deadline.TimedOut() && !result.adopted {
		timeout := attachmentTimeoutError(errAttachmentDeadline)
		if s.deferResumeFailure(timeout) {
			return false, nil
		}
		s.transition(supervisorEvent{kind: supervisorAttachEnded, err: timeout})
		s.notifyAttachment(timeout)
		return false, nil
	}
	if errors.Is(result.event.Err, errDetachAndExit) {
		s.logger.Info("client_stream_closed", "local", request.Local, "endpoint", request.Endpoint, "lifecycle", request.Target.LifecycleID.String(), "session", request.Target.SessionName, "reason", "explicit_detach")
		s.transition(supervisorEvent{kind: supervisorAttachEnded})
		s.notifyLifecycle(LifecycleNoticeDetachAndExit)
		return true, nil
	}
	if errors.Is(result.event.Err, errDetachToPicker) {
		s.logger.Info("client_stream_closed", "local", request.Local, "endpoint", request.Endpoint, "lifecycle", request.Target.LifecycleID.String(), "session", request.Target.SessionName, "reason", "detach_to_picker")
		s.transition(supervisorEvent{kind: supervisorAttachEnded})
		s.notifyLifecycle(LifecycleNoticeDetachToPicker)
		return false, nil
	}
	if result.adopted && result.event.Kind == AttachmentEventLost && run.fg.Attached() {
		// The stream carrying a live attachment was lost while the broker stayed
		// up. The daemon parks the session for resume, so reconnect to the exact
		// session and tab that were showing instead of dropping to the picker.
		if resume, ok := resumeTarget(request, run.fg); ok {
			s.logger.Warn("client_stream_closed", "local", request.Local, "endpoint", request.Endpoint, "lifecycle", request.Target.LifecycleID.String(), "session", request.Target.SessionName, "reason", result.event.Kind.String(), "error", result.event.Err, "cause", errors.Unwrap(result.event.Err), "resume", true)
			s.resume = &resume
			s.resumeErr = result.event.Err
			return false, nil
		}
	}
	if result.adopted && (result.event.Kind == AttachmentEventLost || result.event.Kind == AttachmentEventFailed) {
		s.logger.Warn("client_stream_closed", "local", request.Local, "endpoint", request.Endpoint, "lifecycle", request.Target.LifecycleID.String(), "session", request.Target.SessionName, "reason", result.event.Kind.String(), "error", result.event.Err, "cause", errors.Unwrap(result.event.Err))
		// A resume attempt whose stream was lost before attaching is still a
		// transport failure: retry instead of presenting it.
		if result.event.Kind == AttachmentEventLost && s.resuming {
			s.resumeErr = result.event.Err
			return false, nil
		}
		s.transition(supervisorEvent{kind: supervisorAttachEnded, err: result.event.Err})
		s.notifyLifecycle(LifecycleNoticeDestinationFailed)
		s.notifyAttachment(result.event.Err)
		return false, nil
	}
	// An orderly end, or a run the supervisor retired for a broker loss,
	// returns to the picker with no attachment failure reported. A stream that
	// failed after attachment was lost/failed above, so it can never leave the
	// attached presentation showing.
	s.transition(supervisorEvent{kind: supervisorAttachEnded})
	if !brokerLost {
		s.logger.Info("client_stream_closed", "local", request.Local, "endpoint", request.Endpoint, "lifecycle", request.Target.LifecycleID.String(), "session", request.Target.SessionName, "reason", result.event.Kind.String())
		s.notifyLifecycle(LifecycleNoticeSessionEnded)
	}
	return false, nil
}

// maxAttachmentResumes bounds how many times one lost attachment reconnects
// before the client gives up and returns to the picker. With the supervisor
// backoff this spans a few seconds, enough to ride out a transient drop.
const maxAttachmentResumes = 5

// resumeTarget builds the exact reattach request for a lost attachment: the
// session and tab it last committed, over the same local or remote route. A
// foreground that never committed a session has nothing to resume.
func resumeTarget(request ports.BrokerOpenStreamRequest, fg *attachmentForeground) (pickerAttachmentTarget, bool) {
	target, tab, known := fg.committedSnapshot()
	if !known || target.Validate() != nil {
		return pickerAttachmentTarget{}, false
	}
	resume := request
	resume.Admission = ports.BrokerAdmissionExact
	resume.Target = target
	resume.Name = ""
	return pickerAttachmentTarget{request: resume, tab: attachmentTab{preferred: tab}}, true
}

// waitResume presents Connecting and waits one backoff interval before the
// next resume attempt. It settles early on cancellation or terminal EOF
// (terminated), or on broker loss, which ends the resume and leaves the ready
// loop to observe and retire the connection.
func (s *Supervisor) waitResume(ctx context.Context, input *terminalInputLifetime, service ports.BrokerService, attempt int) (terminated bool, termErr error, brokerLost bool) {
	s.transition(supervisorEvent{kind: supervisorAttachBegin})
	delay := supervisorBackoffDelay(uint64(attempt), s.cfg.Jitter)
	timer := s.cfg.Clock.NewTimer(delay)
	defer stopSupervisorTimer(timer)
	for {
		select {
		case <-timer.C():
			return false, nil, false
		case <-ctx.Done():
			return true, ctx.Err(), false
		case err := <-input.EOF():
			return true, terminalReadCause(err), false
		case <-service.Done():
			s.transition(supervisorEvent{kind: supervisorAttachEnded})
			return false, nil, true
		case <-s.presentationInvalidation():
			s.renderResizeInvalidation()
		case <-s.spinnerTick():
			s.cfg.Spinner.AdvanceSpinner(s.State())
		}
	}
}

// deferResumeFailure records a transport-class failure of a resume attempt so
// runResolvedAttachment retries it while Connecting stays up. Any other
// failure, or a failure outside a resume, is presented by the caller.
func (s *Supervisor) deferResumeFailure(err error) bool {
	if !s.resuming || !resumableFailure(err) {
		return false
	}
	s.logger.Info("client_attachment_resume_failed", "code", brokerErrorCode(err), "error", err, "cause", errors.Unwrap(err))
	s.resumeErr = err
	return true
}

// resumableFailure reports whether a resume attempt failed for a transport
// reason worth retrying. Refusals, protocol failures, and an ambiguous outcome
// are final.
func resumableFailure(err error) bool {
	var lost ports.BrokerStreamLost
	if errors.As(err, &lost) {
		return true
	}
	var typed ports.BrokerError
	if !errors.As(err, &typed) {
		return false
	}
	switch typed.Code {
	case ports.BrokerErrorUnavailable, ports.BrokerErrorTimeout, ports.BrokerErrorAttachmentLost:
		return true
	default:
		return false
	}
}

// reportAttachmentFailure returns to the picker with a typed, visible failure,
// unless a resume attempt defers it for a retry.
func (s *Supervisor) reportAttachmentFailure(err error) {
	if s.deferResumeFailure(err) {
		return
	}
	s.transition(supervisorEvent{kind: supervisorAttachEnded, err: err})
	// Classification order matters: a local refusal never dialed a destination,
	// so it must not be reported as one, and only a genuinely unknown outcome
	// gets its own notice.
	var catalogueErr pickerCatalogueError
	var brokerErr ports.BrokerError
	switch {
	case errors.As(err, &catalogueErr):
		s.notifyLifecycle(LifecycleNoticeSelectionUnavailable)
	case errors.As(err, &brokerErr) && brokerErr.Code == ports.BrokerErrorOutcomeUnknown:
		s.notifyLifecycle(LifecycleNoticeOutcomeUnknown)
	default:
		s.notifyLifecycle(LifecycleNoticeDestinationFailed)
	}
	s.notifyAttachment(err)
}

// notifyAttachment publishes the typed failure to the optional observer.
func (s *Supervisor) notifyAttachment(err error) {
	if s.cfg.Notify == nil || err == nil {
		return
	}
	s.cfg.Notify(s.State(), err)
}
