// Package daemon holds vev's server-side session multiplexer use case.
//
// Lock ordering: never acquire sendMu or session/daemon locks while holding a
// pane lock. Coordinator callbacks and timer methods run without coordinator.mu;
// see the lock-specific comments on attachedClient and renderCoordinator.
package daemon

import (
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/usecase/keys"
	"github.com/bnema/vev/internal/usecase/mouse"
	themeui "github.com/bnema/vev/internal/usecase/theme"
)

type transportSnapshot struct {
	transport   ports.Transport
	incarnation uint64
}

// appliedTheme is the single immutable chrome snapshot consumed by rendering.
// Raw retains terminal defaults for panes; Resolved belongs exclusively to vev
// chrome. Generation changes whenever either terminal input or policy changes.
type appliedTheme struct {
	Raw        themeui.Theme
	Resolved   themeui.ResolvedTheme
	Generation uint64
}

type attachedClient struct {
	tr                   ports.Transport
	transportIncarnation uint64
	output               *outputStateStream
	overlays             *overlayRuntime
	overlayOnce          sync.Once
	clientID             [16]byte
	// roleGeneration is the wire-facing capability generation. roleEffects is
	// the linearization gate for every role-bound observable operation. A role
	// transition freezes and drains this gate before changing either generation
	// or the session/coordinator registries.
	roleGeneration atomic.Uint64
	roleEffects    roleEffectGate
	resumeCapable  bool
	resumeToken    uint64
	parked         bool
	echoAck        atomic.Uint64
	// prepareFailureFallback prevents a direct fallback paint from recursively
	// reporting the same failed prepare through its notice repaint. It is only
	// needed while no render coordinator is installed.
	prepareFailureFallback atomic.Bool
	// pipelineCache is the last successfully emitted composition. pipelineScratch
	// is its attachment-owned alternate buffer; both are only touched under
	// sendMu and must never share mutable backing storage.
	pipelineCache   composeCacheInput
	pipelineScratch composeCacheInput
	renderScratch   renderCaptureScratch // only touched while sendMu is held
	// captureFrames is keyed by pane ownership, not the tab-local PaneID, so
	// snapshots cannot leak when an attachment switches tabs or sessions.
	captureFrames map[*pane]capturedPaneRenderState // only touched while sendMu is held
	// initialSnatchedMu elects exactly one reset-panel sender per role generation.
	// A Welcome handshake and displaced-client cleanup may both discover the
	// same post-transition snatched role; the loser waits for the elected send.
	initialSnatchedMu         sync.Mutex
	initialSnatchedGeneration uint64
	initialSnatchedAttempt    *initialSnatchedPanelAttempt
	size                      domain.Size
	keys                      *keys.Router
	sess                      Guarded[*session]
	mouseScan                 mouse.Scanner
	themeMu                   sync.Mutex
	clientTheme               themeui.Theme
	appliedTheme              appliedTheme
	lastCursor                cursorOut
	renderStages              renderStageHooks // optional render and handoff observability hooks
	// previousSession is guarded independently. It is retained through temporary
	// setSession(nil) hand-offs and cleared only on terminal teardown.
	previousSession Guarded[*session]
	// snatchedInputMu serializes the restricted input parser with its delayed
	// standalone-ESC callback. It is independent of routing and transport locks;
	// role activation and terminal close clear it only after releasing those locks.
	snatchedInputMu      sync.Mutex
	snatchedInputPending []byte
	snatchedInputDrain   bool
	snatchedInputESC     pendingByteTimer
	linkMu               sync.Mutex
	sendMu               sync.Mutex
}

type cursorOut struct {
	valid    bool
	hidden   bool
	row      int
	col      int
	style    int
	hasStyle bool
}

// pendingByteTimer is retained for overlay input timers. Resize timing belongs
// exclusively to renderCoordinator.
type pendingByteTimer struct {
	timer ports.Timer
	done  chan struct{}
}

func (p *pendingByteTimer) retain(clock ports.Clock, delay time.Duration, onFire func(ports.Timer)) {
	p.timer = clock.NewTimer(delay)
	p.done = make(chan struct{})
	go func(timer ports.Timer, done <-chan struct{}) {
		select {
		case <-timer.C():
		case <-done:
			return
		}
		onFire(timer)
	}(p.timer, p.done)
}

func (p *pendingByteTimer) stop() {
	if p.timer != nil {
		p.timer.Stop()
		p.timer = nil
	}
	if p.done != nil {
		close(p.done)
		p.done = nil
	}
}

func (ac *attachedClient) initOverlays() {
	ac.overlayOnce.Do(func() { ac.overlays = newOverlayRuntime(ac) })
}

func (ac *attachedClient) currentSession() *session { return ac.sess.Get() }

func (ac *attachedClient) setSession(sess *session) { ac.sess.Set(sess) }

func (ac *attachedClient) clearPreviousSession() {
	if ac != nil {
		ac.previousSession.Set(nil)
	}
}

// pruneCaptureFrames releases snapshots for panes that have left their owner.
// Callers must not hold daemon, session, tab, or pane locks.
func (ac *attachedClient) pruneCaptureFrames(panes ...*pane) {
	if ac == nil || len(panes) == 0 {
		return
	}
	ac.sendMu.Lock()
	for _, p := range panes {
		delete(ac.captureFrames, p)
	}
	ac.sendMu.Unlock()
}

// clearCaptureFrames releases all pane snapshots when the attachment no longer
// owns a session.
func (ac *attachedClient) clearCaptureFrames() {
	if ac == nil {
		return
	}
	ac.sendMu.Lock()
	ac.captureFrames = nil
	ac.sendMu.Unlock()
}

func (ac *attachedClient) getAppliedTheme() appliedTheme {
	ac.themeMu.Lock()
	defer ac.themeMu.Unlock()
	applied := ac.appliedTheme
	if applied.Generation == 0 {
		// An unattached client has no terminal report yet. Reuse the static
		// neutral cache rather than resolving from a render path.
		applied.Resolved = themeui.ResolvedTheme{Theme: applied.Raw, Styles: fallbackChromeStyles}
	}
	return applied
}

func (ac *attachedClient) getClientTheme() themeui.Theme {
	ac.themeMu.Lock()
	defer ac.themeMu.Unlock()
	return ac.clientTheme
}

// setThemeForTest publishes a complete applied snapshot for tests that do
// not exercise daemon configuration. Production theme changes use
// applyHostThemeLocked, which supplies the active policy.
func (ac *attachedClient) setThemeForTest(t themeui.Theme) {
	ac.setAppliedTheme(appliedTheme{Raw: t, Resolved: themeui.Resolve(t, domain.ThemeAccent{Mode: domain.ThemeAccentAuto})})
}

func (ac *attachedClient) setAppliedTheme(next appliedTheme) {
	ac.themeMu.Lock()
	next.Generation = ac.appliedTheme.Generation + 1
	ac.appliedTheme = next
	ac.themeMu.Unlock()
}

func (ac *attachedClient) setClientTheme(t themeui.Theme) {
	ac.themeMu.Lock()
	ac.clientTheme = t
	ac.themeMu.Unlock()
}

func (ac *attachedClient) transport() ports.Transport {
	ac.linkMu.Lock()
	defer ac.linkMu.Unlock()
	return ac.tr
}

func (ac *attachedClient) replaceTransport(tr ports.Transport) {
	ac.linkMu.Lock()
	ac.tr = tr
	ac.transportIncarnation++
	ac.linkMu.Unlock()
}

// revokeTransport removes tr only if it is still this attachment's current
// link. It returns the revoked transport, so asynchronous retirement can
// close exactly that link without touching a later resume/replacement.
func (ac *attachedClient) revokeTransport(tr ports.Transport) ports.Transport {
	if tr == nil {
		return nil
	}
	ac.linkMu.Lock()
	if ac.tr != tr {
		ac.linkMu.Unlock()
		return nil
	}
	ac.tr = nil
	ac.transportIncarnation++
	ac.linkMu.Unlock()
	ac.clearSnatchedInput()
	return tr
}

// transportSnapshot binds a send to one concrete link lifetime. Callers must
// revalidate it after acquiring sendMu before writing to that link.
func (ac *attachedClient) transportSnapshot() transportSnapshot {
	ac.linkMu.Lock()
	defer ac.linkMu.Unlock()
	return transportSnapshot{transport: ac.tr, incarnation: ac.transportIncarnation}
}

func (ac *attachedClient) transportSnapshotCurrent(expected transportSnapshot) bool {
	ac.linkMu.Lock()
	defer ac.linkMu.Unlock()
	return ac.tr == expected.transport && ac.transportIncarnation == expected.incarnation
}

func (ac *attachedClient) closeCapturedTransport(tr ports.Transport) error {
	if tr == nil {
		return nil
	}
	return tr.Close()
}

func (ac *attachedClient) transportIs(tr ports.Transport) bool {
	ac.linkMu.Lock()
	defer ac.linkMu.Unlock()
	return ac.tr == tr
}

func (ac *attachedClient) currentTransportIs(tr ports.Transport) bool {
	return tr != nil && ac.transportIs(tr)
}

func (ac *attachedClient) ackOutputState(state uint64) {
	ac.sendMu.Lock()
	defer ac.sendMu.Unlock()
	ac.output.ack(state)
}

var errTransportReplaced = errors.New("client transport was replaced")

// sendExpectedTransport writes only when expected is still the attachment's
// current transport incarnation. It preserves sendMu -> linkMu lock ordering.
func (ac *attachedClient) sendExpectedTransport(expected transportSnapshot, f ports.Frame) error {
	ac.sendMu.Lock()
	defer ac.sendMu.Unlock()
	if !ac.transportSnapshotCurrent(expected) {
		return errTransportReplaced
	}
	if expected.transport == nil {
		return errors.New("client transport is nil")
	}
	return expected.transport.Send(f)
}

func (ac *attachedClient) sendExpectedTransportForRole(expected transportSnapshot, f ports.Frame, ticket *roleEffectTicket) error {
	ac.sendMu.Lock()
	defer ac.sendMu.Unlock()
	if ticket == nil || ticket.ended.Load() || !ac.transportSnapshotCurrent(expected) ||
		expected.transport == nil || !ticket.beginTransportSend(expected) {
		return errAttachmentTransition
	}
	err := expected.transport.Send(f)
	if err != nil {
		ticket.reportTransportFailure(expected)
	}
	ticket.endTransportSend()
	return err
}

// boundedSend sends f to ac with a deadline watchdog: if the send (including
// waiting on sendMu behind a wedged paint) does not complete within
// detachNotifyTimeout, the transport is force-closed, failing the in-flight
// write. Detach/kill/shutdown paths use this so they are never gated on a
// client that has stopped draining its socket.
func (d *Daemon) boundedSend(ac *attachedClient, f ports.Frame) {
	_ = d.boundedSendErr(ac, f)
}

func (d *Daemon) boundedSendErr(ac *attachedClient, f ports.Frame) error {
	expected := ac.transportSnapshot()
	if expected.transport == nil {
		return errors.New("client transport is nil")
	}
	tr, err := d.boundedSendWith(expected.transport, func() error {
		return ac.sendExpectedTransport(expected, f)
	})
	if err != nil && errors.Is(err, errSendTimedOut) {
		_ = ac.closeCapturedTransport(tr)
	}
	return err
}

func (d *Daemon) boundedSendOutputErr(ac *attachedClient, b []byte) error {
	_, err := d.boundedSendOutputErrTransport(ac, b)
	return err
}

func (d *Daemon) boundedSendOutputErrTransport(ac *attachedClient, b []byte) (ports.Transport, error) {
	frame := ac.output.sideEffect(b, ac.echoAck.Load())
	expected := ac.transportSnapshot()
	if expected.transport == nil {
		return nil, errors.New("client transport is nil")
	}
	ac.sendMu.Lock()
	if !ac.transportSnapshotCurrent(expected) {
		ac.sendMu.Unlock()
		return expected.transport, errTransportReplaced
	}
	if owned, ok := expected.transport.(ports.OwnedSynchronousTransport); ok {
		err := owned.SendSynchronous(frame)
		ac.sendMu.Unlock()
		return expected.transport, err
	}
	ac.sendMu.Unlock()
	return d.boundedSendWith(expected.transport, func() error {
		return ac.sendExpectedTransport(expected, frame)
	})
}

var errSendTimedOut = errors.New("send timed out")

func (d *Daemon) boundedSendWith(tr ports.Transport, send func() error) (ports.Transport, error) {
	return d.boundedSendWithTimeout(detachNotifyTimeout, tr, send)
}

func (d *Daemon) boundedSendWithTimeout(timeout time.Duration, tr ports.Transport, send func() error) (ports.Transport, error) {
	timer := d.clock.NewTimer(timeout)
	result := make(chan error, 1)
	go func() {
		result <- send()
	}()
	select {
	case err := <-result:
		timer.Stop()
		return tr, err
	case <-timer.C():
		select {
		case err := <-result:
			return tr, err
		default:
		}
		d.log.Warn("bounded send timed out; force closing client transport")
		return tr, errSendTimedOut
	}
}

type detachedAttachmentSnapshot struct {
	ac        *attachedClient
	transport transportSnapshot
}

// notifyDetachedSnapshotAsync sends and closes only the transport incarnation
// captured by atomic session teardown. It cannot touch a later link installed
// on the same attachedClient object.
func (d *Daemon) notifyDetachedSnapshotAsync(snapshot detachedAttachmentSnapshot, reason uint8) {
	if snapshot.ac == nil || snapshot.transport.transport == nil {
		return
	}
	done := make(chan struct{})
	d.mu.Lock()
	// Prune completed entries so the slice stays bounded by the number of
	// notifications actually in flight.
	kept := d.notifies[:0]
	for _, c := range d.notifies {
		select {
		case <-c:
		default:
			kept = append(kept, c)
		}
	}
	d.notifies = append(kept, done)
	d.mu.Unlock()

	go func() {
		defer close(done)
		_, err := d.boundedSendWith(snapshot.transport.transport, func() error {
			return snapshot.ac.sendExpectedTransport(snapshot.transport, frameDetached(reason))
		})
		if errors.Is(err, errSendTimedOut) {
			_ = snapshot.ac.closeCapturedTransport(snapshot.transport.transport)
		}
		revoked := snapshot.ac.revokeTransport(snapshot.transport.transport)
		if revoked == nil {
			revoked = snapshot.transport.transport
		}
		_ = snapshot.ac.closeCapturedTransport(revoked)
		d.log.Info("client detached", "reason", reason)
	}()
}

// waitNotifies blocks until every Detached notification in flight at the time
// of the call has completed. Each is deadline-bounded (boundedSend), so this
// wait is bounded too.
func (d *Daemon) waitNotifies() {
	for _, c := range d.notifiesSnapshot() {
		<-c
	}
}

func (d *Daemon) notifiesSnapshot() []chan struct{} {
	d.mu.Lock()
	defer d.mu.Unlock()
	snapshot := make([]chan struct{}, len(d.notifies))
	copy(snapshot, d.notifies)
	return snapshot
}

type attachClientOptions struct {
	clientID          [16]byte
	resumeCapable     bool
	maxOutputInFlight uint8
}

func (d *Daemon) attachClient(sess *session, tr ports.Transport, sz domain.Size, opts attachClientOptions) (*attachedClient, *attachedClient, error) {
	d.mu.Lock()
	ac := d.prepareAttachedClientLocked(tr, sz, opts)
	d.mu.Unlock()
	result, err := d.transitionAttachment(attachmentTransitionRequest{
		target:            sess,
		next:              ac,
		expectedRole:      attachmentDetached,
		targetRole:        attachmentActive,
		expectedTransport: ac.transportSnapshot(),
		ready:             false,
	})
	if err != nil {
		if tr != nil {
			_ = tr.Close()
		}
		return nil, nil, err
	}
	d.finishAttachedClient(sess, ac, opts)
	d.deferAttachmentTransitionCleanups(result)
	return ac, result.displaced.ac, nil
}

func (d *Daemon) finishAttachedClient(sess *session, ac *attachedClient, opts attachClientOptions) {
	d.touchMRU(sess)
	d.log.Info("client attached", "session", sess.name, "resume", opts.resumeCapable)
	d.applyHostTheme(sess, ac, themeui.Theme{}, true)
}

// prepareAttachedClientLocked allocates one detached attachment. Caller holds
// d.mu only to allocate its resume token; role publication happens later after
// the caller releases every architecture lock.
func (d *Daemon) prepareAttachedClientLocked(tr ports.Transport, sz domain.Size, opts attachClientOptions) *attachedClient {
	resumeToken := uint64(0)
	if opts.resumeCapable {
		resumeToken = d.nextResumeTokenLocked()
	}
	ac := &attachedClient{
		tr:            tr,
		output:        newOutputStateStream(opts.maxOutputInFlight),
		size:          sz,
		clientID:      opts.clientID,
		resumeCapable: opts.resumeCapable,
		resumeToken:   resumeToken,
	}
	ac.initOverlays()
	ac.keys = keys.NewRouter(d.clock, daemonKeyHandler{d: d, ac: ac}, &d.bindings)
	return ac
}

// attachCoordinator is the sole direct attachment handoff. It creates at
// most one coordinator for sess and changes its identity before any caller
// can publish resize or render state for the new client.
func (d *Daemon) attachCoordinator(sess *session, old, current *attachedClient, ready bool) *renderCoordinator {
	rc, cleanup := d.attachCoordinatorDeferred(sess, old, current, ready)
	cleanup.finish()
	return rc
}

func (d *Daemon) attachCoordinatorDeferred(sess *session, old, current *attachedClient, ready bool) (*renderCoordinator, renderLifecycleCleanup) {
	rc := d.ensureRenderCoordinator(sess)
	var cleanup renderLifecycleCleanup
	if old != nil {
		cleanup = rc.beginReplace(old, current, ready)
	} else if current != nil {
		rc.attachWithReadiness(current, ready)
	}
	return rc, cleanup
}

// ensureRenderCoordinator publishes the session's single coordinator without
// binding an attachment lease. Centralized attachment transitions bind the
// lease later while their ordered session locks are still held.
func (d *Daemon) ensureRenderCoordinator(sess *session) *renderCoordinator {
	sess.mu.Lock()
	defer sess.mu.Unlock()
	if rc := sess.renderCoordinator(); rc != nil {
		return rc
	}
	var rc *renderCoordinator
	rc = newRenderCoordinator(renderCoordinatorOptions{
		clock:    d.clock,
		observer: d.runtimeObserver,
		wake: func(w renderWake) {
			// Never reread sess.client here: a wake is bound to both its
			// attachment and its coordinator incarnation. The second check in
			// paint occurs under sendMu, closing a park/resume race after this
			// unlocked coordinator validation.
			if w.lease != nil && rc.wakeCurrent(w) {
				d.paint(sess, w.lease.attachment, w.reset, w.lease)
			}
		},
		ackReady: func() bool {
			sess.mu.Lock()
			attached := sess.client
			sess.mu.Unlock()
			if attached == nil {
				return false
			}
			attached.sendMu.Lock()
			ready := attached.output == nil || !attached.output.atCapacity()
			attached.sendMu.Unlock()
			return ready
		},
	})
	sess.installRenderCoordinator(rc)
	return rc
}

// resetScreenDefaultColors clears the known default foreground/background
// colors on every tab in sess. Called once a client has been detached, this
// makes child OSC 10/11 queries go back to being swallowed (Known=false)
// instead of being answered with the departed client's colors, which the
// next client (with a different terminal theme) may never have reported.
// Mirrors attachClient's reset loop: snapshot sess.tabs under sess.mu,
// release it, then take each tb.mu in turn — never holding sess.mu and
// tb.mu together. Guarded against a race with a newer attach: if sess.client
// is non-nil by the time sess.mu is taken, a new client has already attached
// (and run its own attach-time reset), so this call must leave the tabs
// alone rather than clobbering that client's freshly applied colors.
func (d *Daemon) resetScreenDefaultColors(sess *session) {
	d.applyHostTheme(sess, nil, themeui.Theme{}, true)
}

// firstPaintForTransition is the only active post-transition paint entry
// point. It rejects a stale role capability before rebasing output and carries
// the exact coordinator lease through every resize and render effect.
func (d *Daemon) firstPaintForTransition(token attachmentRoleToken) bool {
	if !token.activeCurrent() {
		return false
	}
	ticket, admitted := token.ac.beginRoleEffect(token)
	if !admitted {
		return false
	}
	token.effect = ticket
	defer token.endRoleEffect()
	if d.afterRoleEffectAdmitted != nil {
		d.afterRoleEffectAdmitted(token)
	}
	if token.rebase {
		token.ac.sendMu.Lock()
		if token.ac.renderStages.handoffRebase != nil {
			token.ac.renderStages.handoffRebase()
		}
		if token.ac.output != nil {
			token.ac.output.rebase()
		}
		// Captures belong to the old session even when pane-local IDs happen
		// to be reused by the destination.
		token.ac.captureFrames = nil
		token.ac.sendMu.Unlock()
	}
	d.firstPaintWithLease(token.sess, token.ac, token.ac.size, token.lease)
	return true
}

// firstPaint guarantees the freshly attached client sees the full screen: if
// the tab size differs from the client's it resizes first, then immediately
// emits a full redraw. It is retained for direct test/headless setup; active
// role transitions use firstPaintForTransition with their exact lease.
func (d *Daemon) firstPaint(sess *session, ac *attachedClient, clientSize domain.Size) {
	d.firstPaintWithLease(sess, ac, clientSize, nil)
}

func (d *Daemon) firstPaintWithLease(sess *session, ac *attachedClient, clientSize domain.Size, lease *attachmentLease) {
	// Global notices raised while nothing was attached surface on this client.
	// Drained before the early return below so a session without an active tab
	// cannot swallow the queue.
	d.drainPendingForFirstPaint(sess, ac)
	tb := sess.activeTab()
	if tb == nil {
		return
	}
	tb.mu.Lock()
	wsz := tb.size
	tb.mu.Unlock()

	outerResizeAccepted := false
	if clientSize.Valid() && wsz != tabSize(clientSize) {
		if lease == nil {
			outerResizeAccepted = d.resizeForFirstPaint(sess, ac, clientSize)
		} else {
			outerResizeAccepted = d.requestTransactionalResizeForLease(sess, ac, lease, clientSize, true)
		}
	}
	d.refreshBarScriptsIfDue(sess, d.clock.Now(), true)
	// Activation can synchronously resize a retained floating pane. An accepted
	// synchronous outer request already includes that pane and emits the reset,
	// but activation still performs its warmup work.
	activationResized := d.activateTabAfterResizeForLease(sess, tb, outerResizeAccepted, ac, lease)
	if !outerResizeAccepted && !activationResized {
		if lease == nil {
			d.invalidateRenderNow(sess, ac, true, "client.go")
		} else if rc := sess.renderCoordinator(); rc != nil && rc.invalidateForLease(ac, lease, renderInvalidation{class: invalidateUrgent, reset: true, producer: "client.go"}) {
			rc.fireCurrent(false)
		}
	}
}

// runConnLoop is the per-connection input router: it pumps client messages
// until detach, EOF, or a transport error. Role is resolved after every Recv so
// a transport displaced while blocked in Recv immediately adopts its restricted
// snatched routing rather than executing one stale active frame.
func (d *Daemon) runConnLoop(ac *attachedClient) {
	tr := ac.transport()
	if tr == nil {
		return
	}
	for {
		if !ac.currentTransportIs(tr) {
			return
		}
		sess := ac.currentSession()
		if sess == nil {
			return
		}
		f, err := tr.Recv()
		if err != nil {
			token := sess.attachmentToken(ac, tr)
			if token.role == attachmentSnatched {
				d.parkOrDropSnatchedAttachment(token)
			} else {
				d.clientGone(sess, ac, tr, false)
			}
			return
		}
		if !ac.currentTransportIs(tr) {
			return
		}
		sess = ac.currentSession()
		if sess == nil {
			return
		}
		token := sess.attachmentToken(ac, tr)
		switch token.role {
		case attachmentActive:
			if d.handleActiveClientFrame(token, f) {
				return
			}
		case attachmentSnatched:
			if d.handleSnatchedClientFrame(token, f) {
				return
			}
		default:
			return
		}
	}
}

// handleActiveClientFrame owns the ordinary attached-client protocol. Keeping
// it separate makes the restricted snatched protocol an explicit allow-list.
func (d *Daemon) handleActiveClientFrame(token attachmentRoleToken, f ports.Frame) bool {
	if !token.activeEffect() {
		return false
	}
	if d.afterActiveFrameDispatch != nil {
		d.afterActiveFrameDispatch(token)
	}
	ticket, admitted := token.ac.beginRoleEffect(token)
	if !admitted {
		return false
	}
	token.effect = ticket
	defer token.endRoleEffect()
	if d.afterRoleEffectAdmitted != nil {
		d.afterRoleEffectAdmitted(token)
	}
	switch f.Type {
	case ports.MsgInput:
		if in, derr := ports.UnmarshalInput(f.Payload); derr == nil {
			d.handleSequencedInputForRole(token, in.InputSeq, in.Data)
		}
	case ports.MsgResize:
		if rz, derr := ports.UnmarshalResize(f.Payload); derr == nil && token.activeEffect() {
			d.requestTransactionalResizeForLease(token.sess, token.ac, token.lease, rz.Size, false)
		}
	case ports.MsgTheme:
		if th, derr := ports.UnmarshalTheme(f.Payload); derr == nil {
			d.applyThemeForRole(token, th)
		}
	case ports.MsgImagePush:
		if ip, derr := ports.UnmarshalImagePush(f.Payload); derr == nil {
			d.handleSequencedImagePushForRole(token, ip.InputSeq, ip)
		}
	case ports.MsgClientNotice:
		if notice, derr := ports.UnmarshalClientNotice(f.Payload); derr == nil {
			d.handleClientNoticeForRole(token, notice)
		} else {
			d.log.Warn("malformed client notice", "err", derr)
		}
	case ports.MsgDetach:
		if token.activeEffect() {
			d.clientGoneForRole(token, true)
			return true
		}
	case ports.MsgAck:
		if ack, derr := ports.UnmarshalAck(f.Payload); derr == nil {
			d.ackActiveOutput(token, ack.AckedStateNum)
		}
	case ports.MsgPing:
		if err := token.sendActiveControl(framePong()); err != nil {
			token.endRoleEffect()
			d.detachOnSendError(token.sess, token.ac, token.transport.transport)
			return true
		}
	default:
		// Unknown/out-of-band client messages are ignored so a newer
		// client can add message types without breaking an older daemon.
	}
	return false
}

func (d *Daemon) ackActiveOutput(token attachmentRoleToken, state uint64) bool {
	ac := token.ac
	if token.effect == nil || token.effect.ended.Load() {
		return false
	}
	ac.sendMu.Lock()
	ac.output.ack(state)
	ac.sendMu.Unlock()
	if rc := token.sess.renderCoordinator(); rc != nil {
		rc.notifyAckForLease(token.lease)
	}
	return true
}

// handleSnatchedClientFrame is intentionally an allow-list. Strict resume and
// quit actions are introduced separately; until then ordinary input is ignored.
func (d *Daemon) handleSnatchedClientFrame(token attachmentRoleToken, f ports.Frame) bool {
	if !token.current() {
		return false
	}
	ticket, admitted := token.ac.beginRoleEffect(token)
	if !admitted {
		return false
	}
	token.effect = ticket
	defer token.endRoleEffect()
	if d.afterRoleEffectAdmitted != nil {
		d.afterRoleEffectAdmitted(token)
	}
	switch f.Type {
	case ports.MsgInput:
		if in, err := ports.UnmarshalInput(f.Payload); err == nil && d.handleSnatchedInput(token, in) {
			return true
		}
	case ports.MsgResize:
		if resize, err := ports.UnmarshalResize(f.Payload); err == nil && resize.Size.Valid() {
			ac := token.ac
			ac.sendMu.Lock()
			if ac.roleGeneration.Load() != token.generation || !ac.transportSnapshotCurrent(token.transport) {
				ac.sendMu.Unlock()
				return false
			}
			ac.size = resize.Size
			ac.sendMu.Unlock()
			if err := d.sendSnatchedPanel(ac, token.transport, token.generation, "", token.effect); err != nil {
				token.endRoleEffect()
				d.parkOrDropSnatchedAttachment(token)
				return true
			}
		}
	case ports.MsgTheme:
		if theme, err := ports.UnmarshalTheme(f.Payload); err == nil {
			ac := token.ac
			ac.sendMu.Lock()
			if ac.roleGeneration.Load() != token.generation || !ac.transportSnapshotCurrent(token.transport) {
				ac.sendMu.Unlock()
				return false
			}
			clientTheme := themeFromMessage(theme)
			ac.setClientTheme(clientTheme)
			ac.setAppliedTheme(d.resolveAppliedTheme(clientTheme))
			ac.sendMu.Unlock()
			if err := d.sendSnatchedPanel(ac, token.transport, token.generation, "", token.effect); err != nil {
				token.endRoleEffect()
				d.parkOrDropSnatchedAttachment(token)
				return true
			}
		}
	case ports.MsgAck:
		if ack, err := ports.UnmarshalAck(f.Payload); err == nil {
			token.ac.ackOutputState(ack.AckedStateNum)
		}
	case ports.MsgPing:
		if err := d.sendSnatchedControl(token, framePong()); err != nil {
			token.endRoleEffect()
			d.parkOrDropSnatchedAttachment(token)
			return true
		}
	case ports.MsgDetach:
		_ = d.sendSnatchedControl(token, frameDetached(ports.ReasonDetach))
		token.endRoleEffect()
		d.dropSnatchedAttachment(token)
		return true
	}
	return false
}

// clientGone detaches ac if it is still the session's current client. The
// session remains registered and headless after the client is gone.
func (d *Daemon) clientGone(sess *session, ac *attachedClient, failed ports.Transport, explicit bool) {
	if failed != nil && !ac.currentTransportIs(failed) {
		return // stale connection loop; a newer transport owns this client
	}
	if !d.detachIfCurrent(sess, ac) {
		return // already displaced by a newer client; nothing to do
	}
	d.finishClientGone(sess, ac, failed, explicit)
}

func (d *Daemon) clientGoneForRole(token attachmentRoleToken, explicit bool) bool {
	if token.effect == nil {
		return false
	}
	token.effect.bindActionEnd(d, "detach")
	token.effect.End()
	if !d.detachIfRoleCurrent(token) {
		return false
	}
	d.finishClientGone(token.sess, token.ac, token.transport.transport, explicit)
	return true
}

func (d *Daemon) finishClientGone(sess *session, ac *attachedClient, failed ports.Transport, explicit bool) {
	if rc := sess.renderCoordinator(); rc != nil {
		rc.noteDetach(ac)
	}
	d.unregisterPreview(ac)
	sess.mu.Lock()
	ephemeral := sess.ephemeral
	sess.mu.Unlock()
	if !ephemeral {
		d.refreshSessionCwd(sess)
	}
	d.log.Info("client detach begin", "session", sess.name, "explicit", explicit, "ephemeral", ephemeral)
	oldTr := failed
	if oldTr == nil {
		oldTr = ac.transport()
	}
	if !explicit && d.parkAttachment(sess, ac) {
		_ = ac.closeCapturedTransport(oldTr)
		d.log.Info("client parked", "session", sess.name)
		return
	}

	d.resetScreenDefaultColors(sess)
	ac.clearPreviousSession()
	if explicit {
		// Synchronous so the ack is delivered before the transport closes
		// (the client is actively awaiting it), but deadline-bounded so a
		// wedged client cannot pin this conn handler and hang Serve's
		// connWg.Wait.
		d.boundedSend(ac, frameDetached(ports.ReasonDetach))
	}
	_ = ac.closeCapturedTransport(oldTr)
	d.log.Info("client detached", "session", sess.name, "explicit", explicit)
}

// detachIfCurrent clears the client iff ac is the current one, reporting

func themeFromMessage(msg ports.Theme) themeui.Theme {
	return themeui.Theme{
		Foreground:   msg.Foreground,
		Background:   msg.Background,
		Palette:      msg.Palette,
		PaletteKnown: msg.PaletteKnown,
		HasFG:        msg.HasForeground,
		HasBG:        msg.HasBackground,
		TrueColor:    msg.TrueColor,
		Known:        msg.HasForeground && msg.HasBackground,
		SchemeKnown:  msg.SchemeKnown,
		Light:        msg.Light,
	}
}

func (d *Daemon) applyTheme(sess *session, ac *attachedClient, msg ports.Theme) {
	clientTheme := themeFromMessage(msg)
	ac.setClientTheme(clientTheme)

	if !d.applyHostTheme(sess, ac, clientTheme, false) {
		return
	}
	d.invalidateRender(sess, ac, true, "client.go")
}

func (d *Daemon) applyThemeForRole(token attachmentRoleToken, msg ports.Theme) bool {
	if !token.activeEffect() {
		return false
	}
	clientTheme := themeFromMessage(msg)
	token.ac.setClientTheme(clientTheme)
	if !token.activeEffect() || !d.applyHostTheme(token.sess, token.ac, clientTheme, false) {
		return false
	}
	if !token.activeEffect() {
		return false
	}
	if rc := token.sess.renderCoordinator(); rc != nil {
		return rc.invalidateForLease(token.ac, token.lease, renderInvalidation{class: invalidateUrgent, reset: true, producer: "client.go"})
	}
	return false
}

func (d *Daemon) resize(sess *session, ac *attachedClient, sz domain.Size) {
	d.requestTransactionalResize(sess, ac, sz, false)
}

// handleClientNotice maps the closed client-event enum to daemon-owned notice
// content. routingMu makes ownership validation and toast mutation one atomic
// attachment-routing operation: replacement also takes routingMu before
// publishing sess.client. Never retain sess.mu while touching notice or overlay
// state, and retain the routingMu -> sess.mu order used by attachment paths.
func (d *Daemon) handleClientNotice(sess *session, ac *attachedClient, notice ports.ClientNotice) {
	if sess == nil || ac == nil {
		return
	}
	tr := ac.transport()
	token := sess.attachmentToken(ac, tr)
	if token.lease == nil {
		// Direct test/headless callers retain pointer-based routing. Production
		// client frames always carry the exact lease through the role path below.
		d.handleClientNoticeDirect(sess, ac, notice)
		return
	}
	d.handleClientNoticeForRole(token, notice)
}

func (d *Daemon) handleClientNoticeForRole(token attachmentRoleToken, notice ports.ClientNotice) {
	d.notices.routingMu.Lock()
	if !token.activeEffect() {
		d.notices.routingMu.Unlock()
		return
	}
	if d.notices.beforeClientNoticeMutation != nil {
		d.notices.beforeClientNoticeMutation()
	}
	if !token.activeEffect() {
		d.notices.routingMu.Unlock()
		return
	}
	repaint := d.mutateClientNotice(token.sess, token.ac, notice)
	d.notices.routingMu.Unlock()
	if repaint && token.activeEffect() {
		d.repaintForNotice(token.ac)
	}
}

func (d *Daemon) handleClientNoticeDirect(sess *session, ac *attachedClient, notice ports.ClientNotice) {
	d.notices.routingMu.Lock()
	sess.mu.Lock()
	current := sess.client == ac
	sess.mu.Unlock()
	if !current {
		d.notices.routingMu.Unlock()
		return
	}
	if d.notices.beforeClientNoticeMutation != nil {
		d.notices.beforeClientNoticeMutation()
	}
	repaint := d.mutateClientNotice(sess, ac, notice)
	d.notices.routingMu.Unlock()
	if repaint {
		d.repaintForNotice(ac)
	}
}

func (d *Daemon) mutateClientNotice(sess *session, ac *attachedClient, notice ports.ClientNotice) bool {
	switch notice.Action {
	case ports.ClientNoticeClipboardFallback:
		return d.recordClientNotice(sess, ac, domain.NoticeError, domain.NoticeClipboard, "image paste failed; sent Ctrl+V")
	case ports.ClientNoticeClipboardTooLarge:
		return d.recordClientNotice(sess, ac, domain.NoticeWarn, domain.NoticeClipboardTooLarge, "image too large to paste")
	case ports.ClientNoticeLinkDegraded:
		return d.recordClientNotice(sess, ac, domain.NoticeWarn, domain.NoticeConnection, "connection degraded")
	case ports.ClientNoticeLinkConnected:
		return d.dismissToastWithoutRepaint(ac, domain.NoticeConnection, sess.id)
	default:
		return false
	}
}

// recordClientNotice mutates notice history and the selected attachment while
// routingMu keeps replacement from changing that selection. Rendering is left
// to the caller after it releases routingMu.
func (d *Daemon) recordClientNotice(sess *session, ac *attachedClient, sev domain.NoticeSeverity, code domain.NoticeCode, message string) bool {
	n := d.notices.record(domain.Notification{
		Code:      code,
		Severity:  sev,
		Message:   message,
		Time:      d.clock.Now(),
		SessionID: sess.id,
	})
	return d.publishToast(ac, n)
}

// resizeForFirstPaint retains attach's synchronous geometry guarantee. The
// returned value reports whether the synchronous request was accepted.
func (d *Daemon) resizeForFirstPaint(sess *session, ac *attachedClient, sz domain.Size) bool {
	return d.requestTransactionalResize(sess, ac, sz, true)
}

// detachOnSendError drops a client whose transport failed, leaving the session
// registered and headless.
func (d *Daemon) detachOnSendError(sess *session, ac *attachedClient, failed ports.Transport) {
	if failed != nil && !ac.currentTransportIs(failed) {
		return
	}
	if d.detachIfCurrent(sess, ac) {
		d.finishSendErrorDetach(sess, ac, failed)
	}
}

func (d *Daemon) detachOnRoleSendError(token attachmentRoleToken, failed ports.Transport) {
	d.detachOnRoleSendErrorUntil(token, failed, nil)
}

func (d *Daemon) detachOnRoleSendErrorUntil(token attachmentRoleToken, failed ports.Transport, done func() <-chan struct{}) {
	if failed != token.transport.transport {
		return
	}
	if d.detachIfRoleCurrentUntil(token, done) {
		d.finishSendErrorDetach(token.sess, token.ac, failed)
	}
}

// reserveRoleSendErrorCleanup accounts for cleanup before End releases the
// role gate. This closes the WaitGroup Add/Wait race with terminal teardown;
// the returned launch function must be invoked immediately after ticket End.
func (d *Daemon) reserveRoleSendErrorCleanup(token attachmentRoleToken, failed ports.Transport) func() {
	d.attachmentCleanupWg.Add(1)
	return func() {
		go func() {
			defer d.attachmentCleanupWg.Done()
			if d.afterRoleSendErrorCleanup != nil {
				defer d.afterRoleSendErrorCleanup()
			}
			if d.beforeRoleSendErrorCleanup != nil {
				d.beforeRoleSendErrorCleanup(token)
			}
			deadline := newRoleEffectDrainDeadline(d.clock)
			defer deadline.stop()
			d.detachOnRoleSendErrorUntil(token, failed, deadline.Done)
		}()
	}
}

func (d *Daemon) finishSendErrorDetach(sess *session, ac *attachedClient, failed ports.Transport) {
	if rc := sess.renderCoordinator(); rc != nil {
		rc.noteDetach(ac)
	}
	d.unregisterPreview(ac)
	if d.parkAttachment(sess, ac) {
		_ = ac.closeCapturedTransport(failed)
		d.log.Warn("parked client after send error", "session", sess.name)
		return
	}
	d.resetScreenDefaultColors(sess)
	ac.clearPreviousSession()
	_ = ac.closeCapturedTransport(failed)
	d.log.Warn("detached client after send error", "session", sess.name)
}

// killSession removes a session and tears down its resources. It is
// idempotent: only the caller that wins the registry delete acts. When the
// registry empties it marks the daemon closing (atomically with the
// empty-check, under d.mu) and signals shutdown.
//
// Teardown ordering matters: context cancel, pty.Close, and the done signal
// run first and unconditionally — never gated behind a client send. The
