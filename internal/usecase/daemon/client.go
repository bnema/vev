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
	// screenOutput is allocated only for proxied attachments and shares output's
	// state-number/ACK window. Both streams are serialized by sendMu.
	screenOutput *structuredOutputStream
	overlays     *overlayRuntime
	overlayOnce  sync.Once
	clientID     [16]byte
	// connectionGeneration is the wire-facing capability generation. attachmentEffects is
	// the linearization gate for every attachment-bound observable operation. A role
	// transition freezes and drains this gate before changing either generation
	// or the session/coordinator registries.
	connectionGeneration atomic.Uint64
	attachmentEffects    attachmentEffectGate
	resumeCapable        bool
	resumeToken          uint64
	// resumeClaimToken is non-zero only between a parked resume claim and its
	// successful Welcome. It lets a failed pre-claim handshake restore the old
	// credential instead of consuming it.
	resumeClaimToken uint64
	parked           bool
	// proxied is negotiated once by Hello and remains immutable for this
	// attachment, including across transport resume.
	proxied bool
	echoAck atomic.Uint64
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
	// proxyCapture retains the last captured proxy frame for damage-aware
	// updates. It is attachment-owned and only touched while sendMu is held.
	proxyCapture proxyCapture
	// sessionMeta is the last authoritative metadata snapshot successfully sent
	// on a proxied attachment. It is only read or written while sendMu is held.
	sessionMeta     ports.SessionMeta
	sessionMetaSent bool
	// captureFrames is keyed by pane ownership, not the tab-local PaneID, so
	// snapshots cannot leak when an attachment switches tabs or sessions.
	captureFrames map[*pane]capturedPaneRenderState // only touched while sendMu is held
	size          domain.Size
	keys          *keys.Router
	// view is attachment-local navigation state. It is never inferred from a
	// session-wide active tab, so multiple attachments can observe different
	// tabs and panes without changing shared session ownership.
	viewMu       sync.Mutex
	view         attachmentView
	sess         Guarded[attachmentSession]
	mouseScan    mouse.Scanner
	themeMu      sync.Mutex
	clientTheme  themeui.Theme
	appliedTheme appliedTheme
	lastCursor   cursorOut
	renderStages renderStageHooks // optional render and handoff observability hooks
	// previousSession is guarded independently. It is retained through temporary
	// setSession(nil) hand-offs and cleared only on terminal teardown.
	previousSession Guarded[attachmentSession]
	linkMu          sync.Mutex
	sendMu          sync.Mutex
	// beforeAttachmentTokenValidation is a deterministic lifecycle-race seam
	// used only by package tests.
	beforeAttachmentTokenValidation func()
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

func (ac *attachedClient) currentAttachmentSession() attachmentSession { return ac.sess.Get() }

// currentSession is the explicit local-only narrowing used until proxy-aware
// consumers are introduced. Local PTY/tab paths must treat non-local entries as
// unavailable rather than infer proxy behavior.
func (ac *attachedClient) currentSession() *session {
	sess, _ := localSession(ac.currentAttachmentSession())
	return sess
}

func (ac *attachedClient) setSession(sess attachmentSession) { ac.sess.Set(sess) }

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
// close exactly that link without touching a later connection incarnation.
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

// ensureScreenOutput is called while sendMu owns the attachment. Production
// constructors allocate this at attachment creation; the lazy path keeps
// hand-built test attachments faithful when they set proxied after creation.
func (ac *attachedClient) ensureScreenOutput() *structuredOutputStream {
	if ac == nil || !ac.proxied || ac.output == nil {
		return nil
	}
	if ac.screenOutput == nil {
		ac.screenOutput = newStructuredOutputStream(ac.output)
	}
	return ac.screenOutput
}

// discardProxyCapture abandons a candidate that was not emitted. The next
// capture clones the authoritative screen once, avoiding replaying pending
// scroll damage onto an already-captured failed candidate.
func (ac *attachedClient) discardProxyCapture() {
	if ac != nil && ac.proxied {
		ac.proxyCapture = proxyCapture{}
	}
}

// rebaseOutput resets both wire representations while retaining the shared
// state-number chain. Callers hold sendMu (or the activation barrier).
func (ac *attachedClient) rebaseOutput() {
	if ac == nil {
		return
	}
	if ac.proxied {
		ac.ensureScreenOutput()
	}
	if ac.output != nil {
		ac.output.rebase()
	}
	if ac.screenOutput != nil {
		ac.screenOutput.rebase()
	}
	ac.proxyCapture = proxyCapture{}
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

// beginExpectedTransportSendLocked validates that expected is still the
// attachment's current transport incarnation and admits ticket's interruptible
// transport send. The caller must already hold ac.sendMu and, on success, owns
// both the send and its matching ticket.endTransportSend.
func (ac *attachedClient) beginExpectedTransportSendLocked(expected transportSnapshot, ticket *attachmentEffectTicket) error {
	if expected.transport == nil || !ac.transportSnapshotCurrent(expected) {
		return errTransportReplaced
	}
	if ticket != nil && (ticket.ended.Load() || !ticket.beginTransportSend(expected)) {
		return errAttachmentTransition
	}
	return nil
}

func (ac *attachedClient) sendExpectedTransportForAttachment(expected transportSnapshot, f ports.Frame, ticket *attachmentEffectTicket) error {
	ac.sendMu.Lock()
	defer ac.sendMu.Unlock()
	// A attachment-bound send requires a live ticket, and reports every rejection,
	// including a replaced transport, as a lost attachment transition.
	if ticket == nil {
		return errAttachmentTransition
	}
	if err := ac.beginExpectedTransportSendLocked(expected, ticket); err != nil {
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
		_, _ = d.boundedSendWith(snapshot.transport.transport, func() error {
			return snapshot.ac.sendExpectedTransport(snapshot.transport, frameDetached(reason))
		})
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
	proxied           bool
}

func (d *Daemon) attachClient(sess *session, tr ports.Transport, sz domain.Size, opts attachClientOptions) (*attachedClient, error) {
	d.mu.Lock()
	ac := d.prepareAttachedClientLocked(tr, sz, opts)
	d.mu.Unlock()
	result, err := d.transitionAttachment(attachmentTransitionRequest{
		target: sess,
		next:   ac,

		expectedTransport: ac.transportSnapshot(),
		ready:             false,
	})
	if err != nil {
		if tr != nil {
			_ = tr.Close()
		}
		return nil, err
	}
	d.finishAttachedClient(sess, ac, opts)
	d.deferAttachmentTransitionCleanups(result)
	return ac, nil
}

func (d *Daemon) finishAttachedClient(sess *session, ac *attachedClient, opts attachClientOptions) {
	d.touchMRU(sess)
	d.log.Info("client attached", "session", sess.name, "resume", opts.resumeCapable)
	d.applyHostTheme(sess, ac, themeui.Theme{}, true)
}

// prepareAttachedClientLocked allocates one detached attachment. Caller holds
// d.mu only to allocate its resume token; attachment publication happens later after
// the caller releases every architecture lock.
func (d *Daemon) prepareAttachedClientLocked(tr ports.Transport, sz domain.Size, opts attachClientOptions) *attachedClient {
	resumeToken := uint64(0)
	if opts.resumeCapable {
		resumeToken = d.nextResumeTokenLocked()
	}
	output := newOutputStateStream(opts.maxOutputInFlight)
	ac := &attachedClient{
		tr:            tr,
		output:        output,
		size:          sz,
		clientID:      opts.clientID,
		resumeCapable: opts.resumeCapable,
		resumeToken:   resumeToken,
		proxied:       opts.proxied,
	}
	if opts.proxied {
		ac.screenOutput = newStructuredOutputStream(output)
	}
	ac.initOverlays()
	ac.keys = keys.NewRouter(d.clock, daemonKeyHandler{d: d, ac: ac}, &d.bindings)
	return ac
}

// attachCoordinator installs the coordinator lease for an attachment. Session
// membership remains independent, so installing one attachment never displaces
// another.
func (d *Daemon) attachCoordinator(sess *session, old, current *attachedClient, ready bool) *renderCoordinator {
	rc, cleanup := d.attachCoordinatorDeferred(sess, old, current, ready)
	cleanup.finish()
	return rc
}

func (d *Daemon) attachCoordinatorDeferred(sess *session, _ *attachedClient, current *attachedClient, ready bool) (*renderCoordinator, renderLifecycleCleanup) {
	rc := d.ensureRenderCoordinator(sess)
	if current != nil {
		rc.attachWithReadiness(current, ready)
	}
	return rc, renderLifecycleCleanup{}
}

// ensureRenderCoordinator publishes the session's single coordinator without
// binding an attachment lease. Centralized attachment transitions bind the
// lease later while their ordered session locks are still held.
func (d *Daemon) ensureRenderCoordinator(sess *session) *renderCoordinator {
	sess.mu.Lock()
	defer sess.mu.Unlock()
	return d.ensureRenderCoordinatorPrelocked(sess)
}

// ensureRenderCoordinatorPrelocked is the non-reentrant coordinator setup seam
// for transactions that already hold sess.mu.
func (d *Daemon) ensureRenderCoordinatorPrelocked(sess *session) *renderCoordinator {
	return d.ensureAttachmentRenderCoordinatorPrelocked(sess)
}

// ensureAttachmentRenderCoordinatorPrelocked publishes the coordinator for an
// exact attachment implementation. The caller holds entry.core().mu; proxy
// VT state is deliberately not touched here.
func (d *Daemon) ensureAttachmentRenderCoordinatorPrelocked(entry attachmentSession) *renderCoordinator {
	if rc := attachmentRenderCoordinator(entry); rc != nil {
		return rc
	}
	var rc *renderCoordinator
	rc = newRenderCoordinator(renderCoordinatorOptions{
		clock:    d.clock,
		observer: d.runtimeObserver,
		wake: func(w renderWake) {
			if !rc.wakeCurrent(w) {
				return
			}
			// The session snapshot is deterministic. Each paint revalidates the
			// attachment's membership and transport fence after that snapshot, so
			// a detached peer cannot stop remaining attachments receiving output.
			attachments := snapshotAttachmentSession(entry)
			for index, attachment := range attachments {
				if !rc.wakeCurrent(w) {
					return
				}
				lease := w.attachmentLeases[attachment]
				if lease == nil || !rc.leaseCurrent(lease, true) {
					continue
				}
				// Pane damage is session-shared; later attachments need a complete
				// frame because the first capture consumes shared damage.
				d.paint(entry, attachment, w.reset || index != 0, lease)
			}
		},
		ackReady: func() bool {
			attachments := snapshotAttachmentSession(entry)
			for _, attached := range attachments {
				attached.sendMu.Lock()
				ready := attached.output == nil || !attached.output.atCapacity()
				attached.sendMu.Unlock()
				if !ready {
					return false
				}
			}
			return true
		},
	})
	installAttachmentRenderCoordinator(entry, rc)
	return rc
}

// resetScreenDefaultColors clears the known default foreground/background
// colors on every tab in sess. Called once a client has been detached, this
// makes child OSC 10/11 queries go back to being swallowed (Known=false)
// instead of being answered with the departed client's colors, which the
// next client (with a different terminal theme) may never have reported.
// Mirrors attachClient's reset loop: snapshot sess.tabs under sess.mu,
// release it, then take each tb.mu in turn — never holding sess.mu and
// tb.mu together. Guarded against a race with a newer attachment: if the
// session has gained an attachment by the time sess.mu is taken, that
// attachment has already run its attach-time reset, so this call must leave
// the tabs alone rather than clobbering freshly applied colors.
func (d *Daemon) resetScreenDefaultColors(sess *session) {
	d.applyHostTheme(sess, nil, themeui.Theme{}, true)
}

// firstPaintForTransition is the only active post-transition paint entry
// point. It rejects a stale attachment capability before rebasing output and carries
// the exact coordinator lease through every resize and render effect.
func (d *Daemon) firstPaintForTransition(token attachmentConnectionToken) bool {
	if !token.attachmentCurrent() {
		return false
	}
	ticket, admitted := token.ac.beginAttachmentEffect(token)
	if !admitted {
		return false
	}
	token.effect = ticket
	defer token.endAttachmentEffect()
	if d.afterAttachmentEffectAdmitted != nil {
		d.afterAttachmentEffectAdmitted(token)
	}
	if token.rebase {
		if d.beforeFirstPaintSendWait != nil {
			d.beforeFirstPaintSendWait(token)
		}
		token.ac.sendMu.Lock()
		if token.ac.renderStages.handoffRebase != nil {
			token.ac.renderStages.handoffRebase()
		}
		token.ac.rebaseOutput()
		// Captures belong to the old session even when pane-local IDs happen
		// to be reused by the destination.
		token.ac.captureFrames = nil
		token.ac.sendMu.Unlock()
	}
	if proxy, ok := token.sess.(*proxySession); ok {
		d.firstProxyPaintWithLease(proxy, token.ac, token.lease)
		return true
	}
	sess, ok := localSession(token.sess)
	if !ok {
		return false
	}
	d.firstPaintWithLease(sess, token.ac, token.ac.size, token.lease)
	return true
}

// firstPaint guarantees the freshly attached client sees the full screen: if
// the tab size differs from the client's it resizes first, then immediately
// emits a full redraw. It is retained for direct test/headless setup; active
// attachment transitions use firstPaintForTransition with their exact lease.
func (d *Daemon) firstPaint(sess *session, ac *attachedClient, clientSize domain.Size) {
	d.firstPaintWithLease(sess, ac, clientSize, nil)
}

func (d *Daemon) firstPaintWithLease(sess *session, ac *attachedClient, clientSize domain.Size, lease *attachmentLease) {
	// Global notices raised while nothing was attached surface on this client.
	// Drained before the early return below so a session without an active tab
	// cannot swallow the queue.
	d.drainPendingForFirstPaint(sess, ac)
	tb := sess.tabForAttachment(ac)
	if tb == nil {
		return
	}
	tb.mu.Lock()
	wsz := tb.size
	tb.mu.Unlock()

	outerResizeAccepted := false
	if clientSize.Valid() && wsz != contentSize(clientSize, ac.proxied) {
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

func (d *Daemon) applyThemeForAttachment(token attachmentConnectionToken, msg ports.Theme) bool {
	if !token.attachmentEffectCurrent() {
		return false
	}
	clientTheme := themeFromMessage(msg)
	token.ac.setClientTheme(clientTheme)
	sess, ok := localSession(token.sess)
	if !ok || !token.attachmentEffectCurrent() || !d.applyHostTheme(sess, token.ac, clientTheme, false) {
		return false
	}
	if !token.attachmentEffectCurrent() {
		return false
	}
	if rc := token.sess.core().coordinator.Load(); rc != nil {
		return rc.invalidateForLease(token.ac, token.lease, renderInvalidation{class: invalidateUrgent, reset: true, producer: "client.go"})
	}
	return false
}

func (d *Daemon) resize(sess *session, ac *attachedClient, sz domain.Size) {
	d.requestTransactionalResize(sess, ac, sz, false)
}

// handleClientNotice maps the closed client-event enum to daemon-owned notice
// content. routingMu makes ownership validation and toast mutation one atomic
// attachment-routing operation: attachment publication also takes routingMu
// before changing membership. Never retain sess.mu while touching notice or overlay
// state, and retain the routingMu -> sess.mu order used by attachment paths.
func (d *Daemon) handleClientNotice(sess *session, ac *attachedClient, notice ports.ClientNotice) {
	if sess == nil || ac == nil {
		return
	}
	tr := ac.transport()
	token := sess.attachmentToken(ac, tr)
	if token.lease == nil {
		// Direct test/headless callers retain pointer-based routing. Production
		// client frames always carry the exact lease through the attachment path below.
		d.handleClientNoticeDirect(sess, ac, notice)
		return
	}
	d.handleClientNoticeForAttachment(token, notice)
}

func (d *Daemon) handleClientNoticeForAttachment(token attachmentConnectionToken, notice ports.ClientNotice) {
	d.notices.routingMu.Lock()
	if !token.attachmentEffectCurrent() {
		d.notices.routingMu.Unlock()
		return
	}
	if d.notices.beforeClientNoticeMutation != nil {
		d.notices.beforeClientNoticeMutation()
	}
	if !token.attachmentEffectCurrent() {
		d.notices.routingMu.Unlock()
		return
	}
	sess, ok := localSession(token.sess)
	if !ok {
		d.notices.routingMu.Unlock()
		return
	}
	repaint := d.mutateClientNotice(sess, token.ac, notice)
	d.notices.routingMu.Unlock()
	if repaint && token.attachmentEffectCurrent() {
		d.repaintForNotice(token.ac)
	}
}

func (d *Daemon) handleClientNoticeDirect(sess *session, ac *attachedClient, notice ports.ClientNotice) {
	d.notices.routingMu.Lock()
	sess.mu.Lock()
	_, current := sess.attachments[ac]
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
// routingMu keeps another connection from changing that selection. Rendering is left
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

// killSession removes a session and tears down its resources. It is
// idempotent: only the caller that wins the registry delete acts. When the
// registry empties it marks the daemon closing (atomically with the
// empty-check, under d.mu) and signals shutdown.
//
// Teardown ordering matters: context cancel, pty.Close, and the done signal
// run first and unconditionally — never gated behind a client send. The
