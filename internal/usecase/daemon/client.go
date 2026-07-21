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
	resumeCapable        bool
	resumeToken          uint64
	parked               bool
	echoAck              atomic.Uint64
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
	size          domain.Size
	keys          *keys.Router
	sess          Guarded[*session]
	mouseScan     mouse.Scanner
	themeMu       sync.Mutex
	clientTheme   themeui.Theme
	appliedTheme  appliedTheme
	lastCursor    cursorOut
	renderStages  renderStageHooks // optional render and handoff observability hooks
	// previousSession is guarded independently. It is retained through temporary
	// setSession(nil) hand-offs and cleared only on terminal teardown.
	previousSession Guarded[*session]
	linkMu          sync.Mutex
	sendMu          sync.Mutex
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
	defer ac.linkMu.Unlock()
	if ac.tr != tr {
		return nil
	}
	ac.tr = nil
	ac.transportIncarnation++
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

func (ac *attachedClient) closeTransport() error {
	tr := ac.transport()
	if tr == nil {
		return nil
	}
	return tr.Close()
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

// send serialises a frame onto the client's transport.
func (ac *attachedClient) send(f ports.Frame) error {
	_, err := ac.sendTransport(f)
	return err
}

func (ac *attachedClient) sendTransport(f ports.Frame) (ports.Transport, error) {
	ac.sendMu.Lock()
	defer ac.sendMu.Unlock()
	tr := ac.transport()
	if tr == nil {
		return nil, errors.New("client transport is nil")
	}
	return tr, tr.Send(f)
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

// notifyDetachedAsync delivers a best-effort Detached notice off the caller's
// goroutine and then closes the transport. Session teardown (killSession) must
// complete regardless of client state, so the notice is both asynchronous and
// deadline-bounded; Serve waits for pending notices before force-closing
// connections so a graceful notice is not raced by the hard close.
func (d *Daemon) notifyDetachedAsync(ac *attachedClient, reason uint8) {
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
		d.boundedSend(ac, frameDetached(reason))
		_ = ac.closeTransport()
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

func (d *Daemon) attachClient(sess *session, tr ports.Transport, sz domain.Size, opts attachClientOptions) (*attachedClient, *attachedClient) {
	ac, old, cleanup := d.attachClientDeferred(sess, tr, sz, opts)
	cleanup.finish()
	return ac, old
}

func (d *Daemon) attachClientDeferred(sess *session, tr ports.Transport, sz domain.Size, opts attachClientOptions) (*attachedClient, *attachedClient, renderLifecycleCleanup) {
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
	ac.setSession(sess)
	ac.keys = keys.NewRouter(d.clock, daemonKeyHandler{d: d, ac: ac}, &d.bindings)
	// Daemon attachment callers hold d.mu, serialising this preparation with
	// other publications. Bind coordinator ownership before sess.client becomes
	// visible: an old deadline can then never target this new output chain.
	sess.mu.Lock()
	old := sess.client
	name := sess.name
	sess.mu.Unlock()
	_, cleanup := d.attachCoordinatorDeferred(sess, old, ac, false)
	sess.mu.Lock()
	sess.client = ac
	sess.mu.Unlock()
	d.touchMRU(sess)
	d.log.Info("client attached", "session", name, "resume", opts.resumeCapable)
	d.applyHostTheme(sess, ac, themeui.Theme{}, true)
	return ac, old, cleanup
}

// handoffCoordinator prepares a cross-session ownership transfer before the
// destination publishes sess.client. Callers may hold d.mu, but this function
// acquires no daemon or session locks while it holds sendMu.
func (d *Daemon) handoffCoordinator(from, target *session, old, current *attachedClient) []renderLifecycleCleanup {
	cleanups := make([]renderLifecycleCleanup, 0, 2)
	if rc := from.renderCoordinator(); rc != nil {
		cleanups = append(cleanups, rc.beginDetach(current))
	}
	if current.renderStages.handoffRebase != nil {
		current.renderStages.handoffRebase()
	}
	current.sendMu.Lock()
	current.output.rebase()
	// Capture snapshots are attachment-owned. Discard them while the attachment
	// is exclusively held so the transfer releases panes no longer reachable
	// from the target session.
	current.captureFrames = nil
	current.sendMu.Unlock()
	_, cleanup := d.attachCoordinatorDeferred(target, old, current, true)
	return append(cleanups, cleanup)
}

func finishRenderLifecycleCleanups(cleanups []renderLifecycleCleanup) {
	for _, cleanup := range cleanups {
		cleanup.finish()
	}
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
	sess.mu.Lock()
	rc := sess.renderCoordinator()
	if rc == nil {
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
				ready := !attached.output.atCapacity()
				attached.sendMu.Unlock()
				return ready
			},
		})
		sess.installRenderCoordinator(rc)
	}
	sess.mu.Unlock()
	var cleanup renderLifecycleCleanup
	if old != nil {
		cleanup = rc.beginReplace(old, current, ready)
	} else if current != nil {
		rc.attachWithReadiness(current, ready)
	}
	return rc, cleanup
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

// retireReplacedClient owns old-link retirement and obsolete render-worker
// joins outside the replacement handshake. If a render already owns sendMu,
// close immediately interrupts it; no Detached frame can safely overtake that
// render. Otherwise the old link gets its bounded protocol notice first.
func (d *Daemon) retireReplacedClient(old *attachedClient, cleanup renderLifecycleCleanup) {
	if old == nil {
		d.attachmentCleanupWg.Go(cleanup.finish)
		return
	}
	oldTransport := old.transportSnapshot().transport
	blockedRender := !old.sendMu.TryLock()
	if !blockedRender {
		old.sendMu.Unlock()
	}

	d.attachmentCleanupWg.Go(func() {
		if !blockedRender {
			// A healthy idle client observes the required replacement notice. Its
			// watchdog still bounds a peer that stops draining after this check.
			d.boundedSend(old, frameDetached(ports.ReasonReplaced))
		}
		_ = old.closeCapturedTransport(old.revokeTransport(oldTransport))
		d.unregisterPreview(old)
		old.clearPreviousSession()
		old.setSession(nil)
		d.log.Info("client displaced")
	})
	// Join independently: the close above releases a blocked render while the
	// new attachment is already fully published.
	d.attachmentCleanupWg.Go(cleanup.finish)
}

// firstPaint guarantees the freshly attached client sees the full screen: if
// the tab size differs from the client's it resizes first, then immediately
// emits a full redraw. Attach must not wait for the resize-idle fallback
// timer.
func (d *Daemon) firstPaint(sess *session, ac *attachedClient, clientSize domain.Size) {
	// Global notices raised while nothing was attached surface on this client.
	// Drained before the early return below so a session without an active tab
	// cannot swallow the queue.
	for _, n := range d.notices.drainPending() {
		d.showToast(ac, n)
	}
	tb := sess.activeTab()
	if tb == nil {
		return
	}
	tb.mu.Lock()
	wsz := tb.size
	tb.mu.Unlock()

	outerResizeAccepted := false
	if clientSize.Valid() && wsz != tabSize(clientSize) {
		outerResizeAccepted = d.resizeForFirstPaint(sess, ac, clientSize)
	}
	d.refreshBarScriptsIfDue(sess, d.clock.Now(), true)
	// Activation can synchronously resize a retained floating pane. An accepted
	// synchronous outer request already includes that pane and emits the reset,
	// but activation still performs its warmup work.
	activationResized := d.activateTabAfterResize(sess, tb, outerResizeAccepted)
	if !outerResizeAccepted && !activationResized {
		d.invalidateRenderNow(sess, ac, true, "client.go")
	}
}

// runConnLoop is the per-connection input router: it pumps client messages
// until detach, EOF, or a transport error.
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
			d.clientGone(sess, ac, tr, false)
			return
		}
		if !ac.currentTransportIs(tr) {
			return
		}
		sess = ac.currentSession()
		if sess == nil {
			return
		}
		switch f.Type {
		case ports.MsgInput:
			if in, derr := ports.UnmarshalInput(f.Payload); derr == nil {
				d.handleSequencedInput(sess, ac, in.InputSeq, in.Data)
			}
		case ports.MsgResize:
			if rz, derr := ports.UnmarshalResize(f.Payload); derr == nil {
				d.resize(sess, ac, rz.Size)
			}
		case ports.MsgTheme:
			if th, derr := ports.UnmarshalTheme(f.Payload); derr == nil {
				d.applyTheme(sess, ac, th)
			}
		case ports.MsgImagePush:
			if ip, derr := ports.UnmarshalImagePush(f.Payload); derr == nil {
				d.handleSequencedImagePush(sess, ac, ip.InputSeq, ip)
			}
		case ports.MsgClientNotice:
			if notice, derr := ports.UnmarshalClientNotice(f.Payload); derr == nil {
				d.handleClientNotice(sess, ac, notice)
			} else {
				d.log.Warn("malformed client notice", "err", derr)
			}
		case ports.MsgDetach:
			d.clientGone(sess, ac, tr, true)
			return
		case ports.MsgAck:
			if ack, derr := ports.UnmarshalAck(f.Payload); derr == nil {
				ac.ackOutputState(ack.AckedStateNum)
				if rc := sess.renderCoordinator(); rc != nil {
					rc.notifyAck()
				}
			}
		case ports.MsgPing:
			_ = ac.send(framePong())
		default:
			// Unknown/out-of-band client messages are ignored so a newer
			// client can add message types without breaking an older daemon.
		}
	}
}

// clientGone detaches ac if it is still the session's current client. The
// session remains registered and headless after the client is gone.
func (d *Daemon) clientGone(sess *session, ac *attachedClient, failed ports.Transport, explicit bool) {
	if failed != nil && !ac.currentTransportIs(failed) {
		return // stale connection loop; a newer transport owns this client
	}
	if !sess.detachIfCurrent(ac) {
		return // already displaced by a newer client; nothing to do
	}
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

func (d *Daemon) applyTheme(sess *session, ac *attachedClient, msg ports.Theme) {
	clientTheme := themeui.Theme{
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
	ac.setClientTheme(clientTheme)

	if !d.applyHostTheme(sess, ac, clientTheme, false) {
		return
	}
	d.invalidateRender(sess, ac, true, "client.go")
}

func (d *Daemon) resize(sess *session, ac *attachedClient, sz domain.Size) {
	d.requestTransactionalResize(sess, ac, sz, false)
}

// handleClientNotice maps the closed client-event enum to daemon-owned notice
// content. The connection loop has already bound sess and ac to this transport;
// verify that ownership is still current before changing presentation state.
func (d *Daemon) handleClientNotice(sess *session, ac *attachedClient, notice ports.ClientNotice) {
	sess.mu.Lock()
	current := sess.client == ac
	sess.mu.Unlock()
	if !current {
		return
	}

	switch notice.Action {
	case ports.ClientNoticeClipboardFallback:
		d.recordClientNotice(sess, ac, domain.NoticeError, domain.NoticeClipboard, "image paste failed; sent Ctrl+V")
	case ports.ClientNoticeClipboardTooLarge:
		d.recordClientNotice(sess, ac, domain.NoticeWarn, domain.NoticeClipboardTooLarge, "image too large to paste")
	case ports.ClientNoticeLinkDegraded:
		d.recordClientNotice(sess, ac, domain.NoticeWarn, domain.NoticeConnection, "connection degraded")
	case ports.ClientNoticeLinkConnected:
		d.dismissToast(ac, domain.NoticeConnection, sess.id)
	}
}

func (d *Daemon) recordClientNotice(sess *session, ac *attachedClient, sev domain.NoticeSeverity, code domain.NoticeCode, message string) {
	n := d.notices.record(domain.Notification{
		Code:      code,
		Severity:  sev,
		Message:   message,
		Time:      d.clock.Now(),
		SessionID: sess.id,
	})
	d.showToast(ac, n)
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
	if sess.detachIfCurrent(ac) {
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
}

// killSession removes a session and tears down its resources. It is
// idempotent: only the caller that wins the registry delete acts. When the
// registry empties it marks the daemon closing (atomically with the
// empty-check, under d.mu) and signals shutdown.
//
// Teardown ordering matters: context cancel, pty.Close, and the done signal
// run first and unconditionally — never gated behind a client send. The
