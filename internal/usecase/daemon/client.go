// Package daemon holds vev's server-side session multiplexer use case.
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

type attachedClient struct {
	tr            ports.Transport
	output        *outputStateStream
	overlays      *overlayRuntime
	overlayOnce   sync.Once
	clientID      [16]byte
	resumeCapable bool
	resumeToken   uint64
	parked        bool
	// coordinatorEpoch is published atomically by renderCoordinator lifecycle
	// transitions. A wake verifies it again after acquiring sendMu, so a parked
	// attachment reused by resume cannot carry an old wake to its new transport.
	coordinatorEpoch      atomic.Uint64
	echoAck               atomic.Uint64
	bars                  barCache           // only touched while sendMu is held
	composed              composedFrameCache // only touched while sendMu is held
	resizePaint           pendingByteTimer   // guarded by sendMu
	resizePaintGeneration uint64             // guarded by sendMu
	resizePaintPending    bool               // guarded by sendMu
	size                  domain.Size
	keys                  *keys.Router
	sess                  Guarded[*session]
	mouseScan             mouse.Scanner
	themeMu               sync.Mutex
	theme                 themeui.Theme
	clientTheme           themeui.Theme
	lastCursor            cursorOut
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

// cancelResizePaint invalidates callbacks before detaching or replacing an
// attachment. It deliberately takes sendMu before any session/daemon lock.
func (ac *attachedClient) cancelResizePaint() {
	ac.sendMu.Lock()
	ac.resizePaintGeneration++
	ac.resizePaintPending = false
	ac.resizePaint.stop()
	ac.sendMu.Unlock()
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

func (ac *attachedClient) getTheme() themeui.Theme {
	ac.themeMu.Lock()
	defer ac.themeMu.Unlock()
	return ac.theme
}

func (ac *attachedClient) getClientTheme() themeui.Theme {
	ac.themeMu.Lock()
	defer ac.themeMu.Unlock()
	return ac.clientTheme
}

func (ac *attachedClient) setTheme(t themeui.Theme) {
	ac.themeMu.Lock()
	ac.theme = t
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
	ac.linkMu.Unlock()
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

// boundedSend sends f to ac with a deadline watchdog: if the send (including
// waiting on sendMu behind a wedged paint) does not complete within
// detachNotifyTimeout, the transport is force-closed, failing the in-flight
// write. Detach/kill/shutdown paths use this so they are never gated on a
// client that has stopped draining its socket.
func (d *Daemon) boundedSend(ac *attachedClient, f ports.Frame) {
	_ = d.boundedSendErr(ac, f)
}

func (d *Daemon) boundedSendErr(ac *attachedClient, f ports.Frame) error {
	tr, err := d.boundedSendWith(func(capture func(ports.Transport)) error {
		tr, err := ac.sendTransport(f)
		capture(tr)
		return err
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
	ac.sendMu.Lock()
	tr := ac.transport()
	if tr == nil {
		ac.sendMu.Unlock()
		return nil, errors.New("client transport is nil")
	}
	if owned, ok := tr.(ports.OwnedSynchronousTransport); ok {
		err := owned.SendSynchronous(frame)
		ac.sendMu.Unlock()
		return tr, err
	}
	ac.sendMu.Unlock()
	return d.boundedSendWith(func(capture func(ports.Transport)) error {
		ac.sendMu.Lock()
		defer ac.sendMu.Unlock()
		tr := ac.transport()
		capture(tr)
		if tr == nil {
			return errors.New("client transport is nil")
		}
		return tr.Send(frame)
	})
}

var errSendTimedOut = errors.New("send timed out")

func (d *Daemon) boundedSendWith(send func(capture func(ports.Transport)) error) (ports.Transport, error) {
	return d.boundedSendWithTimeout(detachNotifyTimeout, send)
}

func (d *Daemon) boundedSendWithTimeout(timeout time.Duration, send func(capture func(ports.Transport)) error) (ports.Transport, error) {
	timer := d.clock.NewTimer(timeout)
	result := make(chan error, 1)
	var (
		capturedMu sync.Mutex
		captured   ports.Transport
	)
	capture := func(tr ports.Transport) {
		capturedMu.Lock()
		captured = tr
		capturedMu.Unlock()
	}
	capturedTransport := func() ports.Transport {
		capturedMu.Lock()
		defer capturedMu.Unlock()
		return captured
	}
	go func() {
		result <- send(capture)
	}()
	select {
	case err := <-result:
		timer.Stop()
		return capturedTransport(), err
	case <-timer.C():
		select {
		case err := <-result:
			return capturedTransport(), err
		default:
		}
		d.log.Warn("bounded send timed out; force closing client transport")
		return capturedTransport(), errSendTimedOut
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
	d.attachCoordinator(sess, old, ac)
	sess.mu.Lock()
	sess.client = ac
	sess.mu.Unlock()
	d.touchMRU(sess)
	d.log.Info("client attached", "session", name, "resume", opts.resumeCapable)
	d.applyHostTheme(sess, ac, d.effectiveTheme(themeui.Theme{}), true)
	return ac, old
}

// handoffCoordinator prepares a cross-session ownership transfer before the
// destination publishes sess.client. It never takes daemon/session locks while
// taking sendMu, avoiding a d.mu/sendMu cycle with transport callbacks.
func (d *Daemon) handoffCoordinator(from, target *session, old, current *attachedClient) {
	if rc := from.renderCoordinator(); rc != nil {
		rc.noteDetach(current)
	}
	current.sendMu.Lock()
	current.output.rebase()
	current.sendMu.Unlock()
	d.attachCoordinator(target, old, current)
}

// attachCoordinator is the sole direct attachment handoff. It creates at
// most one coordinator for sess and changes its identity before any caller
// can publish resize or render state for the new client.
func (d *Daemon) attachCoordinator(sess *session, old, current *attachedClient) *renderCoordinator {
	sess.mu.Lock()
	rc := sess.renderCoordinator()
	if rc == nil {
		rc = newRenderCoordinator(renderCoordinatorOptions{
			clock: d.clock,
			wake: func(w renderWake) {
				// Never reread sess.client here: a wake is bound to both its
				// attachment and its coordinator incarnation. The second check in
				// paint occurs under sendMu, closing a park/resume race after this
				// unlocked coordinator validation.
				if w.attachment != nil && rc.wakeCurrent(w) {
					d.paintCoordinatorWake(sess, w)
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
	if old != nil {
		rc.noteReplace(old, current)
	} else if current != nil {
		rc.attach(current)
	}
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
	d.applyHostTheme(sess, nil, d.effectiveTheme(themeui.Theme{}), true)
}

func (d *Daemon) detachReplacedClient(old *attachedClient) {
	if old == nil {
		return
	}
	old.cancelResizePaint()

	d.unregisterPreview(old)
	old.clearPreviousSession()
	old.setSession(nil)
	d.log.Info("client displaced")
	// Async + bounded: a dead or wedged old client must not stall the new
	// client's handshake.
	d.notifyDetachedAsync(old, ports.ReasonDetach)
}

// firstPaint guarantees the freshly attached client sees the full screen: if
// the tab size differs from the client's it resizes first, then immediately
// emits a full redraw. Attach must not wait for the resize-idle fallback
// timer.
func (d *Daemon) firstPaint(sess *session, ac *attachedClient, clientSize domain.Size) {
	tb := sess.activeTab()
	if tb == nil {
		return
	}
	tb.mu.Lock()
	wsz := tb.size
	tb.mu.Unlock()

	if clientSize.Valid() && wsz != tabSize(clientSize) {
		d.resizeForFirstPaint(sess, ac, clientSize)
	}
	d.refreshBarScriptsIfDue(sess, d.clock.Now(), true)
	d.invalidateRenderNow(sess, ac, true, "client.go")
	d.activateTab(sess, tb)
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
	ac.cancelResizePaint()
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
		Foreground:  msg.Foreground,
		Background:  msg.Background,
		HasFG:       msg.HasForeground,
		HasBG:       msg.HasBackground,
		TrueColor:   msg.TrueColor,
		Known:       msg.HasForeground && msg.HasBackground,
		SchemeKnown: msg.SchemeKnown,
		Light:       msg.Light,
	}
	ac.setClientTheme(clientTheme)
	t := d.effectiveTheme(clientTheme)

	if !d.applyHostTheme(sess, ac, t, false) {
		return
	}
	d.invalidateRender(sess, ac, true, "client.go")
}

func (d *Daemon) resize(sess *session, ac *attachedClient, sz domain.Size) {
	d.resizeWithPaint(sess, ac, sz, true)
}

// resizeForFirstPaint applies attach geometry without installing an idle timer:
// firstPaint immediately emits the required full frame itself.
func (d *Daemon) resizeForFirstPaint(sess *session, ac *attachedClient, sz domain.Size) {
	d.resizeWithPaint(sess, ac, sz, false)
}

func (d *Daemon) resizeWithPaint(sess *session, ac *attachedClient, sz domain.Size, schedulePaint bool) {
	if !sz.Valid() {
		return
	}
	d.exitCopyMode(ac)
	if ac != nil {
		// Suspend emission until every pane has adopted this geometry and the
		// bounded resize paint is armed. PTY readers remain independent.
		ac.sendMu.Lock()
		defer ac.sendMu.Unlock()
		if ac.currentSession() != sess {
			return
		}
		ac.size = sz
		if rc := sess.renderCoordinator(); rc != nil {
			rc.attach(ac)
			rc.recordResizeRequest(sz, ac)
		}
	}
	tbSize := tabSize(sz)

	sess.mu.Lock()
	tabs := append([]*tab(nil), sess.tabs...)
	sess.mu.Unlock()
	if len(tabs) == 0 {
		return
	}

	for _, tb := range tabs {
		tb.mu.Lock()
		tb.size = tbSize
		d.applyLayoutLocked(tb)
		tb.mu.Unlock()
	}
	// Only the shown tab's floating terminal tracks the client size. This is
	// outside tab.mu so its PTY resize cannot block tab state.
	d.resizeActiveFloating(sess.activeTab())
	markSnapshotDirty(sess)
	if ac != nil && schedulePaint {
		d.refreshBarScriptsIfDue(sess, d.clock.Now(), true)
		d.scheduleResizePaintLocked(sess, ac)
	}
}

// detachOnSendError drops a client whose transport failed, leaving the session
// registered and headless.
func (d *Daemon) detachOnSendError(sess *session, ac *attachedClient, failed ports.Transport) {
	if failed != nil && !ac.currentTransportIs(failed) {
		return
	}
	if sess.detachIfCurrent(ac) {
		ac.cancelResizePaint()
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
