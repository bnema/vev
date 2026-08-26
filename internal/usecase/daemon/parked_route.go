package daemon

import (
	"context"
	"crypto/rand"
	"io"
	"time"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
)

const parkedRouteLeaseTTL = 15 * time.Minute

type parkedRouteLease struct {
	id         ports.ParkedRouteLeaseID
	generation uint64
	transport  transportSnapshot
	expiresAt  time.Time
	timer      ports.Timer
	stop       chan struct{}
	suspended  bool
	inFlight   bool
	expired    bool
}

func (lease *parkedRouteLease) matches(id ports.ParkedRouteLeaseID, generation uint64, transport transportSnapshot) bool {
	return lease != nil && lease.id == id && lease.generation == generation && lease.transport == transport
}

func newParkedRouteLeaseID() (ports.ParkedRouteLeaseID, error) {
	var id ports.ParkedRouteLeaseID
	if _, err := io.ReadFull(rand.Reader, id[:]); err != nil {
		return ports.ParkedRouteLeaseID{}, err
	}
	return id, nil
}

func stopParkedRouteLeaseLocked(lease *parkedRouteLease) {
	if lease == nil {
		return
	}
	if lease.timer != nil {
		lease.timer.Stop()
		lease.timer = nil
	}
	if lease.stop != nil {
		close(lease.stop)
		lease.stop = nil
	}
}

func (d *Daemon) armParkedRoute(token attachmentConnectionToken) (ports.ParkedRouteLeaseID, error) {
	ac := token.ac
	if ac == nil || !token.attachmentCurrent() || ac.navigationCapabilities&ports.NavigationCapabilityHomePicker == 0 {
		return ports.ParkedRouteLeaseID{}, errAttachmentTransition
	}
	id, err := newParkedRouteLeaseID()
	if err != nil {
		return ports.ParkedRouteLeaseID{}, err
	}
	lease := &parkedRouteLease{
		id: id, generation: token.generation, transport: token.transport,
		expiresAt: d.clock.Now().Add(parkedRouteLeaseTTL),
		timer:     d.clock.NewTimer(parkedRouteLeaseTTL), stop: make(chan struct{}),
	}
	ac.parkedRouteMu.Lock()
	stopParkedRouteLeaseLocked(ac.parkedRoute)
	ac.parkedRoute = lease
	ac.parkedRouteOutput.Store(false)
	ac.parkedRouteFullPending.Store(false)
	timer, stop := lease.timer, lease.stop
	ac.parkedRouteMu.Unlock()
	go d.watchParkedRouteLease(ac, lease, timer, stop)
	return id, nil
}

func (d *Daemon) watchParkedRouteLease(ac *attachedClient, lease *parkedRouteLease, timer ports.Timer, stop <-chan struct{}) {
	select {
	case <-timer.C():
		d.expireParkedRoute(ac, lease)
	case <-stop:
	}
}

func (d *Daemon) expireParkedRoute(ac *attachedClient, lease *parkedRouteLease) {
	if ac == nil || lease == nil {
		return
	}
	ac.parkedRouteMu.Lock()
	if ac.parkedRoute != lease {
		ac.parkedRouteMu.Unlock()
		return
	}
	lease.timer = nil
	lease.stop = nil
	if lease.inFlight {
		lease.expired = true
		ac.parkedRouteMu.Unlock()
		return
	}
	ac.parkedRoute = nil
	ac.parkedRouteOutput.Store(false)
	ac.parkedRouteFullPending.Store(false)
	ac.parkedRouteMu.Unlock()
	if ac.connectionGeneration.Load() == lease.generation && ac.transportSnapshotCurrent(lease.transport) {
		_ = ac.closeCapturedTransport(lease.transport.transport)
	}
}

func (ac *attachedClient) clearParkedRoute() {
	if ac == nil {
		return
	}
	ac.parkedRouteMu.Lock()
	stopParkedRouteLeaseLocked(ac.parkedRoute)
	ac.parkedRoute = nil
	ac.parkedRouteOutput.Store(false)
	ac.parkedRouteFullPending.Store(false)
	ac.parkedRouteMu.Unlock()
}

func (ac *attachedClient) parkedRouteStatus(token attachmentConnectionToken, id ports.ParkedRouteLeaseID, now time.Time, suspended bool) ports.ParkedRouteStatus {
	if ac == nil || !token.attachmentCurrent() {
		return ports.ParkedRouteRejected
	}
	ac.parkedRouteMu.Lock()
	defer ac.parkedRouteMu.Unlock()
	lease := ac.parkedRoute
	if !lease.matches(id, token.generation, token.transport) {
		return ports.ParkedRouteRejected
	}
	if !now.Before(lease.expiresAt) {
		stopParkedRouteLeaseLocked(lease)
		ac.parkedRoute = nil
		ac.parkedRouteOutput.Store(false)
		ac.parkedRouteFullPending.Store(false)
		return ports.ParkedRouteExpired
	}
	if lease.suspended != suspended {
		return ports.ParkedRouteRejected
	}
	return 0
}

func (ac *attachedClient) prepareParkedRoute(token attachmentConnectionToken, id ports.ParkedRouteLeaseID, now time.Time) ports.ParkedRouteStatus {
	if status := ac.parkedRouteStatus(token, id, now, false); status != 0 {
		return status
	}
	ac.parkedRouteMu.Lock()
	defer ac.parkedRouteMu.Unlock()
	lease := ac.parkedRoute
	if !lease.matches(id, token.generation, token.transport) || lease.suspended {
		return ports.ParkedRouteRejected
	}
	lease.suspended = true
	ac.parkedRouteOutput.Store(true)
	return 0
}

func (ac *attachedClient) consumeParkedRoute(token attachmentConnectionToken, id ports.ParkedRouteLeaseID, now time.Time) ports.ParkedRouteStatus {
	if status := ac.parkedRouteStatus(token, id, now, true); status != 0 {
		return status
	}
	ac.parkedRouteMu.Lock()
	defer ac.parkedRouteMu.Unlock()
	lease := ac.parkedRoute
	if !lease.matches(id, token.generation, token.transport) || !lease.suspended || lease.inFlight {
		return ports.ParkedRouteRejected
	}
	stopParkedRouteLeaseLocked(lease)
	ac.parkedRoute = nil
	return 0
}

func (ac *attachedClient) beginParkedRouteSwitch(token attachmentConnectionToken, id ports.ParkedRouteLeaseID, now time.Time) ports.ParkedRouteStatus {
	if ac == nil || !token.attachmentCurrent() {
		return ports.ParkedRouteRejected
	}
	ac.parkedRouteMu.Lock()
	defer ac.parkedRouteMu.Unlock()
	lease := ac.parkedRoute
	if !lease.matches(id, token.generation, token.transport) || !lease.suspended || lease.inFlight {
		return ports.ParkedRouteRejected
	}
	if lease.expired || !now.Before(lease.expiresAt) {
		stopParkedRouteLeaseLocked(lease)
		ac.parkedRoute = nil
		ac.parkedRouteOutput.Store(false)
		ac.parkedRouteFullPending.Store(false)
		return ports.ParkedRouteExpired
	}
	lease.inFlight = true
	return 0
}

func (ac *attachedClient) rejectParkedRouteSwitch(token attachmentConnectionToken, id ports.ParkedRouteLeaseID, now time.Time) ports.ParkedRouteStatus {
	if ac == nil {
		return ports.ParkedRouteRejected
	}
	ac.parkedRouteMu.Lock()
	defer ac.parkedRouteMu.Unlock()
	lease := ac.parkedRoute
	if !lease.matches(id, token.generation, token.transport) || !lease.inFlight {
		return ports.ParkedRouteRejected
	}
	lease.inFlight = false
	if lease.expired || !now.Before(lease.expiresAt) {
		stopParkedRouteLeaseLocked(lease)
		ac.parkedRoute = nil
		ac.parkedRouteOutput.Store(false)
		ac.parkedRouteFullPending.Store(false)
		return ports.ParkedRouteExpired
	}
	return 0
}

func (ac *attachedClient) consumePublishedParkedRoute(previous, published attachmentConnectionToken, id ports.ParkedRouteLeaseID) ports.ParkedRouteStatus {
	if ac == nil || published.ac != ac || !published.attachmentCurrent() {
		return ports.ParkedRouteRejected
	}
	ac.parkedRouteMu.Lock()
	defer ac.parkedRouteMu.Unlock()
	lease := ac.parkedRoute
	if !lease.matches(id, previous.generation, previous.transport) || !lease.suspended || !lease.inFlight {
		return ports.ParkedRouteRejected
	}
	// Expiry is checked when the request is admitted. Once publication starts,
	// the accepted request must commit atomically instead of turning a completed
	// identity publication into a late pre-commit rejection.
	stopParkedRouteLeaseLocked(lease)
	ac.parkedRoute = nil
	return 0
}

func parkedRouteResponseFrame(response ports.ParkedRouteResponse) (ports.Frame, error) {
	payload := ports.MarshalParkedRouteResponse(response)
	if payload == nil {
		return ports.Frame{}, errAttachmentTransition
	}
	return ports.Frame{Type: ports.MsgParkedRouteResponse, Payload: payload}, nil
}

func (d *Daemon) sendParkedRouteResponse(token attachmentConnectionToken, response ports.ParkedRouteResponse) error {
	frame, err := parkedRouteResponseFrame(response)
	if err != nil {
		return err
	}
	return token.sendControl(frame)
}

// sendParkedRouteResponseLocked preserves response -> full-output ordering.
// Caller holds ac.sendMu and keeps parkedRouteOutput set through this send.
func (d *Daemon) sendParkedRouteResponseLocked(ac *attachedClient, expected transportSnapshot, response ports.ParkedRouteResponse) error {
	frame, err := parkedRouteResponseFrame(response)
	if err != nil {
		return err
	}
	if ac == nil || !ac.transportSnapshotCurrent(expected) || expected.transport == nil {
		return errAttachmentTransition
	}
	return expected.transport.Send(frame)
}

func (ac *attachedClient) releaseParkedOutputLocked() {
	ac.rebaseOutput()
	ac.pipelineCache = composeCacheInput{}
	ac.pipelineScratch = composeCacheInput{}
	ac.parkedRouteFullPending.Store(true)
	ac.parkedRouteOutput.Store(false)
}

func (d *Daemon) parkedRouteResponseFailed(token attachmentConnectionToken) {
	if token.effect == nil || token.effect.ended.Load() {
		return
	}
	launch := d.reserveAttachmentSendErrorCleanup(token, token.transport.transport)
	token.effect.End()
	launch()
}

func (d *Daemon) respondParkedRoute(token attachmentConnectionToken, response ports.ParkedRouteResponse) bool {
	if err := d.sendParkedRouteResponse(token, response); err != nil {
		d.parkedRouteResponseFailed(token)
		return false
	}
	return true
}

func (d *Daemon) handleParkedRouteRequest(token attachmentConnectionToken, request ports.ParkedRouteRequest) {
	if ports.ValidateParkedRouteRequest(request) != nil || token.ac == nil {
		return
	}
	switch request.Action {
	case ports.ParkedRoutePrepare:
		status := token.ac.prepareParkedRoute(token, request.LeaseID, d.clock.Now())
		if status == 0 {
			status = ports.ParkedRouteReady
		}
		d.respondParkedRoute(token, ports.ParkedRouteResponse{RequestID: request.RequestID, Status: status})
	case ports.ParkedRouteResume:
		d.resumeParkedRoute(token, request)
	case ports.ParkedRouteSwitch:
		d.switchParkedRoute(token, request)
	}
}

func (d *Daemon) resumeParkedRoute(token attachmentConnectionToken, request ports.ParkedRouteRequest) {
	status := token.ac.consumeParkedRoute(token, request.LeaseID, d.clock.Now())
	if status != 0 {
		d.respondParkedRoute(token, ports.ParkedRouteResponse{RequestID: request.RequestID, Status: status})
		return
	}
	ac := token.ac
	ac.sendMu.Lock()
	err := d.sendParkedRouteResponseLocked(ac, token.transport, ports.ParkedRouteResponse{RequestID: request.RequestID, Status: ports.ParkedRouteResumed})
	if err == nil {
		ac.releaseParkedOutputLocked()
	}
	ac.sendMu.Unlock()
	if err != nil {
		d.parkedRouteResponseFailed(token)
		return
	}
	if token.sess != nil {
		d.firstPaintWithLease(token.sess, ac, token.lease, true)
	}
}

func (d *Daemon) rejectParkedRouteSwitch(token attachmentConnectionToken, request ports.ParkedRouteRequest, fallback ports.ParkedRouteStatus) {
	status := token.ac.rejectParkedRouteSwitch(token, request.LeaseID, d.clock.Now())
	if status == 0 {
		status = fallback
	}
	if d.respondParkedRoute(token, ports.ParkedRouteResponse{RequestID: request.RequestID, Status: status}) && status == ports.ParkedRouteExpired {
		_ = token.ac.closeCapturedTransport(token.transport.transport)
	}
}

func (d *Daemon) switchParkedRoute(token attachmentConnectionToken, request ports.ParkedRouteRequest) {
	if status := token.ac.beginParkedRouteSwitch(token, request.LeaseID, d.clock.Now()); status != 0 {
		if d.respondParkedRoute(token, ports.ParkedRouteResponse{RequestID: request.RequestID, Status: status}) && status == ports.ParkedRouteExpired {
			_ = token.ac.closeCapturedTransport(token.transport.transport)
		}
		return
	}
	target := *request.Target
	ctx := d.serveCtx
	if ctx == nil {
		ctx = context.Background()
	}
	if err := d.waitForParkedTargetRestore(ctx, target.SessionName); err != nil {
		d.rejectParkedRouteSwitch(token, request, ports.ParkedRouteStaleTarget)
		return
	}

	transition, targetSession, err := d.transitionParkedRouteTarget(token, target)
	if err != nil {
		d.rejectParkedRouteSwitch(token, request, ports.ParkedRouteStaleTarget)
		return
	}
	if status := token.ac.consumePublishedParkedRoute(token, transition.published, request.LeaseID); status != 0 {
		d.abortPublishedAttachmentTransition(transition)
		return
	}
	fresh, admitted := token.ac.beginAttachmentEffect(transition.published)
	if !admitted {
		d.abortPublishedAttachmentTransition(transition)
		return
	}
	freshToken := fresh.connectionToken()
	ac := token.ac
	ac.sendMu.Lock()
	responseErr := d.sendParkedRouteResponseLocked(ac, freshToken.transport, ports.ParkedRouteResponse{RequestID: request.RequestID, Status: ports.ParkedRouteSwitched})
	if responseErr == nil {
		ac.releaseParkedOutputLocked()
	}
	ac.sendMu.Unlock()
	if responseErr != nil {
		launch := d.reserveAttachmentSendErrorCleanup(freshToken, freshToken.transport.transport)
		fresh.End()
		launch()
		d.deferAttachmentTransitionCleanups(transition)
		return
	}
	fresh.End()
	d.touchMRU(targetSession)
	d.deferAttachmentTransitionCleanups(transition)
	transition.published.rebase = false
	d.firstPaintForTransition(transition.published)
}

func (d *Daemon) waitForParkedTargetRestore(parent context.Context, name string) error {
	ctx, cancel := context.WithCancel(parent)
	timer := d.clock.NewTimer(ports.HandshakeTimeout)
	stop := make(chan struct{})
	go func() {
		select {
		case <-timer.C():
			cancel()
		case <-ctx.Done():
		case <-stop:
		}
	}()
	err := d.waitForTargetRestore(ctx, name)
	close(stop)
	timer.Stop()
	cancel()
	return err
}

func (d *Daemon) transitionParkedRouteTarget(token attachmentConnectionToken, target domain.RemoteSessionTarget) (attachmentTransitionResult, *session, error) {
	d.mu.Lock()
	if d.closing {
		d.mu.Unlock()
		return attachmentTransitionResult{}, nil, errAttachmentTransition
	}
	if live := d.findByNameLocked(target.SessionName); live != nil {
		if live.incarnation != target.LifecycleID {
			d.mu.Unlock()
			return attachmentTransitionResult{}, nil, errAttachmentTransition
		}
		tabIndex, ok := remoteTargetTabIndexLocked(live, target)
		if !ok {
			d.mu.Unlock()
			return attachmentTransitionResult{}, nil, errAttachmentTransition
		}
		live.mu.Lock()
		tabID := domain.TabStableID(live.tabs[tabIndex].stableID)
		live.mu.Unlock()
		d.mu.Unlock()
		transition, err := d.transitionAttachment(attachmentTransitionRequest{
			source: token.sess, target: live, next: token.ac,
			expectedTransport: token.transport, sourceToken: &token, action: "parked-route-switch",
			expectedTargetLifecycle: &attachmentLifecycleFence{
				name: target.SessionName, incarnation: target.LifecycleID, checkIncarnation: true,
				tabID: tabID, tabIndex: tabIndex, checkTab: true,
			},
			activateTargetTab: true, targetTabIndex: tabIndex, ready: true,
		})
		return transition, live, err
	}

	inactive, ok := d.inactive[target.SessionName]
	if !ok || !target.Stopped || inactive.incarnation != target.LifecycleID || !inactive.canResume() {
		d.mu.Unlock()
		return attachmentTransitionResult{}, nil, errAttachmentTransition
	}
	tabIndex, ok := remoteTargetTabIndexInactive(inactive, target)
	if !ok {
		d.mu.Unlock()
		return attachmentTransitionResult{}, nil, errAttachmentTransition
	}
	d.mu.Unlock()

	var created *session
	transition, err := d.transitionAttachment(attachmentTransitionRequest{
		source: token.sess, next: token.ac,
		expectedTransport: token.transport, sourceToken: &token, action: "parked-route-restore",
		activateTargetTab: true, targetTabIndex: tabIndex, ready: true,
		createTargetLocked: func() (*session, error) {
			current, ok := d.inactive[target.SessionName]
			if !ok || current.incarnation != target.LifecycleID || !current.canResume() {
				return nil, errAttachmentTransition
			}
			resolved, ok := remoteTargetTabIndexInactive(current, target)
			if !ok || resolved != tabIndex {
				return nil, errAttachmentTransition
			}
			env := copyEnvironment(d.baseEnv)
			cwd := d.dirOrHome(current.cwd)
			var createErr error
			created, createErr = d.resumeRemoteInactiveSessionLocked(target, cwd, token.ac.geometrySnapshot(), env, current)
			return created, createErr
		},
	})
	if err != nil && created != nil {
		_ = d.killSession(created, ports.ReasonSessionKilled, true)
	}
	if err != nil {
		return attachmentTransitionResult{}, created, err
	}
	return transition, created, nil
}
