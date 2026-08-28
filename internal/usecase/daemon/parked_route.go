package daemon

import (
	"context"
	"crypto/rand"
	"io"
	"time"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol"
)

const parkedRouteLeaseTTL = 15 * time.Minute

type parkedRouteLease struct {
	id         protocol.ParkedRouteLeaseID
	generation uint64
	transport  transportSnapshot
	expiresAt  time.Time
	timer      ports.Timer
	stop       chan struct{}
	suspended  bool
	inFlight   bool
	expired    bool
}

func (lease *parkedRouteLease) matches(id protocol.ParkedRouteLeaseID, generation uint64, transport transportSnapshot) bool {
	return lease != nil && lease.id == id && lease.generation == generation && lease.transport == transport
}

func newParkedRouteLeaseID() (protocol.ParkedRouteLeaseID, error) {
	var id protocol.ParkedRouteLeaseID
	if _, err := io.ReadFull(rand.Reader, id[:]); err != nil {
		return protocol.ParkedRouteLeaseID{}, err
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

func (d *Daemon) armParkedRoute(effect *attachmentEffect) (protocol.ParkedRouteLeaseID, error) {
	if !effect.current() {
		return protocol.ParkedRouteLeaseID{}, errAttachmentTransition
	}
	ac := effect.ac
	if ac == nil || ac.navigationCapabilities&protocol.NavigationCapabilityHomePicker == 0 {
		return protocol.ParkedRouteLeaseID{}, errAttachmentTransition
	}
	id, err := newParkedRouteLeaseID()
	if err != nil {
		return protocol.ParkedRouteLeaseID{}, err
	}
	lease := &parkedRouteLease{
		id: id, generation: effect.generation, transport: effect.transport,
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
	if ac.lifecycle.generationValue() == lease.generation && ac.transportSnapshotCurrent(lease.transport) {
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

func (ac *attachedClient) parkedRouteStatus(token attachmentCapability, id protocol.ParkedRouteLeaseID, now time.Time, suspended bool) protocol.ParkedRouteStatus {
	if ac == nil || !token.current() {
		return protocol.ParkedRouteRejected
	}
	ac.parkedRouteMu.Lock()
	defer ac.parkedRouteMu.Unlock()
	lease := ac.parkedRoute
	if !lease.matches(id, token.generation, token.transport) {
		return protocol.ParkedRouteRejected
	}
	if !now.Before(lease.expiresAt) {
		stopParkedRouteLeaseLocked(lease)
		ac.parkedRoute = nil
		ac.parkedRouteOutput.Store(false)
		ac.parkedRouteFullPending.Store(false)
		return protocol.ParkedRouteExpired
	}
	if lease.suspended != suspended {
		return protocol.ParkedRouteRejected
	}
	return 0
}

func (ac *attachedClient) prepareParkedRoute(token attachmentCapability, id protocol.ParkedRouteLeaseID, now time.Time) protocol.ParkedRouteStatus {
	if status := ac.parkedRouteStatus(token, id, now, false); status != 0 {
		return status
	}
	ac.parkedRouteMu.Lock()
	defer ac.parkedRouteMu.Unlock()
	lease := ac.parkedRoute
	if !lease.matches(id, token.generation, token.transport) || lease.suspended {
		return protocol.ParkedRouteRejected
	}
	lease.suspended = true
	ac.parkedRouteOutput.Store(true)
	return 0
}

func (ac *attachedClient) consumeParkedRoute(token attachmentCapability, id protocol.ParkedRouteLeaseID, now time.Time) protocol.ParkedRouteStatus {
	if status := ac.parkedRouteStatus(token, id, now, true); status != 0 {
		return status
	}
	ac.parkedRouteMu.Lock()
	defer ac.parkedRouteMu.Unlock()
	lease := ac.parkedRoute
	if !lease.matches(id, token.generation, token.transport) || !lease.suspended || lease.inFlight {
		return protocol.ParkedRouteRejected
	}
	stopParkedRouteLeaseLocked(lease)
	ac.parkedRoute = nil
	return 0
}

func (ac *attachedClient) beginParkedRouteSwitch(token attachmentCapability, id protocol.ParkedRouteLeaseID, now time.Time) protocol.ParkedRouteStatus {
	if ac == nil || !token.current() {
		return protocol.ParkedRouteRejected
	}
	ac.parkedRouteMu.Lock()
	defer ac.parkedRouteMu.Unlock()
	lease := ac.parkedRoute
	if !lease.matches(id, token.generation, token.transport) || !lease.suspended || lease.inFlight {
		return protocol.ParkedRouteRejected
	}
	if lease.expired || !now.Before(lease.expiresAt) {
		stopParkedRouteLeaseLocked(lease)
		ac.parkedRoute = nil
		ac.parkedRouteOutput.Store(false)
		ac.parkedRouteFullPending.Store(false)
		return protocol.ParkedRouteExpired
	}
	lease.inFlight = true
	return 0
}

func (ac *attachedClient) rejectParkedRouteSwitch(token attachmentCapability, id protocol.ParkedRouteLeaseID, now time.Time) protocol.ParkedRouteStatus {
	if ac == nil {
		return protocol.ParkedRouteRejected
	}
	ac.parkedRouteMu.Lock()
	defer ac.parkedRouteMu.Unlock()
	lease := ac.parkedRoute
	if !lease.matches(id, token.generation, token.transport) || !lease.inFlight {
		return protocol.ParkedRouteRejected
	}
	lease.inFlight = false
	if lease.expired || !now.Before(lease.expiresAt) {
		stopParkedRouteLeaseLocked(lease)
		ac.parkedRoute = nil
		ac.parkedRouteOutput.Store(false)
		ac.parkedRouteFullPending.Store(false)
		return protocol.ParkedRouteExpired
	}
	return 0
}

func (ac *attachedClient) consumePublishedParkedRoute(previous, published attachmentCapability, id protocol.ParkedRouteLeaseID) protocol.ParkedRouteStatus {
	if ac == nil || published.ac != ac || !published.current() {
		return protocol.ParkedRouteRejected
	}
	ac.parkedRouteMu.Lock()
	defer ac.parkedRouteMu.Unlock()
	lease := ac.parkedRoute
	if !lease.matches(id, previous.generation, previous.transport) || !lease.suspended || !lease.inFlight {
		return protocol.ParkedRouteRejected
	}
	// Expiry is checked when the request is admitted. Once publication starts,
	// the accepted request must commit atomically instead of turning a completed
	// identity publication into a late pre-commit rejection.
	stopParkedRouteLeaseLocked(lease)
	ac.parkedRoute = nil
	return 0
}

func parkedRouteResponseFrame(response protocol.ParkedRouteResponse) (ports.Frame, error) {
	payload := ports.MarshalParkedRouteResponse(response)
	if payload == nil {
		return ports.Frame{}, errAttachmentTransition
	}
	return ports.Frame{Type: ports.MsgParkedRouteResponse, Payload: payload}, nil
}

func (d *Daemon) sendParkedRouteResponse(effect *attachmentEffect, response protocol.ParkedRouteResponse) error {
	frame, err := parkedRouteResponseFrame(response)
	if err != nil {
		return err
	}
	return effect.sendControl(frame)
}

// sendParkedRouteResponseLocked preserves response -> full-output ordering.
// Caller holds ac.sendMu and keeps parkedRouteOutput set through this send.
func (d *Daemon) sendParkedRouteResponseLocked(ac *attachedClient, expected transportSnapshot, response protocol.ParkedRouteResponse) error {
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

func (d *Daemon) parkedRouteResponseFailed(effect *attachmentEffect) {
	if effect == nil {
		return
	}
	capability := effect.capability()
	launch := d.reserveAttachmentSendErrorCleanup(capability, capability.transport.transport)
	effect.End()
	launch()
}

func (d *Daemon) respondParkedRoute(effect *attachmentEffect, response protocol.ParkedRouteResponse) bool {
	if effect == nil || effect.ac == nil {
		return false
	}
	sender := effect
	owned := false
	if !sender.current() {
		var admitted bool
		sender, admitted = effect.ac.beginAttachmentEffect(effect.capability())
		if !admitted {
			return false
		}
		owned = true
	}
	if owned {
		defer sender.End()
	}
	if err := d.sendParkedRouteResponse(sender, response); err != nil {
		d.parkedRouteResponseFailed(sender)
		return false
	}
	return true
}

func (d *Daemon) handleParkedRouteRequest(effect *attachmentEffect, request protocol.ParkedRouteRequest) {
	if protocol.ValidateParkedRouteRequest(request) != nil || !effect.current() || effect.ac == nil {
		return
	}
	capability := effect.capability()
	switch request.Action {
	case protocol.ParkedRoutePrepare:
		status := effect.ac.prepareParkedRoute(capability, request.LeaseID, d.clock.Now())
		if status == 0 {
			status = protocol.ParkedRouteReady
		}
		d.respondParkedRoute(effect, protocol.ParkedRouteResponse{RequestID: request.RequestID, Status: status})
	case protocol.ParkedRouteResume:
		d.resumeParkedRoute(effect, request)
	case protocol.ParkedRouteSwitch:
		d.switchParkedRoute(effect, request)
	}
}

func (d *Daemon) resumeParkedRoute(effect *attachmentEffect, request protocol.ParkedRouteRequest) {
	capability := effect.capability()
	status := effect.ac.consumeParkedRoute(capability, request.LeaseID, d.clock.Now())
	if status != 0 {
		d.respondParkedRoute(effect, protocol.ParkedRouteResponse{RequestID: request.RequestID, Status: status})
		return
	}
	ac := effect.ac
	ac.sendMu.Lock()
	err := d.sendParkedRouteResponseLocked(ac, effect.transport, protocol.ParkedRouteResponse{RequestID: request.RequestID, Status: protocol.ParkedRouteResumed})
	if err == nil {
		ac.releaseParkedOutputLocked()
	}
	ac.sendMu.Unlock()
	if err != nil {
		d.parkedRouteResponseFailed(effect)
		return
	}
	if effect.sess != nil {
		d.firstPaintWithLease(effect.sess, ac, effect.lease, true)
	}
}

func (d *Daemon) rejectParkedRouteSwitch(effect *attachmentEffect, request protocol.ParkedRouteRequest, fallback protocol.ParkedRouteStatus) {
	capability := effect.capability()
	status := effect.ac.rejectParkedRouteSwitch(capability, request.LeaseID, d.clock.Now())
	if status == 0 {
		status = fallback
	}
	if d.respondParkedRoute(effect, protocol.ParkedRouteResponse{RequestID: request.RequestID, Status: status}) && status == protocol.ParkedRouteExpired {
		_ = effect.ac.closeCapturedTransport(effect.transport.transport)
	}
}

func (d *Daemon) switchParkedRoute(effect *attachmentEffect, request protocol.ParkedRouteRequest) {
	capability := effect.capability()
	if status := effect.ac.beginParkedRouteSwitch(capability, request.LeaseID, d.clock.Now()); status != 0 {
		if d.respondParkedRoute(effect, protocol.ParkedRouteResponse{RequestID: request.RequestID, Status: status}) && status == protocol.ParkedRouteExpired {
			_ = effect.ac.closeCapturedTransport(effect.transport.transport)
		}
		return
	}
	target := *request.Target
	ctx := d.serveCtx
	if ctx == nil {
		ctx = context.Background()
	}
	if err := d.waitForParkedTargetRestore(ctx, target.SessionName); err != nil {
		d.rejectParkedRouteSwitch(effect, request, protocol.ParkedRouteStaleTarget)
		return
	}

	transition, targetSession, err := d.transitionParkedRouteTarget(effect, target)
	if err != nil {
		d.rejectParkedRouteSwitch(effect, request, protocol.ParkedRouteStaleTarget)
		return
	}
	if status := effect.ac.consumePublishedParkedRoute(capability, transition.published, request.LeaseID); status != 0 {
		d.abortPublishedAttachmentTransition(transition)
		return
	}
	fresh, admitted := effect.ac.beginAttachmentEffect(transition.published)
	if !admitted {
		d.abortPublishedAttachmentTransition(transition)
		return
	}
	freshCapability := fresh.capability()
	ac := effect.ac
	ac.sendMu.Lock()
	responseErr := d.sendParkedRouteResponseLocked(ac, freshCapability.transport, protocol.ParkedRouteResponse{RequestID: request.RequestID, Status: protocol.ParkedRouteSwitched})
	if responseErr == nil {
		ac.releaseParkedOutputLocked()
	}
	ac.sendMu.Unlock()
	if responseErr != nil {
		launch := d.reserveAttachmentSendErrorCleanup(freshCapability, freshCapability.transport.transport)
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

func (d *Daemon) transitionParkedRouteTarget(effect *attachmentEffect, target domain.RemoteSessionTarget) (attachmentTransitionResult, *session, error) {
	capability := effect.capability()
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
			source: effect.sess, target: live, next: effect.ac,
			expectedTransport: effect.transport, sourceCapability: &capability, sourceEffect: effect, action: "parked-route-switch",
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
		source: effect.sess, next: effect.ac,
		expectedTransport: effect.transport, sourceCapability: &capability, sourceEffect: effect, action: "parked-route-restore",
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
			created, createErr = d.resumeRemoteInactiveSessionLocked(target, cwd, effect.ac.geometrySnapshot(), env, current)
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
