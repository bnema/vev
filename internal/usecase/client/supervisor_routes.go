package client

import (
	"errors"

	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol"
)

// Routes and daemon navigation while attached (Plan 003 C4, C5, E3).
//
// The supervisor publishes the client route ledger to the serving daemon after
// the committed initial publication, whenever the attached session changes,
// and on every broker publication. The daemon renders the status-bar MRU,
// other-session bells, and the palette's remote, recent-route, and
// create-on-host rows from it; it never monitors another daemon.
//
// The daemon never dials either. Its navigation requests reach the
// supervisor, which settles them exactly like a picker commit: another tab of
// the attached session switches in place with SelectTab, anything else detaches
// cleanly and swaps through Connecting.
//
//   - RouteNavigationAction and RouteCreateSessionAction name a reference of
//     the published ledger; the supervisor resolves it against the latest
//     broker catalogue and answers a refusal with the typed failure the daemon
//     turns into a toast.
//   - An endpoint-empty AttachTarget names a lifecycle on the serving daemon
//     itself (a stopped session or a close-and-dial handoff); the same-peer
//     offer is confirmed by the worker in place.
//
// Every method runs on the supervisor's run goroutine.

// brokerChanged is the adopted connection's publication wake, or nil.
func (s *Supervisor) brokerChanged() <-chan struct{} {
	if supervisorNil(s.readySub) {
		return nil
	}
	return s.readySub.Changed()
}

// publishRoutes rebuilds the ledger over the live attachment and hands a
// changed snapshot, or the first one of this attachment, to its worker.
func (s *Supervisor) publishRoutes(service ports.BrokerService, run *attachmentRun, request ports.BrokerOpenStreamRequest) {
	target, _, known := s.attachments.committedView()
	if !known {
		return
	}
	if s.routes == nil {
		s.routes = newRouteLedger()
	}
	snapshot, changed := s.routes.build(service.Snapshot(), routeActive{known: true, authority: requestAuthority(request), target: target})
	if snapshot.Generation == 0 || (!changed && s.routesSent == run.token) {
		return
	}
	if s.attachments.publishRoutes(run.token, snapshot) {
		s.routesSent = run.token
	}
}

func requestAuthority(request ports.BrokerOpenStreamRequest) routeAuthority {
	if request.Local {
		return routeAuthority{local: true}
	}
	return routeAuthority{endpoint: request.Endpoint}
}

// settleDaemonNavigation carries out one daemon navigation request over the
// live attachment.
func (s *Supervisor) settleDaemonNavigation(service ports.BrokerService, overlay *attachmentPickerOverlay, message protocol.ServerMessage) {
	target, ok := s.resolveDaemonNavigation(service, overlay.run.token, overlay.request, message)
	if !ok {
		return
	}
	if sameAttachmentTarget(overlay.request, s.attachments.committedTargetOrZero(), target.request) && target.tab.stopped == nil {
		_, current, _ := s.attachments.committedView()
		if target.tab.preferred != "" && target.tab.preferred != current {
			s.attachments.requestTabSelection(overlay.run.token, target.tab.preferred)
		}
		return
	}
	if overlay.active {
		overlay.exit()
	}
	s.pendingSwap = &target
	overlay.swapping = true
	s.attachments.requestDetach(overlay.run.token)
}

// takeSettledNavigation adopts a navigation request the worker handed over
// just before its attachment ended, such as a close-and-dial handoff whose
// daemon detached the source right after sending it.
func (s *Supervisor) takeSettledNavigation(service ports.BrokerService, token AttachmentToken, request ports.BrokerOpenStreamRequest) {
	message, ok := s.attachments.takeNavigation()
	if !ok || s.pendingSwap != nil {
		return
	}
	if target, ok := s.resolveDaemonNavigation(service, token, request, message); ok {
		s.pendingSwap = &target
	}
}

// resolveDaemonNavigation turns one daemon request into an exact attachment
// target. A refusal is answered on the attachment with the typed failure.
func (s *Supervisor) resolveDaemonNavigation(service ports.BrokerService, token AttachmentToken, request ports.BrokerOpenStreamRequest, message protocol.ServerMessage) (pickerAttachmentTarget, bool) {
	switch typed := message.(type) {
	case protocol.RouteNavigationAction:
		fail := func(code protocol.RouteFailureCode) (pickerAttachmentTarget, bool) {
			s.attachments.replyNavigation(token, protocol.RouteNavigationFailure{Key: typed.Key, Generation: typed.Generation, Code: code})
			return pickerAttachmentTarget{}, false
		}
		routed, ok := s.resolveRoute(protocol.RouteRef{Key: typed.Key, Generation: typed.Generation})
		if !ok || routed.host {
			return fail(protocol.RouteFailureStaleSelection)
		}
		target, err := s.resolveRouteSelection(service, pickerSelectionRef{
			kind: pickerSelectionExact, local: routed.authority.local, endpoint: routed.authority.endpoint,
			lifecycle: routed.target.LifecycleID, name: routed.target.SessionName,
		})
		if err != nil {
			return fail(routeNavigationFailureCode(err))
		}
		return target, true
	case protocol.RouteCreateSessionAction:
		fail := func(code protocol.RouteFailureCode) (pickerAttachmentTarget, bool) {
			s.attachments.replyNavigation(token, protocol.SessionCreationFailure{RequestID: typed.RequestID, Code: code})
			return pickerAttachmentTarget{}, false
		}
		routed, ok := s.resolveRoute(protocol.RouteRef{Key: typed.Key, Generation: typed.Generation})
		if !ok {
			return fail(protocol.RouteFailureStaleSelection)
		}
		target, err := s.resolveRouteSelection(service, pickerSelectionRef{
			kind: pickerSelectionCreateNamed, local: routed.authority.local, endpoint: routed.authority.endpoint,
			createName: typed.SessionName,
		})
		if err != nil {
			return fail(routeNavigationFailureCode(err))
		}
		return target, true
	case protocol.AttachTarget:
		return s.resolveServingHandoff(service, request, typed)
	default:
		return pickerAttachmentTarget{}, false
	}
}

// resolveServingHandoff resolves an endpoint-empty handoff: the serving
// daemon itself named one of its own lifecycles, so the attachment's own
// authority is kept and only the exact target and tab change. The daemon
// revalidates the exact identity on Hello.
func (s *Supervisor) resolveServingHandoff(service ports.BrokerService, request ports.BrokerOpenStreamRequest, handoff protocol.AttachTarget) (pickerAttachmentTarget, bool) {
	if handoff.Endpoint != "" || handoff.SessionTarget != nil || handoff.ExactTarget == nil || handoff.Intent != protocol.IntentAttach {
		s.offerPickerNotice("daemon-handoff", "the daemon asked for a destination this client cannot open")
		return pickerAttachmentTarget{}, false
	}
	stream, err := service.NextStreamID()
	if err != nil {
		return pickerAttachmentTarget{}, false
	}
	next := request
	next.Stream = stream
	next.Connection = service.ConnectionID()
	next.Admission = ports.BrokerAdmissionExact
	next.Target = *handoff.ExactTarget
	next.Name = ""
	next.Purpose = ports.BrokerStreamAttachment
	next.StartMode = ports.BrokerDaemonStartIfNeeded
	if next.Validate() != nil {
		return pickerAttachmentTarget{}, false
	}
	return pickerAttachmentTarget{request: next, tab: attachmentTab{preferred: handoff.PreferredTabID}}, true
}

func (s *Supervisor) resolveRoute(ref protocol.RouteRef) (routeLedgerTarget, bool) {
	if s.routes == nil || ref.IsZero() {
		return routeLedgerTarget{}, false
	}
	return s.routes.resolve(ref)
}

// resolveRouteSelection resolves one route through the same catalogue rules
// as a picker commit, against the latest broker publication.
func (s *Supervisor) resolveRouteSelection(service ports.BrokerService, selection pickerSelectionRef) (pickerAttachmentTarget, error) {
	stream, err := service.NextStreamID()
	if err != nil {
		return pickerAttachmentTarget{}, err
	}
	snapshot := service.Snapshot()
	authority := pickerAuthorityInSnapshot(snapshot, selection)
	if authority.found {
		// The ledger keys a host by endpoint; the registration is the
		// catalogue's current one.
		selection.registration = authority.observation.Registration
	}
	base := pickerResolveBase{Connection: service.ConnectionID(), Stream: stream}
	request, tab, err := resolvePickerTarget(snapshot.Epoch, authority, selection, base, true)
	if err != nil {
		return pickerAttachmentTarget{}, err
	}
	return pickerAttachmentTarget{request: request, tab: tab}, nil
}

func routeNavigationFailureCode(err error) protocol.RouteFailureCode {
	var catalogueErr pickerCatalogueError
	if errors.As(err, &catalogueErr) {
		switch catalogueErr.Code {
		case pickerCatalogueGone:
			return protocol.RouteFailureNoSuchRoute
		case pickerCatalogueReplaced:
			return protocol.RouteFailureTargetChanged
		case pickerCatalogueEpochStale:
			return protocol.RouteFailureStaleSelection
		}
	}
	return protocol.RouteFailureUnavailable
}
