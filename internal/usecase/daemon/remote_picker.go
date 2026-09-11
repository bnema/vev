package daemon

import (
	"encoding/base64"
	"fmt"
	"math"
	"time"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol/catalogue"
)

// directoryInventoryStaleAge is the presentation freshness threshold: past
// this age inventory renders stale, but staleness never gates activation.
const directoryInventoryStaleAge = 30 * time.Second

func remoteSessionStateStopped(state catalogue.RemoteCatalogSessionState) bool {
	return state == catalogue.RemoteCatalogSessionDown
}

func remoteCatalogSessionTarget(key domain.RemoteSessionKey, session catalogue.RemoteCatalogSession) (domain.RemoteSessionKey, domain.RemoteSessionTarget) {
	key.LifecycleID = session.LifecycleID
	key.DisplayOrigin = domain.RemoteDisplayOrigin(key.Host)
	target := domain.RemoteSessionTarget{
		Endpoint:      key.Host,
		DisplayOrigin: key.DisplayOrigin,
		LifecycleID:   session.LifecycleID,
		SessionName:   session.Name,
		Stopped:       remoteSessionStateStopped(session.State),
	}
	tabs := catalogue.CatalogTabs(session)
	if len(tabs) == 0 {
		return key, target
	}
	if target.Stopped {
		first := tabs[0]
		if first.ID != "" {
			target.StoppedTab = domain.NewStableTabSelector(domain.TabStableID(first.ID))
		} else {
			tabCount := min(len(tabs), math.MaxUint16)
			target.StoppedTab = domain.NewOrdinalTabSelector(0, first.Name, uint16(tabCount))
		}
		return key, target
	}
	active := 0
	for i, tab := range tabs {
		if session.ActiveTabID != "" && tab.ID == session.ActiveTabID {
			active = i
			break
		}
	}
	target.LiveTabID = domain.TabStableID(tabs[active].ID)
	return key, target
}

// directoryInventoryStale reports whether the host's last success is past
// the presentation freshness threshold. Staleness renders; it never gates.
func directoryInventoryStale(host ports.RemoteHostSnapshot, now time.Time) bool {
	if host.LastSuccess.IsZero() {
		return host.InventoryKnown
	}
	return !now.Before(host.LastSuccess.Add(directoryInventoryStaleAge))
}

func remotePickerView(key domain.RemoteSessionKey, session catalogue.RemoteCatalogSession, host ports.RemoteHostSnapshot, now time.Time) pickerSessionView {
	key, target := remoteCatalogSessionTarget(key, session)
	stopped := remoteSessionStateStopped(session.State)
	broken := session.State == catalogue.RemoteCatalogSessionBroken
	tabs := catalogue.CatalogTabs(session)
	viewTabs := make([]pickerTabEntry, 0, len(tabs))
	active := 0
	for i, tab := range tabs {
		if session.ActiveTabID != "" && tab.ID == session.ActiveTabID {
			active = i
		}
		name := tab.Name
		if name == "" {
			name = fmt.Sprintf("%d", int(tab.Index)+1)
		}
		viewTabs = append(viewTabs, pickerTabEntry{
			TabID:     domain.TabStableID(tab.ID),
			Name:      name,
			RawName:   tab.Name,
			Detail:    tab.Detail,
			Attention: tab.Attention,
		})
	}
	// Keep the structured session identity on rows even when one tab ID is
	// malformed or a broken session cannot be activated. Cursor navigation
	// must remain possible, while sendRemoteAttachTargetForAttachment will
	// fail closed on the invalid target.
	remoteTarget := &target
	reason := directorySessionReason(host, session, target)
	targetValid := remoteTarget.Validate() == nil
	activation := pickerRemoteUnavailable
	if !broken && targetValid && host.Availability != domain.RemoteAvailabilityIncompatible {
		// Known inventory is attemptable at any age: freshness is
		// presentation information, not attach authority. Exact session,
		// lifecycle and tab validation still applies at submit, and the
		// destination rejects precisely.
		if stopped {
			activation = pickerRemoteRestart
		} else {
			activation = pickerRemoteAttach
		}
	}

	return pickerSessionView{
		ID:                 key.ID(),
		Name:               key.Display(),
		RemoteKey:          &key,
		RemoteTarget:       remoteTarget,
		RemoteHost:         key.Host,
		RemoteAvailability: directoryPickerAvailability(host, now),
		RemoteDetail:       directoryPickerDetail(host, session, reason, activation, now),
		RemoteReason:       reason,
		RemoteActivation:   activation,
		Tabs:               viewTabs,
		Active:             active,
		Stopped:            stopped,
		CannotAcceptMoves:  true,
	}
}

// directorySessionReason labels a session row for display. It never gates
// activation except through broken, invalid, or incompatible states; age and
// observation failures render while the row stays attemptable.
func directorySessionReason(host ports.RemoteHostSnapshot, session catalogue.RemoteCatalogSession, target domain.RemoteSessionTarget) string {
	if session.State == catalogue.RemoteCatalogSessionBroken {
		return domain.RemoteReasonSessionBroken
	}
	if target.Validate() != nil {
		return domain.RemoteReasonIdentityChanged
	}
	switch host.Availability {
	case domain.RemoteAvailabilityReachable:
		if host.Checking {
			return domain.RemoteReasonRefreshing
		}
		return ""
	case domain.RemoteAvailabilityUnreachable:
		return domain.RemoteReasonHostUnreachable
	case domain.RemoteAvailabilityIncompatible:
		return domain.RemoteReasonVersionMismatch
	case domain.RemoteAvailabilityAuthFailed:
		return domain.RemoteReasonAuthFailure
	case domain.RemoteAvailabilityInvalidResponse:
		return domain.RemoteReasonMalformed
	default:
		return domain.RemoteReasonRefreshing
	}
}

func directoryPickerAvailability(host ports.RemoteHostSnapshot, now time.Time) pickerRemoteAvailability {
	switch host.Availability {
	case domain.RemoteAvailabilityReachable:
		if directoryInventoryStale(host, now) {
			return pickerRemoteStale
		}
		return pickerRemoteFresh
	case domain.RemoteAvailabilityIncompatible:
		return pickerRemoteVersionMismatch
	case domain.RemoteAvailabilityUnknown:
		return pickerRemoteCached
	default:
		if host.Checking {
			return pickerRemoteCached
		}
		return pickerRemoteStale
	}
}

func directoryPickerDetail(host ports.RemoteHostSnapshot, session catalogue.RemoteCatalogSession, reason string, activation pickerRemoteActivation, now time.Time) string {
	if host.Checking {
		return "checking remote…"
	}
	stopped := remoteSessionStateStopped(session.State)
	broken := session.State == catalogue.RemoteCatalogSessionBroken
	switch host.Availability {
	case domain.RemoteAvailabilityReachable:
		if !host.LastSuccess.IsZero() && directoryInventoryStale(host, now) {
			return "catalog stale since " + host.LastSuccess.Format(time.RFC3339)
		}
	case domain.RemoteAvailabilityUnreachable:
		if host.InventoryKnown && !host.LastSuccess.IsZero() {
			return "stale since " + host.LastSuccess.Format(time.RFC3339)
		}
		return "unreachable"
	case domain.RemoteAvailabilityIncompatible:
		return "version mismatch"
	case domain.RemoteAvailabilityAuthFailed:
		return "authentication failed"
	case domain.RemoteAvailabilityInvalidResponse:
		return "catalog malformed"
	default:
		return "checking remote…"
	}
	switch {
	case broken:
		return "session broken"
	case reason == domain.RemoteReasonIdentityChanged:
		return "identity changed"
	case activation == pickerRemoteRestart:
		return "stopped — Enter to restart"
	case session.State == catalogue.RemoteCatalogSessionUp:
		return "up"
	case stopped:
		return "stopped"
	default:
		return string(session.State)
	}
}

// remotePickerCheckingView is the non-actionable placeholder published
// while the remote directory has never been initialized: it distinguishes
// "still checking remotes" from "no remotes configured". Enter on the
// row fails safe through the usual unavailable-target path.
func remotePickerCheckingView() pickerSessionView {
	return pickerSessionView{
		ID:                domain.SessionID("remote:checking"),
		Name:              "checking remotes…",
		RemoteActivation:  pickerRemoteUnavailable,
		CannotAcceptMoves: true,
	}
}

func remotePickerHostView(host ports.RemoteHostSnapshot, now time.Time) pickerSessionView {
	return pickerSessionView{
		ID:                 domain.SessionID("remote-host:" + base64.RawURLEncoding.EncodeToString([]byte(host.Endpoint))),
		Name:               host.Endpoint,
		RemoteHost:         host.Endpoint,
		RemoteReason:       directoryHostReason(host),
		RemoteAvailability: directoryPickerAvailability(host, now),
		RemoteDetail:       directoryHostDetail(host),
		RemoteActivation:   pickerRemoteUnavailable,
		CannotAcceptMoves:  true,
	}
}

func directoryHostReason(host ports.RemoteHostSnapshot) string {
	switch host.Availability {
	case domain.RemoteAvailabilityReachable:
		return ""
	case domain.RemoteAvailabilityUnreachable:
		return domain.RemoteReasonHostUnreachable
	case domain.RemoteAvailabilityIncompatible:
		return domain.RemoteReasonVersionMismatch
	case domain.RemoteAvailabilityAuthFailed:
		return domain.RemoteReasonAuthFailure
	case domain.RemoteAvailabilityInvalidResponse:
		return domain.RemoteReasonMalformed
	default:
		return domain.RemoteReasonRefreshing
	}
}

func directoryHostDetail(host ports.RemoteHostSnapshot) string {
	if host.Checking {
		return "checking remote…"
	}
	switch host.Availability {
	case domain.RemoteAvailabilityReachable:
		return "no sessions"
	case domain.RemoteAvailabilityUnreachable:
		return "unreachable"
	case domain.RemoteAvailabilityIncompatible:
		return "version mismatch"
	case domain.RemoteAvailabilityAuthFailed:
		return "authentication failed"
	case domain.RemoteAvailabilityInvalidResponse:
		return "catalog malformed"
	default:
		return "checking remote…"
	}
}
