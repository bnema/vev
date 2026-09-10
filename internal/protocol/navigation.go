package protocol

import "github.com/bnema/vev/internal/domain"

type NavigationCapabilities uint8

const (
	NavigationCapabilityHomePicker NavigationCapabilities = 1 << iota
	NavigationCapabilityBack
	NavigationCapabilityInventory
	NavigationCapabilityClientPicker
)

type StartupOverlay uint8

const (
	StartupOverlayNone StartupOverlay = iota
	StartupOverlaySessionPicker
)

type NavigationAction uint8

const (
	NavigationOpenHomePicker NavigationAction = 1
	NavigationBack           NavigationAction = 2
)

type ParkedRouteLeaseID [16]byte

func (id ParkedRouteLeaseID) IsZero() bool { return id == ParkedRouteLeaseID{} }

type NavigationDirective struct {
	CauseActionID uint64
	Action        NavigationAction
	LeaseID       ParkedRouteLeaseID
}

type ParkedRouteAction uint8

const (
	ParkedRoutePrepare ParkedRouteAction = iota + 1
	ParkedRouteResume
	ParkedRouteSwitch
)

type ParkedRouteRequest struct {
	RequestID uint64
	LeaseID   ParkedRouteLeaseID
	Action    ParkedRouteAction
	Target    *domain.RemoteSessionTarget
}

type ParkedRouteStatus uint8

const (
	ParkedRouteReady ParkedRouteStatus = iota + 1
	ParkedRouteResumed
	ParkedRouteSwitched
	ParkedRouteRejected
	ParkedRouteExpired
	ParkedRouteStaleTarget
)

type ParkedRouteResponse struct {
	RequestID uint64
	Status    ParkedRouteStatus
}

func validNavigationCapabilities(capabilities NavigationCapabilities) bool {
	return capabilities&^(NavigationCapabilityHomePicker|NavigationCapabilityBack|NavigationCapabilityInventory|NavigationCapabilityClientPicker) == 0
}

func validStartupOverlay(overlay StartupOverlay) bool {
	return overlay == StartupOverlayNone || overlay == StartupOverlaySessionPicker
}

func ValidateNavigation(capabilities NavigationCapabilities, overlay StartupOverlay, homePickerRoute bool) error {
	if !validNavigationCapabilities(capabilities) || !validStartupOverlay(overlay) {
		return ErrInvalidNavigation
	}
	home := capabilities&NavigationCapabilityHomePicker != 0
	back := capabilities&NavigationCapabilityBack != 0
	if (home && !homePickerRoute) || back != (overlay == StartupOverlaySessionPicker) {
		return ErrInvalidNavigation
	}
	if homePickerRoute && back {
		return ErrInvalidNavigation
	}
	return nil
}

func validateHelloNavigation(h Hello) error {
	picker := h.NavigationCapabilities & NavigationCapabilityClientPicker
	rest := h.NavigationCapabilities &^ NavigationCapabilityClientPicker
	h = Hello{
		Intent: h.Intent, Size: h.Size, PixelWidth: h.PixelWidth, PixelHeight: h.PixelHeight,
		Name: h.Name, TermEnv: h.TermEnv, Cwd: h.Cwd, Env: h.Env,
		PreferredTabID: h.PreferredTabID, ExactTarget: h.ExactTarget, RemoteTarget: h.RemoteTarget,
		EnvironmentPolicy: h.EnvironmentPolicy, NavigationCapabilities: rest, StartupOverlay: h.StartupOverlay,
	}
	if h.Intent == IntentNew {
		if err := ValidateNavigation(rest, h.StartupOverlay, true); err != nil {
			return err
		}
		return ValidateNavigation(picker, StartupOverlayNone, false)
	}
	if h.Intent != IntentAttach && h.Intent != IntentResume {
		if rest != 0 || h.StartupOverlay != StartupOverlayNone {
			return ErrInvalidNavigation
		}
		return ValidateNavigation(picker, StartupOverlayNone, false)
	}
	return ValidateNavigation(rest, h.StartupOverlay, h.RemoteTarget != nil || h.EnvironmentPolicy == EnvironmentPolicyDaemonOwned)
}

func ValidateParkedRouteRequest(request ParkedRouteRequest) error {
	if request.RequestID == 0 || request.LeaseID.IsZero() {
		return ErrInvalidNavigation
	}
	switch request.Action {
	case ParkedRoutePrepare, ParkedRouteResume:
		if request.Target != nil {
			return ErrInvalidNavigation
		}
	case ParkedRouteSwitch:
		if request.Target == nil || validateRemoteTarget(*request.Target) != nil {
			return ErrInvalidNavigation
		}
	default:
		return ErrInvalidNavigation
	}
	return nil
}
