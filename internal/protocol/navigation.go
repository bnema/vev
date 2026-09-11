package protocol

// NavigationCapabilities advertises optional client-driven navigation
// features. Presenting a picker is not one of them: every attached client
// renders the picker itself, so the serving daemon only negotiates the
// features it must act on.
type NavigationCapabilities uint8

const (
	NavigationCapabilityInventory NavigationCapabilities = 1 << iota
)

func validNavigationCapabilities(capabilities NavigationCapabilities) bool {
	return capabilities&^NavigationCapabilityInventory == 0
}

// ValidateNavigation rejects capabilities this protocol does not define and
// accepts the inventory capability only on the intents whose attachment can
// serve it. Both the client before dialing and the serving daemon on Hello use
// this rule, so a request the client admits is one the daemon accepts.
func ValidateNavigation(intent uint8, capabilities NavigationCapabilities) error {
	if !validNavigationCapabilities(capabilities) {
		return ErrInvalidNavigation
	}
	switch intent {
	case IntentNew, IntentAttach, IntentResume:
		return nil
	default:
		if capabilities != 0 {
			return ErrInvalidNavigation
		}
		return nil
	}
}
