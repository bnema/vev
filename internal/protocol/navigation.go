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

// validateHelloNavigation accepts the optional inventory capability on the
// intents that can consume it and rejects any other advertised feature.
func validateHelloNavigation(h Hello) error {
	switch h.Intent {
	case IntentAttach, IntentResume, IntentNew:
		return ValidateNavigation(h.NavigationCapabilities)
	default:
		if h.NavigationCapabilities != 0 {
			return ErrInvalidNavigation
		}
		return nil
	}
}

// ValidateNavigation rejects capabilities this protocol does not define.
func ValidateNavigation(capabilities NavigationCapabilities) error {
	if !validNavigationCapabilities(capabilities) {
		return ErrInvalidNavigation
	}
	return nil
}
