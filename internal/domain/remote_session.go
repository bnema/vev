package domain

import (
	"encoding/base64"
	"strings"
)

// RemoteSessionKey identifies a discovered remote session independently from
// its display label.
type RemoteSessionKey struct {
	Host        string
	Name        string
	LifecycleID SessionLifecycleID
	// DisplayOrigin is presentation-only. When empty, Display derives a
	// conservative origin from Host for legacy callers; picker targets should
	// populate it explicitly and never parse Display back into routing data.
	DisplayOrigin string
}

// Validate reports whether the host target and session name are both valid.
func (k RemoteSessionKey) Validate() error {
	if err := ValidateRemoteHostTarget(k.Host); err != nil {
		return err
	}
	return ValidateSessionName(k.Name)
}

// ID returns the opaque, delimiter-safe ID for this remote session. Routing
// retains the structured key; callers must not recover it by decoding this ID.
func (k RemoteSessionKey) ID() SessionID {
	host := base64.RawURLEncoding.EncodeToString([]byte(k.Host))
	name := base64.RawURLEncoding.EncodeToString([]byte(k.Name))
	if k.LifecycleID != (SessionLifecycleID{}) {
		lifecycle := base64.RawURLEncoding.EncodeToString(k.LifecycleID[:])
		return SessionID("remote:" + host + "." + name + "." + lifecycle)
	}
	return SessionID("remote:" + host + "." + name)
}

// Display returns the presentation label for this remote session.
func (k RemoteSessionKey) Display() string {
	origin := k.DisplayOrigin
	if origin == "" {
		origin = k.Host
		if i := strings.LastIndexByte(origin, '@'); i > 0 && i < len(origin)-1 {
			origin = origin[i+1:]
		}
	}
	return k.Name + "@" + origin
}
