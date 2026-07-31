package domain

import "encoding/base64"

// RemoteSessionKey identifies a discovered remote session independently from
// its display label.
type RemoteSessionKey struct {
	Host string
	Name string
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
	return SessionID("remote:" + host + "." + name)
}

// Display returns the presentation label for this remote session.
func (k RemoteSessionKey) Display() string {
	return k.Name + "@" + k.Host
}
