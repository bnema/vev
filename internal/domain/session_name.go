package domain

import (
	"errors"
	"regexp"
)

const sessionNamePattern = `[A-Za-z0-9][A-Za-z0-9._-]{0,63}`

var (
	// ErrInvalidSessionName is returned when a new or renamed session name is unsafe.
	ErrInvalidSessionName = errors.New("invalid session name: must match [A-Za-z0-9][A-Za-z0-9._-]{0,63}")
	sessionNameRE         = regexp.MustCompile(`^` + sessionNamePattern + `$`)
)

// ValidateSessionName rejects unsafe names for newly-created or renamed sessions.
func ValidateSessionName(name string) error {
	if !sessionNameRE.MatchString(name) {
		return ErrInvalidSessionName
	}
	return nil
}
