package client

import (
	"errors"
	"path/filepath"
	"strings"
	"unicode/utf8"

	"github.com/bnema/vev/internal/ports"
)

// SessionEnvironmentProvenance names where a session environment came from.
type SessionEnvironmentProvenance uint8

const (
	// SessionEnvironmentUnspecified is the zero value and never validates.
	SessionEnvironmentUnspecified SessionEnvironmentProvenance = iota
	// SessionEnvironmentLocalCLI carries environment and working directory
	// captured from a local CLI invocation.
	SessionEnvironmentLocalCLI
	// SessionEnvironmentLocalPicker carries environment and working directory
	// captured from a local picker selection.
	SessionEnvironmentLocalPicker
	// SessionEnvironmentRemote carries no local environment: remote sessions
	// must not forward local variables or a local working directory.
	SessionEnvironmentRemote
)

// SessionEnvironment carries the environment variables and working directory
// proposed for a session creation.
type SessionEnvironment struct {
	Provenance SessionEnvironmentProvenance
	Env        []string
	Cwd        string
}

// Clone returns a deep copy with an independent environment slice.
func (e SessionEnvironment) Clone() SessionEnvironment {
	out := e
	if e.Env != nil {
		out.Env = make([]string, len(e.Env))
		copy(out.Env, e.Env)
	}
	return out
}

// Validate reports whether the environment is well-formed for its provenance.
func (e SessionEnvironment) Validate() error {
	switch e.Provenance {
	case SessionEnvironmentLocalCLI, SessionEnvironmentLocalPicker:
		if len(e.Env) > ports.BrokerMaxEnvEntries {
			return errors.New("client: session environment has too many entries")
		}
		for _, entry := range e.Env {
			if !utf8.ValidString(entry) {
				return errors.New("client: session environment entry is not valid UTF-8")
			}
			if strings.ContainsRune(entry, 0) {
				return errors.New("client: session environment entry contains NUL")
			}
			if len(entry) > ports.BrokerMaxEnvEntryBytes {
				return errors.New("client: session environment entry too long")
			}
			name, _, found := strings.Cut(entry, "=")
			if !found || name == "" {
				return errors.New("client: session environment entry must be NAME=value")
			}
		}
		if e.Cwd != "" {
			if !utf8.ValidString(e.Cwd) {
				return errors.New("client: session environment cwd is not valid UTF-8")
			}
			if strings.ContainsRune(e.Cwd, 0) {
				return errors.New("client: session environment cwd contains NUL")
			}
			if len(e.Cwd) > 65535 {
				return errors.New("client: session environment cwd too long")
			}
			if !filepath.IsAbs(e.Cwd) {
				return errors.New("client: session environment cwd must be absolute")
			}
		}
		return nil
	case SessionEnvironmentRemote:
		if len(e.Env) != 0 {
			return errors.New("client: remote session environment must not carry environment entries")
		}
		if e.Cwd != "" {
			return errors.New("client: remote session environment must not carry a working directory")
		}
		return nil
	default:
		return errors.New("client: session environment provenance is unspecified or unknown")
	}
}
