package daemon

import (
	"encoding/json"
	"strings"
)

// Session-bound variables describe the desktop or login the user attached
// from. A local client-owned attach refreshes only these; every other variable
// (SHELL, PATH, HOME, ...) keeps the value the session was created with.
//
// Desktop variables are set when the client has them and never removed, so an
// attach from a plain SSH or console login cannot strip a graphical session.
var sessionDesktopEnvironment = []string{
	"WAYLAND_DISPLAY",
	"DISPLAY",
	"XAUTHORITY",
	"DBUS_SESSION_BUS_ADDRESS",
	"XDG_RUNTIME_DIR",
	"XDG_CURRENT_DESKTOP",
	"XDG_SESSION_TYPE",
	"XDG_SESSION_DESKTOP",
	"XDG_SESSION_CLASS",
	"XDG_SESSION_ID",
}

// Login variables follow the latest attach exactly: a value the client lacks is
// removed, because a stale agent socket or SSH peer is worse than none.
var sessionLoginEnvironment = []string{
	"SSH_AUTH_SOCK",
	"SSH_AGENT_PID",
	"SSH_CONNECTION",
	"SSH_CLIENT",
	"SSH_TTY",
}

// adoptClientEnvironmentLocked applies a client-owned environment to the
// session's future PTY children. The caller holds sess.mu.
func (s *session) adoptClientEnvironmentLocked(client []string) {
	if s.envProvisional {
		s.env = copyEnvironment(client)
		s.envProvisional = false
		return
	}
	s.env = refreshSessionEnvironment(s.env, client)
}

// refreshSessionEnvironment returns current with its session-bound variables
// refreshed from client. Refreshed variables move to the end; all others keep
// their original bytes and order.
func refreshSessionEnvironment(current, client []string) []string {
	updates := make(map[string]string, len(sessionDesktopEnvironment)+len(sessionLoginEnvironment))
	drop := make(map[string]bool, len(sessionLoginEnvironment))
	for _, key := range sessionDesktopEnvironment {
		if value, ok := lookupEnvironment(client, key); ok {
			updates[key] = value
		}
	}
	for _, key := range sessionLoginEnvironment {
		if value, ok := lookupEnvironment(client, key); ok {
			updates[key] = value
		} else {
			drop[key] = true
		}
	}
	out := make([]string, 0, len(current)+len(updates))
	for _, entry := range current {
		key, _, _ := environmentEntry(entry)
		if _, updated := updates[key]; updated || drop[key] {
			continue
		}
		out = append(out, entry)
	}
	for _, key := range sessionBoundEnvironment() {
		if value, ok := updates[key]; ok {
			out = append(out, key+"="+value)
		}
	}
	return out
}

func sessionBoundEnvironment() []string {
	return append(append([]string(nil), sessionDesktopEnvironment...), sessionLoginEnvironment...)
}

// lookupEnvironment returns the first value for key, matching getenv(3).
func lookupEnvironment(env []string, key string) (string, bool) {
	for _, entry := range env {
		if name, value, ok := environmentEntry(entry); ok && name == key {
			return value, true
		}
	}
	return "", false
}

// sessionEnvironmentExport renders the session-bound variables a running shell
// should apply. JSON maps each variable to its value, or null when the shell
// should unset it. Text output is one KEY=value line per set variable.
// Desktop variables the session never saw are omitted rather than unset, so a
// shell keeps any value it obtained on its own.
func sessionEnvironmentExport(env []string, asJSON bool) (string, error) {
	values := make(map[string]*string)
	var text strings.Builder
	for _, key := range sessionBoundEnvironment() {
		if value, ok := lookupEnvironment(env, key); ok {
			values[key] = &value
			text.WriteString(key + "=" + value + "\n")
		}
	}
	for _, key := range sessionLoginEnvironment {
		if _, ok := values[key]; !ok {
			values[key] = nil
		}
	}
	if !asJSON {
		return text.String(), nil
	}
	encoded, err := json.Marshal(values)
	if err != nil {
		return "", err
	}
	return string(encoded) + "\n", nil
}
