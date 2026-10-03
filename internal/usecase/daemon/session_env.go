package daemon

import (
	"encoding/json"
	"slices"
	"strings"

	"github.com/bnema/vev/internal/protocol"
)

// Display variables describe one graphical session and move as a group: a
// client with a non-empty WAYLAND_DISPLAY or DISPLAY replaces the whole group and removes
// members it lacks, so a switch to another compositor or an X11-only desktop
// never leaves a stale socket behind. A client without a display (console)
// or reached over SSH leaves the group untouched: an X-forwarded DISPLAY
// belongs to that one connection and must not replace the local desktop.
var sessionDisplayEnvironment = []string{
	"WAYLAND_DISPLAY",
	"DISPLAY",
	"XAUTHORITY",
	"XDG_CURRENT_DESKTOP",
	"XDG_SESSION_TYPE",
	"XDG_SESSION_DESKTOP",
	"XDG_SESSION_CLASS",
	"XDG_SESSION_ID",
}

// User variables belong to the user login rather than one display: with
// systemd, every graphical session of a user shares the runtime dir and user
// bus. They are set when the client has them and never removed.
var sessionUserEnvironment = []string{
	"DBUS_SESSION_BUS_ADDRESS",
	"XDG_RUNTIME_DIR",
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

// setClientEnvironment records the attachment's own client-owned environment;
// nil means the daemon owns it and the attachment never changes session env.
func (ac *attachedClient) setClientEnvironment(env []string) {
	if env == nil {
		ac.clientEnv.Store(nil)
		return
	}
	owned := append(make([]string, 0, len(env)), env...)
	ac.clientEnv.Store(&owned)
}

// clientEnvironment returns the attachment's client-owned environment, or
// ok=false when the daemon owns it. The slice is immutable; do not modify it.
func (ac *attachedClient) clientEnvironment() ([]string, bool) {
	env := ac.clientEnv.Load()
	if env == nil {
		return nil, false
	}
	return *env, true
}

// helloClientEnvironment returns the Hello environment when the client owns
// it, or nil for a daemon-owned (remote) attach.
func helloClientEnvironment(h protocol.Hello) []string {
	if h.EnvironmentPolicy == protocol.EnvironmentPolicyDaemonOwned {
		return nil
	}
	if h.Env == nil {
		return []string{}
	}
	return h.Env
}

// environmentSeedLocked returns a copy of the session environment for a new
// session seeded from this one, and whether it is still the daemon's own
// (provisional). The caller holds s.mu.
func (s *session) environmentSeedLocked() ([]string, bool) {
	return copyEnvironment(s.env), s.envProvisional
}

// markEnvironmentProvisional records that sess.env came from the daemon's own
// process, so the first client-owned environment replaces it.
func markEnvironmentProvisional(sess *session) {
	sess.mu.Lock()
	sess.envProvisional = true
	sess.mu.Unlock()
}

// adoptClientEnvironmentLocked applies a client-owned environment to the
// session's future PTY children. The caller holds sess.mu.
//
// A provisional environment (the daemon's own, see envProvisional) is replaced
// wholesale, except that a client without its own display keeps the session's
// display group when it has one, matching refreshSessionEnvironment.
func (s *session) adoptClientEnvironmentLocked(client []string) {
	if !s.envProvisional {
		s.env = refreshSessionEnvironment(s.env, client)
		return
	}
	next := copyEnvironment(client)
	if !ownsDisplay(client) && hasDisplay(s.env) {
		next = replaceEnvironmentGroup(next, s.env, sessionDisplayEnvironment)
	}
	s.env = next
	s.envProvisional = false
}

// refreshSessionEnvironment returns current with its session-bound variables
// refreshed from client. Entries keep their position; duplicates of a refreshed
// variable collapse into its first entry, and new variables are appended.
func refreshSessionEnvironment(current, client []string) []string {
	if ownsDisplay(client) {
		current = replaceEnvironmentGroup(current, client, sessionDisplayEnvironment)
	}
	for _, key := range sessionUserEnvironment {
		if value, ok := lookupEnvironment(client, key); ok {
			current = setEnvironment(current, key, &value)
		}
	}
	return replaceEnvironmentGroup(current, client, sessionLoginEnvironment)
}

// replaceEnvironmentGroup sets every key of group in env to its value in
// source, removing keys source lacks. It returns a new slice.
func replaceEnvironmentGroup(env, source, group []string) []string {
	for _, key := range group {
		if value, ok := lookupEnvironment(source, key); ok {
			env = setEnvironment(env, key, &value)
		} else {
			env = setEnvironment(env, key, nil)
		}
	}
	return env
}

// setEnvironment returns a copy of env with key set to *value in place of its
// first entry (appended when absent), or removed when value is nil. Later
// duplicates of key are dropped. env itself is never modified.
func setEnvironment(env []string, key string, value *string) []string {
	out := make([]string, 0, len(env)+1)
	found := false
	for _, entry := range env {
		if name, _, _ := environmentEntry(entry); name == key {
			if !found && value != nil {
				out = append(out, key+"="+*value)
			}
			found = true
			continue
		}
		out = append(out, entry)
	}
	if !found && value != nil {
		out = append(out, key+"="+*value)
	}
	return out
}

// ownsDisplay reports whether client is a local graphical session whose
// display group should replace the session's. SSH clients never do, even with
// X forwarding.
func ownsDisplay(client []string) bool {
	if _, ssh := lookupEnvironment(client, "SSH_CONNECTION"); ssh {
		return false
	}
	return hasDisplay(client)
}

// hasDisplay reports whether env belongs to a graphical session. An empty
// value names no display (it usually means "no X here"), so it does not count.
func hasDisplay(env []string) bool {
	wayland, _ := lookupEnvironment(env, "WAYLAND_DISPLAY")
	x11, _ := lookupEnvironment(env, "DISPLAY")
	return wayland != "" || x11 != ""
}

// sessionBoundEnvironment lists the variables that follow the attaching client.
// A local client-owned attach refreshes only these; every other variable
// (SHELL, PATH, HOME, ...) keeps the value the session was created with.
var sessionBoundEnvironment = slices.Concat(sessionDisplayEnvironment, sessionUserEnvironment, sessionLoginEnvironment)

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
// should unset it; it is the machine format. Text output is one KEY=value line
// per set variable, for people.
// Missing login variables are always unset. Missing display variables are
// unset only when the session has a display; otherwise they are omitted, so a
// shell keeps any value it obtained on its own.
func sessionEnvironmentExport(env []string, asJSON bool) (string, error) {
	values := make(map[string]*string)
	var text strings.Builder
	for _, key := range sessionBoundEnvironment {
		if value, ok := lookupEnvironment(env, key); ok {
			values[key] = &value
			text.WriteString(key + "=" + value + "\n")
		}
	}
	unset := sessionLoginEnvironment
	if hasDisplay(env) {
		unset = slices.Concat(sessionDisplayEnvironment, sessionLoginEnvironment)
	}
	for _, key := range unset {
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
