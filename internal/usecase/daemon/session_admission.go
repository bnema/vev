package daemon

import (
	"path/filepath"
	"slices"
	"strings"
	"unicode/utf8"

	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol"
)

// admissionCwdMaxBytes bounds the client-supplied working directory a local
// admitted attachment may carry. It mirrors the Hello wire bound (uint16
// length) so an admission can never authorize a value the wire could not carry.
const admissionCwdMaxBytes = 65535

// sessionHelloAdmission reads the provisioned admission metadata the accepting
// side stamped onto an admitted connection, through the optional structural
// provider seam. A connection built without that seam - a direct package
// caller, a legacy sessionwire constructor, or a raw test transport - reports
// present=false, and the daemon then follows its legacy validation unchanged.
// The provider hands back a defensive copy, so the admission the daemon
// validates can never be mutated through the connection.
func sessionHelloAdmission(tr ports.ServerConnection) (ports.SessionAdmission, bool) {
	provider, ok := tr.(ports.SessionAdmissionProvider)
	if !ok || provider == nil {
		return ports.SessionAdmission{}, false
	}
	admission, ok := provider.SessionAdmission()
	if !ok {
		return ports.SessionAdmission{}, false
	}
	return admission, true
}

// admissionError is the typed refusal for a Hello that contradicts, or is not
// covered by, the provisioned admission it arrived under. It is a client
// refusal rather than an internal failure: the caller can surface it and retry
// under the accepted authority.
func admissionError(text string) error {
	return &protoErr{protocol.ErrNoSuchTarget, text}
}

// validateSessionHelloAdmission enforces the closed admission contract for one
// Hello that arrived on an admitted connection. The admission is the accepting
// side's own authority, so every field the peer declared in Hello is checked
// against it rather than trusted:
//   - the connection must be an attachment stream, so a control or observation
//     stream can never become an attachment by sending a Hello;
//   - a local admission carries a client-owned policy and an optional, bounded,
//     absolute working directory, while a remote admission carries a
//     daemon-owned policy and no client environment or working directory at all;
//   - the Hello's locality, environment policy, and ordered environment must
//     equal the admitted values exactly;
//   - the closed admission variant maps to exactly one intent and credential
//     shape, so a create can never smuggle a resume token or a target, and an
//     exact attach can never create.
func validateSessionHelloAdmission(h protocol.Hello, admission ports.SessionAdmission) error {
	if err := admission.Validate(); err != nil {
		return admissionError("invalid session admission")
	}
	if admission.Purpose != ports.BrokerStreamAttachment {
		return admissionError("stream admission is not an attachment")
	}
	switch admission.Origin {
	case ports.SessionOriginLocal:
		if admission.Policy.EnvironmentPolicy != protocol.EnvironmentPolicyClientOwned {
			return admissionError("local admission requires a client-owned environment policy")
		}
		if err := validateLocalAdmissionCwd(h.Cwd); err != nil {
			return err
		}
	case ports.SessionOriginRemote:
		if admission.Policy.EnvironmentPolicy != protocol.EnvironmentPolicyDaemonOwned {
			return admissionError("remote admission requires a daemon-owned environment policy")
		}
		if len(h.Env) != 0 || h.Cwd != "" {
			return admissionError("remote admission carries a client environment or working directory")
		}
	default:
		return admissionError("unknown session admission origin")
	}
	if h.Remote != (admission.Origin == ports.SessionOriginRemote) {
		return admissionError("hello locality contradicts the admitted origin")
	}
	if h.EnvironmentPolicy != admission.Policy.EnvironmentPolicy {
		return admissionError("hello environment policy contradicts the admitted policy")
	}
	if !slices.Equal(h.Env, admission.Env) {
		return admissionError("hello environment contradicts the admitted environment")
	}
	switch admission.Admission {
	case ports.BrokerAdmissionCreateEphemeral:
		if h.Intent != protocol.IntentEphemeral {
			return admissionError("ephemeral admission requires an ephemeral hello")
		}
		if h.Name != "" || h.ExactTarget != nil || h.SessionTarget != nil || h.ResumeToken != 0 {
			return admissionError("ephemeral admission forbids a name, target, or resume token")
		}
	case ports.BrokerAdmissionCreateNamed:
		if h.Intent != protocol.IntentNew {
			return admissionError("named admission requires a new-session hello")
		}
		if h.Name != admission.Name {
			return admissionError("hello name contradicts the admitted session name")
		}
		if h.ExactTarget != nil || h.SessionTarget != nil || h.ResumeToken != 0 {
			return admissionError("named admission forbids a target or resume token")
		}
	case ports.BrokerAdmissionExact:
		if h.ExactTarget == nil || *h.ExactTarget != admission.Target {
			return admissionError("hello exact target contradicts the admitted target")
		}
		if h.Name != admission.Target.SessionName {
			return admissionError("hello name contradicts the admitted target")
		}
		if h.ResumeToken == 0 {
			if h.Intent != protocol.IntentAttach {
				return admissionError("fresh exact admission requires an attach hello")
			}
		} else if h.Intent != protocol.IntentResume {
			return admissionError("resume exact admission requires a resume hello")
		}
		if h.SessionTarget != nil && (h.SessionTarget.LifecycleID != admission.Target.LifecycleID ||
			h.SessionTarget.SessionName != admission.Target.SessionName) {
			return admissionError("session target contradicts the admitted target")
		}
	default:
		return admissionError("unknown attachment admission")
	}
	return nil
}

// validateLocalAdmissionCwd bounds the client-supplied working directory of a
// local admitted attachment. It stays a client data field rather than a proof:
// an absent value falls back to the daemon's existing home resolution, and a
// present value must be a bounded, valid, absolute path so it can never name a
// relative or malformed location.
func validateLocalAdmissionCwd(cwd string) error {
	if cwd == "" {
		return nil
	}
	if len(cwd) > admissionCwdMaxBytes {
		return admissionError("client working directory is too long")
	}
	if !utf8.ValidString(cwd) || strings.ContainsRune(cwd, 0) {
		return admissionError("client working directory is invalid")
	}
	if !filepath.IsAbs(cwd) {
		return admissionError("client working directory is not absolute")
	}
	return nil
}

// validateResumeAdmittedTarget refuses an admitted exact resume whose target
// does not name the exact session lifecycle the resume token owns, before any
// claim, transport replacement, or ownership mutation. An unknown or
// still-parking credential reports no owned session and returns nil, so the
// existing fail-closed resume path keeps its own semantics for that race.
func (d *Daemon) validateResumeAdmittedTarget(token uint64, target protocol.ExactSessionTarget) error {
	if token == 0 {
		return nil
	}
	sess := d.resumeTokenOwnedSession(token)
	if sess == nil {
		return nil
	}
	sess.mu.Lock()
	name, incarnation := sess.name, sess.incarnation
	sess.mu.Unlock()
	if name != target.SessionName || incarnation != target.LifecycleID {
		return admissionError("resume target contradicts the admitted session")
	}
	return nil
}

// resumeTokenOwnedSession resolves the exact session a resume credential
// currently owns: the parked entry, an in-flight parking entry, or the live
// attachment holding the token. It takes d.mu then a session lock, matching the
// daemon's existing lock order, and returns nil when no session is owned.
func (d *Daemon) resumeTokenOwnedSession(token uint64) *session {
	d.mu.Lock()
	defer d.mu.Unlock()
	if parked := d.parked[token]; parked != nil {
		return parked.sess
	}
	if pending := d.parking[token]; pending != nil {
		return pending.sess
	}
	for _, sess := range d.sessions {
		if sess == nil {
			continue
		}
		sess.mu.Lock()
		for candidate := range sess.attachments {
			if candidate.attachmentActivity() == attachmentActive && candidate.resumeCapable && candidate.resumeToken == token {
				sess.mu.Unlock()
				return sess
			}
		}
		sess.mu.Unlock()
	}
	return nil
}
