package domain

// RemoteHealth is the notice-relevant state of one observed remote host.
type RemoteHealth struct {
	// Key identifies the host across observations; it becomes the notice
	// scope so a recovery replaces the visible failure toast.
	Key string
	// Origin is the display label, never a raw endpoint.
	Origin          string
	Availability    RemoteAvailability
	Failure         RemoteFailureKind
	VersionMismatch bool
	// Episode increments on each new failure episode.
	Episode uint64
}

// Failing reports a host that cannot serve sessions right now.
func (h RemoteHealth) Failing() bool {
	if h.VersionMismatch {
		return true
	}
	switch h.Availability {
	case RemoteAvailabilityUnreachable, RemoteAvailabilityAuthFailed,
		RemoteAvailabilityInvalidResponse, RemoteAvailabilityNoDaemon,
		RemoteAvailabilityIncompatible:
		return true
	default:
		return false
	}
}

func (h RemoteHealth) healthy() bool {
	return h.Availability == RemoteAvailabilityReachable && !h.VersionMismatch
}

// RemoteHealthNotice decides whether a host transition is worth one notice:
// a new failure episode, or a recovery after a failure. Ordinary progress
// (checking, stale catalogue, first sighting of a healthy host) is silent.
// seen is false when prev is not known.
func RemoteHealthNotice(prev RemoteHealth, seen bool, cur RemoteHealth) (Notification, bool) {
	n := Notification{Code: NoticeRemoteObservation, Scope: cur.Key}
	switch {
	case cur.Failing():
		if seen && prev.Failing() && prev.Episode == cur.Episode {
			return Notification{}, false
		}
		n.Severity = NoticeError
		if cur.Availability == RemoteAvailabilityNoDaemon {
			n.Severity = NoticeWarn
		}
		n.Message = "Remote check failed: " + cur.Origin + " — " + remoteFailureText(cur)
		return n, true
	case seen && prev.Failing() && cur.healthy():
		n.Severity = NoticeInfo
		n.Message = "Remote host reconnected: " + cur.Origin
		return n, true
	default:
		return Notification{}, false
	}
}

func remoteFailureText(h RemoteHealth) string {
	if h.Availability == RemoteAvailabilityIncompatible || h.VersionMismatch {
		return "remote vev version is incompatible"
	}
	switch h.Failure {
	case RemoteFailureAuthentication:
		return "SSH authentication failed; verify non-interactive SSH access"
	case RemoteFailureTrust:
		return "SSH host verification failed; verify the host key policy"
	case RemoteFailureIncompatible:
		return "remote vev version is incompatible"
	case RemoteFailureInvalidResponse:
		return "remote catalog response is invalid"
	case RemoteFailureTimeout:
		return "SSH timed out"
	}
	switch h.Availability {
	case RemoteAvailabilityAuthFailed:
		return "SSH authentication failed; verify non-interactive SSH access"
	case RemoteAvailabilityInvalidResponse:
		return "remote catalog response is invalid"
	case RemoteAvailabilityNoDaemon:
		return "no vev daemon is running"
	default:
		return "SSH connection failed; verify SSH access"
	}
}
