package domain

// Failing reports an availability that cannot serve sessions right now.
func (a RemoteAvailability) Failing() bool {
	switch a {
	case RemoteAvailabilityUnreachable, RemoteAvailabilityAuthFailed,
		RemoteAvailabilityInvalidResponse, RemoteAvailabilityNoDaemon,
		RemoteAvailabilityIncompatible:
		return true
	default:
		return false
	}
}

// RemoteHealth is the notice-relevant state of one observed remote host.
type RemoteHealth struct {
	Availability    RemoteAvailability
	VersionMismatch bool
	// Episode increments on each new failure episode.
	Episode uint64
}

// Failing reports a host that cannot serve sessions right now.
func (h RemoteHealth) Failing() bool {
	return h.VersionMismatch || h.Availability.Failing()
}

func (h RemoteHealth) healthy() bool {
	return h.Availability == RemoteAvailabilityReachable && !h.VersionMismatch
}

// settled reports a state worth remembering: in-between states (unknown,
// checking) keep the last settled one so a later recovery is still seen.
func (h RemoteHealth) settled() bool {
	return h.Failing() || h.Availability == RemoteAvailabilityReachable
}

// RemoteHealthEvent is what one host transition is worth telling the user.
type RemoteHealthEvent uint8

const (
	RemoteHealthSilent RemoteHealthEvent = iota
	// RemoteHealthFailed is a new failure episode or a new failure class.
	RemoteHealthFailed
	// RemoteHealthRecovered is a host serving again after a real failure.
	RemoteHealthRecovered
)

// RemoteHealthTransition judges one host observation against the last
// remembered state (seen is false when there is none). It returns the state
// to remember and the event worth one notice. Ordinary progress (checking,
// first sighting of a healthy host) is silent. A host whose daemon just
// started after "no daemon" was never disconnected, so it is silent too.
func RemoteHealthTransition(prev RemoteHealth, seen bool, cur RemoteHealth) (RemoteHealth, RemoteHealthEvent) {
	next := cur
	if seen && !cur.settled() {
		next = prev
	}
	switch {
	case cur.Failing():
		if seen && prev.Failing() && prev.Episode == cur.Episode && prev.Availability == cur.Availability {
			return next, RemoteHealthSilent
		}
		return next, RemoteHealthFailed
	case seen && prev.Failing() && prev.Availability != RemoteAvailabilityNoDaemon && cur.healthy():
		return next, RemoteHealthRecovered
	default:
		return next, RemoteHealthSilent
	}
}
