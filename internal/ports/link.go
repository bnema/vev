package ports

import "time"

// LinkState reports best-effort transport health without changing Transport.
// Implementations may distinguish authenticated path contact from useful
// stream progress when classifying the link.
type LinkState int

const (
	// LinkStateConnected means a complete record or meaningful acknowledgement
	// progress was observed recently.
	LinkStateConnected LinkState = iota
	// LinkStateDegraded means authenticated packets still reach the path, but
	// complete records and acknowledgement progress have stalled.
	LinkStateDegraded
	// LinkStateProbing means packet contact is silent or pending data has made no
	// round-trip progress long enough for path recovery to begin.
	LinkStateProbing
	// LinkStateOffline means authenticated packet contact has been absent long
	// enough for the client to resume over its fallback transport.
	LinkStateOffline
	// LinkStateDead means prolonged authenticated-packet silence has made the
	// transport terminal.
	LinkStateDead
)

func (s LinkState) String() string {
	switch s {
	case LinkStateConnected:
		return "connected"
	case LinkStateDegraded:
		return "degraded"
	case LinkStateProbing:
		return "probing"
	case LinkStateOffline:
		return "offline"
	case LinkStateDead:
		return "dead"
	default:
		return "unknown"
	}
}

// LinkEvent is emitted when a transport's observed link state changes.
type LinkEvent struct {
	State LinkState
	At    time.Time
	Err   error
}

// LinkStateReporter is an optional boundary-safe port implemented by transports
// that can report link degradation before Transport fails terminally.
//
// LinkEvents returns a best-effort event stream. Implementations must not block
// transport progress while publishing events; use a buffered channel and drop
// events when the consumer is slow. The channel is not required to close on
// transport shutdown, so consumers should also watch their own context or the
// underlying Transport result.
type LinkStateReporter interface {
	LinkState() LinkState
	LinkEvents() <-chan LinkEvent
}
