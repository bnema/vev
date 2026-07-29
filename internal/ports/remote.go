package ports

import "log/slog"

// RemoteTransportMode selects the transport used for remote attach after CLI parsing.
type RemoteTransportMode string

const (
	RemoteTransportUDP   RemoteTransportMode = "udp"
	RemoteTransportStdio RemoteTransportMode = "stdio"
)

// RemoteDialerFactory builds the transport-specific dialer for a remote target.
// Implementations live in adapters; app wiring consumes this interface.
type RemoteDialerFactory interface {
	DialerForRemote(target string, session string, mode RemoteTransportMode, log *slog.Logger) (Dialer, error)
}

// RemoteHostStore persists pinned and learned remote host targets in one
// versioned state file across processes.
type RemoteHostStore interface {
	// Hosts returns pinned hosts in stored order and learned hosts in lexical order.
	Hosts() (pinned, learned []string, err error)
	AddPinned(target string) error
	RemovePinned(target string) error
	Remember(target string) error
	Forget(target string) error
	// Remove deletes target from both pinned and learned lists atomically.
	Remove(target string) error
}

// RemoteCatalogSession is one live session in the remote discovery catalog.
// State is an explicit string contract (running|stopped|broken), not SessionState.
type RemoteCatalogSession struct {
	Name      string `json:"name"`
	State     string `json:"state"`
	Ephemeral bool   `json:"ephemeral"`
	Tabs      uint16 `json:"tabs"`
	Attached  bool   `json:"attached"`
}

// RemoteCatalog is the versioned JSON envelope returned by remote-catalog --json.
type RemoteCatalog struct {
	ProtocolVersion uint16                 `json:"protocol_version"`
	Sessions        []RemoteCatalogSession `json:"sessions"`
}
