package ports

import (
	"context"
	"fmt"
	"log/slog"
)

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

// RemoteCatalogClient fetches a versioned session catalog from a remote host.
type RemoteCatalogClient interface {
	List(ctx context.Context, target string) (RemoteCatalog, error)
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

// RemoteCatalogVersionMismatchError is returned when a remote catalog envelope
// reports a protocol_version that is not equal to ProtocolVersion.
type RemoteCatalogVersionMismatchError struct {
	Got  uint16
	Want uint16
}

func (e *RemoteCatalogVersionMismatchError) Error() string {
	return fmt.Sprintf("remote catalog: protocol version mismatch: got %d, want %d", e.Got, e.Want)
}
