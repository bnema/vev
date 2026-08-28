package ports

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"slices"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/protocol"
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
	// Remove deletes target from both pinned and learned lists atomically and
	// reports whether the target existed in either list.
	Remove(target string) (deleted bool, err error)
}

// RemoteHostLearner records the remote target after a successful attach.
// It deliberately has no arguments because app captures the validated target
// while constructing the client dependency.
type RemoteHostLearner interface {
	RememberRemoteHost() error
}

// RemoteCatalogClient fetches a versioned session catalog from a remote host.
type RemoteCatalogClient interface {
	List(ctx context.Context, target string) (RemoteCatalog, error)
}

// RemotePreviewClient fetches one bounded, exact-target viewport without
// attaching to or mutating the remote session.
type RemotePreviewClient interface {
	Preview(ctx context.Context, target domain.RemoteSessionTarget, width, height uint16) (protocol.RemotePreview, error)
}

const (
	// RemoteCatalogSchemaVersion is independent from the IPC protocol. A
	// catalogue can be rejected without changing the attachment wire layout.
	RemoteCatalogSchemaVersion uint16 = 3

	RemoteCatalogMaxHosts        = 64
	RemoteCatalogMaxSessions     = 256
	RemoteCatalogMaxTabsPerSess  = 128
	RemoteCatalogMaxNameBytes    = 256
	RemoteCatalogMaxDetailBytes  = 512
	RemoteCatalogMaxIDBytes      = 128
	RemoteCatalogMaxCatalogBytes = 1 << 20
)

// RemoteCatalogCacheEntry is one immutable exact-schema host snapshot persisted
// for immediate remote discovery at daemon startup. Cache writers strip dynamic
// Detail and Attention fields before persistence.
type RemoteCatalogCacheEntry struct {
	Host      string
	FetchedAt time.Time
	Sessions  []RemoteCatalogSession
}

// RemoteCatalogCache persists complete remote discovery snapshots independently
// from the remote host registry.
type RemoteCatalogCache interface {
	Load() ([]RemoteCatalogCacheEntry, error)
	Store([]RemoteCatalogCacheEntry) error
}

// RemoteCatalogTab is one bounded, untrusted tab presentation hint. Detail
// and Attention are deliberately omitted from the durable cache.
type RemoteCatalogTab struct {
	ID        string `json:"id"`
	Index     uint16 `json:"index"`
	Name      string `json:"name"`
	Detail    string `json:"detail,omitempty"`
	Attention bool   `json:"attention,omitempty"`
}

// RemoteCatalogSessionState is the JSON catalogue lifecycle contract. It is
// deliberately independent from SessionState, whose numeric values are part of
// the binary client/daemon protocol.
type RemoteCatalogSessionState string

const (
	RemoteCatalogSessionUp     RemoteCatalogSessionState = "up"
	RemoteCatalogSessionDown   RemoteCatalogSessionState = "down"
	RemoteCatalogSessionBroken RemoteCatalogSessionState = "broken"
)

// Valid reports whether the state is one of the bounded catalogue values.
func (s RemoteCatalogSessionState) Valid() bool {
	switch s {
	case RemoteCatalogSessionUp, RemoteCatalogSessionDown, RemoteCatalogSessionBroken:
		return true
	default:
		return false
	}
}

// RemoteCatalogSession is one exact-identity session in the remote discovery
// catalogue. Tabs is always an ordered typed snapshot; older count-only peers
// are rejected at the schema seam.
type RemoteCatalogSession struct {
	LifecycleID domain.SessionLifecycleID `json:"lifecycle_id"`
	Name        string                    `json:"name"`
	State       RemoteCatalogSessionState `json:"state"`
	Ephemeral   bool                      `json:"ephemeral"`
	Tabs        []RemoteCatalogTab        `json:"tabs"`
	Attached    bool                      `json:"attached"`
	LastUsedSeq uint64                    `json:"last_used_seq,omitempty"`
	ActiveTabID string                    `json:"active_tab_id,omitempty"`
	Reason      string                    `json:"reason,omitempty"`
}

// CatalogTabs returns an independent copy of the ordered tab snapshot.
func CatalogTabs(session RemoteCatalogSession) []RemoteCatalogTab {
	return slices.Clone(session.Tabs)
}

// CatalogTabCount returns the typed tab count.
func CatalogTabCount(session RemoteCatalogSession) int {
	return len(session.Tabs)
}

// SaturateUint16 clamps a catalogue tab count to the list protocol range.
func SaturateUint16(n int) uint16 {
	if n <= 0 {
		return 0
	}
	if n >= 1<<16 {
		return math.MaxUint16
	}
	return uint16(n)
}

// RemoteCatalog is the independently versioned JSON envelope returned by
// remote-catalog --json. Both protocol and catalogue schema versions are
// mandatory and must match exactly.
type RemoteCatalog struct {
	ProtocolVersion uint16                 `json:"protocol_version"`
	SchemaVersion   uint16                 `json:"schema_version"`
	Sessions        []RemoteCatalogSession `json:"sessions"`
}

// RemoteCatalogVersionMismatchError is returned when a remote catalog envelope
// reports an incompatible protocol or catalogue schema version.
type RemoteCatalogVersionMismatchError struct {
	Got  uint16
	Want uint16
	Kind string
}

func (e *RemoteCatalogVersionMismatchError) Error() string {
	kind := e.Kind
	if kind == "" {
		kind = "protocol"
	}
	return fmt.Sprintf("remote catalog: %s version mismatch: got %d, want %d", kind, e.Got, e.Want)
}

var (
	ErrInvalidRemoteCatalog       = errors.New("ports: invalid remote catalog")
	ErrRemoteCatalogTooLarge      = errors.New("ports: remote catalog exceeds size limit")
	ErrRemoteCatalogUnknownState  = errors.New("ports: remote catalog has unknown state")
	ErrRemoteCatalogInvalidReason = errors.New("ports: remote catalog has unknown reason")
)

// ValidateRemoteCatalog applies the exact current schema and all bounds before
// a catalogue is cached or rendered.
func ValidateRemoteCatalog(c RemoteCatalog) error {
	if c.ProtocolVersion != protocol.Version {
		return &RemoteCatalogVersionMismatchError{Got: c.ProtocolVersion, Want: protocol.Version, Kind: "protocol"}
	}
	if c.SchemaVersion != RemoteCatalogSchemaVersion {
		return &RemoteCatalogVersionMismatchError{Got: c.SchemaVersion, Want: RemoteCatalogSchemaVersion, Kind: "catalog"}
	}
	if len(c.Sessions) > RemoteCatalogMaxSessions {
		return fmt.Errorf("%w: too many sessions", ErrRemoteCatalogTooLarge)
	}
	seenLifecycles := make(map[domain.SessionLifecycleID]string, len(c.Sessions))
	seenNames := make(map[string]struct{}, len(c.Sessions))
	bytes := 0
	for _, session := range c.Sessions {
		if !validRemoteCatalogText(session.Name) || !validRemoteCatalogText(string(session.State)) || !validRemoteCatalogText(session.Reason) {
			return fmt.Errorf("%w: invalid session text", ErrInvalidRemoteCatalog)
		}
		if len(session.Name) > RemoteCatalogMaxNameBytes || len(session.Reason) > RemoteCatalogMaxDetailBytes {
			return fmt.Errorf("%w: session text too long", ErrRemoteCatalogTooLarge)
		}
		if err := domain.ValidateSessionName(session.Name); err != nil {
			return fmt.Errorf("%w: session name: %v", ErrInvalidRemoteCatalog, err)
		}
		if !session.State.Valid() {
			return fmt.Errorf("%w: %q", ErrRemoteCatalogUnknownState, session.State)
		}
		if session.ActiveTabID != "" {
			if err := domain.ValidateTabStableID(domain.TabStableID(session.ActiveTabID)); err != nil {
				return fmt.Errorf("%w: invalid active tab ID", ErrInvalidRemoteCatalog)
			}
		}
		if session.Reason != "" && !validRemoteCatalogReason(session.Reason) {
			return fmt.Errorf("%w: %q", ErrRemoteCatalogInvalidReason, session.Reason)
		}
		if session.LifecycleID == (domain.SessionLifecycleID{}) {
			return fmt.Errorf("%w: zero lifecycle ID", ErrInvalidRemoteCatalog)
		}
		if _, exists := seenNames[session.Name]; exists {
			return fmt.Errorf("%w: duplicate session name %q", ErrInvalidRemoteCatalog, session.Name)
		}
		seenNames[session.Name] = struct{}{}
		if prior, exists := seenLifecycles[session.LifecycleID]; exists {
			return fmt.Errorf("%w: lifecycle ID reused by %q and %q", ErrInvalidRemoteCatalog, prior, session.Name)
		}
		seenLifecycles[session.LifecycleID] = session.Name
		if session.Tabs == nil {
			return fmt.Errorf("%w: missing tab list", ErrInvalidRemoteCatalog)
		}
		tabs := CatalogTabs(session)
		if len(tabs) == 0 {
			if session.ActiveTabID != "" {
				return fmt.Errorf("%w: active tab is absent", ErrInvalidRemoteCatalog)
			}
		} else {
			if len(tabs) > RemoteCatalogMaxTabsPerSess {
				return fmt.Errorf("%w: too many tabs", ErrRemoteCatalogTooLarge)
			}
			tabIDs := make(map[string]struct{}, len(tabs))
			for i, tab := range tabs {
				if !validRemoteCatalogText(tab.ID) || !validRemoteCatalogText(tab.Name) || !validRemoteCatalogText(tab.Detail) {
					return fmt.Errorf("%w: invalid tab text", ErrInvalidRemoteCatalog)
				}
				if len(tab.ID) > RemoteCatalogMaxIDBytes || len(tab.Name) > RemoteCatalogMaxNameBytes || len(tab.Detail) > RemoteCatalogMaxDetailBytes {
					return fmt.Errorf("%w: tab text too long", ErrRemoteCatalogTooLarge)
				}
				if tab.Index != uint16(i) {
					return fmt.Errorf("%w: tab indexes are not ordered", ErrInvalidRemoteCatalog)
				}
				if session.State == RemoteCatalogSessionUp && tab.ID == "" {
					return fmt.Errorf("%w: up tab has zero ID", ErrInvalidRemoteCatalog)
				}
				if tab.ID != "" {
					if err := domain.ValidateTabStableID(domain.TabStableID(tab.ID)); err != nil {
						return fmt.Errorf("%w: invalid tab ID", ErrInvalidRemoteCatalog)
					}
					if _, duplicate := tabIDs[tab.ID]; duplicate {
						return fmt.Errorf("%w: duplicate tab ID", ErrInvalidRemoteCatalog)
					}
					tabIDs[tab.ID] = struct{}{}
				}
				bytes += len(tab.ID) + len(tab.Name) + len(tab.Detail)
			}
			if session.ActiveTabID != "" {
				if session.State != RemoteCatalogSessionUp {
					return fmt.Errorf("%w: down or broken session has an active tab", ErrInvalidRemoteCatalog)
				}
				if _, ok := tabIDs[session.ActiveTabID]; !ok {
					return fmt.Errorf("%w: active tab is absent", ErrInvalidRemoteCatalog)
				}
			}
		}
		bytes += len(session.Name) + len(session.State) + len(session.Reason)
	}
	if bytes > RemoteCatalogMaxCatalogBytes {
		return ErrRemoteCatalogTooLarge
	}
	return nil
}

// ValidateRemoteCatalogCacheEntries validates the bounded exact-schema cache
// DTO before it is loaded into picker state or written to disk.
func ValidateRemoteCatalogCacheEntries(entries []RemoteCatalogCacheEntry) error {
	if len(entries) > RemoteCatalogMaxHosts {
		return fmt.Errorf("%w: too many hosts", ErrRemoteCatalogTooLarge)
	}
	seenHosts := make(map[string]struct{}, len(entries))
	for _, entry := range entries {
		if err := domain.ValidateRemoteHostTarget(entry.Host); err != nil || len(entry.Host) > RemoteCatalogMaxNameBytes {
			return fmt.Errorf("%w: invalid host", ErrInvalidRemoteCatalog)
		}
		if entry.FetchedAt.IsZero() || entry.FetchedAt.UnixNano() <= 0 {
			return fmt.Errorf("%w: invalid fetched time", ErrInvalidRemoteCatalog)
		}
		if _, exists := seenHosts[entry.Host]; exists {
			return fmt.Errorf("%w: duplicate host", ErrInvalidRemoteCatalog)
		}
		seenHosts[entry.Host] = struct{}{}
		catalog := RemoteCatalog{ProtocolVersion: protocol.Version, SchemaVersion: RemoteCatalogSchemaVersion, Sessions: entry.Sessions}
		if err := ValidateRemoteCatalog(catalog); err != nil {
			return fmt.Errorf("%w: host %q: %v", ErrInvalidRemoteCatalog, entry.Host, err)
		}
	}
	return nil
}

func validRemoteCatalogText(value string) bool {
	if !utf8.ValidString(value) {
		return false
	}
	for _, r := range value {
		if unicode.IsControl(r) {
			return false
		}
	}
	return true
}

func validRemoteCatalogReason(reason string) bool {
	switch reason {
	case "", "refreshing", "catalog_stale", "host_unreachable", "version_mismatch", "session_down", "session_broken", "identity_changed", "not_found", "timeout", "malformed":
		return true
	default:
		return false
	}
}
