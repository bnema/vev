package ports

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"slices"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/bnema/vev/internal/domain"
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
	Preview(ctx context.Context, target domain.RemoteSessionTarget, width, height uint16) (RemotePreview, error)
}

const (
	// RemoteCatalogSchemaVersion is independent from the IPC protocol. A
	// catalogue can be rejected without changing the attachment wire layout.
	RemoteCatalogSchemaVersion uint16 = 1

	RemoteCatalogMaxHosts        = 64
	RemoteCatalogMaxSessions     = 256
	RemoteCatalogMaxTabsPerSess  = 128
	RemoteCatalogMaxNameBytes    = 256
	RemoteCatalogMaxDetailBytes  = 512
	RemoteCatalogMaxIDBytes      = 128
	RemoteCatalogMaxCatalogBytes = 1 << 20
)

// RemoteCatalogCacheEntry is one immutable host catalog snapshot persisted for
// immediate remote discovery at daemon startup. The legacy-shaped Sessions
// field is retained as a source-compatible container, but cache writers strip
// dynamic Detail and Attention fields before persistence.
type RemoteCatalogCacheEntry struct {
	Host      string
	FetchedAt time.Time
	Sessions  []RemoteCatalogSession
}

// RemoteCatalogCacheTab is the durable subset of a tab catalogue entry.
type RemoteCatalogCacheTab struct {
	ID    string `json:"id,omitempty"`
	Index uint16 `json:"index"`
	Name  string `json:"name"`
}

// RemoteCatalogCacheSession is the durable subset of a remote session entry.
type RemoteCatalogCacheSession struct {
	LifecycleID domain.SessionLifecycleID `json:"lifecycle_id,omitempty"`
	Name        string                    `json:"name"`
	State       string                    `json:"state"`
	Ephemeral   bool                      `json:"ephemeral"`
	Attached    bool                      `json:"attached"`
	LastUsedSeq uint64                    `json:"last_used_seq,omitempty"`
	ActiveTabID string                    `json:"active_tab_id,omitempty"`
	Tabs        []RemoteCatalogCacheTab   `json:"tabs"`
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

// RemoteCatalogSession is one session in the remote discovery catalog. State
// is an explicit string contract (running|stopped|broken), not SessionState.
//
// Tabs is interface-typed only to keep old v0.x callers that supplied a count
// source-compatible while the versioned schema carries []RemoteCatalogTab.
// New code must use CatalogTabs and never infer routing identity from a count.
type RemoteCatalogSession struct {
	LifecycleID domain.SessionLifecycleID `json:"lifecycle_id,omitempty"`
	Name        string                    `json:"name"`
	State       string                    `json:"state"`
	Ephemeral   bool                      `json:"ephemeral"`
	Tabs        any                       `json:"tabs"`
	Attached    bool                      `json:"attached"`
	LastUsedSeq uint64                    `json:"last_used_seq,omitempty"`
	ActiveTabID string                    `json:"active_tab_id,omitempty"`
	Reason      string                    `json:"reason,omitempty"`
}

// MarshalJSON omits a zero lifecycle ID for legacy count-only catalogues.
// Encoding/json does not apply omitempty to a zero array that implements
// encoding.TextMarshaler, so the pointer is made explicit here.
func (s RemoteCatalogSession) MarshalJSON() ([]byte, error) {
	type envelope struct {
		LifecycleID *domain.SessionLifecycleID `json:"lifecycle_id,omitempty"`
		Name        string                     `json:"name"`
		State       string                     `json:"state"`
		Ephemeral   bool                       `json:"ephemeral"`
		Tabs        any                        `json:"tabs"`
		Attached    bool                       `json:"attached"`
		LastUsedSeq uint64                     `json:"last_used_seq,omitempty"`
		ActiveTabID string                     `json:"active_tab_id,omitempty"`
		Reason      string                     `json:"reason,omitempty"`
	}
	var lifecycle *domain.SessionLifecycleID
	if s.LifecycleID != (domain.SessionLifecycleID{}) {
		id := s.LifecycleID
		lifecycle = &id
	}
	return json.Marshal(envelope{LifecycleID: lifecycle, Name: s.Name, State: s.State, Ephemeral: s.Ephemeral, Tabs: s.Tabs, Attached: s.Attached, LastUsedSeq: s.LastUsedSeq, ActiveTabID: s.ActiveTabID, Reason: s.Reason})
}

// UnmarshalJSON preserves the legacy numeric tab-count representation while
// decoding the current array representation into concrete tab values.
func (s *RemoteCatalogSession) UnmarshalJSON(data []byte) error {
	type envelope struct {
		LifecycleID domain.SessionLifecycleID `json:"lifecycle_id"`
		Name        string                    `json:"name"`
		State       string                    `json:"state"`
		Ephemeral   bool                      `json:"ephemeral"`
		Tabs        json.RawMessage           `json:"tabs"`
		Attached    bool                      `json:"attached"`
		LastUsedSeq uint64                    `json:"last_used_seq"`
		ActiveTabID string                    `json:"active_tab_id"`
		Reason      string                    `json:"reason"`
	}
	var raw envelope
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	*s = RemoteCatalogSession{LifecycleID: raw.LifecycleID, Name: raw.Name, State: raw.State, Ephemeral: raw.Ephemeral, Attached: raw.Attached, LastUsedSeq: raw.LastUsedSeq, ActiveTabID: raw.ActiveTabID, Reason: raw.Reason}
	if len(raw.Tabs) == 0 || string(raw.Tabs) == "null" {
		s.Tabs = 0
		return nil
	}
	var tabs []RemoteCatalogTab
	if err := json.Unmarshal(raw.Tabs, &tabs); err == nil {
		s.Tabs = tabs
		return nil
	}
	var count uint16
	if err := json.Unmarshal(raw.Tabs, &count); err != nil {
		return err
	}
	if count == math.MaxUint16 {
		s.Tabs = count
	} else {
		s.Tabs = int(count)
	}
	return nil
}

// CatalogTabs returns a copied typed tab list. Legacy count-only entries have
// no stable tab identity and therefore return nil.
func CatalogTabs(session RemoteCatalogSession) []RemoteCatalogTab {
	tabs, ok := session.Tabs.([]RemoteCatalogTab)
	if !ok {
		return nil
	}
	return slices.Clone(tabs)
}

// CatalogTabCount returns the count represented by either schema form.
func CatalogTabCount(session RemoteCatalogSession) int {
	switch tabs := session.Tabs.(type) {
	case []RemoteCatalogTab:
		return len(tabs)
	case uint16:
		return int(tabs)
	case uint32:
		return int(tabs)
	case int:
		return tabs
	case float64:
		return int(tabs)
	default:
		return 0
	}
}

// SaturateUint16 clamps n to the uint16 range for legacy command output only.
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
// remote-catalog --json. ProtocolVersion remains for legacy peers; new peers
// must also provide SchemaVersion.
type RemoteCatalog struct {
	ProtocolVersion uint16                 `json:"protocol_version"`
	SchemaVersion   uint16                 `json:"schema_version,omitempty"`
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

// ValidateRemoteCatalog applies all limits before a catalogue is cached or
// rendered. It is deliberately strict for the current schema; zero schema is
// the read-only compatibility form used by pre-parity v0.x peers.
func ValidateRemoteCatalog(c RemoteCatalog) error {
	if c.ProtocolVersion != ProtocolVersion {
		return &RemoteCatalogVersionMismatchError{Got: c.ProtocolVersion, Want: ProtocolVersion, Kind: "protocol"}
	}
	if c.SchemaVersion != 0 && c.SchemaVersion != RemoteCatalogSchemaVersion {
		return &RemoteCatalogVersionMismatchError{Got: c.SchemaVersion, Want: RemoteCatalogSchemaVersion, Kind: "catalog"}
	}
	if len(c.Sessions) > RemoteCatalogMaxSessions {
		return fmt.Errorf("%w: too many sessions", ErrRemoteCatalogTooLarge)
	}
	seen := make(map[domain.SessionLifecycleID]string, len(c.Sessions))
	bytes := 0
	for _, session := range c.Sessions {
		if !validRemoteCatalogText(session.Name) || !validRemoteCatalogText(session.State) || !validRemoteCatalogText(session.Reason) {
			return fmt.Errorf("%w: invalid session text", ErrInvalidRemoteCatalog)
		}
		if len(session.Name) > RemoteCatalogMaxNameBytes || len(session.Reason) > RemoteCatalogMaxDetailBytes {
			return fmt.Errorf("%w: session text too long", ErrRemoteCatalogTooLarge)
		}
		if err := domain.ValidateSessionName(session.Name); err != nil {
			return fmt.Errorf("%w: session name: %v", ErrInvalidRemoteCatalog, err)
		}
		if !validRemoteCatalogState(session.State) {
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
		if c.SchemaVersion != 0 {
			if session.LifecycleID == (domain.SessionLifecycleID{}) {
				return fmt.Errorf("%w: zero lifecycle ID", ErrInvalidRemoteCatalog)
			}
			if prior, exists := seen[session.LifecycleID]; exists {
				return fmt.Errorf("%w: lifecycle ID reused by %q and %q", ErrInvalidRemoteCatalog, prior, session.Name)
			}
			seen[session.LifecycleID] = session.Name
		}
		tabs := CatalogTabs(session)
		if len(tabs) == 0 {
			if c.SchemaVersion != 0 {
				if session.ActiveTabID != "" {
					return fmt.Errorf("%w: active tab is absent", ErrInvalidRemoteCatalog)
				}
				if session.State == "running" && session.Tabs == nil {
					return fmt.Errorf("%w: missing tab list", ErrInvalidRemoteCatalog)
				}
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
				if session.State == "running" && tab.ID == "" {
					return fmt.Errorf("%w: running tab has zero ID", ErrInvalidRemoteCatalog)
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
				if session.State != "running" {
					return fmt.Errorf("%w: stopped or broken session has an active tab", ErrInvalidRemoteCatalog)
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

// ValidateRemoteCatalogCacheEntries validates the bounded cache DTO before it
// is loaded into picker state or written to disk. A cache may contain legacy
// count-only sessions, but any complete typed snapshot must carry lifecycle
// identity and the same tab invariants as a live catalog.
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
		complete := false
		for _, session := range entry.Sessions {
			if session.LifecycleID != (domain.SessionLifecycleID{}) || CatalogTabs(session) != nil {
				complete = true
				break
			}
		}
		schema := uint16(0)
		if complete {
			schema = RemoteCatalogSchemaVersion
		}
		catalog := RemoteCatalog{ProtocolVersion: ProtocolVersion, SchemaVersion: schema, Sessions: entry.Sessions}
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

func validRemoteCatalogState(state string) bool {
	switch state {
	case "running", "stopped", "broken":
		return true
	default:
		return false
	}
}

func validRemoteCatalogReason(reason string) bool {
	switch reason {
	case "", "refreshing", "catalog_stale", "host_unreachable", "version_mismatch", "session_stopped", "session_broken", "identity_changed", "not_found", "timeout", "malformed":
		return true
	default:
		return false
	}
}
