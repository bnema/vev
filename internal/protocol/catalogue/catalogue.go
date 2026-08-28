// Package catalogue owns the independently versioned remote discovery schema.
package catalogue

import (
	"errors"
	"fmt"
	"math"
	"slices"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/protocol"
)

const (
	// RemoteCatalogSchemaVersion is independent from the binary session protocol.
	RemoteCatalogSchemaVersion uint16 = 3

	RemoteCatalogMaxHosts        = 64
	RemoteCatalogMaxSessions     = 256
	RemoteCatalogMaxTabsPerSess  = 128
	RemoteCatalogMaxNameBytes    = 256
	RemoteCatalogMaxDetailBytes  = 512
	RemoteCatalogMaxIDBytes      = 128
	RemoteCatalogMaxCatalogBytes = 1 << 20
)

// RemoteCatalogCacheEntry is one immutable exact-schema host snapshot.
type RemoteCatalogCacheEntry struct {
	Host      string
	FetchedAt time.Time
	Sessions  []RemoteCatalogSession
}

// RemoteCatalogTab is one bounded, untrusted tab presentation hint.
type RemoteCatalogTab struct {
	ID        string `json:"id"`
	Index     uint16 `json:"index"`
	Name      string `json:"name"`
	Detail    string `json:"detail,omitempty"`
	Attention bool   `json:"attention,omitempty"`
}

// RemoteCatalogSessionState is the JSON catalogue lifecycle contract.
type RemoteCatalogSessionState string

const (
	RemoteCatalogSessionUp     RemoteCatalogSessionState = "up"
	RemoteCatalogSessionDown   RemoteCatalogSessionState = "down"
	RemoteCatalogSessionBroken RemoteCatalogSessionState = "broken"
)

func (s RemoteCatalogSessionState) Valid() bool {
	switch s {
	case RemoteCatalogSessionUp, RemoteCatalogSessionDown, RemoteCatalogSessionBroken:
		return true
	default:
		return false
	}
}

// RemoteCatalogSession is one exact-identity session in the remote catalogue.
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

func CatalogTabs(session RemoteCatalogSession) []RemoteCatalogTab { return slices.Clone(session.Tabs) }
func CatalogTabCount(session RemoteCatalogSession) int            { return len(session.Tabs) }

func SaturateUint16(n int) uint16 {
	if n <= 0 {
		return 0
	}
	if n >= 1<<16 {
		return math.MaxUint16
	}
	return uint16(n)
}

// RemoteCatalog is the versioned JSON envelope returned by remote-catalog.
type RemoteCatalog struct {
	ProtocolVersion uint16                 `json:"protocol_version"`
	SchemaVersion   uint16                 `json:"schema_version"`
	Sessions        []RemoteCatalogSession `json:"sessions"`
}

// RemoteCatalogVersionMismatchError reports an incompatible protocol or schema version.
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

// ValidateRemoteCatalog applies the exact current schema and all bounds before use.
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
		if !validText(session.Name) || !validText(string(session.State)) || !validText(session.Reason) {
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
		if session.Reason != "" && !validReason(session.Reason) {
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
		tabs := session.Tabs
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
				if !validText(tab.ID) || !validText(tab.Name) || !validText(tab.Detail) {
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

// ValidateRemoteCatalogCacheEntries validates the bounded exact-schema cache DTO.
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

func validText(value string) bool {
	if !utf8.ValidString(value) {
		return false
	}
	for _, r := range value {
		if unicode.IsControl(r) || unicode.Is(unicode.Bidi_Control, r) || r == '\u2028' || r == '\u2029' {
			return false
		}
	}
	return true
}

func validReason(reason string) bool {
	switch reason {
	case "", "refreshing", "catalog_stale", "host_unreachable", "version_mismatch", "session_down", "session_broken", "identity_changed", "not_found", "timeout", "malformed":
		return true
	default:
		return false
	}
}
