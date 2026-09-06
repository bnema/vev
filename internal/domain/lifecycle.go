package domain

import (
	"errors"
	"fmt"
	"io"
	"strings"
	"unicode"
	"unicode/utf8"
)

// SessionLifecycleID is the opaque identity of one session lifecycle.
//
// IncarnationID predates the remote picker and is the durable implementation
// used by snapshots and the catalogue. This alias deliberately keeps one
// identity type rather than introducing a second, independently comparable
// UUID. A lifecycle ID changes only when a session is destroyed and recreated.
type SessionLifecycleID = IncarnationID

// NewSessionLifecycleID allocates a non-zero lifecycle identity.
func NewSessionLifecycleID(r io.Reader) (SessionLifecycleID, error) {
	return NewIncarnationID(r)
}

// TabSelectorKind identifies the exact selector form used for a stopped tab.
type TabSelectorKind uint8

const (
	TabSelectorByStableID TabSelectorKind = iota + 1
	TabSelectorByOrdinal
)

// TabSelector is an exact tab identity handoff. Stable IDs are preferred for
// persisted tabs. The ordinal form is a compatibility selector for catalogue
// records that predate stable stopped-tab IDs; raw name and expected count are
// required so a reordered or replaced tab cannot be selected accidentally.
type TabSelector struct {
	Kind          TabSelectorKind
	StableID      TabStableID
	Ordinal       uint16
	RawName       string
	ExpectedCount uint16
}

// NewStableTabSelector creates an exact stable-ID selector.
func NewStableTabSelector(id TabStableID) TabSelector {
	return TabSelector{Kind: TabSelectorByStableID, StableID: id}
}

// NewOrdinalTabSelector creates an exact legacy-compatible selector.
func NewOrdinalTabSelector(ordinal uint16, rawName string, expectedCount uint16) TabSelector {
	return TabSelector{Kind: TabSelectorByOrdinal, Ordinal: ordinal, RawName: rawName, ExpectedCount: expectedCount}
}

// Validate rejects selectors that could silently resolve a different tab.
func (s TabSelector) Validate() error {
	switch s.Kind {
	case TabSelectorByStableID:
		if s.Ordinal != 0 || s.RawName != "" || s.ExpectedCount != 0 || ValidateTabStableID(s.StableID) != nil {
			return errors.New("invalid stable tab selector")
		}
	case TabSelectorByOrdinal:
		if s.ExpectedCount == 0 || s.Ordinal >= s.ExpectedCount || s.StableID != "" {
			return errors.New("invalid ordinal tab selector")
		}
		if err := validateTabSelectorName(s.RawName); err != nil {
			return err
		}
	default:
		return errors.New("unknown tab selector kind")
	}
	return nil
}

// TabSelectorTab is the immutable metadata needed to resolve a selector.
type TabSelectorTab struct {
	ID   TabStableID
	Name string
}

// Resolve returns the selected tab index only when the complete selector still
// describes the supplied ordered tab metadata.
func (s TabSelector) Resolve(tabs []TabSelectorTab) (int, bool) {
	if s.Validate() != nil || (len(tabs) != int(s.ExpectedCount) && s.Kind == TabSelectorByOrdinal) {
		return 0, false
	}
	switch s.Kind {
	case TabSelectorByStableID:
		if len(tabs) == 0 {
			return 0, false
		}
		found := -1
		for i, tab := range tabs {
			if tab.ID != s.StableID {
				continue
			}
			if found != -1 {
				return 0, false
			}
			found = i
		}
		return found, found >= 0
	case TabSelectorByOrdinal:
		index := int(s.Ordinal)
		return index, index < len(tabs) && tabs[index].Name == s.RawName
	default:
		return 0, false
	}
}

// RemoteSessionTarget is the structured identity used by picker-based remote
// access. Endpoint and DisplayOrigin are intentionally separate values: the
// latter is presentation-only and is never parsed back into a route. Stopped
// records the selected down-row resume intent; it remains set while that exact
// lifecycle is created and attached as a live runtime.
type RemoteSessionTarget struct {
	Endpoint      string
	DisplayOrigin string
	LifecycleID   SessionLifecycleID
	SessionName   string
	LiveTabID     TabStableID
	StoppedTab    TabSelector
	Stopped       bool
}

// Validate validates route, presentation, lifecycle, and selector identity as
// independent contract fields.
func (t RemoteSessionTarget) Validate() error {
	if err := ValidateRemoteHostTarget(t.Endpoint); err != nil {
		return fmt.Errorf("remote route endpoint: %w", err)
	}
	if err := ValidateRemoteDisplayOrigin(t.DisplayOrigin); err != nil {
		return fmt.Errorf("remote display origin: %w", err)
	}
	if t.LifecycleID == (SessionLifecycleID{}) {
		return errors.New("remote lifecycle ID is zero")
	}
	if err := ValidateSessionName(t.SessionName); err != nil {
		return err
	}
	if t.Stopped {
		if t.LiveTabID != "" {
			return errors.New("down remote target cannot carry a live tab selector")
		}
		if t.StoppedTab != (TabSelector{}) && t.StoppedTab.Validate() != nil {
			return errors.New("down remote target has an invalid tab selector")
		}
		return nil
	}
	if err := ValidateTabStableID(t.LiveTabID); err != nil {
		return errors.New("up remote target requires a stable tab ID")
	}
	if t.StoppedTab != (TabSelector{}) {
		return errors.New("up remote target cannot carry a down selector")
	}
	return nil
}

// ResolveTab resolves the selected tab against authoritative metadata. A down
// record without retained tab metadata carries no selector and may choose only
// an unambiguous default tab: the empty durable record before creation or its
// sole fresh tab immediately afterward. It never guesses among multiple tabs.
func (t RemoteSessionTarget) ResolveTab(tabs []TabSelectorTab) (int, bool) {
	if t.Validate() != nil {
		return 0, false
	}
	if t.Stopped {
		if t.StoppedTab == (TabSelector{}) {
			return 0, len(tabs) <= 1
		}
		return t.StoppedTab.Resolve(tabs)
	}
	return NewStableTabSelector(t.LiveTabID).Resolve(tabs)
}

// ValidateRemoteDisplayOrigin validates presentation data without imposing
// SSH route grammar. Aliases, users, ports, and bracketed IPv6 remain opaque.
func ValidateRemoteDisplayOrigin(origin string) error {
	return validateRemoteText(origin, 256, "remote display origin")
}

// ValidateTabStableID validates the opaque stable tab identity used by
// picker and attachment handoffs.
func ValidateTabStableID(value TabStableID) error {
	return validateStableID(string(value), "tab")
}

// ValidatePaneStableID validates the session/tab-qualified focused pane identity.
func ValidatePaneStableID(value PaneStableID) error {
	return validateStableID(string(value), "pane")
}

func validateStableID(value, kind string) error {
	if value == "" || len(value) > 128 || !utf8.ValidString(value) {
		return fmt.Errorf("invalid stable %s ID", kind)
	}
	for _, r := range value {
		if unicode.IsControl(r) || unicode.IsSpace(r) {
			return fmt.Errorf("invalid stable %s ID", kind)
		}
	}
	return nil
}

func validateTabSelectorName(value string) error {
	if len(value) > 256 {
		return errors.New("tab name exceeds 256 bytes")
	}
	if !utf8.ValidString(value) {
		return errors.New("tab name is not valid UTF-8")
	}
	for _, r := range value {
		if unicode.IsControl(r) || r == '\n' || r == '\r' || r == '\x1b' {
			return errors.New("tab name contains control characters")
		}
	}
	return nil
}

func validateRemoteText(value string, maxBytes int, label string) error {
	if value == "" {
		return fmt.Errorf("%s is empty", label)
	}
	if len(value) > maxBytes {
		return fmt.Errorf("%s exceeds %d bytes", label, maxBytes)
	}
	if !utf8.ValidString(value) {
		return fmt.Errorf("%s is not valid UTF-8", label)
	}
	if strings.TrimSpace(value) != value {
		return fmt.Errorf("%s has surrounding whitespace", label)
	}
	for _, r := range value {
		if unicode.IsControl(r) || r == '\n' || r == '\r' || r == '\x1b' {
			return fmt.Errorf("%s contains control characters", label)
		}
	}
	return nil
}
