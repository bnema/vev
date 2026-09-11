package daemon

import (
	"crypto/sha256"
	"encoding/base64"
	"math"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/usecase/picker"
)

// This file owns picker destination eligibility and the structured lines the
// serving daemon publishes. The daemon knows lifecycles, tab identities, and
// remote activation; the presenting client only renders. Keys are opaque and
// resolve back to picker.Target values inside the daemon.

type pickerRemoteAvailability uint8

const (
	pickerRemoteNone pickerRemoteAvailability = iota
	pickerRemoteCached
	pickerRemoteFresh
	pickerRemoteStale
	pickerRemoteVersionMismatch
)

// pickerRemoteActivation is the picker action authorized by the current remote
// catalog snapshot. It is presentation state only; the structured remote
// target remains the exact route and is revalidated at commit time.
type pickerRemoteActivation uint8

const (
	pickerRemoteUnavailable pickerRemoteActivation = iota
	pickerRemoteAttach
	pickerRemoteRestart
)

// pickerSessionView is the daemon-private projection of one session or remote
// catalogue row used to compute destination eligibility.
type pickerSessionView struct {
	ID                domain.SessionID
	Section           string
	Incarnation       domain.IncarnationID
	Name              string
	TargetName        string
	Tabs              []pickerTabEntry
	Active            int
	Stopped           bool
	ExpectedCreatedAt *int64
	RemoteKey         *domain.RemoteSessionKey
	HideRemoteOrigin  bool
	// RemoteTarget is the structured route/lifecycle identity for picker rows.
	// It is never reconstructed from Name or a rendered label.
	RemoteTarget *domain.RemoteSessionTarget
	// RemoteHost marks remote host status rows that have no session key.
	RemoteHost         string
	RemoteAvailability pickerRemoteAvailability
	RemoteDetail       string
	RemoteReason       string
	RemoteActivation   pickerRemoteActivation
	// CannotAcceptMoves reports whether this session cannot receive a moved tab
	// or pane. False for ordinary local (and stopped) sessions; true for
	// restricted remote rows.
	CannotAcceptMoves bool
}

// pickerTabEntry is one tab row; Name is drawn emphasized, Detail muted.
type pickerTabEntry struct {
	TabID     domain.TabStableID // stable tab identity; independent of its current index
	Name      string             // tab display name
	RawName   string             // unformatted name used by exact remote selectors
	Detail    string             // " (paneTitle)" or "", drawn muted
	Attention bool               // draw the attention marker right after Name, before Detail
}

// pickerSourceFilter names the row the attachment currently displays.
type pickerSourceFilter struct {
	Session     domain.SessionID
	Incarnation domain.IncarnationID
	TabID       domain.TabStableID
	RemoteKey   *domain.RemoteSessionKey
}

// pickerLine is one published line plus its source-private target. cursor
// marks the row the attachment currently displays; defaultCursor marks the row
// a fresh interaction starts on when nothing matches the current view.
type pickerLine struct {
	line          protocol.PickerLine
	target        picker.Target
	cursor        bool
	defaultCursor bool
}

// pickerLineSet is one published source: ordered lines, the opaque key map,
// and the cursor hint.
type pickerLineSet struct {
	lines  []protocol.PickerLine
	keys   map[string]picker.Target
	cursor protocol.PickerCursor
}

// pickerLineSetFor projects one eligibility view set into structured lines in
// view order (never canonicalized: order is semantically significant).
func pickerLineSetFor(views []pickerSessionView, intent protocol.PickerIntent, source, current pickerSourceFilter) pickerLineSet {
	set := pickerLineSet{keys: make(map[string]picker.Target)}
	entries := make([]pickerLine, 0)
	for _, view := range views {
		if view.Section != "" {
			entries = append(entries, pickerLine{line: protocol.PickerLine{Kind: protocol.PickerLineSection, Label: view.Section, Dim: true}})
		}
		entries = append(entries, pickerSessionLines(view, intent, source, current)...)
	}
	for i, entry := range entries {
		set.lines = append(set.lines, entry.line)
		if entry.line.Key == "" {
			continue
		}
		if _, dup := set.keys[entry.line.Key]; !dup {
			set.keys[entry.line.Key] = entry.target
		}
		if entry.cursor {
			set.cursor = protocol.PickerCursor{Key: entry.line.Key, Index: i}
		}
	}
	if set.cursor.Key == "" {
		// Prefer the active tab of a session over the first published row.
		for i, entry := range entries {
			if !entry.defaultCursor {
				continue
			}
			set.cursor = protocol.PickerCursor{Key: entry.line.Key, Index: i}
			break
		}
	}
	if set.cursor.Key == "" && len(set.lines) > 0 {
		// Last resort: the first row the source authorised as a cursor
		// destination. A row kept for inspection or skipped by the source is
		// never hinted.
		for i, line := range set.lines {
			if !line.Focusable {
				continue
			}
			if target, ok := set.keys[line.Key]; ok && !target.Stopped {
				set.cursor = protocol.PickerCursor{Key: line.Key, Index: i}
				break
			}
		}
	}
	return set
}

// pickerRowKey derives the opaque key of one picker row. Local rows are keyed by
// incarnation and name, which stays readable for the common case. A remote row
// also carries its owning endpoint: the same machine can be registered under
// more than one target (a pinned alias and one learned by attach), and two
// endpoints may legitimately expose the same session lifecycle and name, so an
// unqualified key would collide and the whole snapshot would be rejected. The
// digest keeps the key unique per endpoint while staying inside the bounded key
// length.
func pickerRowKey(view pickerSessionView, name string) string {
	if view.RemoteKey == nil {
		return pickerClientKey(view.Incarnation, name)
	}
	digest := sha256.Sum256([]byte(view.RemoteKey.Host + "\x00" + string(view.Incarnation[:]) + "\x00" + name))
	return "remote:" + base64.RawURLEncoding.EncodeToString(digest[:16])
}

// pickerSessionLines is the sole owner of intent-specific destination
// eligibility. The daemon supplies canonical lifecycle/tab snapshots without
// prefiltering.
func pickerSessionLines(view pickerSessionView, intent protocol.PickerIntent, source, current pickerSourceFilter) []pickerLine {
	stopped := view.Stopped
	if view.RemoteTarget != nil {
		stopped = view.RemoteTarget.Stopped
	}
	if intent != protocol.PickerIntentNavigation && stopped {
		return nil
	}
	if intent == protocol.PickerIntentMoveTab && pickerSourceMatchesSession(source, view) {
		return nil
	}
	if intent == protocol.PickerIntentMoveTab && len(view.Tabs) == 0 && !view.CannotAcceptMoves {
		return nil
	}

	targetName := view.TargetName
	if intent == protocol.PickerIntentNavigation && !stopped && view.ExpectedCreatedAt == nil {
		targetName = ""
	} else if targetName == "" {
		targetName = view.Name
	}
	expectedCreatedAt, hasExpectedCreatedAt := int64Value(view.ExpectedCreatedAt)
	common := picker.Target{
		Session: view.ID, Incarnation: view.Incarnation, Name: targetName,
		RemoteKey: view.RemoteKey, RemoteTarget: view.RemoteTarget, RemoteHost: view.RemoteHost,
		UnavailableReason: view.RemoteReason, TabIndex: -1, Stopped: stopped,
		ExpectedCreatedAt: optionalInt64(expectedCreatedAt, hasExpectedCreatedAt),
	}
	headerLine := pickerLine{
		line: protocol.PickerLine{
			Key: pickerRowKey(view, common.Name), Kind: pickerLineKindForView(view),
			Label: view.Name, Detail: pickerStatusDetailFor(view, stopped), Stopped: stopped,
			Status: pickerStatusFor(view, stopped), StatusDetail: view.RemoteDetail, Ephemeral: pickerEphemeral(view),
		},
		target: common,
	}
	if view.RemoteKey != nil {
		headerLine.line.Label = view.RemoteKey.Name
		if !view.HideRemoteOrigin {
			display := view.RemoteKey.Display()
			headerLine.line.Detail = display[len(view.RemoteKey.Name):]
		}
	}
	header := headerLine
	headerSelectable, headerFocusable, headerDim := pickerHeaderEligibility(view, intent, stopped)
	header.line.Actions = pickerActionsFor(view, intent, headerSelectable)
	header.line.Focusable = headerFocusable
	header.line.Dim = headerDim
	if headerFocusable && pickerSelectionMatches(header, current, intent) {
		header.cursor = true
	}
	if !headerFocusable {
		header.line.Actions = 0
		header.target = picker.Target{}
	}

	lines := []pickerLine{header}
	for i, tab := range view.Tabs {
		if intent == protocol.PickerIntentMovePane && pickerSourceMatchesTab(source, view, tab) {
			continue
		}
		tabTarget := common
		tabTarget.TabID, tabTarget.TabIndex = tab.TabID, i
		// Each tab row carries its own structured remote target: a stopped row
		// restores the tab it names, a live row binds its stable tab ID.
		if resolved, ok := pickerTabRemoteTarget(view, tab, i); ok {
			tabTarget.RemoteTarget = &resolved
		}
		tabLine := pickerLine{
			line: protocol.PickerLine{
				Key:   pickerRowKey(view, common.Name+"#"+string(tab.TabID)),
				Kind:  protocol.PickerLineTab,
				Label: tab.Name, Detail: tab.Detail, Attention: tab.Attention,
				Stopped: stopped, StatusDetail: view.RemoteDetail, Ephemeral: pickerEphemeral(view),
			},
			target: tabTarget,
		}
		selectable, focusable, dim := pickerTabEligibility(view, intent, tab, i)
		tabLine.line.Actions = pickerActionsFor(view, intent, selectable)
		tabLine.line.Focusable = focusable
		tabLine.line.Dim = dim
		if !focusable {
			tabLine.line.Actions = 0
			tabLine.target = picker.Target{}
		}
		if focusable && pickerSelectionMatches(tabLine, current, intent) {
			tabLine.cursor = true
		}
		// A fresh navigation starts on the session's active tab, exactly like
		// the daemon-owned model did.
		if focusable && intent == protocol.PickerIntentNavigation && i == normalizedActiveTab(view) {
			tabLine.defaultCursor = true
		}
		lines = append(lines, tabLine)
	}
	if intent == protocol.PickerIntentMovePane && len(lines) == 1 && !view.CannotAcceptMoves {
		return nil
	}
	return lines
}

func pickerHeaderEligibility(view pickerSessionView, intent protocol.PickerIntent, stopped bool) (selectable, focusable, dim bool) {
	if intent == protocol.PickerIntentMoveTab {
		selectable, focusable = true, true
	} else if view.ID != "" && pickerViewHasTarget(view) {
		selectable, focusable = false, false
	}
	if intent == protocol.PickerIntentNavigation && stopped && len(view.Tabs) == 0 && pickerViewHasTarget(view) {
		selectable, focusable = true, true
	}
	if pickerViewIsRemote(view) {
		if view.RemoteTarget != nil {
			selectable = intent == protocol.PickerIntentNavigation && stopped && len(view.Tabs) == 0 && pickerRemoteActivatable(view) && view.RemoteTarget.Validate() == nil
			focusable = intent == protocol.PickerIntentNavigation && len(view.Tabs) == 0
		} else {
			selectable = false
			focusable = intent == protocol.PickerIntentNavigation && view.RemoteHost != ""
		}
		dim = view.RemoteActivation == pickerRemoteUnavailable
	}
	if intent != protocol.PickerIntentNavigation && view.CannotAcceptMoves {
		selectable, focusable, dim = false, false, true
	}
	return selectable, focusable, dim
}

func pickerTabEligibility(view pickerSessionView, intent protocol.PickerIntent, tab pickerTabEntry, index int) (selectable, focusable, dim bool) {
	selectable, focusable = intent != protocol.PickerIntentMoveTab, intent != protocol.PickerIntentMoveTab
	if pickerViewIsRemote(view) {
		if view.RemoteTarget == nil {
			focusable, selectable = false, false
		} else {
			resolved, resolvable := pickerTabRemoteTarget(view, tab, index)
			focusable = true
			selectable = resolvable && resolved.Validate() == nil && intent == protocol.PickerIntentNavigation && pickerRemoteActivatable(view)
		}
		dim = view.RemoteActivation == pickerRemoteUnavailable
	}
	if intent != protocol.PickerIntentNavigation && view.CannotAcceptMoves {
		selectable, focusable, dim = false, false, true
	}
	return selectable, focusable, dim
}

// pickerTabRemoteTarget resolves the per-tab copy of a remote session's exact
// route. A stopped session targets the tab the row names (stable ID when known,
// otherwise its ordinal and raw name); a live session binds the tab's stable
// ID. The second result is false when no exact target can be expressed.
func pickerTabRemoteTarget(view pickerSessionView, tab pickerTabEntry, index int) (domain.RemoteSessionTarget, bool) {
	if view.RemoteTarget == nil {
		return domain.RemoteSessionTarget{}, false
	}
	target := *view.RemoteTarget
	if target.Stopped {
		target.LiveTabID = ""
		if tab.TabID != "" {
			target.StoppedTab = domain.NewStableTabSelector(tab.TabID)
			return target, true
		}
		if target.StoppedTab == (domain.TabSelector{}) {
			return target, true
		}
		selector, ok := remoteStoppedOrdinalSelector(index, tab.RawName, len(view.Tabs))
		if !ok {
			return domain.RemoteSessionTarget{}, false
		}
		target.StoppedTab = selector
		return target, true
	}
	target.StoppedTab = domain.TabSelector{}
	target.LiveTabID = tab.TabID
	return target, true
}

// pickerActionsFor maps eligibility to the actions the source authorises. The
// daemon revalidates key and lifecycle at commit, so these bits are a
// presentation contract, never authority.
// normalizedActiveTab clamps a session's active tab index into its tab range.
func normalizedActiveTab(view pickerSessionView) int {
	if view.Active < 0 || view.Active >= len(view.Tabs) {
		return 0
	}
	return view.Active
}

func pickerActionsFor(view pickerSessionView, intent protocol.PickerIntent, selectable bool) protocol.PickerLineActions {
	if !selectable {
		return 0
	}
	if intent != protocol.PickerIntentNavigation {
		return protocol.PickerCanMove
	}
	actions := protocol.PickerCanNavigate
	if !pickerViewIsRemote(view) && pickerViewHasTarget(view) {
		actions |= protocol.PickerCanKill
	}
	return actions
}

func pickerViewHasTarget(view pickerSessionView) bool {
	return view.ID != "" && view.Name != ""
}

// pickerLineKindForView names one published header row's shape. A remote host
// status row carries no session key and no structured route: it is published
// for inspection, so it is a host row rather than a session row.
func pickerLineKindForView(view pickerSessionView) protocol.PickerLineKind {
	if pickerViewHostStatus(view) {
		return protocol.PickerLineHost
	}
	return protocol.PickerLineSession
}

// pickerViewHostStatus reports a catalogue host line: an endpoint with no
// session identity behind it.
func pickerViewHostStatus(view pickerSessionView) bool {
	return view.RemoteHost != "" && view.RemoteKey == nil && view.RemoteTarget == nil
}

func pickerViewIsRemote(view pickerSessionView) bool {
	return view.RemoteKey != nil || view.RemoteTarget != nil || view.RemoteHost != ""
}

func pickerEphemeral(view pickerSessionView) bool {
	return view.RemoteKey != nil || view.RemoteTarget != nil || view.RemoteHost != ""
}

func pickerRemoteActivatable(view pickerSessionView) bool {
	return view.RemoteActivation == pickerRemoteAttach || view.RemoteActivation == pickerRemoteRestart
}

func pickerStatusFor(view pickerSessionView, stopped bool) protocol.PickerLineStatus {
	remote := pickerViewIsRemote(view)
	if remote {
		switch view.RemoteReason {
		case domain.RemoteReasonHostUnreachable:
			return protocol.PickerLineStatusDown
		case domain.RemoteReasonVersionMismatch:
			return protocol.PickerLineStatusVersion
		case domain.RemoteReasonRefreshing, domain.RemoteReasonCatalogStale:
			return protocol.PickerLineStatusStale
		case domain.RemoteReasonMalformed, domain.RemoteReasonSessionBroken, domain.RemoteReasonIdentityChanged, domain.RemoteReasonAuthFailure:
			return protocol.PickerLineStatusError
		}
	}
	if stopped {
		return protocol.PickerLineStatusStopped
	}
	if !remote {
		return protocol.PickerLineStatusNone
	}
	if view.RemoteActivation == pickerRemoteAttach {
		return protocol.PickerLineStatusUp
	}
	return protocol.PickerLineStatusError
}

// pickerStatusDetailFor fills the muted detail of a header line: a stopped
// session reads "stopped" and a remote reason replaces it.
func pickerStatusDetailFor(view pickerSessionView, stopped bool) string {
	if view.RemoteReason != "" {
		return view.RemoteReason
	}
	if stopped {
		return "stopped"
	}
	return ""
}

func pickerSelectionMatches(line pickerLine, current pickerSourceFilter, intent protocol.PickerIntent) bool {
	target := line.target
	if target.Session != current.Session || (current.Incarnation != (domain.IncarnationID{}) && target.Incarnation != current.Incarnation) {
		return false
	}
	if target.RemoteKey != nil && line.line.Kind == protocol.PickerLineSession {
		return line.line.Actions != 0 && (current.RemoteKey == nil || *target.RemoteKey == *current.RemoteKey)
	}
	if intent == protocol.PickerIntentMoveTab {
		return line.line.Kind == protocol.PickerLineSession
	}
	// A stopped session with no retained tab metadata uses its selectable
	// header as the default restart target and matches on exact session
	// identity.
	if current.TabID == "" && target.Stopped {
		return line.line.Kind == protocol.PickerLineSession && line.line.Actions != 0
	}
	if line.line.Kind != protocol.PickerLineTab {
		return false
	}
	return target.TabID == current.TabID
}

func pickerSourceMatchesSession(source pickerSourceFilter, view pickerSessionView) bool {
	if source.Session != view.ID {
		return false
	}
	return source.Incarnation == (domain.IncarnationID{}) || source.Incarnation == view.Incarnation
}

func pickerSourceMatchesTab(source pickerSourceFilter, view pickerSessionView, tab pickerTabEntry) bool {
	return pickerSourceMatchesSession(source, view) && source.TabID == tab.TabID
}

func remoteStoppedOrdinalSelector(index int, rawName string, tabCount int) (domain.TabSelector, bool) {
	if tabCount > math.MaxUint16 || index < 0 || index >= tabCount {
		return domain.TabSelector{}, false
	}
	return domain.NewOrdinalTabSelector(uint16(index), rawName, uint16(tabCount)), true
}

func int64Value(value *int64) (int64, bool) {
	if value == nil {
		return 0, false
	}
	return *value, true
}

func optionalInt64(value int64, present bool) *int64 {
	if !present {
		return nil
	}
	copyValue := value
	return &copyValue
}
