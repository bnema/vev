package protocol

import "fmt"

// PickerServingSourceID names the source the serving daemon owns: every
// attachment publishes its own rows under this one source identity.
const (
	PickerServingSourceID = "serving"
	PickerHomeSourceID    = "home"
)

// Client-picker interaction bounds. A source publishes at most
// PickerInteractionMaxLines structured lines; the complete encoded snapshot
// stays below PickerInteractionMaxEncodedBytes, the same 4 MiB export budget
// shared with navigation inventory.
const (
	PickerInteractionMaxLines        = 4096
	PickerInteractionMaxDisplayBytes = 256
	PickerInteractionMaxEncodedBytes = 4 << 20
	PickerInteractionMaxSourceBytes  = 128
	PickerInteractionMaxTitleBytes   = 256
)

// PickerIntent names which destinations a picker offers and which mutation a
// selection performs. The daemon owns the intent: it captures move sources and
// decides which rows are eligible.
type PickerIntent uint8

const (
	PickerIntentNavigation PickerIntent = iota + 1
	PickerIntentMovePane
	PickerIntentMoveTab
)

// PickerAction names the mutation one selection requests.
type PickerAction uint8

const (
	PickerActionNavigate PickerAction = iota + 1
	PickerActionMove
	PickerActionKill
)

// PickerLineKind names the row shape. Section rows are group headers and
// carry no key; presentation-only rows (host status) stay focusable but not
// selectable.
type PickerLineKind uint8

const (
	PickerLineSection PickerLineKind = iota + 1
	PickerLineSession
	PickerLineTab
	PickerLineHost
)

// PickerLineStatus is the bounded state badge drawn on a row.
type PickerLineStatus uint8

const (
	PickerLineStatusNone PickerLineStatus = iota
	PickerLineStatusUp
	PickerLineStatusStopped
	PickerLineStatusDown
	PickerLineStatusStale
	PickerLineStatusVersion
	PickerLineStatusError
)

// PickerLineActions is the set of actions a row admits. The presenting client
// offers only what the owning source authorised; the source revalidates the
// action and the row lifecycle at commit.
type PickerLineActions uint8

const (
	PickerCanNavigate PickerLineActions = 1 << iota
	PickerCanMove
	PickerCanKill
)

// PickerSourceStatus is the per-source outcome. A failed source never
// invalidates another source's lines.
type PickerSourceStatus uint8

const (
	PickerSourceOK PickerSourceStatus = iota + 1
	PickerSourceUnavailable
	PickerSourceTooLarge
)

// PickerLine is one structured row. Key is opaque and source-assigned; it
// resolves to a target source-side only. Display strings carry presentation
// facts, never endpoints, credentials, environment, CWD, or pane content.
// Lines are published in the source's own order (recency for local sessions)
// and Ephemeral marks sessions created for one attach, which is what the
// presenting client needs to offer its local grouped order.
type PickerLine struct {
	Key          string
	Kind         PickerLineKind
	Label        string
	Detail       string
	Status       PickerLineStatus
	StatusDetail string
	Attention    bool
	Stopped      bool
	Dim          bool
	// Focusable reports that the source authorises this row as a cursor
	// destination for this interaction, independently of whether it admits an
	// action. A Focusable row with no Actions is deliberately inspectable and
	// never committable; a row that is neither Focusable nor actionable is
	// rendered but skipped by cursor navigation.
	Focusable bool
	Actions   PickerLineActions
	Ephemeral bool
}

// Selectable reports whether the row admits at least one action.
func (line PickerLine) Selectable() bool { return line.Actions != 0 }

// PickerCursor restores the selected row by opaque key with its list index.
type PickerCursor struct {
	Key   string
	Index int
}

// PickerOffer opens one interaction. The daemon sends it for intents it
// initiates (a session-picker command or a move request); it carries the
// acquisition barrier the client must display before owning the terminal and
// the opaque identity of the captured move source. RequestID echoes the
// PickerBegin that asked for the interaction, and is zero for daemon-driven
// opens.
type PickerOffer struct {
	InteractionID uint64
	RequestID     uint64
	Intent        PickerIntent
	MoveSourceKey string
	BarrierEpoch  uint64
	BarrierState  uint64
	SizeEpoch     uint64
	Title         string
}

// PickerBegin asks the serving daemon to open one interaction. Startup
// pickers and client-driven navigation requests use it; a daemon that already
// executed a picker command sends PickerOffer directly.
type PickerBegin struct {
	RequestID uint64
	Intent    PickerIntent
}

// PickerControlOperation selects a stateless request to the client's stable
// local picker authority. It never creates an attachment.
type PickerControlOperation uint8

const (
	PickerControlSnapshot PickerControlOperation = iota + 1
	PickerControlResolve
	PickerControlObserve
)

// PickerControlMaxTargets bounds one observation request. A periodic Runner
// polls a small fixed working set of exact targets per query; a full directory
// belongs to navigation inventory, not to this stateless probe.
const PickerControlMaxTargets = 64

// PickerRoutePresence reports whether an exact target still resolves to one
// live route. Unknown is a first-class answer: an unanswerable probe must
// never be reported as Absent, because the caller would otherwise act on a
// false negative.
type PickerRoutePresence uint8

const (
	PickerRoutePresent PickerRoutePresence = iota + 1
	PickerRouteAbsent
	PickerRouteUnknown
)

// PickerRouteObservation is one exact-target answer. Attention is meaningful
// only for Present targets: Absent and Unknown observations always carry
// false, so a stale flag can never be mistaken for live attention.
type PickerRouteObservation struct {
	Target    ExactSessionTarget
	Presence  PickerRoutePresence
	Attention bool
}

// PickerControlRequest queries, resolves, or observes the navigation picker
// owned by the client's local daemon. Version stays first for strict
// pre-handshake peeking. The operation payloads are exclusive: snapshot and
// resolve carry source/key identity, observe carries one bounded exact-target
// list, and every field an operation does not own stays zero.
type PickerControlRequest struct {
	Version        uint16
	RequestID      uint64
	Operation      PickerControlOperation
	SourceRevision uint64
	SourceID       string
	Key            string
	Targets        []ExactSessionTarget
}

// PickerControlResponse returns one complete authoritative snapshot, one
// revalidated attach target, or the observation list for an observe request.
// The operation payloads are exclusive.
type PickerControlResponse struct {
	RequestID    uint64
	Operation    PickerControlOperation
	Status       PickerSourceStatus
	Snapshot     *PickerSnapshot
	Resolved     *AttachTarget
	Observations []PickerRouteObservation
}

// PickerProjection is one complete daemon-authorised presentation. Recent and
// Grouped contain the same destination keys and actions but may differ in
// order, sections, and display labels.
type PickerProjection struct {
	Lines  []PickerLine
	Cursor PickerCursor
}

// PickerSnapshot atomically publishes both presentation projections of one
// source at one revision. The client chooses a projection but never derives
// grouping or ordering policy from row metadata.
type PickerSnapshot struct {
	InteractionID  uint64
	SourceID       string
	SourceRevision uint64
	Status         PickerSourceStatus
	StatusDetail   string
	// Lines/Cursor are retained as construction aliases inside this v0 branch;
	// normalization copies them into both projections before encoding.
	Lines   []PickerLine
	Cursor  PickerCursor
	Recent  PickerProjection
	Grouped PickerProjection
}

// PickerSelection commits the opaque row key displayed at SourceRevision.
// CauseActionID correlates ui-driver input like NavigationInventorySelection.
// RequestID echoes the PickerBegin that opened a client-initiated
// interaction, and is zero otherwise.
type PickerSelection struct {
	CauseActionID  uint64
	RequestID      uint64
	InteractionID  uint64
	SourceID       string
	SourceRevision uint64
	Key            string
	Action         PickerAction
}

// PickerResult reports a completed mutation that changed source state, so the
// client can refresh without guessing. Rejections use PickerFailure.
type PickerResult struct {
	CauseActionID uint64
	RequestID     uint64
	InteractionID uint64
	SourceID      string
	Key           string
	Action        PickerAction
}

// PickerFailureCode classifies client-picker selection failures.
type PickerFailureCode uint8

const (
	PickerStaleRevision PickerFailureCode = iota + 1
	PickerUnknownKey
	PickerRetiredTarget
	PickerNavigationFailed
	PickerUnknownSource
	PickerActionFailed
)

// PickerFailure reports a rejected selection with a bounded code.
type PickerFailure struct {
	CauseActionID uint64
	RequestID     uint64
	InteractionID uint64
	SourceID      string
	Key           string
	Action        PickerAction
	Code          PickerFailureCode
}

// PickerClose asks the serving daemon to retire one interaction. It is
// idempotent: a close for an unknown or already retired interaction is a
// no-op, never an error.
type PickerClose struct {
	InteractionID uint64
	RequestID     uint64
}

// PickerClosed confirms that the serving daemon retired one interaction. The
// barrier names the daemon output boundary the client must display before it
// releases the terminal.
type PickerClosed struct {
	InteractionID uint64
	BarrierEpoch  uint64
	BarrierState  uint64
}

func validPickerControlOperation(operation PickerControlOperation) bool {
	return operation == PickerControlSnapshot || operation == PickerControlResolve || operation == PickerControlObserve
}

func validPickerRoutePresence(presence PickerRoutePresence) bool {
	switch presence {
	case PickerRoutePresent, PickerRouteAbsent, PickerRouteUnknown:
		return true
	default:
		return false
	}
}

// ValidatePickerControlRequest enforces explicit, exclusive operation payloads.
// Snapshot and resolve must carry no targets; observe must carry 1..
// PickerControlMaxTargets distinct valid exact targets and no source/key
// identity, so a query can never smuggle a second payload shape.
func ValidatePickerControlRequest(request PickerControlRequest) error {
	if request.Version == 0 || request.RequestID == 0 || !validPickerControlOperation(request.Operation) {
		return ErrInvalidNavigation
	}
	switch request.Operation {
	case PickerControlSnapshot:
		if request.SourceRevision != 0 || request.SourceID != "" || request.Key != "" || len(request.Targets) != 0 {
			return ErrInvalidNavigation
		}
		return nil
	case PickerControlResolve:
		if len(request.Targets) != 0 {
			return ErrInvalidNavigation
		}
		if request.SourceRevision == 0 || !validPickerSourceID(request.SourceID) || !validPickerKey(request.Key) {
			return ErrInvalidNavigation
		}
		return nil
	default:
		if request.SourceRevision != 0 || request.SourceID != "" || request.Key != "" {
			return ErrInvalidNavigation
		}
		if len(request.Targets) == 0 || len(request.Targets) > PickerControlMaxTargets {
			return ErrInvalidNavigation
		}
		seen := make(map[ExactSessionTarget]struct{}, len(request.Targets))
		for _, target := range request.Targets {
			if err := target.Validate(); err != nil {
				return ErrInvalidNavigation
			}
			if _, dup := seen[target]; dup {
				return ErrInvalidNavigation
			}
			seen[target] = struct{}{}
		}
		return nil
	}
}

// validatePickerRouteObservations requires one bounded, distinct observation
// per requested target and refuses an attention flag on anything but a present
// target.
func validatePickerRouteObservations(observations []PickerRouteObservation) error {
	if len(observations) == 0 || len(observations) > PickerControlMaxTargets {
		return ErrInvalidNavigation
	}
	seen := make(map[ExactSessionTarget]struct{}, len(observations))
	for _, observation := range observations {
		if !validPickerRoutePresence(observation.Presence) {
			return ErrInvalidNavigation
		}
		if err := observation.Target.Validate(); err != nil {
			return ErrInvalidNavigation
		}
		if observation.Presence != PickerRoutePresent && observation.Attention {
			return ErrInvalidNavigation
		}
		if _, dup := seen[observation.Target]; dup {
			return ErrInvalidNavigation
		}
		seen[observation.Target] = struct{}{}
	}
	return nil
}

// ValidatePickerControlResponse enforces the exclusive response union: exactly
// the payload owned by the operation and status is present, and a failed
// status never carries payload bytes at all.
func ValidatePickerControlResponse(response PickerControlResponse) error {
	if response.RequestID == 0 || !validPickerControlOperation(response.Operation) || !validPickerSourceStatus(response.Status) {
		return ErrInvalidNavigation
	}
	if response.Status != PickerSourceOK {
		if response.Snapshot != nil || response.Resolved != nil || len(response.Observations) != 0 {
			return ErrInvalidNavigation
		}
		return nil
	}
	switch response.Operation {
	case PickerControlSnapshot:
		if response.Snapshot == nil || response.Resolved != nil || len(response.Observations) != 0 {
			return ErrInvalidNavigation
		}
		return ValidatePickerSnapshot(*response.Snapshot)
	case PickerControlResolve:
		if response.Resolved == nil || response.Snapshot != nil || len(response.Observations) != 0 {
			return ErrInvalidNavigation
		}
		return ValidateAttachTarget(*response.Resolved)
	default:
		if response.Snapshot != nil || response.Resolved != nil {
			return ErrInvalidNavigation
		}
		return validatePickerRouteObservations(response.Observations)
	}
}

func validPickerIntent(intent PickerIntent) bool {
	switch intent {
	case PickerIntentNavigation, PickerIntentMovePane, PickerIntentMoveTab:
		return true
	default:
		return false
	}
}

func validPickerAction(action PickerAction) bool {
	switch action {
	case PickerActionNavigate, PickerActionMove, PickerActionKill:
		return true
	default:
		return false
	}
}

func validPickerLineKind(kind PickerLineKind) bool {
	switch kind {
	case PickerLineSection, PickerLineSession, PickerLineTab, PickerLineHost:
		return true
	default:
		return false
	}
}

func validPickerLineStatus(status PickerLineStatus) bool {
	return status <= PickerLineStatusError
}

func validPickerLineActions(actions PickerLineActions) bool {
	return actions&^(PickerCanNavigate|PickerCanMove|PickerCanKill) == 0
}

func validPickerSourceStatus(status PickerSourceStatus) bool {
	switch status {
	case PickerSourceOK, PickerSourceUnavailable, PickerSourceTooLarge:
		return true
	default:
		return false
	}
}

func validPickerFailureCode(code PickerFailureCode) bool {
	switch code {
	case PickerStaleRevision, PickerUnknownKey, PickerRetiredTarget, PickerNavigationFailed, PickerUnknownSource, PickerActionFailed:
		return true
	default:
		return false
	}
}

func validPickerKey(value string) bool {
	return validInventoryKey(value)
}

func validPickerDisplay(value string) bool {
	if len(value) > PickerInteractionMaxDisplayBytes {
		return false
	}
	return validInventoryDisplay(value)
}

func validPickerSourceID(value string) bool {
	if value == "" || len(value) > PickerInteractionMaxSourceBytes {
		return false
	}
	return validInventoryDisplay(value)
}

// ValidatePickerOffer enforces a nonzero interaction, a known intent, bounded
// display strings, and a move source only on move intents.
func ValidatePickerOffer(offer PickerOffer) error {
	if offer.InteractionID == 0 || !validPickerIntent(offer.Intent) {
		return ErrInvalidNavigation
	}
	if !validPickerDisplay(offer.Title) {
		return ErrInvalidNavigation
	}
	if offer.MoveSourceKey != "" {
		if offer.Intent == PickerIntentNavigation || !validPickerKey(offer.MoveSourceKey) {
			return ErrInvalidNavigation
		}
	}
	return nil
}

// ValidatePickerBegin enforces a nonzero request ID and a known intent.
func ValidatePickerBegin(begin PickerBegin) error {
	if begin.RequestID == 0 || !validPickerIntent(begin.Intent) {
		return ErrInvalidNavigation
	}
	return nil
}

// ValidatePickerSnapshot enforces a nonzero interaction/source revision, a
// bounded source identity, and unique opaque keys with display safety. Order
// is preserved verbatim: lines are never re-sorted here.
// ValidatePickerSnapshot enforces the envelope, the per-line shape, unique row
// keys, and a cursor that names a published row. Every rejection wraps
// ErrInvalidNavigation with the offending row, because a refused snapshot
// otherwise leaves an operator with nothing but a silent, closed picker.
func ValidatePickerSnapshot(snapshot PickerSnapshot) error {
	snapshot = NormalizePickerSnapshot(snapshot)
	if snapshot.InteractionID == 0 || snapshot.SourceRevision == 0 {
		return fmt.Errorf("%w: snapshot envelope", ErrInvalidNavigation)
	}
	if !validPickerSourceID(snapshot.SourceID) || !validPickerSourceStatus(snapshot.Status) {
		return fmt.Errorf("%w: snapshot source %q", ErrInvalidNavigation, snapshot.SourceID)
	}
	if !validPickerDisplay(snapshot.StatusDetail) {
		return fmt.Errorf("%w: snapshot status detail", ErrInvalidNavigation)
	}
	recentKeys, err := validatePickerProjection("recent", snapshot.Recent)
	if err != nil {
		return err
	}
	groupedKeys, err := validatePickerProjection("grouped", snapshot.Grouped)
	if err != nil {
		return err
	}
	if len(recentKeys) != len(groupedKeys) {
		return fmt.Errorf("%w: projection destination count", ErrInvalidNavigation)
	}
	for key, recent := range recentKeys {
		grouped, ok := groupedKeys[key]
		if !ok || recent != grouped {
			return fmt.Errorf("%w: projection destination %q", ErrInvalidNavigation, key)
		}
	}
	return nil
}

func NormalizePickerSnapshot(snapshot PickerSnapshot) PickerSnapshot {
	if len(snapshot.Recent.Lines) == 0 && len(snapshot.Lines) != 0 {
		snapshot.Recent = PickerProjection{Lines: snapshot.Lines, Cursor: snapshot.Cursor}
	}
	if len(snapshot.Grouped.Lines) == 0 && len(snapshot.Lines) != 0 {
		snapshot.Grouped = PickerProjection{Lines: snapshot.Lines, Cursor: snapshot.Cursor}
	}
	return snapshot
}

func validatePickerProjection(name string, projection PickerProjection) (map[string]PickerLineActions, error) {
	if len(projection.Lines) > PickerInteractionMaxLines {
		return nil, fmt.Errorf("%w: %s has %d lines", ErrInvalidNavigation, name, len(projection.Lines))
	}
	seen := make(map[string]PickerLineActions, len(projection.Lines))
	for i, line := range projection.Lines {
		if !validPickerLineKind(line.Kind) || !validPickerLineStatus(line.Status) || !validPickerLineActions(line.Actions) {
			return nil, fmt.Errorf("%w: %s line %d kind/status/actions", ErrInvalidNavigation, name, i)
		}
		if !validPickerDisplay(line.Label) || !validPickerDisplay(line.Detail) || !validPickerDisplay(line.StatusDetail) {
			return nil, fmt.Errorf("%w: %s line %d display", ErrInvalidNavigation, name, i)
		}
		// Focus eligibility is published explicitly: a row that admits an
		// action is always a cursor destination, and a section header is never
		// one. A row that is neither is rendered and skipped.
		if line.Actions != 0 && !line.Focusable {
			return nil, fmt.Errorf("%w: %s line %d action without focus", ErrInvalidNavigation, name, i)
		}
		if line.Kind == PickerLineSection {
			if line.Key != "" || line.Focusable || line.Actions != 0 {
				return nil, fmt.Errorf("%w: %s line %d section key or action", ErrInvalidNavigation, name, i)
			}
			continue
		}
		if !validPickerKey(line.Key) {
			return nil, fmt.Errorf("%w: %s line %d key %q", ErrInvalidNavigation, name, i, line.Key)
		}
		if _, dup := seen[line.Key]; dup {
			return nil, fmt.Errorf("%w: %s line %d repeats key %q", ErrInvalidNavigation, name, i, line.Key)
		}
		seen[line.Key] = line.Actions
	}
	if projection.Cursor.Key != "" {
		if !validPickerKey(projection.Cursor.Key) {
			return nil, fmt.Errorf("%w: %s cursor key %q", ErrInvalidNavigation, name, projection.Cursor.Key)
		}
		if _, ok := seen[projection.Cursor.Key]; !ok {
			return nil, fmt.Errorf("%w: %s cursor key %q is not a published row", ErrInvalidNavigation, name, projection.Cursor.Key)
		}
	}
	if projection.Cursor.Index < 0 || projection.Cursor.Index > len(projection.Lines) {
		return nil, fmt.Errorf("%w: %s cursor index %d of %d lines", ErrInvalidNavigation, name, projection.Cursor.Index, len(projection.Lines))
	}
	return seen, nil
}

// ValidatePickerClose requires a nonzero interaction.
func ValidatePickerClose(close PickerClose) error {
	if close.InteractionID == 0 {
		return ErrInvalidNavigation
	}
	return nil
}

// ValidatePickerClosed requires a nonzero interaction.
func ValidatePickerClosed(closed PickerClosed) error {
	if closed.InteractionID == 0 {
		return ErrInvalidNavigation
	}
	return nil
}

// ValidatePickerSelection enforces nonzero interaction/source revision, a
// bounded source identity, a known action, and a known key shape. Unknown
// keys, foreign sources, and stale revisions reject at resolve time, not here.
func ValidatePickerSelection(selection PickerSelection) error {
	if selection.InteractionID == 0 || selection.SourceRevision == 0 {
		return ErrInvalidNavigation
	}
	if !validPickerSourceID(selection.SourceID) || !validPickerAction(selection.Action) {
		return ErrInvalidNavigation
	}
	if !validPickerKey(selection.Key) {
		return ErrInvalidNavigation
	}
	return nil
}

// ValidatePickerResult enforces matching identity for a completed mutation.
func ValidatePickerResult(result PickerResult) error {
	if result.InteractionID == 0 || !validPickerSourceID(result.SourceID) || !validPickerAction(result.Action) {
		return ErrInvalidNavigation
	}
	if !validPickerKey(result.Key) {
		return ErrInvalidNavigation
	}
	return nil
}

// ValidatePickerFailure enforces matching IDs and bounded codes.
func ValidatePickerFailure(failure PickerFailure) error {
	if failure.InteractionID == 0 || !validPickerAction(failure.Action) {
		return ErrInvalidNavigation
	}
	if failure.SourceID != "" && !validPickerSourceID(failure.SourceID) {
		return ErrInvalidNavigation
	}
	if failure.Key != "" && !validPickerKey(failure.Key) {
		return ErrInvalidNavigation
	}
	if !validPickerFailureCode(failure.Code) {
		return ErrInvalidNavigation
	}
	return nil
}
