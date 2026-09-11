package protocol

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
	Actions      PickerLineActions
	Ephemeral    bool
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

// PickerSnapshot publishes the complete line set of one source at one
// revision. Row order is semantically significant and never canonicalized.
// Revisions are per source: a refresh of one source never invalidates a
// selection displayed from another.
type PickerSnapshot struct {
	InteractionID  uint64
	SourceID       string
	SourceRevision uint64
	Status         PickerSourceStatus
	StatusDetail   string
	Lines          []PickerLine
	Cursor         PickerCursor
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
func ValidatePickerSnapshot(snapshot PickerSnapshot) error {
	if snapshot.InteractionID == 0 || snapshot.SourceRevision == 0 {
		return ErrInvalidNavigation
	}
	if !validPickerSourceID(snapshot.SourceID) || !validPickerSourceStatus(snapshot.Status) {
		return ErrInvalidNavigation
	}
	if !validPickerDisplay(snapshot.StatusDetail) {
		return ErrInvalidNavigation
	}
	if len(snapshot.Lines) > PickerInteractionMaxLines {
		return ErrInvalidNavigation
	}
	seen := make(map[string]struct{}, len(snapshot.Lines))
	for _, line := range snapshot.Lines {
		if !validPickerLineKind(line.Kind) || !validPickerLineStatus(line.Status) || !validPickerLineActions(line.Actions) {
			return ErrInvalidNavigation
		}
		if !validPickerDisplay(line.Label) || !validPickerDisplay(line.Detail) || !validPickerDisplay(line.StatusDetail) {
			return ErrInvalidNavigation
		}
		if line.Kind == PickerLineSection {
			if line.Key != "" {
				return ErrInvalidNavigation
			}
			continue
		}
		if !validPickerKey(line.Key) {
			return ErrInvalidNavigation
		}
		if _, dup := seen[line.Key]; dup {
			return ErrInvalidNavigation
		}
		seen[line.Key] = struct{}{}
	}
	if snapshot.Cursor.Key != "" {
		if !validPickerKey(snapshot.Cursor.Key) {
			return ErrInvalidNavigation
		}
		if _, ok := seen[snapshot.Cursor.Key]; !ok {
			return ErrInvalidNavigation
		}
	}
	if snapshot.Cursor.Index < 0 || snapshot.Cursor.Index > len(snapshot.Lines) {
		return ErrInvalidNavigation
	}
	return nil
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
