package protocol

// Client-picker interaction bounds. Rows mirror the local inventory export
// limit; the complete encoded snapshot stays below MaxFrameLen with the 4
// MiB documented export budget shared with navigation inventory.
const (
	PickerInteractionMaxRows         = 4096
	PickerInteractionMaxDisplayBytes = 256
	PickerInteractionMaxEncodedBytes = 4 << 20
)

// PickerFailureCode classifies client-picker selection failures.
type PickerFailureCode uint8

const (
	PickerStaleRevision PickerFailureCode = iota + 1
	PickerUnknownKey
	PickerRetiredTarget
	PickerNavigationFailed
)

// PickerRow is one opaque selectable row. Key is daemon-assigned and
// resolves to a navigation target daemon-side only; display strings carry
// presentation facts, never endpoints, credentials, environment, CWD, or
// pane content.
type PickerRow struct {
	Key     string
	Display string
	Detail  string
	Stopped bool
}

// PickerCursor restores the selected row by opaque key with its list index.
type PickerCursor struct {
	Key   string
	Index int
}

// PickerSnapshot replaces the whole client-picker model. Row order is
// semantically significant and never canonicalized. BarrierEpoch and
// BarrierState name the admitted output boundary the client must display
// before rendering; SizeEpoch forces a revision bump when geometry changes
// mid-interaction.
type PickerSnapshot struct {
	InteractionID uint64
	Revision      uint64
	Title         string
	Rows          []PickerRow
	Cursor        PickerCursor
	BarrierEpoch  uint64
	BarrierState  uint64
	SizeEpoch     uint64
}

// PickerClose ends an interaction. Either side sends it: the daemon on
// supersede/commit/teardown, the client on cancel.
type PickerClose struct {
	InteractionID uint64
	Revision      uint64
}

// PickerSelection commits the opaque row key displayed at Revision.
// CauseActionID correlates ui-driver input like NavigationInventorySelection.
type PickerSelection struct {
	CauseActionID uint64
	InteractionID uint64
	Revision      uint64
	Key           string
}

// PickerFailure reports a rejected selection with a bounded code.
type PickerFailure struct {
	CauseActionID uint64
	InteractionID uint64
	Key           string
	Code          PickerFailureCode
}

func validPickerFailureCode(code PickerFailureCode) bool {
	switch code {
	case PickerStaleRevision, PickerUnknownKey, PickerRetiredTarget, PickerNavigationFailed:
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

// ValidatePickerSnapshot enforces nonzero interaction/revision, bounded
// title/rows/cursor, unique opaque keys, and display safety. Order is
// preserved verbatim: rows are never re-sorted here.
func ValidatePickerSnapshot(snapshot PickerSnapshot) error {
	if snapshot.InteractionID == 0 || snapshot.Revision == 0 {
		return ErrInvalidNavigation
	}
	if !validPickerDisplay(snapshot.Title) {
		return ErrInvalidNavigation
	}
	if len(snapshot.Rows) > PickerInteractionMaxRows {
		return ErrInvalidNavigation
	}
	seen := make(map[string]struct{}, len(snapshot.Rows))
	for _, row := range snapshot.Rows {
		if !validPickerKey(row.Key) || !validPickerDisplay(row.Display) || !validPickerDisplay(row.Detail) {
			return ErrInvalidNavigation
		}
		if _, dup := seen[row.Key]; dup {
			return ErrInvalidNavigation
		}
		seen[row.Key] = struct{}{}
	}
	if snapshot.Cursor.Key != "" {
		if !validPickerKey(snapshot.Cursor.Key) {
			return ErrInvalidNavigation
		}
		if _, ok := seen[snapshot.Cursor.Key]; !ok {
			return ErrInvalidNavigation
		}
	}
	if snapshot.Cursor.Index < 0 || snapshot.Cursor.Index > len(snapshot.Rows) {
		return ErrInvalidNavigation
	}
	return nil
}

// ValidatePickerClose requires a nonzero interaction; Revision may be zero
// when the sender never displayed a snapshot (cancel before first publish).
func ValidatePickerClose(close PickerClose) error {
	if close.InteractionID == 0 {
		return ErrInvalidNavigation
	}
	return nil
}

// ValidatePickerSelection enforces nonzero interaction/revision, a known
// key shape, and a cause-free shape contract. Unknown keys and stale
// revisions reject at resolve time, not here.
func ValidatePickerSelection(selection PickerSelection) error {
	if selection.InteractionID == 0 || selection.Revision == 0 {
		return ErrInvalidNavigation
	}
	if !validPickerKey(selection.Key) {
		return ErrInvalidNavigation
	}
	return nil
}

// ValidatePickerFailure enforces matching IDs and bounded codes.
func ValidatePickerFailure(failure PickerFailure) error {
	if failure.InteractionID == 0 {
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
