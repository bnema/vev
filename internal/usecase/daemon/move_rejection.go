package daemon

import (
	"errors"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/usecase/layout"
)

// moveRejectionReason is the daemon API category for a rejected move. It keeps
// callers independent of topology and layout implementation errors.
type moveRejectionReason uint8

const (
	moveRejectionInvalid moveRejectionReason = iota + 1
	moveRejectionNoDestination
	moveRejectionFloatingWarming
	moveRejectionFinalSourceFloating
	moveRejectionStaleTarget
	moveRejectionTooSmall
	moveRejectionPaneIDExhausted
)

// moveRejection carries a stable category while Unwrap preserves identities
// relied on by lower-level move tests and callers.
type moveRejection struct {
	Reason moveRejectionReason
	msg    string
	cause  error
}

func (e *moveRejection) Error() string { return e.msg }
func (e *moveRejection) Unwrap() error { return e.cause }

// moveRejectionDescriptor is the canonical presentation for one rejection
// reason. Palette and control adapters only wrap these values for their
// respective transports.
type moveRejectionDescriptor struct {
	Reason          moveRejectionReason
	PaletteCode     domain.NoticeCode
	PaletteSeverity domain.NoticeSeverity
	PaletteText     string
	CommandCode     uint16
	CommandText     string
}

func moveRejectionDescriptorFor(err error) (moveRejectionDescriptor, bool) {
	reason, ok := moveRejectionReasonFor(err)
	if !ok {
		return moveRejectionDescriptor{}, false
	}
	return moveRejectionDescriptorByReason(reason), true
}

func moveRejectionReasonFor(err error) (moveRejectionReason, bool) {
	var rejection *moveRejection
	if errors.As(err, &rejection) {
		return rejection.Reason, true
	}
	if errors.Is(err, errMoveLifecycleUnavailable) {
		return moveRejectionStaleTarget, true
	}
	return 0, false
}

func moveRejectionDescriptorByReason(reason moveRejectionReason) moveRejectionDescriptor {
	descriptor := moveRejectionDescriptor{
		Reason:          reason,
		PaletteCode:     domain.NoticeInternal,
		PaletteSeverity: domain.NoticeError,
		PaletteText:     "Move failed.",
		CommandCode:     protocol.ErrNoSuchTarget,
		CommandText:     "move request is invalid",
	}
	switch reason {
	case moveRejectionNoDestination:
		descriptor.PaletteCode = domain.NoticeSessionUnavailable
		descriptor.PaletteSeverity = domain.NoticeWarn
		descriptor.PaletteText = "No destination available."
		descriptor.CommandText = "no move destination available"
	case moveRejectionFloatingWarming:
		descriptor.PaletteCode = domain.NoticeSessionUnavailable
		descriptor.PaletteSeverity = domain.NoticeWarn
		descriptor.PaletteText = "Wait for the floating pane to finish opening."
		descriptor.CommandText = "wait for the floating pane to finish opening"
	case moveRejectionFinalSourceFloating:
		descriptor.PaletteCode = domain.NoticeLayoutTooSmall
		descriptor.PaletteSeverity = domain.NoticeWarn
		descriptor.PaletteText = "Close the floating pane or move the whole tab."
		descriptor.CommandText = "close the floating pane or move the whole tab"
	case moveRejectionStaleTarget:
		descriptor.PaletteCode = domain.NoticeSessionUnavailable
		descriptor.PaletteSeverity = domain.NoticeWarn
		descriptor.PaletteText = "Destination is no longer available."
		descriptor.CommandText = "move target is no longer available"
	case moveRejectionTooSmall:
		descriptor.PaletteCode = domain.NoticeLayoutTooSmall
		descriptor.PaletteSeverity = domain.NoticeWarn
		descriptor.PaletteText = "Not enough space in destination tab."
		descriptor.CommandText = "not enough space in destination tab"
	case moveRejectionPaneIDExhausted:
		descriptor.PaletteText = "Destination pane IDs are exhausted."
		descriptor.CommandText = "destination pane IDs are exhausted"
	}
	return descriptor
}

func movePickerUserError(err error) error {
	if err == nil {
		return nil
	}
	var userErr *domain.UserError
	if errors.As(err, &userErr) {
		return err
	}
	descriptor, ok := moveRejectionDescriptorFor(err)
	if !ok {
		descriptor = moveRejectionDescriptorByReason(moveRejectionInvalid)
	}
	if descriptor.PaletteSeverity == domain.NoticeWarn {
		return domain.UserWarn(descriptor.PaletteCode, descriptor.PaletteText, err)
	}
	return domain.UserErr(descriptor.PaletteCode, descriptor.PaletteText, err)
}

func isMoveCommandError(err error) bool {
	_, ok := moveRejectionReasonFor(err)
	return ok
}

func moveCommandFailure(err error) protocol.CommandResult {
	descriptor, ok := moveRejectionDescriptorFor(err)
	if !ok {
		descriptor = moveRejectionDescriptorByReason(moveRejectionInvalid)
	}
	return commandFailure(descriptor.CommandCode, descriptor.CommandText)
}

func normalizeMoveRejection(err error) error {
	if err == nil {
		return nil
	}
	var rejection *moveRejection
	if errors.As(err, &rejection) {
		return err
	}
	if errors.Is(err, layout.ErrTooSmall) {
		return errMoveTooSmall
	}
	if errors.Is(err, errMoveLifecycleUnavailable) {
		return &moveRejection{Reason: moveRejectionStaleTarget, msg: errMoveStaleTarget.msg, cause: errors.Join(errMovePaneInvalid, err)}
	}
	return &moveRejection{Reason: moveRejectionInvalid, msg: errMovePaneInvalid.msg, cause: errors.Join(errMovePaneInvalid, err)}
}

var (
	errMovePaneInvalid   = &moveRejection{Reason: moveRejectionInvalid, msg: "move request is invalid"}
	errNoMoveDestination = &moveRejection{
		Reason: moveRejectionNoDestination,
		msg:    "no eligible move destination",
	}
	errMoveFloatingWarming = &moveRejection{
		Reason: moveRejectionFloatingWarming,
		msg:    "floating pane is still opening",
	}
	errMovePaneFloatingSibling = errors.New("cannot remove a source tab with a floating pane")
	errMoveFinalSourceFloating = &moveRejection{
		Reason: moveRejectionFinalSourceFloating,
		msg:    "final source pane cannot move while its tab has a floating pane",
		cause:  errMovePaneFloatingSibling,
	}
	errMoveStaleTarget = &moveRejection{
		Reason: moveRejectionStaleTarget,
		msg:    "move target is no longer available",
		cause:  errMovePaneInvalid,
	}
	errMoveTooSmall = &moveRejection{
		Reason: moveRejectionTooSmall,
		msg:    "move destination is too small",
		cause:  layout.ErrTooSmall,
	}
	errMovePaneIDExhausted = &moveRejection{
		Reason: moveRejectionPaneIDExhausted,
		msg:    "destination pane IDs exhausted",
	}
)
