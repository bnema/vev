package daemon

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/usecase/layout"
)

func TestMoveRejectionCategoriesPreserveLegacyIdentity(t *testing.T) {
	tests := []struct {
		name   string
		err    error
		reason moveRejectionReason
		legacy error
	}{
		{name: "no destination", err: errNoMoveDestination, reason: moveRejectionNoDestination},
		{name: "warming", err: errMoveFloatingWarming, reason: moveRejectionFloatingWarming},
		{name: "installed final source floating", err: errMoveFinalSourceFloating, reason: moveRejectionFinalSourceFloating, legacy: errMovePaneFloatingSibling},
		{name: "stale target", err: errMoveStaleTarget, reason: moveRejectionStaleTarget, legacy: errMovePaneInvalid},
		{name: "too small", err: errMoveTooSmall, reason: moveRejectionTooSmall, legacy: layout.ErrTooSmall},
		{name: "pane ID exhaustion", err: errMovePaneIDExhausted, reason: moveRejectionPaneIDExhausted},
		{name: "generic invalid", err: errMovePaneInvalid, reason: moveRejectionInvalid},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var rejection *moveRejection
			require.ErrorAs(t, tt.err, &rejection)
			require.Equal(t, tt.reason, rejection.Reason)
			if tt.legacy != nil {
				require.True(t, errors.Is(tt.err, tt.legacy))
			}
		})
	}
	require.NotErrorIs(t, errMoveFloatingWarming, errMoveFinalSourceFloating)
}

func TestNormalizeMoveRejectionDoesNotLeakInternalErrors(t *testing.T) {
	internal := errors.New("layout implementation detail")
	got := normalizeMoveRejection(internal)
	var rejection *moveRejection
	require.ErrorAs(t, got, &rejection)
	require.Equal(t, moveRejectionInvalid, rejection.Reason)
	require.Equal(t, "move request is invalid", got.Error())
	require.ErrorIs(t, got, internal)
	require.ErrorIs(t, got, errMovePaneInvalid)
}

func TestMoveRejectionPresentationParity(t *testing.T) {
	tests := []struct {
		name            string
		err             error
		reason          moveRejectionReason
		paletteCode     domain.NoticeCode
		paletteSeverity domain.NoticeSeverity
		paletteText     string
		commandText     string
	}{
		{name: "no destination", err: errNoMoveDestination, reason: moveRejectionNoDestination, paletteCode: domain.NoticeSessionUnavailable, paletteSeverity: domain.NoticeWarn, paletteText: "No destination available.", commandText: "no move destination available"},
		{name: "warming", err: errMoveFloatingWarming, reason: moveRejectionFloatingWarming, paletteCode: domain.NoticeSessionUnavailable, paletteSeverity: domain.NoticeWarn, paletteText: "Wait for the floating pane to finish opening.", commandText: "wait for the floating pane to finish opening"},
		{name: "installed final source floating", err: errMoveFinalSourceFloating, reason: moveRejectionFinalSourceFloating, paletteCode: domain.NoticeLayoutTooSmall, paletteSeverity: domain.NoticeWarn, paletteText: "Close the floating pane or move the whole tab.", commandText: "close the floating pane or move the whole tab"},
		{name: "too small", err: errMoveTooSmall, reason: moveRejectionTooSmall, paletteCode: domain.NoticeLayoutTooSmall, paletteSeverity: domain.NoticeWarn, paletteText: "Not enough space in destination tab.", commandText: "not enough space in destination tab"},
		{name: "stale", err: errMoveStaleTarget, reason: moveRejectionStaleTarget, paletteCode: domain.NoticeSessionUnavailable, paletteSeverity: domain.NoticeWarn, paletteText: "Destination is no longer available.", commandText: "move target is no longer available"},
		{name: "lifecycle target", err: errMoveLifecycleUnavailable, reason: moveRejectionStaleTarget, paletteCode: domain.NoticeSessionUnavailable, paletteSeverity: domain.NoticeWarn, paletteText: "Destination is no longer available.", commandText: "move target is no longer available"},
		{name: "pane ID exhaustion", err: errMovePaneIDExhausted, reason: moveRejectionPaneIDExhausted, paletteCode: domain.NoticeInternal, paletteSeverity: domain.NoticeError, paletteText: "Destination pane IDs are exhausted.", commandText: "destination pane IDs are exhausted"},
		{name: "generic invalid", err: errMovePaneInvalid, reason: moveRejectionInvalid, paletteCode: domain.NoticeInternal, paletteSeverity: domain.NoticeError, paletteText: "Move failed.", commandText: "move request is invalid"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			descriptor, ok := moveRejectionDescriptorFor(tt.err)
			require.True(t, ok)
			require.Equal(t, tt.reason, descriptor.Reason)

			paletteErr := movePickerUserError(tt.err)
			var userErr *domain.UserError
			require.ErrorAs(t, paletteErr, &userErr)
			require.Equal(t, descriptor.PaletteCode, userErr.Code)
			require.Equal(t, descriptor.PaletteSeverity, userErr.Severity)
			require.Equal(t, descriptor.PaletteText, userErr.Msg)
			require.Equal(t, tt.paletteCode, userErr.Code)
			require.Equal(t, tt.paletteSeverity, userErr.Severity)
			require.Equal(t, tt.paletteText, userErr.Msg)

			result := moveCommandFailure(tt.err)
			require.False(t, result.OK)
			require.Equal(t, descriptor.CommandCode, result.Code)
			require.Equal(t, descriptor.CommandText, result.Text)
			require.Equal(t, protocol.ErrNoSuchTarget, result.Code)
			require.Equal(t, tt.commandText, result.Text)
		})
	}
}
