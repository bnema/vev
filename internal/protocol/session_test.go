package protocol

import (
	"errors"
	"testing"

	renderer "github.com/bnema/vev-vt"
	"github.com/bnema/vev/internal/domain"
	"github.com/stretchr/testify/require"
)

func TestOutputSemanticValidation(t *testing.T) {
	valid := Output{Epoch: 1, New: 1, Full: true, Size: domain.Size{Cols: 80, Rows: 24}}
	require.NoError(t, ValidateOutput(valid))

	invalid := valid
	invalid.Epoch = 0
	require.ErrorIs(t, ValidateOutput(invalid), ErrInvalidOutput)

	invalid = valid
	invalid.Data = make([]byte, MaxOutputDataLen+1)
	require.ErrorIs(t, ValidateOutput(invalid), ErrInvalidOutput)
}

func TestRouteAndPreviewSemanticValidation(t *testing.T) {
	require.Error(t, ValidateRouteLabel("\x1b[31m", false))
	require.NoError(t, RouteRef{}.Validate())
	require.Error(t, (RouteRef{Key: 1}).Validate())

	target := domain.RemoteSessionTarget{
		Endpoint: "remote", DisplayOrigin: "remote", LifecycleID: domain.SessionLifecycleID{1},
		SessionName: "work", LiveTabID: "tab-1",
	}
	request := RemotePreviewRequest{Version: RemotePreviewSchemaVersion, Target: target, Width: 1, Height: 1}
	require.NoError(t, ValidateRemotePreviewRequest(request))
	preview := RemotePreview{
		Version: RemotePreviewSchemaVersion, Status: RemotePreviewOK,
		LifecycleID: target.LifecycleID, TabID: target.LiveTabID, Revision: 1,
		Width: 1, Height: 1, Cells: []renderer.Cell{{Rune: 'x'}},
	}
	require.NoError(t, ValidateRemotePreview(preview))
	preview.Cells[0].Rune = '\n'
	require.ErrorIs(t, ValidateRemotePreview(preview), ErrInvalidRemotePreview)
}

func TestPresentationErrorsPreserveClassification(t *testing.T) {
	require.True(t, errors.Is(ValidateAck(Ack{}), ErrInvalidAck))
	require.True(t, errors.Is(ValidateParkedRouteRequest(ParkedRouteRequest{}), ErrInvalidNavigation))
}
