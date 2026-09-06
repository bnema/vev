package uidriver

import (
	"encoding/json"
	"errors"
	"testing"

	"github.com/bnema/vev/internal/ports"
	"github.com/stretchr/testify/require"
)

func TestCaptureTextCellsAndOwnedContext(t *testing.T) {
	snapshot := ports.UISnapshot{Revision: 7, Columns: 4, Rows: 2, Context: ports.UIContext{AttachmentHandle: "a", Generation: 2, Status: ports.UIStatusAttached}, Cells: []ports.UICell{{Text: "界", Width: 2}, {Continuation: true}, {Text: "x", Width: 1}, {Width: 1}, {Width: 1}, {Width: 1}, {Width: 1}, {Width: 1}}}
	snapshot.Cells[2].Style = ports.UIStyle{Foreground: ports.UIColor{Kind: ports.UIColorRGB, R: 1, G: 2, B: 3}, Background: ports.UIColor{Kind: ports.UIColorIndexed, Index: 4}, Inverse: true}
	result, err := publicCapture(snapshot, formatBoth)
	require.NoError(t, err)
	require.Equal(t, "界x \n    ", *result.Text)
	require.Len(t, result.Cells, 8)
	require.True(t, result.Cells[1].Continuation)
	require.Equal(t, colorRGB, result.Cells[2].Style.Foreground.Kind)
	require.True(t, result.Cells[2].Style.Inverse)
	result.Cells[2].Text = "changed"
	require.Equal(t, "x", snapshot.Cells[2].Text)
	encoded, err := json.Marshal(result)
	require.NoError(t, err)
	require.Contains(t, string(encoded), `"attachment":"a"`)
	require.NotContains(t, string(encoded), "AttachmentHandle")
}

func TestCaptureRejectsExcessGeometryWithoutResizing(t *testing.T) {
	_, err := publicCapture(ports.UISnapshot{Columns: maxColumns + 1, Rows: 1}, formatCells)
	var uiErr *ports.UIError
	require.ErrorAs(t, err, &uiErr)
	require.Equal(t, ports.UIErrCaptureTooLarge, uiErr.Code)
}

func TestErrorResponseNeverExportsUnderlyingMessage(t *testing.T) {
	response := errorEnvelope(9, errors.New("private viewport or environment"))
	encoded, err := json.Marshal(response)
	require.NoError(t, err)
	require.NotContains(t, string(encoded), "private")
	require.Equal(t, ports.UIErrUnavailable, response.Error.Code)
	accepted := errorEnvelope(10, &ports.UIError{Code: ports.UIErrTimeout, Accepted: true, ActionID: 7})
	require.True(t, accepted.Error.Accepted)
	require.Equal(t, uint64(7), accepted.Error.ActionID)
}
