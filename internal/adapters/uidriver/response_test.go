package uidriver

import (
	"encoding/json"
	"errors"
	"strings"
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

func TestCaptureFitsMaximumViewportForStructuredFormats(t *testing.T) {
	snapshot := ports.UISnapshot{Columns: maxColumns, Rows: maxRows, Cells: make([]ports.UICell, maxColumns*maxRows)}
	for _, format := range []captureFormat{formatCells, formatBoth} {
		t.Run(string(format), func(t *testing.T) {
			require.True(t, captureFitsResponse(snapshot, format))
		})
	}
	capture, err := publicCapture(snapshot, formatCells)
	require.NoError(t, err)
	encoded, err := json.Marshal(capture)
	require.NoError(t, err)
	require.Less(t, len(encoded), maxResponseBytes)
}

func TestColorResponseSerializesZeroComponents(t *testing.T) {
	tests := []struct {
		name  string
		color colorResponse
		want  string
	}{
		{name: "default", color: colorResponse{Kind: colorDefault}, want: `{"kind":"default","index":0,"r":0,"g":0,"b":0}`},
		{name: "indexed zero", color: colorResponse{Kind: colorIndexed}, want: `{"kind":"indexed","index":0,"r":0,"g":0,"b":0}`},
		{name: "rgb zero", color: colorResponse{Kind: colorRGB}, want: `{"kind":"rgb","index":0,"r":0,"g":0,"b":0}`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			encoded, err := json.Marshal(test.color)
			require.NoError(t, err)
			require.Equal(t, test.want, string(encoded))
		})
	}
}

func TestCaptureRejectsOversizedCellBeforeSerialization(t *testing.T) {
	snapshot := ports.UISnapshot{Revision: 1, Columns: 1, Rows: 1, Cells: []ports.UICell{{Text: strings.Repeat("x", maxResponseBytes), Width: 1}}}
	_, err := publicCapture(snapshot, formatCells)
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
