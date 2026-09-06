package uidriver

import (
	"context"
	"errors"
	"strings"
	"unicode/utf8"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
)

type envelope struct {
	Version int            `json:"version"`
	ID      uint64         `json:"id"`
	Result  any            `json:"result,omitempty"`
	Error   *responseError `json:"error,omitempty"`
}

type responseError struct {
	Code     ports.UIErrorCode `json:"code"`
	Message  string            `json:"message"`
	Accepted bool              `json:"accepted"`
	ActionID uint64            `json:"action_id,omitempty"`
}

// Ready is connection discovery, never an attachment registry.
type Ready struct {
	Attachment string                     `json:"attachment"`
	Generation uint64                     `json:"generation"`
	Control    bool                       `json:"control"`
	Status     ports.UIPresentationStatus `json:"status"`
}

type contextResponse struct {
	Attachment      string                     `json:"attachment"`
	Generation      uint64                     `json:"generation"`
	Session         sessionResponse            `json:"session"`
	Focus           focusResponse              `json:"focus"`
	Epoch           uint64                     `json:"output_epoch"`
	State           uint64                     `json:"output_state"`
	ViewRevision    uint64                     `json:"view_revision"`
	ViewPublication uint64                     `json:"view_publication"`
	Status          ports.UIPresentationStatus `json:"status"`
}

type sessionResponse struct {
	LifecycleID string `json:"lifecycle_id"`
	SessionName string `json:"session_name"`
}

type focusResponse struct {
	TabID  domain.TabStableID  `json:"tab_id"`
	PaneID domain.PaneStableID `json:"pane_id"`
}

type cursorResponse struct {
	Row      int  `json:"row"`
	Column   int  `json:"column"`
	Visible  bool `json:"visible"`
	Style    int  `json:"style"`
	StyleSet bool `json:"style_set"`
}

type colorKind string

const (
	colorDefault colorKind = "default"
	colorIndexed colorKind = "indexed"
	colorRGB     colorKind = "rgb"
)

type colorResponse struct {
	Kind  colorKind `json:"kind"`
	Index uint8     `json:"index"`
	R     uint8     `json:"r"`
	G     uint8     `json:"g"`
	B     uint8     `json:"b"`
}

type styleResponse struct {
	Foreground     colorResponse `json:"foreground"`
	Background     colorResponse `json:"background"`
	Bold           bool          `json:"bold"`
	Dim            bool          `json:"dim"`
	Italic         bool          `json:"italic"`
	Underline      bool          `json:"underline"`
	UnderlineStyle uint8         `json:"underline_style"`
	Blink          bool          `json:"blink"`
	Inverse        bool          `json:"inverse"`
	Strikethrough  bool          `json:"strikethrough"`
}

type cellResponse struct {
	Text         string        `json:"text"`
	Width        int           `json:"width"`
	Continuation bool          `json:"continuation"`
	Style        styleResponse `json:"style"`
}

type captureResponse struct {
	Revision uint64          `json:"revision"`
	Context  contextResponse `json:"context"`
	Columns  int             `json:"columns"`
	Rows     int             `json:"rows"`
	Cursor   cursorResponse  `json:"cursor"`
	Text     *string         `json:"text,omitempty"`
	Cells    []cellResponse  `json:"cells,omitempty"`
}

type actionResponse struct {
	ActionID uint64               `json:"action_id"`
	Accepted bool                 `json:"accepted"`
	Status   ports.UIActionStatus `json:"status"`
	Revision uint64               `json:"revision"`
	Context  contextResponse      `json:"context"`
}

type waitResponse struct {
	ActionID     uint64               `json:"action_id,omitempty"`
	ActionStatus ports.UIActionStatus `json:"action_status,omitempty"`
	Revision     uint64               `json:"revision"`
	Context      contextResponse      `json:"context"`
}

func publicContext(value ports.UIContext) contextResponse {
	var lifecycleID string
	if value.Route.Target.LifecycleID != (domain.SessionLifecycleID{}) {
		lifecycleID = value.Route.Target.LifecycleID.String()
	}
	return contextResponse{Attachment: value.AttachmentHandle, Generation: value.Generation, Session: sessionResponse{LifecycleID: lifecycleID, SessionName: value.Route.Target.SessionName}, Focus: focusResponse{TabID: value.TabID, PaneID: value.FocusedPaneID}, Epoch: value.OutputEpoch, State: value.OutputState, ViewRevision: value.ViewRevision, ViewPublication: value.ViewPublication, Status: value.Status}
}

func publicColor(value ports.UIColor) colorResponse {
	result := colorResponse{Kind: colorDefault}
	switch value.Kind {
	case ports.UIColorIndexed:
		result.Kind = colorIndexed
		result.Index = value.Index
	case ports.UIColorRGB:
		result.Kind = colorRGB
		result.R, result.G, result.B = value.R, value.G, value.B
	}
	return result
}

func publicCapture(snapshot ports.UISnapshot, format captureFormat) (captureResponse, error) {
	result := captureResponse{}
	if snapshot.Columns < 1 || snapshot.Rows < 1 || snapshot.Columns > maxColumns || snapshot.Rows > maxRows {
		return result, &ports.UIError{Code: ports.UIErrCaptureTooLarge}
	}
	if len(snapshot.Cells) != snapshot.Columns*snapshot.Rows {
		return result, ports.ErrUIUnavailable
	}
	if !captureFitsResponse(snapshot, format) {
		return result, &ports.UIError{Code: ports.UIErrCaptureTooLarge}
	}
	result = captureResponse{Revision: snapshot.Revision, Context: publicContext(snapshot.Context), Columns: snapshot.Columns, Rows: snapshot.Rows, Cursor: cursorResponse{Row: snapshot.Cursor.Row, Column: snapshot.Cursor.Column, Visible: snapshot.Cursor.Visible, Style: snapshot.Cursor.Style, StyleSet: snapshot.Cursor.StyleSet}}
	if format == formatText || format == formatBoth {
		var text strings.Builder
		for row := 0; row < snapshot.Rows; row++ {
			if row > 0 {
				text.WriteByte('\n')
			}
			for _, cell := range snapshot.Cells[row*snapshot.Columns : (row+1)*snapshot.Columns] {
				if cell.Continuation {
					continue
				}
				if cell.Text == "" {
					text.WriteByte(' ')
				} else {
					text.WriteString(cell.Text)
				}
			}
		}
		value := text.String()
		result.Text = &value
	}
	if format == formatCells || format == formatBoth {
		result.Cells = make([]cellResponse, len(snapshot.Cells))
		for i, cell := range snapshot.Cells {
			s := cell.Style
			result.Cells[i] = cellResponse{Text: cell.Text, Width: cell.Width, Continuation: cell.Continuation, Style: styleResponse{Foreground: publicColor(s.Foreground), Background: publicColor(s.Background), Bold: s.Bold, Dim: s.Dim, Italic: s.Italic, Underline: s.Underline, UnderlineStyle: s.UnderlineStyle, Blink: s.Blink, Inverse: s.Inverse, Strikethrough: s.Strikethrough}}
		}
	}
	return result, nil
}

// captureFitsResponse rejects a malicious or corrupted snapshot before
// constructing a potentially large response object. The estimate is
// intentionally conservative for JSON string escaping and cell style fields.
func captureFitsResponse(snapshot ports.UISnapshot, format captureFormat) bool {
	const captureOverhead = 4096
	const cellOverhead = 320
	size := captureOverhead
	for _, cell := range snapshot.Cells {
		if !utf8.ValidString(cell.Text) {
			return false
		}
		if format == formatCells || format == formatBoth {
			if len(cell.Text) > maxResponseBytes || size > maxResponseBytes-len(cell.Text)-cellOverhead {
				return false
			}
			size += len(cell.Text) + cellOverhead
		}
		if (format == formatText || format == formatBoth) && !cell.Continuation {
			if size > maxResponseBytes-len(cell.Text)-1 {
				return false
			}
			size += len(cell.Text) + 1
		}
	}
	return size <= maxResponseBytes
}

// Never serialize arbitrary error text: lower layers may include endpoints,
// input, screen content, or environment. The public code is the safe message.
func errorEnvelope(id uint64, err error) envelope {
	failure := responseError{Code: ports.UIErrUnavailable}
	var uiErr *ports.UIError
	if errors.As(err, &uiErr) {
		failure.Code = uiErr.Code
		failure.Accepted = uiErr.Accepted
		failure.ActionID = uiErr.ActionID
	} else if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		failure.Code = ports.UIErrTimeout
	}
	failure.Message = string(failure.Code)
	return envelope{Version: apiVersion, ID: id, Error: &failure}
}
