package sessionwire

import (
	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/protocol/wire"
)

func inputToWire(message protocol.Input) *wire.Input {
	return &wire.Input{InputSeq: message.InputSeq, ActionId: message.ActionID, Data: append([]byte(nil), message.Data...)}
}

func inputFromWire(message *wire.Input) (protocol.Input, error) {
	if message == nil {
		return protocol.Input{}, errProtoConvertRange
	}
	return protocol.Input{InputSeq: message.GetInputSeq(), ActionID: message.GetActionId(), Data: append([]byte(nil), message.GetData()...)}, nil
}

func resizeToWire(message protocol.Resize) (*wire.Resize, error) {
	cols, rows, pixelWidth, pixelHeight, err := geometryToWire(message.Size, message.PixelWidth, message.PixelHeight)
	if err != nil {
		return nil, err
	}
	if err := protocol.ValidateGeometry(domain.Geometry{Size: message.Size, PixelWidth: message.PixelWidth, PixelHeight: message.PixelHeight}); err != nil {
		return nil, err
	}
	return &wire.Resize{Cols: cols, Rows: rows, PixelWidth: pixelWidth, PixelHeight: pixelHeight}, nil
}

func resizeFromWire(message *wire.Resize) (protocol.Resize, error) {
	var resize protocol.Resize
	if message == nil {
		return resize, errProtoConvertRange
	}
	cols, err := mustUint16(message.GetCols())
	if err != nil {
		return protocol.Resize{}, err
	}
	rows, err := mustUint16(message.GetRows())
	if err != nil {
		return protocol.Resize{}, err
	}
	pixelWidth, err := mustUint16(message.GetPixelWidth())
	if err != nil {
		return protocol.Resize{}, err
	}
	pixelHeight, err := mustUint16(message.GetPixelHeight())
	if err != nil {
		return protocol.Resize{}, err
	}
	resize = protocol.Resize{Size: domain.Size{Cols: int(cols), Rows: int(rows)}, PixelWidth: int(pixelWidth), PixelHeight: int(pixelHeight)}
	if err := protocol.ValidateGeometry(domain.Geometry{Size: resize.Size, PixelWidth: resize.PixelWidth, PixelHeight: resize.PixelHeight}); err != nil {
		return protocol.Resize{}, err
	}
	return resize, nil
}

func themeToWire(message protocol.Theme) *wire.Theme {
	palette := make([]*wire.RGB, 0, len(message.Palette))
	for _, color := range message.Palette {
		color := color
		palette = append(palette, rgbToWire(color))
	}
	return &wire.Theme{
		HasForeground: message.HasForeground,
		Foreground:    rgbToWire(message.Foreground),
		HasBackground: message.HasBackground,
		Background:    rgbToWire(message.Background),
		TrueColor:     message.TrueColor,
		SchemeKnown:   message.SchemeKnown,
		Light:         message.Light,
		PaletteKnown:  uint32(message.PaletteKnown),
		Palette:       palette,
	}
}

func themeFromWire(message *wire.Theme) (protocol.Theme, error) {
	var theme protocol.Theme
	if message == nil {
		return theme, errProtoConvertRange
	}
	if len(message.GetPalette()) != len(theme.Palette) {
		return protocol.Theme{}, errProtoConvertRange
	}
	foreground, err := rgbFromWire(message.GetForeground())
	if err != nil {
		return protocol.Theme{}, err
	}
	background, err := rgbFromWire(message.GetBackground())
	if err != nil {
		return protocol.Theme{}, err
	}
	theme.HasForeground = message.GetHasForeground()
	theme.Foreground = foreground
	theme.HasBackground = message.GetHasBackground()
	theme.Background = background
	theme.TrueColor = message.GetTrueColor()
	theme.SchemeKnown = message.GetSchemeKnown()
	theme.Light = message.GetLight()
	known := message.GetPaletteKnown()
	if known > 0xFFFF {
		return protocol.Theme{}, errProtoConvertRange
	}
	theme.PaletteKnown = uint16(known)
	for i, color := range message.GetPalette() {
		converted, err := rgbFromWire(color)
		if err != nil {
			return protocol.Theme{}, err
		}
		theme.Palette[i] = converted
	}
	return theme, nil
}

func ackToWire(message protocol.Ack) (*wire.Ack, error) {
	if err := protocol.ValidateAck(message); err != nil {
		return nil, err
	}
	return &wire.Ack{Epoch: message.Epoch, State: message.State}, nil
}

func ackFromWire(message *wire.Ack) (protocol.Ack, error) {
	if message == nil {
		return protocol.Ack{}, protocol.ErrInvalidAck
	}
	ack := protocol.Ack{Epoch: message.GetEpoch(), State: message.GetState()}
	if err := protocol.ValidateAck(ack); err != nil {
		return protocol.Ack{}, err
	}
	return ack, nil
}

func imagePushToWire(message protocol.ImagePush) *wire.ImagePush {
	return &wire.ImagePush{InputSeq: message.InputSeq, Mime: message.Mime, Data: append([]byte(nil), message.Data...)}
}

func imagePushFromWire(message *wire.ImagePush) (protocol.ImagePush, error) {
	if message == nil {
		return protocol.ImagePush{}, errProtoConvertRange
	}
	return protocol.ImagePush{InputSeq: message.GetInputSeq(), Mime: message.GetMime(), Data: append([]byte(nil), message.GetData()...)}, nil
}

func clientNoticeToWire(message protocol.ClientNotice) (*wire.ClientNotice, error) {
	if err := protocol.ValidateClientNotice(message); err != nil {
		return nil, err
	}
	return &wire.ClientNotice{Action: uint32(message.Action)}, nil
}

func clientNoticeFromWire(message *wire.ClientNotice) (protocol.ClientNotice, error) {
	if message == nil {
		return protocol.ClientNotice{}, protocol.ErrInvalidClientNotice
	}
	action, err := mustUint8(message.GetAction())
	if err != nil {
		return protocol.ClientNotice{}, protocol.ErrInvalidClientNotice
	}
	notice := protocol.ClientNotice{Action: action}
	if err := protocol.ValidateClientNotice(notice); err != nil {
		return protocol.ClientNotice{}, err
	}
	return notice, nil
}

func detachedToWire(message protocol.Detached) (*wire.Detached, error) {
	if err := protocol.ValidateDetached(message); err != nil {
		return nil, err
	}
	return &wire.Detached{Reason: uint32(message.Reason)}, nil
}

func detachedFromWire(message *wire.Detached) (protocol.Detached, error) {
	if message == nil {
		return protocol.Detached{}, protocol.ErrInvalidDetached
	}
	reason, err := mustUint8(message.GetReason())
	if err != nil {
		return protocol.Detached{}, protocol.ErrInvalidDetached
	}
	detached := protocol.Detached{Reason: reason}
	if err := protocol.ValidateDetached(detached); err != nil {
		return protocol.Detached{}, err
	}
	return detached, nil
}

func killToWire(message protocol.Kill) *wire.Kill {
	return &wire.Kill{Name: message.Name, Scope: uint32(message.Scope), RequestId: message.RequestID}
}

func killFromWire(message *wire.Kill) (protocol.Kill, error) {
	if message == nil {
		return protocol.Kill{}, errProtoConvertRange
	}
	scope, err := enum8[protocol.KillScope](message.GetScope())
	if err != nil {
		return protocol.Kill{}, err
	}
	if scope != protocol.KillSession && scope != protocol.KillDaemon && scope != protocol.KillAll {
		return protocol.Kill{}, errProtoConvertRange
	}
	if message.GetRequestId() == 0 {
		return protocol.Kill{}, errProtoConvertRange
	}
	return protocol.Kill{RequestID: message.GetRequestId(), Name: message.GetName(), Scope: scope}, nil
}

func killResultToWire(message protocol.KillResult) *wire.KillResult {
	out := &wire.KillResult{RequestId: message.RequestID, Outcome: uint32(message.Outcome), Code: uint32(message.Code), Text: message.Text}
	for _, failure := range message.Failures {
		out.Failures = append(out.Failures, &wire.KillFailure{Class: failure.Class, Name: failure.Name, Text: failure.Text})
	}
	return out
}

func killResultFromWire(message *wire.KillResult) (protocol.KillResult, error) {
	if message == nil || message.GetRequestId() == 0 || message.GetOutcome() < uint32(protocol.KillSucceeded) || message.GetOutcome() > uint32(protocol.KillOutcomeUnknown) {
		return protocol.KillResult{}, errProtoConvertRange
	}
	code, err := mustUint16(message.GetCode())
	if err != nil {
		return protocol.KillResult{}, err
	}
	out := protocol.KillResult{RequestID: message.GetRequestId(), Outcome: protocol.KillOutcome(message.GetOutcome()), Code: code, Text: message.GetText()}
	for _, failure := range message.GetFailures() {
		if failure == nil {
			return protocol.KillResult{}, errProtoConvertRange
		}
		out.Failures = append(out.Failures, protocol.KillFailure{Class: failure.GetClass(), Name: failure.GetName(), Text: failure.GetText()})
	}
	return out, nil
}

func sessionsToWire(message protocol.Sessions) *wire.Sessions {
	out := &wire.Sessions{}
	for _, info := range message.Sessions {
		out.Sessions = append(out.Sessions, &wire.SessionInfo{
			SessionId: info.SessionID,
			Name:      info.Name,
			State:     uint32(info.State),
			Ephemeral: info.Ephemeral,
			Tabs:      uint32(info.Tabs),
			Attached:  info.Attached,
		})
	}
	return out
}

func sessionsFromWire(message *wire.Sessions) (protocol.Sessions, error) {
	var sessions protocol.Sessions
	if message == nil {
		return sessions, nil
	}
	for _, info := range message.GetSessions() {
		tabs := info.GetTabs()
		if tabs > 0xFFFF {
			return protocol.Sessions{}, errProtoConvertRange
		}
		state, err := enum8[protocol.SessionState](info.GetState())
		if err != nil {
			return protocol.Sessions{}, err
		}
		sessions.Sessions = append(sessions.Sessions, protocol.SessionInfo{
			SessionID: info.GetSessionId(),
			Name:      info.GetName(),
			State:     state,
			Ephemeral: info.GetEphemeral(),
			Tabs:      uint16(tabs),
			Attached:  info.GetAttached(),
		})
	}
	return sessions, nil
}

func errorToWire(message protocol.ErrorMsg) *wire.ErrorMsg {
	return &wire.ErrorMsg{Code: uint32(message.Code), Text: message.Text}
}

func errorFromWire(message *wire.ErrorMsg) (protocol.ErrorMsg, error) {
	if message == nil {
		return protocol.ErrorMsg{}, errProtoConvertRange
	}
	code, err := mustUint16(message.GetCode())
	if err != nil {
		return protocol.ErrorMsg{}, err
	}
	return protocol.ErrorMsg{Code: code, Text: message.GetText()}, nil
}

func commandResultToWire(message protocol.CommandResult) (*wire.CommandResult, error) {
	if !message.Valid() {
		return nil, errProtoConvertRange
	}
	return &wire.CommandResult{RequestId: message.RequestID, Outcome: wire.CommandOutcome(message.Outcome), Code: uint32(message.Code), Text: message.Text, Output: message.Output}, nil
}

func commandResultFromWire(message *wire.CommandResult) (protocol.CommandResult, error) {
	// The outcome is narrowed through a closed-range check before the cast:
	// a 32-bit wire value whose low byte would otherwise truncate into a valid
	// outcome (257 -> Succeeded, 259 -> Unknown) is refused, not aliased.
	if message == nil || message.GetOutcome() < wire.CommandOutcome_COMMAND_OUTCOME_SUCCEEDED || message.GetOutcome() > wire.CommandOutcome_COMMAND_OUTCOME_UNKNOWN {
		return protocol.CommandResult{}, errProtoConvertRange
	}
	code, err := mustUint16(message.GetCode())
	if err != nil {
		return protocol.CommandResult{}, err
	}
	result := protocol.CommandResult{RequestID: message.GetRequestId(), Outcome: protocol.CommandOutcome(message.GetOutcome()), Code: code, Text: message.GetText(), Output: message.GetOutput()}
	if !result.Valid() {
		return protocol.CommandResult{}, errProtoConvertRange
	}
	return result, nil
}

func uiFenceToWire(message protocol.UIFence) *wire.UIFence {
	return &wire.UIFence{ActionId: message.ActionID}
}

func uiFenceFromWire(message *wire.UIFence) (protocol.UIFence, error) {
	if message == nil {
		return protocol.UIFence{}, errProtoConvertRange
	}
	return protocol.UIFence{ActionID: message.GetActionId()}, nil
}

func selectTabToWire(message protocol.SelectTab) (*wire.SelectTab, error) {
	if err := message.Validate(); err != nil {
		return nil, err
	}
	return &wire.SelectTab{TabId: string(message.TabID)}, nil
}

func selectTabFromWire(message *wire.SelectTab) (protocol.SelectTab, error) {
	if message == nil {
		return protocol.SelectTab{}, errProtoConvertRange
	}
	selected := protocol.SelectTab{TabID: domain.TabStableID(message.GetTabId())}
	if err := selected.Validate(); err != nil {
		return protocol.SelectTab{}, err
	}
	return selected, nil
}

func uiReceiptToWire(message protocol.UIReceipt) (*wire.UIReceipt, error) {
	if err := message.Validate(); err != nil {
		return nil, err
	}
	return &wire.UIReceipt{ActionId: message.ActionID, Epoch: message.Epoch, State: message.State, ViewPublication: message.ViewPublication, Outcome: uint32(message.Outcome)}, nil
}

func uiReceiptFromWire(message *wire.UIReceipt) (protocol.UIReceipt, error) {
	if message == nil {
		return protocol.UIReceipt{}, errProtoConvertRange
	}
	outcome, err := enum8[protocol.UIReceiptOutcome](message.GetOutcome())
	if err != nil {
		return protocol.UIReceipt{}, err
	}
	receipt := protocol.UIReceipt{
		ActionID:        message.GetActionId(),
		Epoch:           message.GetEpoch(),
		State:           message.GetState(),
		ViewPublication: message.GetViewPublication(),
		Outcome:         outcome,
	}
	if err := receipt.Validate(); err != nil {
		return protocol.UIReceipt{}, err
	}
	return receipt, nil
}

func uiViewUpdateToWire(message protocol.UIViewUpdate) (*wire.UIViewUpdate, error) {
	context, err := viewContextToWire(message.Context)
	if err != nil {
		return nil, err
	}
	return &wire.UIViewUpdate{Epoch: message.Epoch, State: message.State, Context: context}, nil
}

func uiViewUpdateFromWire(message *wire.UIViewUpdate) (protocol.UIViewUpdate, error) {
	if message == nil {
		return protocol.UIViewUpdate{}, errProtoConvertRange
	}
	context, err := viewContextFromWire(message.GetContext())
	if err != nil {
		return protocol.UIViewUpdate{}, err
	}
	return protocol.UIViewUpdate{Epoch: message.GetEpoch(), State: message.GetState(), Context: context}, nil
}
