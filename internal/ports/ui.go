package ports

import (
	"context"
	"errors"
	"time"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/protocol"
)

// UIPresentationStatus describes what the client is currently presenting.
type UIPresentationStatus string

const (
	UIStatusAttached      UIPresentationStatus = "attached"
	UIStatusTransitioning UIPresentationStatus = "transitioning"
	UIStatusReconnecting  UIPresentationStatus = "reconnecting"
	UIStatusDetached      UIPresentationStatus = "detached"
)

// UIColorKind identifies how a terminal color is represented.
type UIColorKind uint8

const (
	UIColorDefault UIColorKind = iota
	UIColorIndexed
	UIColorRGB
)

// UIColor is an owned terminal color value.
type UIColor struct {
	Kind  UIColorKind
	Index uint8
	R     uint8
	G     uint8
	B     uint8
}

// UIStyle is the rendered style of one terminal cell.
type UIStyle struct {
	Foreground     UIColor
	Background     UIColor
	Bold           bool
	Dim            bool
	Italic         bool
	Underline      bool
	UnderlineStyle uint8
	Blink          bool
	Inverse        bool
	Strikethrough  bool
}

// UICell is one row-major viewport cell. Continuation cells have Width zero.
type UICell struct {
	Text         string
	Width        int
	Continuation bool
	Style        UIStyle
}

// UICursor is the cursor captured with a viewport revision.
type UICursor struct {
	Row      int
	Column   int
	Visible  bool
	Style    int
	StyleSet bool
}

// UIContext binds rendered cells to client and daemon semantic state.
type UIContext struct {
	AttachmentHandle string
	Generation       uint64
	Route            protocol.CommittedRouteIdentity
	TabID            domain.TabStableID
	FocusedPaneID    domain.PaneStableID
	OutputEpoch      uint64
	OutputState      uint64
	ViewRevision     uint64
	ViewPublication  uint64
	Status           UIPresentationStatus
}

// UISnapshot is an immutable, owned viewport publication.
type UISnapshot struct {
	Revision          uint64
	Context           UIContext
	Columns           int
	Rows              int
	Cursor            UICursor
	Cells             []UICell
	AutoWrap          bool
	ApplicationCursor bool
}

// UIError reports a sanitized operation failure without captured content.
type UIError struct {
	Code     string
	Accepted bool
	ActionID uint64
}

func (e *UIError) Error() string { return e.Code }

// UIActionRequest contains one validated operation, not raw terminal bytes.
type UIActionRequest struct {
	Attachment string
	Generation uint64
	Keys       []string
	Text       string
	Timeout    time.Duration
}

type UIActionResult struct {
	ActionID uint64
	Accepted bool
	Status   string
	Revision uint64
	Context  UIContext
}

type UIExpect struct {
	TextContains *string
	Session      *protocol.ExactSessionTarget
	Focus        *UIFocus
	Status       *UIPresentationStatus
}

type UIFocus struct {
	TabID  domain.TabStableID
	PaneID domain.PaneStableID
}

type UIWaitRequest struct {
	Attachment  string
	AfterAction uint64
	Expect      UIExpect
	Timeout     time.Duration
}

type UIWaitResult struct {
	ActionID     uint64
	ActionStatus string
	Revision     uint64
	Context      UIContext
}

// UIService is attachment-scoped. Implementations bound actions and waits;
// adapters own permissions, decoding, socket quotas and response serialization.
type UIService interface {
	Capture(attachment string) (UISnapshot, error)
	Action(ctx context.Context, request UIActionRequest) (UIActionResult, error)
	Wait(ctx context.Context, request UIWaitRequest) (UIWaitResult, error)
}

var ErrUIUnavailable = errors.New("UI capture unavailable")

// UIObservationSink receives successfully emitted terminal bytes and flush
// boundaries. Implementations must copy bytes before returning and never block.
type UIObservationSink interface {
	ObserveTerminalWrite(data []byte)
	ObserveTerminalFlush()
	ObserveTerminalResize(geometry domain.Geometry)
	InvalidateTerminalObservation()
}

// UIOutputTransaction binds terminal output to coherent semantic context.
type UIOutputTransaction interface {
	BeginOutput(context UIContext)
	EndOutput(success bool)
	PublishContext(context UIContext) error
}

// UIState exposes the latest immutable publication and a coalesced change
// signal. Snapshot returns ErrUIUnavailable rather than stale state.
type UIState interface {
	Snapshot() (UISnapshot, error)
	Changes() <-chan struct{}
}
