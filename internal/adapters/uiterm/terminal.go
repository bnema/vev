// Package uiterm provides a deterministic VT-backed client terminal for UI
// automation and an observation sink for interactive terminals.
package uiterm

import (
	"context"
	"errors"
	"io"
	"sync"

	vt "github.com/bnema/vev-vt"
	core "github.com/bnema/vev-vt/core"
	"github.com/bnema/vev/internal/adapters/term"
	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
)

const (
	MaxColumns      = 512
	MaxRows         = 256
	replyQueueDepth = 64
)

var (
	_ ports.Terminal            = (*Terminal)(nil)
	_ ports.UIState             = (*Terminal)(nil)
	_ ports.UIOutputTransaction = (*Terminal)(nil)
	_ ports.UIObservationSink   = (*Terminal)(nil)
)

type Terminal struct {
	mu sync.Mutex

	screen    *vt.Screen
	geometry  domain.Geometry
	context   ports.UIContext
	latest    ports.UISnapshot
	revision  uint64
	available bool
	dirty     bool
	closed    bool
	txDepth   int
	txFailed  bool

	changes chan struct{}
	replies chan []byte
	inR     *io.PipeReader
	inW     *io.PipeWriter
	cancel  context.CancelFunc
	done    chan struct{}
}

func New(ctx context.Context, geometry domain.Geometry, attachmentHandle string) (*Terminal, error) {
	geometry = geometry.NormalizePixels()
	if geometry.Cols <= 0 || geometry.Rows <= 0 || geometry.Cols > MaxColumns || geometry.Rows > MaxRows {
		return nil, errors.New("uiterm: geometry outside supported bounds")
	}
	ctx, cancel := context.WithCancel(ctx)
	inR, inW := io.Pipe()
	t := &Terminal{
		screen:    vt.NewScreen(geometry.Cols, geometry.Rows),
		geometry:  geometry,
		context:   ports.UIContext{AttachmentHandle: attachmentHandle, Generation: 1},
		available: true,
		changes:   make(chan struct{}, 1),
		replies:   make(chan []byte, replyQueueDepth),
		inR:       inR,
		inW:       inW,
		cancel:    cancel,
		done:      make(chan struct{}),
	}
	t.screen.OnResponse = func(response []byte) {
		copyResponse := append([]byte(nil), response...)
		select {
		case t.replies <- copyResponse:
		default:
			t.available = false
		}
	}
	go t.runReplies(ctx)
	return t, nil
}

func (t *Terminal) runReplies(ctx context.Context) {
	defer close(t.done)
	for {
		select {
		case <-ctx.Done():
			_ = t.inW.CloseWithError(ctx.Err())
			return
		case response := <-t.replies:
			if _, err := t.inW.Write(response); err != nil {
				return
			}
		}
	}
}

func (t *Terminal) EnterRaw() (func() error, error) {
	if _, err := t.Write(term.VisualEnterSequence()); err != nil {
		return nil, err
	}
	if err := t.Flush(); err != nil {
		return nil, err
	}
	var once sync.Once
	var restoreErr error
	return func() error {
		once.Do(func() {
			_, restoreErr = t.Write(term.VisualRestoreSequence())
			if restoreErr == nil {
				restoreErr = t.Flush()
			}
			t.Close()
		})
		return restoreErr
	}, nil
}

func (t *Terminal) Geometry() (domain.Geometry, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		return domain.Geometry{}, io.ErrClosedPipe
	}
	return t.geometry, nil
}

func (t *Terminal) ResizeEvents() <-chan domain.Geometry {
	ch := make(chan domain.Geometry)
	close(ch)
	return ch
}

func (t *Terminal) In() io.Reader  { return t.inR }
func (t *Terminal) Out() io.Writer { return t }

func (t *Terminal) Write(data []byte) (int, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		return 0, io.ErrClosedPipe
	}
	t.screen.Write(data)
	t.dirty = true
	if !t.available {
		return len(data), errors.New("uiterm: terminal response queue overflow")
	}
	return len(data), nil
}

func (t *Terminal) Flush() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		return io.ErrClosedPipe
	}
	if t.txDepth == 0 {
		t.publishLocked()
	}
	return nil
}

func (t *Terminal) BeginOutput(context ports.UIContext) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.txDepth == 0 {
		if context.AttachmentHandle == "" {
			context.AttachmentHandle = t.context.AttachmentHandle
		}
		if context.Generation == 0 {
			context.Generation = t.context.Generation
		}
		t.context = context
		t.txFailed = false
	}
	t.txDepth++
}

func (t *Terminal) EndOutput(success bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.txDepth == 0 {
		return
	}
	if !success {
		t.txFailed = true
	}
	t.txDepth--
	if t.txDepth != 0 {
		return
	}
	if t.txFailed {
		t.available = false
		return
	}
	t.publishLocked()
}

func (t *Terminal) PublishContext(context ports.UIContext) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed || !t.available {
		return ports.ErrUIUnavailable
	}
	if context.AttachmentHandle == "" {
		context.AttachmentHandle = t.context.AttachmentHandle
	}
	if context.Generation == 0 {
		context.Generation = t.context.Generation
	}
	t.context = context
	t.publishLocked()
	return nil
}

func (t *Terminal) Snapshot() (ports.UISnapshot, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if !t.available || t.latest.Revision == 0 {
		return ports.UISnapshot{}, ports.ErrUIUnavailable
	}
	return cloneSnapshot(t.latest), nil
}

func (t *Terminal) Changes() <-chan struct{} { return t.changes }

func (t *Terminal) ObserveTerminalWrite(data []byte) {
	_, _ = t.Write(data)
}

func (t *Terminal) ObserveTerminalFlush() {
	_ = t.Flush()
}

func (t *Terminal) ObserveTerminalResize(geometry domain.Geometry) {
	geometry = geometry.NormalizePixels()
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed || geometry.Cols <= 0 || geometry.Rows <= 0 || geometry.Cols > MaxColumns || geometry.Rows > MaxRows {
		t.available = false
		return
	}
	t.geometry = geometry
	t.screen.Resize(geometry.Cols, geometry.Rows)
	t.dirty = true
}

func (t *Terminal) InvalidateTerminalObservation() {
	t.mu.Lock()
	t.available = false
	t.mu.Unlock()
	t.signalChange()
}

func (t *Terminal) Close() {
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		return
	}
	t.closed = true
	t.available = false
	t.cancel()
	_ = t.inR.Close()
	t.mu.Unlock()
	<-t.done
	t.signalChange()
}

func (t *Terminal) publishLocked() {
	if !t.available {
		return
	}
	screen := t.screen.Snapshot()
	t.revision++
	t.latest = convertSnapshot(screen, t.geometry, t.context, t.revision)
	t.dirty = false
	t.signalChange()
}

func (t *Terminal) signalChange() {
	select {
	case t.changes <- struct{}{}:
	default:
	}
}

func convertSnapshot(screen vt.ScreenSnapshot, geometry domain.Geometry, context ports.UIContext, revision uint64) ports.UISnapshot {
	cells := make([]ports.UICell, 0, screen.Columns()*screen.Rows())
	for row := 0; row < screen.Rows(); row++ {
		for column := 0; column < screen.Columns(); column++ {
			cell := screen.Cell(column, row)
			text := ""
			width := 0
			if !cell.Continuation {
				text = string(cell.Rune)
				width = core.RuneWidth(cell.Rune)
			}
			cells = append(cells, ports.UICell{Text: text, Width: width, Continuation: cell.Continuation, Style: convertStyle(cell.Style)})
		}
	}
	cursor := screen.Cursor()
	modes := screen.Modes()
	return ports.UISnapshot{
		Revision: revision, Context: context,
		Columns: geometry.Cols, Rows: geometry.Rows,
		Cursor: ports.UICursor{Row: cursor.Row, Column: cursor.Col, Visible: cursor.Visible, Style: cursor.Style, StyleSet: cursor.StyleSet},
		Cells:  cells, AutoWrap: modes.AutoWrap, ApplicationCursor: modes.ApplicationCursor,
	}
}

func convertStyle(style core.Style) ports.UIStyle {
	return ports.UIStyle{
		Foreground: convertColor(style.Foreground, style.ForegroundRGB, style.HasForegroundRGB),
		Background: convertColor(style.Background, style.BackgroundRGB, style.HasBackgroundRGB),
		Bold:       style.Bold, Dim: style.Attrs&core.AttrDim != 0, Italic: style.Italic,
		Underline: style.Attrs&core.AttrUnderline != 0, UnderlineStyle: uint8(style.UnderlineStyle),
		Blink: style.Attrs&core.AttrBlink != 0, Inverse: style.Inverse,
		Strikethrough: style.Attrs&core.AttrStrikethrough != 0,
	}
}

func convertColor(index int, rgb core.RGB, hasRGB bool) ports.UIColor {
	if hasRGB {
		return ports.UIColor{Kind: ports.UIColorRGB, R: rgb.R, G: rgb.G, B: rgb.B}
	}
	if index >= 0 && index <= 255 {
		return ports.UIColor{Kind: ports.UIColorIndexed, Index: uint8(index)}
	}
	return ports.UIColor{Kind: ports.UIColorDefault}
}

func cloneSnapshot(snapshot ports.UISnapshot) ports.UISnapshot {
	snapshot.Cells = append([]ports.UICell(nil), snapshot.Cells...)
	return snapshot
}
