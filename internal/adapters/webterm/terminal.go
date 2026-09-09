// Package webterm connects the ordinary client terminal boundary to a browser.
package webterm

import (
	"context"
	"errors"
	"io"
	"sync"

	vt "github.com/bnema/vev-vt"
	renderer "github.com/bnema/vev-vt/core"
	"github.com/bnema/vev/internal/adapters/term"
	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
)

const (
	MaxColumns      = 512
	MaxRows         = 256
	inputQueueDepth = 64
)

// Terminal owns a VT mirror of successfully flushed client output. Input and
// terminal query replies share one ordered writer, just like a physical TTY.
type Terminal struct {
	mu       sync.Mutex
	screen   *vt.Screen
	geometry domain.Geometry
	latest   vt.ScreenSnapshot
	changes  chan struct{}
	resize   chan domain.Geometry
	input    chan []byte
	reader   *io.PipeReader
	writer   *io.PipeWriter
	cancel   context.CancelFunc
	done     chan struct{}
	closed   bool
}

var _ ports.Terminal = (*Terminal)(nil)

func New(ctx context.Context, geometry domain.Geometry) (*Terminal, error) {
	if geometry.Cols < 1 || geometry.Rows < 1 || geometry.Cols > MaxColumns || geometry.Rows > MaxRows {
		return nil, errors.New("webterm: invalid geometry")
	}
	ctx, cancel := context.WithCancel(ctx)
	reader, writer := io.Pipe()
	t := &Terminal{screen: vt.NewScreen(geometry.Cols, geometry.Rows), geometry: geometry, changes: make(chan struct{}, 1), resize: make(chan domain.Geometry, 1), input: make(chan []byte, inputQueueDepth), reader: reader, writer: writer, cancel: cancel, done: make(chan struct{})}
	t.screen.OnResponse = func(data []byte) {
		select {
		case t.input <- append([]byte(nil), data...):
		default:
			cancel()
		}
	}
	// Match #terminal's default colors in app.css so the ordinary theme
	// discovery enables native inactive-pane and modal-backdrop dimming.
	t.screen.SetDefaultColors(renderer.RGB{R: 216, G: 216, B: 216}, renderer.RGB{R: 16, G: 16, B: 16}, true)
	t.latest = t.screen.Snapshot()
	go func() {
		defer close(t.done)
		stop := context.AfterFunc(ctx, func() { _ = writer.CloseWithError(ctx.Err()); _ = reader.CloseWithError(ctx.Err()) })
		defer stop()
		for {
			select {
			case <-ctx.Done():
				return
			case data := <-t.input:
				if _, err := writer.Write(data); err != nil {
					return
				}
			}
		}
	}()
	return t, nil
}

func (t *Terminal) EnterRaw() (func() error, error) {
	if _, err := t.Write(term.VisualEnterSequence()); err != nil {
		return nil, err
	}
	if err := t.Flush(); err != nil {
		return nil, err
	}
	return func() error { return nil }, nil
}
func (t *Terminal) In() io.Reader                        { return t.reader }
func (t *Terminal) Out() io.Writer                       { return t }
func (t *Terminal) ResizeEvents() <-chan domain.Geometry { return t.resize }
func (t *Terminal) Changes() <-chan struct{}             { return t.changes }
func (t *Terminal) Geometry() (domain.Geometry, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		return domain.Geometry{}, io.ErrClosedPipe
	}
	return t.geometry, nil
}
func (t *Terminal) Write(data []byte) (int, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		return 0, io.ErrClosedPipe
	}
	t.screen.Write(data)
	return len(data), nil
}
func (t *Terminal) Flush() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		return io.ErrClosedPipe
	}
	t.latest = t.screen.Snapshot()
	select {
	case t.changes <- struct{}{}:
	default:
	}
	return nil
}
func (t *Terminal) Snapshot() vt.ScreenSnapshot {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.latest
}
func (t *Terminal) Resize(geometry domain.Geometry) error {
	if geometry.Cols < 1 || geometry.Rows < 1 || geometry.Cols > MaxColumns || geometry.Rows > MaxRows {
		return errors.New("webterm: invalid geometry")
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		return io.ErrClosedPipe
	}
	if t.geometry == geometry {
		return nil
	}
	t.geometry = geometry
	t.screen.SetGeometry(vt.Geometry{Cols: geometry.Cols, Rows: geometry.Rows, PixelWidth: geometry.PixelWidth, PixelHeight: geometry.PixelHeight})
	select {
	case <-t.resize:
	default:
	}
	t.resize <- geometry
	return nil
}
func (t *Terminal) Send(ctx context.Context, data []byte) error {
	if len(data) == 0 {
		return nil
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.done:
		return io.ErrClosedPipe
	case t.input <- append([]byte(nil), data...):
		return nil
	default:
		return errors.New("webterm: input queue full")
	}
}
func (t *Terminal) Close() {
	t.mu.Lock()
	t.closed = true
	t.mu.Unlock()
	t.cancel()
	_ = t.reader.Close()
	_ = t.writer.Close()
	<-t.done
}
