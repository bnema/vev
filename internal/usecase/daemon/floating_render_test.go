package daemon

import (
	"errors"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
)

func TestCalculateFloatingGeometry(t *testing.T) {
	tests := []struct {
		name    string
		content domain.Rect
		cfg     domain.FloatingConfig
		want    floatingGeometry
	}{
		{"eighty percent centered", domain.Rect{X: 3, Y: 1, Width: 101, Height: 51}, domain.FloatingConfig{Width: 80, Height: 80}, floatingGeometry{Bounds: domain.Rect{X: 13, Y: 6, Width: 80, Height: 40}, Inner: domain.Rect{X: 14, Y: 7, Width: 78, Height: 38}}},
		{"one percent clamps to one", domain.Rect{Y: 1, Width: 100, Height: 20}, domain.FloatingConfig{Width: 1, Height: 1}, floatingGeometry{Bounds: domain.Rect{X: 49, Y: 10, Width: 1, Height: 1}, Inner: domain.Rect{X: 49, Y: 10, Width: 1, Height: 1}}},
		{"full size", domain.Rect{Y: 1, Width: 100, Height: 20}, domain.FloatingConfig{Width: 100, Height: 100}, floatingGeometry{Bounds: domain.Rect{Y: 1, Width: 100, Height: 20}, Inner: domain.Rect{X: 1, Y: 2, Width: 98, Height: 18}}},
		{"tiny axes omit borders", domain.Rect{X: 2, Y: 1, Width: 2, Height: 1}, domain.FloatingConfig{Width: 100, Height: 100}, floatingGeometry{Bounds: domain.Rect{X: 2, Y: 1, Width: 2, Height: 1}, Inner: domain.Rect{X: 2, Y: 1, Width: 2, Height: 1}}},
		{"percent clamps", domain.Rect{Width: 10, Height: 10}, domain.FloatingConfig{Width: 101, Height: -1}, floatingGeometry{Bounds: domain.Rect{Width: 10, Height: 1, Y: 4}, Inner: domain.Rect{X: 1, Width: 8, Y: 4, Height: 1}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, calculateFloatingGeometry(tt.content, tt.cfg))
		})
	}
}

func TestResizeFloatingPaneFailureAndSerialization(t *testing.T) {
	t.Run("failure preserves state", func(t *testing.T) {
		pty := &resizePTY{err: errors.New("nope")}
		p := newPane("floating", pty, domain.Size{Cols: 5, Rows: 4})
		p.rect = domain.Rect{X: 8, Y: 3, Width: 5, Height: 4}
		d := newTestDaemon(t, nil, stubClock{})
		require.False(t, d.resizeFloatingPane(p, domain.Rect{X: 2, Y: 2, Width: 9, Height: 7}))
		require.Equal(t, domain.Rect{X: 8, Y: 3, Width: 5, Height: 4}, p.rect)
		require.Equal(t, 5, p.screen.Frame.Width)
		require.Equal(t, 4, p.screen.Frame.Height)
	})

	t.Run("competing resizes serialize", func(t *testing.T) {
		pty := &resizePTY{entered: make(chan struct{}), release: make(chan struct{})}
		p := newPane("floating", pty, domain.Size{Cols: 2, Rows: 2})
		d := newTestDaemon(t, nil, stubClock{})
		first := domain.Rect{Width: 4, Height: 3}
		second := domain.Rect{Width: 8, Height: 6}
		done1 := make(chan bool, 1)
		done2 := make(chan bool, 1)
		go func() { done1 <- d.resizeFloatingPane(p, first) }()
		<-pty.entered
		go func() { done2 <- d.resizeFloatingPane(p, second) }()
		require.Never(t, func() bool { return pty.calls() > 1 }, 30*time.Millisecond, time.Millisecond)
		close(pty.release)
		require.True(t, <-done1)
		require.True(t, <-done2)
		require.Equal(t, second, p.rect)
		require.Equal(t, []domain.Size{{Cols: 4, Rows: 3}, {Cols: 8, Rows: 6}}, pty.sizes())
	})
}

type resizePTY struct {
	mu      sync.Mutex
	resizes []domain.Size
	err     error
	entered chan struct{}
	release chan struct{}
}

func (p *resizePTY) Resize(sz domain.Size) error {
	p.mu.Lock()
	p.resizes = append(p.resizes, sz)
	n := len(p.resizes)
	p.mu.Unlock()
	if n == 1 && p.entered != nil {
		close(p.entered)
		<-p.release
	}
	return p.err
}
func (p *resizePTY) calls() int { p.mu.Lock(); defer p.mu.Unlock(); return len(p.resizes) }
func (p *resizePTY) sizes() []domain.Size {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]domain.Size(nil), p.resizes...)
}
func (*resizePTY) Read([]byte) (int, error)     { return 0, io.EOF }
func (*resizePTY) Write(b []byte) (int, error)  { return len(b), nil }
func (*resizePTY) Close() error                 { return nil }
func (*resizePTY) Pid() int                     { return 0 }
func (*resizePTY) ForegroundPgid() (int, error) { return 0, nil }
