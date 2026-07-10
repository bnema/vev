package daemon

import (
	"errors"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
	themeui "github.com/bnema/vev/internal/usecase/theme"
	"github.com/bnema/vev/pkg/renderer"
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

func TestComposeFloatingFrameDoesNotMutateSourceAndDamagesTitle(t *testing.T) {
	p := newPane("floating", nil, domain.Size{Cols: 6, Rows: 3})
	p.screen.Frame.Set(0, 0, renderer.Cell{Rune: 'F'})
	p.title.processName, p.title.terminalTitle, p.title.generation = "nvim", "project/main.go", 1
	base := renderer.NewFrame(40, 12)
	base.Set(2, 2, renderer.Cell{Rune: 'B'})
	content := domain.Rect{Y: 1, Width: 40, Height: 10}
	cache := &composedFrameCache{}
	frame, _ := composeFloatingFrame(base, nil, p, 1, content, domain.FloatingConfig{Width: 80, Height: 80}, tabLayoutSnapshot{}, themeui.Theme{}, cache, false)
	require.Equal(t, 'B', base.At(2, 2).Rune, "backdrop must only touch the composed destination")
	geometry := calculateFloatingGeometry(content, domain.FloatingConfig{Width: 80, Height: 80})
	require.Equal(t, '┌', frame.At(geometry.Bounds.X, geometry.Bounds.Y).Rune)
	require.Equal(t, 'F', frame.At(geometry.Inner.X, geometry.Inner.Y).Rune)
	var gotTitle strings.Builder
	for x := geometry.Bounds.X + 2; x < geometry.Bounds.X+geometry.Bounds.Width-2; x++ {
		gotTitle.WriteRune(frame.At(x, geometry.Bounds.Y).Rune)
	}
	require.Equal(t, "nvim: project/main.go", strings.TrimRight(gotTitle.String(), "─"))

	p.mu.Lock()
	p.title.generation++
	p.mu.Unlock()
	_, damage := composeFloatingFrame(base, nil, p, 1, content, domain.FloatingConfig{Width: 80, Height: 80}, tabLayoutSnapshot{}, themeui.Theme{}, cache, false)
	require.Len(t, damage, 1)
	require.Equal(t, geometry.Bounds.Y, damage[0].Y)
	require.Equal(t, 1, damage[0].Height)
}

func TestComposeFloatingFrameRendersPaneOwnedCommandFallback(t *testing.T) {
	d := newTestDaemon(t, nil, stubClock{})
	d.shell = "/usr/bin/fish"
	p := newPane("floating", nil, domain.Size{Cols: 6, Rows: 3})
	p.title.displayFallback = floatingCommandFallback("btop --utf", d.shell)
	require.Equal(t, "btop", d.refreshPaneDisplayTitle(nil, p, true))

	base := renderer.NewFrame(40, 12)
	content := domain.Rect{Y: 1, Width: 40, Height: 10}
	cfg := domain.FloatingConfig{Width: 80, Height: 80}
	frame, _ := composeFloatingFrame(base, nil, p, 1, content, cfg, tabLayoutSnapshot{}, themeui.Theme{}, &composedFrameCache{}, false)
	geometry := calculateFloatingGeometry(content, cfg)
	var gotTitle strings.Builder
	for x := geometry.Bounds.X + 2; x < geometry.Bounds.X+geometry.Bounds.Width-2; x++ {
		gotTitle.WriteRune(frame.At(x, geometry.Bounds.Y).Rune)
	}
	require.Equal(t, "btop", strings.TrimRight(gotTitle.String(), "─"))
}

func TestComposeFloatingFrameSynchronizesWithPTYReader(t *testing.T) {
	p := newPane("floating", nil, domain.Size{Cols: 80, Rows: 24})
	base := renderer.NewFrame(80, 24)
	content := domain.Rect{Width: 80, Height: 24}
	cfg := domain.FloatingConfig{Width: 100, Height: 100}

	start := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		<-start
		for range 500 {
			// This is the same pane lock used by ptyReader around Screen.Write.
			p.mu.Lock()
			p.screen.Write([]byte("\x1b[Hreader"))
			p.mu.Unlock()
		}
	}()
	go func() {
		defer wg.Done()
		<-start
		cache := &composedFrameCache{}
		for range 500 {
			composeFloatingFrame(base, nil, p, 1, content, cfg, tabLayoutSnapshot{}, themeui.Theme{}, cache, false)
		}
	}()
	close(start)
	wg.Wait()
}

func TestDrawFloatingBorderOmitsTinyAxes(t *testing.T) {
	frame := renderer.NewFrame(2, 1)
	drawFloatingBorder(frame, domain.Rect{Width: 2, Height: 1}, "title", renderer.Style{})
	for _, cell := range frame.Cells {
		require.Equal(t, renderer.BlankCell().Rune, cell.Rune)
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
