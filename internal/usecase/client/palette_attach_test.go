package client

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/pkg/renderer"
)

type paletteAttachClock struct{}

func (paletteAttachClock) Now() time.Time { return time.Time{} }
func (paletteAttachClock) NewTimer(time.Duration) ports.Timer {
	return paletteAttachTimer{ch: make(chan time.Time)}
}

type paletteAttachTimer struct{ ch chan time.Time }

func (t paletteAttachTimer) C() <-chan time.Time    { return t.ch }
func (paletteAttachTimer) Reset(time.Duration) bool { return false }
func (paletteAttachTimer) Stop() bool               { return true }

type paletteAttachReader struct {
	data []byte
	once sync.Once
}

func (r *paletteAttachReader) Read(p []byte) (int, error) {
	read := false
	r.once.Do(func() { read = true })
	if read {
		return copy(p, r.data), nil
	}
	select {}
}

type paletteAttachTerminal struct {
	in     io.Reader
	out    bytes.Buffer
	resize chan domain.Size
}

func (t *paletteAttachTerminal) EnterRaw() (func() error, error) {
	return func() error { return nil }, nil
}
func (t *paletteAttachTerminal) Size() (domain.Size, error) {
	return domain.Size{Cols: 80, Rows: 24}, nil
}
func (t *paletteAttachTerminal) ResizeEvents() <-chan domain.Size { return t.resize }
func (t *paletteAttachTerminal) In() io.Reader                    { return t.in }
func (t *paletteAttachTerminal) Out() io.Writer                   { return &t.out }
func (t *paletteAttachTerminal) Flush() error                     { return nil }

type paletteAttachTransport struct {
	mu       sync.Mutex
	sent     []ports.Frame
	themes   []ports.Theme
	finals   int
	finalSet chan struct{}
}

func (t *paletteAttachTransport) Send(frame ports.Frame) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.sent = append(t.sent, frame)
	if frame.Type == ports.MsgTheme {
		theme, err := ports.UnmarshalTheme(frame.Payload)
		if err != nil {
			return err
		}
		t.themes = append(t.themes, theme)
		if theme.HasForeground && theme.HasBackground {
			t.finals++
			if t.finals == 1 {
				close(t.finalSet)
			}
		}
	}
	return nil
}

func (t *paletteAttachTransport) Recv() (ports.Frame, error) {
	t.mu.Lock()
	first := len(t.sent) == 1
	t.mu.Unlock()
	if first {
		return ports.Frame{Type: ports.MsgWelcome, Payload: ports.MarshalWelcome(ports.Welcome{SessionID: "s"})}, nil
	}
	<-t.finalSet
	return ports.Frame{Type: ports.MsgDetached, Payload: ports.MarshalDetached(ports.Detached{Reason: ports.ReasonDetach})}, nil
}
func (*paletteAttachTransport) Close() error { return nil }

func TestAttachPublishesOnlyClearedAndDefinitiveInitialPalette(t *testing.T) {
	input := &paletteAttachReader{data: []byte("\x1b]10;#010203\a\x1b]11;#040506\a\x1b]4;2;#102030\a\x1b[?2031;1$y")}
	term := &paletteAttachTerminal{in: input, resize: make(chan domain.Size)}
	transport := &paletteAttachTransport{finalSet: make(chan struct{})}
	ms := &milestones{}
	runner := &Runner{term: term, clock: paletteAttachClock{}, logger: slog.New(slog.DiscardHandler)}
	attempt := &attachAttempt{
		runner: runner, transport: transport, request: AttachRequest{Intent: ports.IntentAttach},
		milestones: ms, themeState: &terminalThemeState{},
		enterRaw:  func() error { ms.rawEntered = true; return nil },
		reconnect: &reconnectUI{term: term, rawEntered: new(bool)},
	}

	result := attempt.run(context.Background())
	require.NoError(t, result.err)
	require.Equal(t, paletteColorBatch, term.out.String())

	transport.mu.Lock()
	defer transport.mu.Unlock()
	require.Len(t, transport.themes, 2)
	cleared, definitive := transport.themes[0], transport.themes[1]
	require.Zero(t, cleared.PaletteKnown)
	require.False(t, cleared.HasForeground)
	require.True(t, definitive.HasForeground)
	require.Equal(t, renderer.RGB{R: 1, G: 2, B: 3}, definitive.Foreground)
	require.True(t, definitive.HasBackground)
	require.Equal(t, renderer.RGB{R: 4, G: 5, B: 6}, definitive.Background)
	require.Equal(t, uint16(1<<2), definitive.PaletteKnown)
}
