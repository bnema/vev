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

	renderer "github.com/bnema/vev-vt"
	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol"
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
	mu   sync.Mutex
	data []byte
	done chan struct{}
	once sync.Once
}

func newPaletteAttachReader(data []byte) *paletteAttachReader {
	return &paletteAttachReader{data: append([]byte(nil), data...), done: make(chan struct{})}
}

// Read retains unread bytes after a short read and blocks only until close.
// This keeps the test terminal reader faithful to io.Reader and makes a
// failed test teardown deterministic instead of leaving a goroutine parked.
func (r *paletteAttachReader) Read(p []byte) (int, error) {
	r.mu.Lock()
	if len(r.data) != 0 {
		n := copy(p, r.data)
		r.data = r.data[n:]
		r.mu.Unlock()
		return n, nil
	}
	r.mu.Unlock()
	<-r.done
	return 0, io.EOF
}

func (r *paletteAttachReader) close() { r.once.Do(func() { close(r.done) }) }

type paletteAttachTerminal struct {
	in     io.Reader
	out    bytes.Buffer
	resize chan domain.Geometry
}

func (t *paletteAttachTerminal) EnterRaw() (func() error, error) {
	return func() error { return nil }, nil
}
func (t *paletteAttachTerminal) Geometry() (domain.Geometry, error) {
	return domain.Geometry{Size: domain.Size{Cols: 80, Rows: 24}}, nil
}
func (t *paletteAttachTerminal) ResizeEvents() <-chan domain.Geometry { return t.resize }
func (t *paletteAttachTerminal) In() io.Reader                        { return t.in }
func (t *paletteAttachTerminal) Out() io.Writer                       { return &t.out }
func (t *paletteAttachTerminal) Flush() error                         { return nil }

type paletteAttachTransport struct {
	mu       sync.Mutex
	sent     []ports.Frame
	themes   []protocol.Theme
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
		return ports.Frame{Type: ports.MsgWelcome, Payload: ports.MarshalWelcome(protocol.Welcome{SessionID: "s"})}, nil
	}
	<-t.finalSet
	return ports.Frame{Type: ports.MsgDetached, Payload: ports.MarshalDetached(protocol.Detached{Reason: protocol.ReasonDetach})}, nil
}
func (*paletteAttachTransport) Close() error { return nil }

func TestPaletteAttachReaderPreservesShortReadRemainderAndCloses(t *testing.T) {
	reader := newPaletteAttachReader([]byte("abcdef"))
	first := make([]byte, 2)
	n, err := reader.Read(first)
	require.NoError(t, err)
	require.Equal(t, "ab", string(first[:n]))

	rest := make([]byte, 8)
	n, err = reader.Read(rest)
	require.NoError(t, err)
	require.Equal(t, "cdef", string(rest[:n]))

	reader.close()
	n, err = reader.Read(rest)
	require.Zero(t, n)
	require.ErrorIs(t, err, io.EOF)
}

func TestAttachPublishesOnlyClearedAndDefinitiveInitialPalette(t *testing.T) {
	input := newPaletteAttachReader([]byte("\x1b]10;#010203\a\x1b]11;#040506\a\x1b]4;2;#102030\a\x1b[?2031;1$y"))
	t.Cleanup(input.close)
	term := &paletteAttachTerminal{in: input, resize: make(chan domain.Geometry)}
	transport := &paletteAttachTransport{finalSet: make(chan struct{})}
	ms := &milestones{}
	runner := &Runner{term: term, clock: paletteAttachClock{}, logger: slog.New(slog.DiscardHandler)}
	attempt := &attachAttempt{
		runner: runner, transport: transport, request: AttachRequest{Intent: protocol.IntentAttach},
		milestones: ms, themeState: &terminalThemeState{},
		enterRaw:  func() error { ms.rawEntered = true; return nil },
		reconnect: &reconnectUI{term: term, rawEntered: new(bool)},
	}

	result := attempt.run(context.Background())
	require.NoError(t, result.err)
	require.Equal(t, paletteColorBatch, term.out.String())

	transport.mu.Lock()
	defer transport.mu.Unlock()
	require.Len(t, transport.sent, 3)
	require.Equal(t, []ports.MsgType{ports.MsgHello, ports.MsgTheme, ports.MsgTheme}, []ports.MsgType{
		transport.sent[0].Type, transport.sent[1].Type, transport.sent[2].Type,
	}, "the protocol requires Hello before the two palette publications")
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
