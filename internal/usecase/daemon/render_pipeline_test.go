package daemon

import (
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/usecase/layout"
	"github.com/bnema/vev/internal/usecase/ui"
	"github.com/bnema/vev/pkg/renderer"
	"github.com/bnema/vev/pkg/vt"
	"github.com/stretchr/testify/require"
)

// scriptedReplayTransport is intentionally a ports.Transport only: replay
// assertions must not depend on any adapter's framing implementation.
type scriptedReplayTransport struct {
	t      *testing.T
	frames []ports.Frame
	next   int
}

func (s *scriptedReplayTransport) Send(got ports.Frame) error {
	s.t.Helper()
	if s.next >= len(s.frames) {
		s.t.Errorf("unexpected frame %#v", got)
		return nil
	}
	want := s.frames[s.next]
	s.next++
	require.Equal(s.t, want.Type, got.Type)
	require.Equal(s.t, want.Payload, got.Payload)
	return nil
}
func (*scriptedReplayTransport) Recv() (ports.Frame, error) { return ports.Frame{}, io.EOF }
func (*scriptedReplayTransport) Close() error               { return nil }

func TestTransportReplayFinalShadowAndTerminalBytes(t *testing.T) {
	frames := []ports.Frame{
		{Type: ports.MsgOutput, Payload: ports.MarshalOutput(ports.Output{BaseStateNum: 0, NewStateNum: 1, Data: []byte("\x1b[2J\x1b[Hone\r\ntwo")})},
		{Type: ports.MsgOutput, Payload: ports.MarshalOutput(ports.Output{BaseStateNum: 1, NewStateNum: 2, EchoAck: 7, Data: []byte("\x1b[2;1HTWO")})},
	}
	transport := &scriptedReplayTransport{t: t, frames: frames}
	terminal := vt.NewScreen(8, 3)
	var transcript []byte
	for _, frame := range frames {
		require.NoError(t, transport.Send(frame))
		output, err := ports.UnmarshalOutput(frame.Payload)
		require.NoError(t, err)
		require.Equal(t, frame.Payload, ports.MarshalOutput(output), "output payload must remain byte exact")
		transcript = append(transcript, output.Data...)
		terminal.Write(output.Data)
	}
	require.Equal(t, len(frames), transport.next)
	require.Equal(t, "\x1b[2J\x1b[Hone\r\ntwo\x1b[2;1HTWO", string(transcript))
	require.Equal(t, []string{"one     ", "TWO     ", "        "}, frameRows(terminal.Frame), "terminal replay is the final renderer shadow")
}

func TestCapturePaneRenderStateOwnsVisibleFrameAndConsumesDamage(t *testing.T) {
	p := newPane("p", nil, domain.Size{Cols: 8, Rows: 2})
	p.screen.ClearDamage()
	p.screen.Write([]byte("old"))

	p.mu.Lock()
	captured := capturePaneRenderStateLocked(p, domain.Rect{Width: 3, Height: 1}, damageCaptureConsume)
	p.mu.Unlock()

	require.Equal(t, 3, captured.frame.Width)
	require.Equal(t, 1, captured.frame.Height)
	require.Empty(t, p.screen.Damage())
	p.mu.Lock()
	p.screen.Write([]byte("new"))
	p.mu.Unlock()
	require.Equal(t, 'o', captured.frame.At(0, 0).Rune, "capture must not alias the mutable VT frame")
}

func TestCapturePaneRenderStatePreviewIsNonDestructive(t *testing.T) {
	p := newPane("p", nil, domain.Size{Cols: 8, Rows: 2})
	p.screen.ClearDamage()
	p.screen.Write([]byte("preview"))

	p.mu.Lock()
	_ = capturePaneRenderStateLocked(p, domain.Rect{Width: 8, Height: 2}, damageCapturePreview)
	p.mu.Unlock()

	require.NotEmpty(t, p.screen.Damage())
}

func TestCapturePaneRenderStateMalformedDamageFallsBackToFullRedraw(t *testing.T) {
	p := newPane("p", nil, domain.Size{Cols: 8, Rows: 2})
	p.screen.ClearDamage()
	p.screen.Write([]byte("x"))
	p.screen.Damage()[0] = renderer.Damage{Kind: renderer.DamageText, X: -1, Y: 0, Width: 4, Height: 1}

	p.mu.Lock()
	captured := capturePaneRenderStateLocked(p, domain.Rect{Width: 8, Height: 2}, damageCaptureConsume)
	p.mu.Unlock()

	require.Equal(t, []renderer.Damage{renderer.FullRedraw()}, captured.damage)
}

func TestCapturePaneRenderStateCollapsedConsumesBoundedly(t *testing.T) {
	p := newPane("p", nil, domain.Size{Cols: 8, Rows: 2})
	p.screen.ClearDamage()
	p.screen.Write([]byte("hidden"))

	p.mu.Lock()
	captured := capturePaneRenderStateLocked(p, domain.Rect{}, damageCaptureConsume)
	p.mu.Unlock()

	require.Zero(t, captured.frame.Width)
	require.Zero(t, captured.frame.Height)
	require.Empty(t, p.screen.Damage(), "capture owner cleans pending hidden-pane damage")
}

func TestPaintACKBlockedDoesNotDestructivelyCapture(t *testing.T) {
	pty, release := newBlockingPTY(t)
	t.Cleanup(release)
	d, sess, _, sends := newManualSessionWithPTYs(t, pty)
	ac := sess.client
	ac.sendMu.Lock()
	ac.output.next = ac.output.maxOutstanding
	ac.output.acked = 0
	ac.sendMu.Unlock()
	p := sess.tabs[0].focusedPane()
	p.mu.Lock()
	p.screen.ClearDamage()
	p.screen.Write([]byte("blocked"))
	p.mu.Unlock()

	d.paint(sess, ac, false)

	p.mu.Lock()
	require.NotEmpty(t, p.screen.Damage())
	p.mu.Unlock()
	select {
	case frame := <-sends:
		t.Fatalf("ACK-blocked paint sent %#v", frame)
	default:
	}
}

func TestComposeFrameUsesCapturedValuesAfterOwnersMutate(t *testing.T) {
	state := &capturedRenderState{
		layout: capturedTabLayout{area: domain.Rect{Width: 4, Height: 1}, valid: true},
		panes: []capturedPaneRenderState{{
			id: "p", frame: func() renderer.Frame {
				f := renderer.NewFrame(4, 1)
				for i, r := range "old " {
					f.Set(i, 0, renderer.Cell{Rune: r, Style: renderer.DefaultStyle()})
				}
				return f
			}(), placement: layout.Placement{ID: "p", Content: domain.Rect{Width: 4, Height: 1}}, focused: true,
			damage: []renderer.Damage{renderer.FullRedraw()},
		}},
		bars:   barState{},
		cursor: capturedCursorInputs{visible: true, renderable: true, content: domain.Rect{Width: 4, Height: 1}},
	}
	cache := composeCacheInput{}
	out := composeFrame(*state, cache)
	state.panes[0].frame.Set(0, 0, renderer.Cell{Rune: 'X', Style: renderer.DefaultStyle()})
	require.Equal(t, "old ", rowText(out.frame.Row(1)))
	require.False(t, out.cursor.hidden)
}

func TestProductionRenderUsesOnlyTypedComposeEntryPoint(t *testing.T) {
	for _, name := range []string{"render.go", "floating_render.go", "picker.go"} {
		data, err := os.ReadFile(filepath.Join(".", name))
		require.NoError(t, err)
		source := string(data)
		for _, forbidden := range []string{
			"func composeClientFrame", "func composeTabFrame", "func composeFloatingFrame",
		} {
			require.NotContains(t, source, forbidden, "%s retains duplicate production composition path", name)
		}
	}
	data, err := os.ReadFile("render_pipeline.go")
	require.NoError(t, err)
	body := string(data)
	start := strings.Index(body, "func composeFrame(")
	require.NotEqual(t, -1, start)
	end := strings.Index(body[start:], "\nfunc ")
	if end > 0 {
		body = body[start : start+end]
	} else {
		body = body[start:]
	}
	for _, forbidden := range []string{".Lock()", ".Unlock()", ".Send(", ".prepare("} {
		require.NotContains(t, body, forbidden, "composeFrame must remain ownership- and transport-free")
	}
}

func TestComposeEmitExactReplayTiledFloatingBarsOverlayAndCursor(t *testing.T) {
	paneFrame := renderer.NewFrame(12, 5)
	for y, text := range []string{"AAAAAAAAAAAA", "BBBBBBBBBBBB", "CCCCCCCCCCCC", "DDDDDDDDDDDD", "EEEEEEEEEEEE"} {
		for x, r := range text {
			paneFrame.Set(x, y, renderer.Cell{Rune: r, Style: renderer.DefaultStyle()})
		}
	}
	floatingFrame := renderer.NewFrame(4, 1)
	for x, r := range "FLOAT"[:4] {
		floatingFrame.Set(x, 0, renderer.Cell{Rune: r, Style: renderer.DefaultStyle()})
	}
	modalInner := renderer.NewFrame(6, 1)
	for x, r := range "PROMPT" {
		modalInner.Set(x, 0, renderer.Cell{Rune: r, Style: renderer.DefaultStyle()})
	}
	state := capturedRenderState{
		reset:    true,
		layout:   capturedTabLayout{area: domain.Rect{Width: 12, Height: 5}, valid: true, focus: "p", placements: []layout.Placement{{ID: "p", Content: domain.Rect{Width: 12, Height: 5}}}},
		panes:    []capturedPaneRenderState{{id: "p", frame: paneFrame, placement: layout.Placement{ID: "p", Content: domain.Rect{Width: 12, Height: 5}}, focused: true, damage: []renderer.Damage{renderer.FullRedraw()}}},
		floating: capturedFloatingRenderState{visible: true, pane: capturedPaneRenderState{id: "f", frame: floatingFrame}, geometry: floatingGeometry{Bounds: domain.Rect{X: 3, Y: 1, Width: 6, Height: 3}, Inner: domain.Rect{X: 4, Y: 2, Width: 4, Height: 1}}, title: "float", generation: 1},
		bars:     barState{status: statusSnapshot{session: "sess", tabs: []statusTab{{name: "tab", active: true}}}, topRight: "R", bottomRight: "B"},
		overlays: capturedOverlayRenderState{promptActive: true, prompt: capturedModal{modal: ui.Modal{FixedWidth: 8, FixedHeight: 3, Title: "Prompt"}, inner: modalInner}},
		cursor:   capturedCursorInputs{row: 1, col: 2, visible: true, renderable: true, content: domain.Rect{X: 4, Y: 2, Width: 4, Height: 1}},
	}
	composed := composeFrame(state, composeCacheInput{})
	require.Equal(t, []string{" tab       R", "AAAAAAAAAAAA", "BB┌Prompt┐BB", "CC│PROMPT│CC", "DD└──────┘DD", "EEEEEEEEEEEE", " sess      B"}, frameRows(composed.frame))
	require.True(t, composed.cursor.hidden, "overlay owns cursor visibility")

	stream := newOutputStateStream()
	prepared, err := stream.prepare(composed.frame, composed.damage, composed.reset)
	require.NoError(t, err)
	terminalBytes := append(append([]byte(nil), prepared.data...), (&attachedClient{}).encodeCursorTail(composed.cursor, true)...)
	require.Equal(t, "\x1b[1;1H\x1b[0;7m tab \x1b[0m      R\x1b[2;1HAAAAAAAAAAAA\x1b[3;1HBB┌Prompt┐BB\x1b[4;1HCC│PROMPT│CC\x1b[5;1HDD└──────┘DD\x1b[6;1HEEEEEEEEEEEE\x1b[7;1H\x1b[0;7m sess \x1b[0m     B\x1b[0m\x1b[?25l", string(terminalBytes))
	client := vt.NewScreen(composed.frame.Width, composed.frame.Height)
	client.Write(terminalBytes)
	require.Equal(t, frameRows(composed.frame), frameRows(client.Frame))
	again, err := stream.renderer.Draw(composed.frame, nil)
	require.NoError(t, err)
	require.Empty(t, again, "renderer shadow must exactly equal the composed frame")
}

func frameRows(frame renderer.Frame) []string {
	rows := make([]string, frame.Height)
	for y := range rows {
		rows[y] = rowText(frame.Row(y))
	}
	return rows
}

func TestComposeEmitExactReplaySafeAndUnsafeScroll(t *testing.T) {
	for _, tt := range []struct {
		name      string
		placement layout.Placement
		initial   []string
		scrolled  []string
		damage    []renderer.Damage
		wantBytes string
	}{
		{name: "safe full width", placement: layout.Placement{ID: "p", Content: domain.Rect{Width: 4, Height: 3}}, initial: []string{"AAAA", "BBBB", "CCCC"}, scrolled: []string{"BBBB", "CCCC", "N   "}, damage: []renderer.Damage{{Kind: renderer.DamageScrollUp, X: 0, Y: 0, Width: 4, Height: 3, Count: 1}, {Kind: renderer.DamageText, X: 0, Y: 2, Width: 4, Height: 1}}, wantBytes: "\x1b[0m\x1b[2;4r\x1b[4;1H\n\x1b[r\x1b[4;1HN\x1b[K\x1b[0m"},
		{name: "unsafe partial width", placement: layout.Placement{ID: "p", Content: domain.Rect{X: 2, Width: 2, Height: 3}}, initial: []string{"AA", "BB", "CC"}, scrolled: []string{"BB", "CC", "N "}, damage: []renderer.Damage{{Kind: renderer.DamageText, X: 2, Y: 0, Width: 2, Height: 3}}, wantBytes: "\x1b[2;3HBB\x1b[3;3HCC\x1b[4;3HN \x1b[0m"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			makeState := func(rows []string, damage []renderer.Damage, reset bool) capturedRenderState {
				f := renderer.NewFrame(tt.placement.Content.Width, tt.placement.Content.Height)
				for y, text := range rows {
					for x, r := range text {
						f.Set(x, y, renderer.Cell{Rune: r, Style: renderer.DefaultStyle()})
					}
				}
				return capturedRenderState{reset: reset, layout: capturedTabLayout{area: domain.Rect{Width: 4, Height: 3}, valid: true, focus: "p", placements: []layout.Placement{tt.placement}}, panes: []capturedPaneRenderState{{id: "p", frame: f, placement: tt.placement, focused: true, damage: damage}}, cursor: capturedCursorInputs{visible: false}}
			}
			stream := newOutputStateStream()
			first := composeFrame(makeState(tt.initial, []renderer.Damage{renderer.FullRedraw()}, true), composeCacheInput{})
			firstBytes, err := stream.render(first.frame, first.damage, first.reset)
			require.NoError(t, err)
			client := vt.NewScreen(4, 5)
			client.Write(firstBytes)

			second := composeFrame(makeState(tt.scrolled, tt.damage, false), first.cache)
			secondBytes, err := stream.render(second.frame, second.damage, second.reset)
			require.NoError(t, err)
			require.Equal(t, tt.wantBytes, string(secondBytes))
			client.Write(secondBytes)
			require.Equal(t, frameRows(second.frame), frameRows(client.Frame))
			again, err := stream.renderer.Draw(second.frame, nil)
			require.NoError(t, err)
			require.Empty(t, again, "renderer shadow must equal replayed terminal state")
		})
	}
}
