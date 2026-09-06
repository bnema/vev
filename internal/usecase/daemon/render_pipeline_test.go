package daemon

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	vt "github.com/bnema/vev-vt"
	renderer "github.com/bnema/vev-vt/ansi"
	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/protocol/wire"
	"github.com/bnema/vev/internal/testutil/replaytest"
	scopy "github.com/bnema/vev/internal/usecase/copy"
	"github.com/bnema/vev/internal/usecase/layout"
	"github.com/bnema/vev/internal/usecase/notices"
	"github.com/bnema/vev/internal/usecase/palette"
	"github.com/bnema/vev/internal/usecase/picker"
	promptui "github.com/bnema/vev/internal/usecase/prompt"
	themeui "github.com/bnema/vev/internal/usecase/theme"
	"github.com/bnema/vev/internal/usecase/ui"
	"github.com/bnema/vev/internal/usecase/visualsearch"
	"github.com/stretchr/testify/require"
)

// scriptedReplayTransport is intentionally a wire.Transport only: replay
// assertions must not depend on any adapter's framing implementation.
type scriptedReplayTransport struct {
	t      *testing.T
	frames []wire.Frame
	next   int
}

func (s *scriptedReplayTransport) Send(got wire.Frame) error {
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
func (*scriptedReplayTransport) Recv() (wire.Frame, error) { return wire.Frame{}, io.EOF }
func (*scriptedReplayTransport) Close() error              { return nil }

func TestTransportReplayFinalShadowAndTerminalBytes(t *testing.T) {
	frames := replaytest.Transcript()
	transport := &scriptedReplayTransport{t: t, frames: frames}
	terminal := vt.NewScreen(8, 3)
	for _, frame := range frames {
		require.NoError(t, transport.Send(frame))
		output, err := wire.UnmarshalOutput(frame.Payload)
		require.NoError(t, err)
		require.Equal(t, frame.Payload, mustMarshalOutput(output), "output payload must remain byte exact")
		terminal.Write(output.Data)
	}
	require.Equal(t, len(frames), transport.next)
	require.Equal(t, []string{"one     ", "TWO     ", "        "}, frameRows(terminal), "terminal replay is the final renderer shadow")
}

func TestCapturePaneRenderStateOwnsVisibleFrameWithoutConsumingDamage(t *testing.T) {
	p := newPane("p", nil, domain.Size{Cols: 8, Rows: 2})
	p.screen.ClearDamage()
	p.screen.Write([]byte("old"))

	p.mu.Lock()
	captured := capturePaneRenderStateLocked(p, domain.Rect{Width: 3, Height: 1})
	p.mu.Unlock()

	require.Equal(t, 3, captured.frame.Width)
	require.Equal(t, 1, captured.frame.Height)
	require.NotEmpty(t, p.screen.Damage(), "capture is only a transactional snapshot")
	p.mu.Lock()
	p.screen.Write([]byte("new"))
	p.mu.Unlock()
	require.Equal(t, 'o', captured.frame.At(0, 0).Rune, "capture must not alias the mutable VT frame")
}

func TestCapturePaneRenderStateIsAlwaysNonDestructive(t *testing.T) {
	p := newPane("p", nil, domain.Size{Cols: 8, Rows: 2})
	p.screen.ClearDamage()
	p.screen.Write([]byte("preview"))

	p.mu.Lock()
	_ = capturePaneRenderStateLocked(p, domain.Rect{Width: 8, Height: 2})
	p.mu.Unlock()

	require.NotEmpty(t, p.screen.Damage())
}

func TestCapturePaneRenderStateMalformedDamageFallsBackToFullRedraw(t *testing.T) {
	p := newPane("p", nil, domain.Size{Cols: 8, Rows: 2})
	p.screen.ClearDamage()
	p.screen.Write([]byte("x"))
	p.screen.Damage()[0] = renderer.Damage{Kind: renderer.DamageText, X: -1, Y: 0, Width: 4, Height: 1}

	p.mu.Lock()
	captured := capturePaneRenderStateLocked(p, domain.Rect{Width: 8, Height: 2})
	p.mu.Unlock()

	require.Equal(t, []renderer.Damage{renderer.FullRedraw()}, captured.damage)
}

func TestCapturePaneRenderStateCollapsedRetainsDamage(t *testing.T) {
	p := newPane("p", nil, domain.Size{Cols: 8, Rows: 2})
	p.screen.ClearDamage()
	p.screen.Write([]byte("hidden"))

	p.mu.Lock()
	captured := capturePaneRenderStateLocked(p, domain.Rect{})
	p.mu.Unlock()

	require.Zero(t, captured.frame.Width)
	require.Zero(t, captured.frame.Height)
	require.NotEmpty(t, p.screen.Damage(), "capture keeps hidden-pane damage pending until emission commits")
}

func TestPaintACKBlockedDoesNotDestructivelyCapture(t *testing.T) {
	pty, release := newBlockingPTY(t)
	t.Cleanup(release)
	d, sess, _, sends := newManualSessionWithPTYs(t, pty)
	ac := sess.snapshotAttachments()[0]
	// Resolve the initial attachment view before filling the output window; a
	// first target repair is an epoch boundary by design.
	require.NotNil(t, sess.tabForAttachment(ac))
	ac.sendMu.Lock()
	ac.output.next = ac.output.maxOutstanding
	ac.output.acked = 0
	ac.output.syncCapacityLocked()
	ac.sendMu.Unlock()
	p := sess.tabs[0].focusedPane()
	p.mu.Lock()
	p.screen.ClearDamage()
	p.screen.Write([]byte("blocked"))
	p.mu.Unlock()

	d.paint(sess, ac, false, nil)

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
	files, err := filepath.Glob("*.go")
	require.NoError(t, err)
	for _, name := range files {
		parsed, err := parser.ParseFile(token.NewFileSet(), name, nil, 0)
		require.NoError(t, err)
		for _, declaration := range parsed.Decls {
			switch declaration := declaration.(type) {
			case *ast.FuncDecl:
				forbidden := declaration.Name.Name == "composeFloatingFrame" || declaration.Name.Name == "copyTargetRectLocked" || strings.HasPrefix(declaration.Name.Name, "composeClientFrame") || strings.HasPrefix(declaration.Name.Name, "composeTabFrame")
				require.False(t, forbidden, "%s declares forbidden legacy render helper %s", name, declaration.Name.Name)
			case *ast.GenDecl:
				for _, spec := range declaration.Specs {
					typeSpec, ok := spec.(*ast.TypeSpec)
					require.False(t, ok && typeSpec.Name.Name == "composedFrameCache", "%s declares forbidden legacy render cache", name)
				}
			}
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

func TestCursorCandidateDoesNotPublishDuringPreparation(t *testing.T) {
	output := &attachmentOutput{lastCursor: cursorOut{valid: true, row: 1, col: 2, style: 1, hasStyle: true}}
	desired := cursorOut{row: 3, col: 4, style: 2, hasStyle: true}

	candidate := output.prepareCursorTail(desired, false)

	require.Equal(t, cursorOut{valid: true, row: 1, col: 2, style: 1, hasStyle: true}, output.lastCursor)
	require.NotEmpty(t, candidate.data)
	require.Equal(t, cursorOut{valid: true, row: 3, col: 4, style: 2, hasStyle: true}, candidate.next)
}

func TestEmitFrameFailedSendDoesNotPublishCursorOrOutputState(t *testing.T) {
	d, sess, ac, sends := newManualSessionWithPTYs(t)
	healthy := ac.transport()
	beforeCursor := cursorOut{valid: true, row: 1, col: 2, style: 1, hasStyle: true}
	ac.output.lastCursor = beforeCursor
	ac.replaceTransport(cacheFailTransport{})
	state := cacheState("failed", 1)
	state.attachment = ac
	composed := composeFrame(state, ac.pipelineCache, ac.pipelineScratch)
	composed.cursor = cursorOut{row: 3, col: 4, style: 2, hasStyle: true}

	ac.sendMu.Lock()
	require.True(t, d.emitFrame(sess, ac, &state, composed))

	require.Equal(t, beforeCursor, ac.output.lastCursor)
	require.Zero(t, ac.output.next)
	probe, err := ac.output.renderer.Prepare(composed.frame, nil, false)
	require.NoError(t, err)
	require.NotEmpty(t, probe.Bytes(), "failed output must not commit the renderer shadow")

	sess.mu.Lock()
	sess.registerAttachmentLocked(ac)
	sess.mu.Unlock()
	ac.setSession(sess)
	ac.replaceTransport(healthy)
	state.attachment = ac
	ac.sendMu.Lock()
	require.True(t, d.emitFrame(sess, ac, &state, composed))
	require.Equal(t, cursorOut{valid: true, row: 3, col: 4, style: 2, hasStyle: true}, ac.output.lastCursor)
	require.Equal(t, uint64(1), ac.output.next)
	out, err := wire.UnmarshalOutput((<-sends).Payload)
	require.NoError(t, err)
	require.Zero(t, out.Base)
	require.Equal(t, uint64(1), out.New)
}

func TestEmitFrameNoByteSuccessCommitsTransactionWithoutStateFrame(t *testing.T) {
	d, sess, ac, sends := newManualSessionWithPTYs(t, nil)
	state := cacheState("steady", 1)
	state.attachment = ac
	state.route.Target = protocol.ExactSessionTarget{LifecycleID: sess.incarnation, SessionName: sess.name}
	state.view.tabID = domain.TabStableID(sess.tabs[0].stableID)
	state.view.revision = ac.viewSnapshot().revision
	initial := composeFrame(state, ac.pipelineCache, ac.pipelineScratch)
	ac.sendMu.Lock()
	require.True(t, d.emitFrame(sess, ac, &state, initial))
	<-sends

	p := newPane("collapsed", nil, domain.Size{Cols: 1, Rows: 1})
	p.mu.Lock()
	p.screen.ClearDamage()
	p.screen.Write([]byte("x"))
	capture := p.screen.CaptureDamage()
	p.mu.Unlock()

	beforeNext := ac.output.next
	beforeCursor := ac.output.lastCursor
	state.receipts = []damageReceipt{{pane: p, generation: capture.Generation}}
	noByte := composedRenderFrame{
		frame:  initial.frame,
		cursor: initial.cursor,
		cache:  initial.cache,
	}
	noByte.cache.layoutFingerprint = "no-byte-committed"
	ac.sendMu.Lock()
	require.True(t, d.emitFrame(sess, ac, &state, noByte))

	require.Equal(t, beforeNext, ac.output.next)
	require.Equal(t, beforeCursor, ac.output.lastCursor)
	require.Equal(t, "no-byte-committed", ac.pipelineCache.layoutFingerprint)
	p.mu.Lock()
	require.Empty(t, p.screen.Damage())
	p.mu.Unlock()
	select {
	case frame := <-sends:
		t.Fatalf("no-byte transaction sent state frame %#v", frame)
	default:
	}

	state.focusedPaneID = "pane-2"
	state.panes[0].stableID = "pane-2"
	ac.sendMu.Lock()
	require.True(t, d.emitFrame(sess, ac, &state, noByte))
	frame := <-sends
	require.Equal(t, wire.MsgUIViewUpdate, frame.Type)
	update, err := wire.UnmarshalUIViewUpdate(frame.Payload)
	require.NoError(t, err)
	require.Equal(t, state.focusedPaneID, update.Context.FocusedPaneID)
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
		route:    protocol.CommittedRouteIdentity{Target: protocol.ExactSessionTarget{LifecycleID: domain.SessionLifecycleID{1}, SessionName: "sess"}},
		view:     attachmentView{tabID: "tab-1"},
		reset:    true,
		layout:   capturedTabLayout{area: domain.Rect{Width: 12, Height: 5}, valid: true, focus: "p", placements: []layout.Placement{{ID: "p", Content: domain.Rect{Width: 12, Height: 5}}}},
		panes:    []capturedPaneRenderState{{id: "p", stableID: "pane-1", frame: paneFrame, placement: layout.Placement{ID: "p", Content: domain.Rect{Width: 12, Height: 5}}, focused: true, damage: []renderer.Damage{renderer.FullRedraw()}}},
		floating: capturedFloatingRenderState{visible: true, pane: capturedPaneRenderState{id: "f", frame: floatingFrame}, geometry: floatingGeometry{Mode: ui.PresentationFloating, Bounds: domain.Rect{X: 3, Y: 1, Width: 6, Height: 3}, Inner: domain.Rect{X: 4, Y: 2, Width: 4, Height: 1}}, title: "float", generation: 1},
		bars:     barState{status: statusSnapshot{session: "sess", tabs: []statusTab{{name: "tab", active: true}}}, topRight: "R", bottomRight: "B"},
		overlays: capturedOverlayRenderState{promptActive: true, prompt: capturedModal{active: true, title: "Prompt", presentation: (ui.Modal{FixedWidth: 8, FixedHeight: 3, Title: "Prompt"}).Resolve(domain.Size{Cols: 12, Rows: 7}), inner: modalInner}},
		cursor:   capturedCursorInputs{row: 1, col: 2, visible: true, renderable: true, content: domain.Rect{X: 4, Y: 2, Width: 4, Height: 1}},
		styles:   resolveStyles(nil),
	}
	composed := composeFrame(state, composeCacheInput{})
	require.Equal(t, []string{" tab       R", "AAAAAAAAAAAA", "BBB┌─fl─┐BBB", "───Prompt───", "PROMPT      ", "            ", " sess      B"}, frameRows(composed.frame))
	require.True(t, composed.cursor.hidden, "overlay owns cursor visibility")

	stream := newOutputStateStream()
	prepared, err := stream.prepareFrame(nil, &state, composed.frame, composed.damage, composed.reset, composed.cursor)
	require.NoError(t, err)
	var outputFrame wire.Frame
	require.NoError(t, prepared.send(0, outputFrameSender(func(frame wire.Frame) error {
		outputFrame = frame
		return nil
	})))
	output, err := wire.UnmarshalOutput(outputFrame.Payload)
	require.NoError(t, err)
	terminalBytes := output.Data
	require.Equal(t, "\x1b[1;1H\x1b[0;7m tab \x1b[0m      R\x1b[2;1HAAAAAAAAAAAA\x1b[3;1HBBB┌─fl─┐BBB\x1b[4;1H───Prompt───\x1b[5;1HPROMPT\x1b[K\x1b[B\x1b[2K\x1b[7;1H\x1b[0;7m sess \x1b[0m     B\x1b[0m\x1b[?25l", string(terminalBytes))
	client := vt.NewScreen(composed.frame.Width, composed.frame.Height)
	client.Write(terminalBytes)
	require.Equal(t, frameRows(composed.frame), frameRows(client))
	again, err := stream.renderer.Draw(composed.frame, nil)
	require.NoError(t, err)
	require.Empty(t, again, "renderer shadow must exactly equal the composed frame")
}

func frameRows(frame renderer.CellSource) []string {
	rows := make([]string, frame.Rows())
	cells := make([]renderer.Cell, frame.Columns())
	for y := range rows {
		for x := range cells {
			cells[x] = frame.Cell(x, y)
		}
		rows[y] = rowText(cells)
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
			require.Equal(t, frameRows(second.frame), frameRows(client))
			again, err := stream.renderer.Draw(second.frame, nil)
			require.NoError(t, err)
			require.Empty(t, again, "renderer shadow must equal replayed terminal state")
		})
	}
}

func TestComposeCapturedOverlaysUsesCachedModalRoles(t *testing.T) {
	muted := renderer.Style{Foreground: 1, Background: 2}
	active := renderer.Style{Foreground: 3, Background: 4}
	interior := renderer.Style{Foreground: 5, Background: 6}
	modal := ui.Modal{FixedWidth: 6, FixedHeight: 4, Title: "x"}
	presentation := modal.Resolve(domain.Size{Cols: 10, Rows: 8})

	for _, tt := range []struct {
		name    string
		focused bool
		border  renderer.Style
	}{
		{name: "unfocused", focused: false, border: muted},
		{name: "focused", focused: true, border: active},
	} {
		t.Run(tt.name, func(t *testing.T) {
			state := capturedRenderState{
				styles:   themeui.Styles{PickerBase: interior, BorderMuted: muted, BorderActive: active},
				overlays: capturedOverlayRenderState{prompt: capturedModal{active: true, title: modal.Title, presentation: presentation, inner: renderer.NewFrame(1, 1), focused: tt.focused}},
			}
			frame, _ := composeCapturedOverlays(state, renderer.NewFrame(10, 8), nil)
			bounds := presentation.Bounds
			require.True(t, frame.At(bounds.X, bounds.Y).Style.Equal(tt.border), "border uses cached focused role")
			require.True(t, frame.At(bounds.X+2, bounds.Y+2).Style.Equal(interior), "unrendered modal interior uses cached chrome role")
		})
	}
}

func TestComposeFrameModalBackdropDimsCompleteFrameIncludingToasts(t *testing.T) {
	const (
		width       = 100
		contentRows = 6
	)
	styles := resolveStyles(nil)
	theme := backdropTheme()
	leftPlacement := layout.Placement{
		ID:       "left",
		TitleBar: domain.Rect{Width: 49, Height: 1},
		Content:  domain.Rect{Y: 1, Width: 49, Height: 5},
	}
	rightPlacement := layout.Placement{
		ID:       "right",
		TitleBar: domain.Rect{X: 50, Width: 50, Height: 1},
		Content:  domain.Rect{X: 50, Y: 1, Width: 50, Height: 5},
	}
	state := capturedRenderState{
		reset: true,
		layout: capturedTabLayout{
			area:        domain.Rect{Width: width, Height: contentRows},
			focus:       "left",
			placements:  []layout.Placement{leftPlacement, rightPlacement},
			dividers:    []layout.Divider{{Rect: domain.Rect{X: 49, Width: 1, Height: contentRows}, Dir: layout.Horizontal}},
			fingerprint: "modal-backdrop",
			valid:       true,
		},
		panes: []capturedPaneRenderState{
			{id: "left", title: "left", frame: cachePaneFrame(49, 5, 'L'), placement: leftPlacement, focused: true, damage: []renderer.Damage{renderer.FullRedraw()}},
			{id: "right", title: "right", frame: cachePaneFrame(50, 5, 'R'), placement: rightPlacement, damage: []renderer.Damage{renderer.FullRedraw()}},
		},
		overlays: capturedOverlayRenderState{notices: []domain.Notification{{Code: domain.NoticeClipboard, Severity: domain.NoticeInfo, Message: "copied", Count: 1}}},
		styles:   styles,
		theme:    theme,
	}
	base := composeFrame(state, composeCacheInput{})
	require.NotEmpty(t, base.cache.toastFootprints)

	state.reset = false
	state.panes[0].damage = nil
	state.panes[1].damage = nil
	state.overlays.promptActive = true
	state.overlays.prompt = capturedModal{
		active: true,
		title:  "Prompt",
		presentation: ui.Presentation{
			Bounds:  domain.Rect{X: 80, Y: 5, Width: 10, Height: 2},
			Inner:   domain.Rect{X: 81, Y: 6, Width: 8, Height: 1},
			Borders: ui.BorderAll,
		},
		inner:   renderer.NewFrame(8, 1),
		focused: true,
	}
	composed := composeFrame(state, base.cache)
	dimmer := themeui.NewDimmer(theme)
	toast := base.cache.toastFootprints[0]

	for name, point := range map[string][2]int{
		"top bar":      {0, 0},
		"pane chrome":  {0, 1},
		"pane content": {0, 2},
		"divider":      {49, 2},
		"bottom bar":   {0, contentRows + 1},
		"toast":        {toast.X, toast.Y},
	} {
		t.Run(name, func(t *testing.T) {
			x, y := point[0], point[1]
			require.Equal(t, dimmer.Dim(base.frame.At(x, y).Style).Canonical(), composed.frame.At(x, y).Style.Canonical())
		})
	}
	require.Equal(t, []renderer.Damage{renderer.FullRedraw()}, composed.damage)
	require.False(t, composed.cache.valid)
	require.Equal(t, captureTestFrame(base.cache.frame), captureTestFrame(composed.cache.frame), "the reusable cache remains toast-free and unadorned")
}

func TestComposeFrameFloatingBackdropDimsCompleteFrameIncludingToasts(t *testing.T) {
	const (
		width       = 100
		contentRows = 6
	)
	placement := layout.Placement{ID: "pane", Content: domain.Rect{Width: width, Height: contentRows}}
	theme := backdropTheme()
	state := capturedRenderState{
		reset:  true,
		layout: capturedTabLayout{area: domain.Rect{Width: width, Height: contentRows}, focus: "pane", placements: []layout.Placement{placement}, fingerprint: "floating-toast-backdrop", valid: true},
		panes: []capturedPaneRenderState{{
			id: "pane", frame: cachePaneFrame(width, contentRows, 'P'), placement: placement, focused: true, damage: []renderer.Damage{renderer.FullRedraw()},
		}},
		overlays: capturedOverlayRenderState{notices: []domain.Notification{{Code: domain.NoticeClipboard, Severity: domain.NoticeInfo, Message: "copied", Count: 1}}},
		styles:   resolveStyles(nil),
		theme:    theme,
	}
	toastOnly := composeFrame(state, composeCacheInput{})
	require.NotEmpty(t, toastOnly.cache.toastFootprints)
	toast := toastOnly.cache.toastFootprints[0]

	state.floating = capturedFloatingRenderState{
		visible: true,
		focused: true,
		pane:    capturedPaneRenderState{id: "floating", frame: renderer.NewFrame(8, 1)},
		geometry: floatingGeometry{
			Mode:   ui.PresentationFloating,
			Bounds: domain.Rect{X: 40, Y: 2, Width: 10, Height: 3},
			Inner:  domain.Rect{X: 41, Y: 3, Width: 8, Height: 1},
		},
		generation: 1,
	}
	composed := composeFrame(state, composeCacheInput{})
	dimmer := themeui.NewDimmer(theme)

	for name, point := range map[string][2]int{
		"top bar":    {0, 0},
		"bottom bar": {0, contentRows + 1},
		"toast":      {toast.X, toast.Y},
	} {
		t.Run(name, func(t *testing.T) {
			x, y := point[0], point[1]
			require.Equal(t, dimmer.Dim(toastOnly.frame.At(x, y).Style).Canonical(), composed.frame.At(x, y).Style.Canonical())
		})
	}
	require.Equal(t, captureTestFrame(toastOnly.cache.frame), captureTestFrame(composed.cache.frame), "floating decoration must not enter the reusable base cache")
}

func TestComposeCapturedOverlaysMatchesKeyboardPriority(t *testing.T) {
	theme := backdropTheme()
	lowerStyle := renderer.Style{Foreground: 1, Background: 2}
	higherStyle := renderer.Style{Foreground: 3, Background: 4}
	modalAt := func(x int, r rune, style renderer.Style) capturedModal {
		inner := renderer.NewFrame(1, 1)
		inner.Set(0, 0, renderer.Cell{Rune: r, Style: style})
		bounds := domain.Rect{X: x, Y: 1, Width: 1, Height: 1}
		return capturedModal{active: true, presentation: ui.Presentation{Bounds: bounds, Inner: bounds}, inner: inner}
	}

	for _, tt := range []struct {
		name   string
		lower  string
		higher string
	}{
		{name: "notices above copy search", lower: "copy search", higher: "notices"},
		{name: "picker above notices", lower: "notices", higher: "picker"},
		{name: "palette above picker", lower: "picker", higher: "palette"},
		{name: "prompt above palette", lower: "palette", higher: "prompt"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			state := capturedRenderState{theme: theme, styles: themeui.Styles{PickerBase: renderer.DefaultStyle()}}
			setModal := func(name string, modal capturedModal) {
				switch name {
				case "copy search":
					state.overlays.copySearch = modal
				case "notices":
					state.overlays.noticesOverlay = modal
				case "picker":
					state.overlays.picker = modal
				case "palette":
					state.overlays.palette = modal
				case "prompt":
					state.overlays.prompt = modal
				}
			}
			setModal(tt.lower, modalAt(1, 'L', lowerStyle))
			setModal(tt.higher, modalAt(3, 'H', higherStyle))

			frame, damage := composeCapturedOverlays(state, renderer.NewFrame(5, 3), nil)

			require.Equal(t, 'L', frame.At(1, 1).Rune)
			require.Equal(t, themeui.NewDimmer(theme).Dim(lowerStyle).Canonical(), frame.At(1, 1).Style.Canonical(), "the lower-priority modal is part of the higher modal backdrop")
			require.Equal(t, 'H', frame.At(3, 1).Rune)
			require.Equal(t, higherStyle, frame.At(3, 1).Style, "the keyboard owner remains visually topmost")
			require.Equal(t, []renderer.Damage{renderer.FullRedraw()}, damage)
		})
	}
}

func TestCaptureOverlayLayersRendersEveryModelToResolvedInner(t *testing.T) {
	state := capturedRenderState{
		layout: capturedTabLayout{area: domain.Rect{Width: 79, Height: 38}},
		styles: fallbackChromeStyles,
	}
	snapshot := scopy.NewSnapshotFromRows(nil, 79, 38)
	snap := &overlayRenderSnapshot{
		copySearchModel:      visualsearch.New(snapshot),
		pickerActive:         true,
		pickerModel:          picker.New(nil, picker.SelectionConfig{}),
		noticesOverlayActive: true,
		noticesOverlayModel:  notices.New(nil, time.Time{}),
		paletteActive:        true,
		paletteModel:         palette.New(nil),
		promptActive:         true,
		promptModel:          promptui.New(" Prompt ", ""),
	}

	captureOverlayLayers(&state, snap, domain.PaletteConfig{})

	for name, modal := range map[string]capturedModal{
		"copy search": state.overlays.copySearch,
		"picker":      state.overlays.picker,
		"notices":     state.overlays.noticesOverlay,
		"palette":     state.overlays.palette,
		"prompt":      state.overlays.prompt,
	} {
		t.Run(name, func(t *testing.T) {
			require.True(t, modal.active)
			require.Equal(t, ui.PresentationDrawer, modal.presentation.Mode)
			require.Equal(t, rectSize(modal.presentation.Inner), domain.Size{Cols: modal.inner.Width, Rows: modal.inner.Height})
		})
	}
}

func TestCopyModalInnerClipsDestinationAndSource(t *testing.T) {
	base := renderer.NewFrame(4, 3)
	inner := renderer.NewFrame(4, 2)
	for y, text := range []string{"ABCD", "EFGH"} {
		for x, r := range text {
			inner.Set(x, y, renderer.Cell{Rune: r})
		}
	}

	copyModalInner(base, domain.Rect{X: -1, Y: 2, Width: 5, Height: 2}, inner)

	require.Equal(t, "BCD ", rowText(base.Row(2)))
}

func TestComposeCapturedOverlaysKeepsZeroHeightModalActive(t *testing.T) {
	presentation := (ui.Modal{FixedHeight: 11, Title: "Search"}).Resolve(domain.Size{Cols: 79, Rows: 4})
	require.Zero(t, presentation.Bounds.Height)
	state := capturedRenderState{
		styles: themeui.Styles{PickerBase: renderer.DefaultStyle()},
		overlays: capturedOverlayRenderState{
			copySearch: capturedModal{active: true, title: "Search", presentation: presentation},
		},
	}

	_, damage := composeCapturedOverlays(state, renderer.NewFrame(79, 4), nil)

	require.Equal(t, []renderer.Damage{renderer.FullRedraw()}, damage)
}

func TestNoticeStylesFromMapsWarnToDedicatedRoleDistinctFromInfo(t *testing.T) {
	terminal := renderer.DefaultStyle()
	muted := renderer.Style{Foreground: 1}
	active := renderer.Style{Foreground: 2}
	warn := renderer.Style{Foreground: 3}
	styles := themeui.Styles{PickerBase: terminal, BorderMuted: muted, BorderActive: active, BorderWarn: warn}

	got := noticeStylesFrom(styles)

	require.True(t, got.Text.Equal(terminal), "toast content keeps the terminal background")
	require.Equal(t, active, got.BoxError)
	require.Equal(t, muted, got.BoxInfo)
	// BoxWarn must use the dedicated BorderWarn role, not fall back to the
	// same muted role BoxInfo uses - that was the pre-existing bug this
	// role fixes (Warn and Info toasts were visually identical).
	require.Equal(t, warn, got.BoxWarn)
	require.NotEqual(t, got.BoxInfo, got.BoxWarn)
}

func TestEmitFrameSkipsTransportSendWhenAttachmentEffectFenceRejects(t *testing.T) {
	d, sess, ac, sends := newManualSessionWithPTYs(t, nil)
	token := sess.captureAttachmentCapability(ac, ac.transport())
	effect, admitted := ac.beginAttachmentEffect(token)
	require.True(t, admitted)
	ac.lifecycle.mu.Lock()
	ac.lifecycle.capability.transport = transportSnapshot{}
	ac.lifecycle.mu.Unlock()

	state := cacheState("stale", 1)
	state.attachment = ac
	composed := composeFrame(state, composeCacheInput{})
	ac.sendMu.Lock()
	require.True(t, d.emitFrame(sess, ac, &state, composed, &runtimeMarkBatch{attachmentEffect: effect}))
	d.attachmentCleanupWg.Wait()

	require.Zero(t, ac.output.next, "rejected transport effect must not commit output state")
	select {
	case frame := <-sends:
		t.Fatalf("rejected transport effect was sent: %v", frame.Type)
	default:
	}
}
