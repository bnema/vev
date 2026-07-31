package daemon

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	portsmocks "github.com/bnema/vev/internal/ports/mocks"
	"github.com/bnema/vev/internal/usecase/command"
	"github.com/bnema/vev/internal/usecase/keys"
	"github.com/bnema/vev/internal/usecase/mouse"
	"github.com/bnema/vev/internal/usecase/picker"
)

func newProxyInputHarness(t *testing.T) (*Daemon, *proxySession, *attachedClient, *proxyTestTransport, proxyKeyHandler) {
	t.Helper()
	d := newTestDaemon(t, nil, stubClock{})
	proxy, err := newProxySession(domain.RemoteSessionKey{Host: "arch", Name: "work"}, domain.Size{Cols: 80, Rows: 24})
	require.NoError(t, err)
	link := newProxyTestTransport()
	proxy.mu.Lock()
	proxy.transport = link
	proxy.linkGeneration = 1
	proxy.meta = ports.SessionMeta{SessionName: "work", Tabs: []ports.SessionTabMeta{{Index: 0, Name: "shell"}, {Index: 1, Name: "build"}}}
	proxy.mu.Unlock()
	ac := &attachedClient{size: domain.Size{Cols: 80, Rows: 24}, output: newOutputStateStream()}
	ac.initOverlays()
	token := roleEffectForTest(t, attachProxyInputRole(t, d, proxy, ac))
	return d, proxy, ac, link, proxyKeyHandler{d: d, proxy: proxy, ac: ac, roleToken: token}
}

func attachProxyInputRole(t *testing.T, d *Daemon, proxy *proxySession, ac *attachedClient) attachmentRoleToken {
	t.Helper()
	clientTransport := newProxyTestTransport()
	ac.replaceTransport(clientTransport)
	ac.keys = keys.NewRouter(d.clock, daemonKeyHandler{d: d, ac: ac}, &d.bindings)
	d.mu.Lock()
	require.True(t, d.registerSessionLocked(proxy))
	d.mu.Unlock()
	transition, err := d.transitionAttachment(attachmentTransitionRequest{
		target: proxy, next: ac,
		expectedRole: attachmentDetached, targetRole: attachmentActive,
		expectedTransport: ac.transportSnapshot(), ready: true,
	})
	require.NoError(t, err)
	d.deferAttachmentTransitionCleanups(transition)
	return transition.published
}

func roleEffectForTest(t *testing.T, token attachmentRoleToken) attachmentRoleToken {
	t.Helper()
	effect, admitted := token.ac.beginRoleEffect(token)
	require.True(t, admitted)
	token.effect = effect
	t.Cleanup(effect.End)
	return token
}

func requireProxyInputFrame(t *testing.T, link *proxyTestTransport, wantSeq uint64, want []byte) {
	t.Helper()
	select {
	case frame := <-link.sent:
		require.Equal(t, ports.MsgInput, frame.Type)
		input, err := ports.UnmarshalInput(frame.Payload)
		require.NoError(t, err)
		require.Equal(t, wantSeq, input.InputSeq)
		require.Equal(t, want, input.Data)
	default:
		t.Fatal("missing proxy input frame")
	}
	select {
	case frame := <-link.sent:
		t.Fatalf("input was sent more than once: %#v", frame)
	default:
	}
}

func requireNoProxyFrame(t *testing.T, link *proxyTestTransport) {
	t.Helper()
	select {
	case frame := <-link.sent:
		t.Fatalf("unexpected proxy frame: %#v", frame)
	default:
	}
}

func TestProxyInputForwardsOrdinaryPasteMouseAndEscapeBytesExactlyOnce(t *testing.T) {
	_, _, _, link, handler := newProxyInputHarness(t)
	paste := append(append(append([]byte(nil), ports.BracketedPasteOpenMarker...), []byte("a\x1bj\x00")...), ports.BracketedPasteCloseMarker...)
	inputs := []struct {
		name string
		raw  []byte
	}{
		{name: "ordinary keyboard", raw: []byte("hello")},
		{name: "bracketed paste", raw: paste},
		{name: "terminal escape", raw: []byte("\x1b[15~")},
	}
	for i, input := range inputs {
		t.Run(input.name, func(t *testing.T) {
			handler.Forward(input.raw)
			requireProxyInputFrame(t, link, uint64(i+1), input.raw)
		})
	}

	mouseRaw := []byte("\x1b[<0;12;8M")
	handler.Mouse(mouse.Event{Raw: mouseRaw})
	requireProxyInputFrame(t, link, 4, mouseRaw)
}

func TestProxyInputRemoteActionsForwardOriginalChordWithoutLocalExecution(t *testing.T) {
	remoteActions := []keys.Action{
		keys.ActionSwitchTab1, keys.ActionSwitchTab9,
		keys.ActionFocusPaneLeft, keys.ActionFocusPaneRight, keys.ActionFocusPaneUp, keys.ActionFocusPaneDown,
		keys.ActionToggleFloatingPane,
		keys.ActionGrowPaneWidth, keys.ActionShrinkPaneWidth, keys.ActionGrowPaneHeight, keys.ActionShrinkPaneHeight,
		keys.ActionEqualizePanes, keys.ActionConsumeOrExpelPaneLeft, keys.ActionConsumeOrExpelPaneRight,
	}
	for _, action := range remoteActions {
		t.Run(action.Name(), func(t *testing.T) {
			_, proxy, _, link, handler := newProxyInputHarness(t)
			before := proxy.snapshotView(viewOptions{tabDetails: true})
			raw := []byte{keys.ESC, byte('A' + action%26), 0x00}
			handler.Action(action, raw)
			requireProxyInputFrame(t, link, 1, raw)
			require.Equal(t, before, proxy.snapshotView(viewOptions{tabDetails: true}), "remote action must not mutate local proxy metadata")
		})
	}
}

func TestProxyInputRouterPreservesCustomAndDelayedActionBytes(t *testing.T) {
	t.Run("router action bytes", func(t *testing.T) {
		_, _, _, link, handler := newProxyInputHarness(t)
		bindings := keys.DefaultBindings()
		var published atomic.Pointer[keys.Bindings]
		published.Store(bindings)
		router := keys.NewRouter(stubClock{}, handler, &published)

		router.Route([]byte{keys.ESC, 'j'})
		requireProxyInputFrame(t, link, 1, []byte{keys.ESC, 'j'})
	})

	t.Run("retained escape reacquires its original role", func(t *testing.T) {
		d, proxy, ac, link, initial := newProxyInputHarness(t)
		base := initial.roleToken
		base.effect.End()
		base.effect = nil

		firstEffect, admitted := ac.beginRoleEffect(base)
		require.True(t, admitted)
		first := base
		first.effect = firstEffect
		ac.keys.RouteWithHandler([]byte{keys.ESC}, proxyKeyHandler{d: d, proxy: proxy, ac: ac, roleToken: first})
		firstEffect.End()

		secondEffect, admitted := ac.beginRoleEffect(base)
		require.True(t, admitted)
		second := base
		second.effect = secondEffect
		ac.keys.RouteWithHandler([]byte{'j'}, proxyKeyHandler{d: d, proxy: proxy, ac: ac, roleToken: second})
		secondEffect.End()

		requireProxyInputFrame(t, link, 1, []byte{keys.ESC, 'j'})
	})
}

func TestProxyInputLocalPaletteNeverForwards(t *testing.T) {
	d, _, ac, link, handler := newProxyInputHarness(t)
	token := handler.roleToken
	handler.Action(keys.ActionOpenPalette, []byte{keys.ESC, ' '})
	require.True(t, ac.overlays.paletteActive())
	requireNoProxyFrame(t, link)

	// Keyboard and mouse navigation are consumed by the local overlay before
	// either the proxy router or proxy mouse sender can observe them.
	d.handleInputForRole(token, []byte("x"))
	d.handleInputForRole(token, []byte("\x1b[<0;12;8M"))
	requireNoProxyFrame(t, link)
}

func TestProxyInputAttentionOwnership(t *testing.T) {
	t.Run("pending remote tab attention forwards original chord", func(t *testing.T) {
		_, proxy, _, link, handler := newProxyInputHarness(t)
		proxy.mu.Lock()
		proxy.meta.Tabs[1].Attention = true
		proxy.mu.Unlock()
		raw := []byte{keys.ESC, 'a'}
		handler.Action(keys.ActionJumpAttention, raw)
		requireProxyInputFrame(t, link, 1, raw)
	})

	t.Run("without remote attention changes selected session locally", func(t *testing.T) {
		d, _, ac, link, handler := newProxyInputHarness(t)
		tb := newTab(newQuietPTY(), domain.Size{Cols: 80, Rows: 22})
		tb.attention = true
		tb.attentionAt = time.Unix(1, 0)
		local := &session{sessionCore: sessionCore{id: "local", name: "local"}, tabs: []*tab{tb}}
		publishTiledPaneOwners(local, tb)
		d.mu.Lock()
		require.True(t, d.registerSessionLocked(local))
		d.mu.Unlock()

		handler.Action(keys.ActionJumpAttention, []byte{keys.ESC, 'a'})

		require.Same(t, local, ac.currentSession())
		requireNoProxyFrame(t, link)
	})
}

func TestProxyInputProxiedRemoteOwnershipGuards(t *testing.T) {
	_, proxy, _, link, _ := newProxyInputHarness(t)
	require.True(t, proxy.caps.cannotAcceptMoves, "a proxy must reject move destinations locally")
	require.True(t, proxy.caps.cannotYieldMoves, "a proxy must reject move sources locally")
	requireNoProxyFrame(t, link)

	cfg := domain.NavConfig{OverflowTabs: true, OverflowSessions: true}
	require.Equal(t, domain.NavConfig{OverflowTabs: true}, proxiedNavConfig(cfg), "focus overflow must not cross remote-daemon sessions")
	require.False(t, proxiedJumpSearchesOtherSessions(true), "proxied jump attention must stay in its attached session")
	require.True(t, proxiedJumpSearchesOtherSessions(false))
}

func TestProxyPaletteCommandOwnershipIsComplete(t *testing.T) {
	expected := map[string]proxyPaletteOwnership{
		"new-tab": proxyPaletteRemote, "new-session": proxyPaletteLocal, "close-tab": proxyPaletteRemote,
		"split-right": proxyPaletteRemote, "split-left": proxyPaletteRemote, "split-up": proxyPaletteRemote, "split-down": proxyPaletteRemote,
		"consume-or-expel-pane-left": proxyPaletteRemote, "consume-or-expel-pane-right": proxyPaletteRemote,
		"stack-pane": proxyPaletteRemote, "toggle-stack": proxyPaletteRemote, "toggle-floating-pane": proxyPaletteRemote, "close-pane": proxyPaletteRemote,
		"move-pane": proxyPaletteRejected, "move-tab": proxyPaletteRejected,
		"focus-pane-left": proxyPaletteRemote, "focus-pane-right": proxyPaletteRemote, "focus-pane-up": proxyPaletteRemote, "focus-pane-down": proxyPaletteRemote,
		"resize-pane": proxyPaletteRemote, "grow-pane-width": proxyPaletteRemote, "shrink-pane-width": proxyPaletteRemote,
		"grow-pane-height": proxyPaletteRemote, "shrink-pane-height": proxyPaletteRemote, "equalize-panes": proxyPaletteRemote,
		"next-tab": proxyPaletteRemote, "previous-tab": proxyPaletteRemote,
		"back-session": proxyPaletteLocal, "jump-recent-session": proxyPaletteLocal, "session-picker": proxyPaletteLocal,
		"notifications": proxyPaletteLocal, "yank-last-notification": proxyPaletteLocal,
		"visual-mode": proxyPaletteRejected, "rename-session": proxyPaletteRejected, "rename-tab": proxyPaletteRemote,
		"detach": proxyPaletteLocal,
	}

	commands := command.PaletteRegistry()
	require.Len(t, expected, len(commands), "ownership table must change whenever the palette registry changes")
	for _, cmd := range commands {
		want, covered := expected[cmd.Slug]
		require.True(t, covered, "palette-visible command %q has no ownership assertion", cmd.Slug)
		require.Equal(t, want, proxyPaletteCommandOwnership(cmd.Slug), cmd.Slug)
		require.Equal(t, want == proxyPaletteRemote, proxyAttachedCommandOwnedRemotely(cmd.Slug), cmd.Slug)
	}
}

func TestProxyPaletteCanonicalLocalCommandsNeverReachRemote(t *testing.T) {
	t.Run("new session opens local transition prompt", func(t *testing.T) {
		d, proxy, ac, link, handler := newProxyInputHarness(t)
		handler.enterPalette()
		d.handlePaletteInput(ac, []byte("CNS\r"), handler.roleToken.effect)

		require.False(t, ac.overlays.paletteActive())
		require.True(t, ac.overlays.promptActive())
		require.Same(t, proxy, ac.currentAttachmentSession())
		requireNoProxyFrame(t, link)
	})

	t.Run("notification yank uses local notice center and clipboard", func(t *testing.T) {
		d, proxy, ac, link, handler := newProxyInputHarness(t)
		d.notices.record(domain.Notification{Code: domain.NoticeInternal, Message: "local notice", Time: time.Unix(1, 0)})
		handler.enterPalette()
		client, ok := ac.transport().(*proxyTestTransport)
		require.True(t, ok)
		for len(client.sent) > 0 {
			<-client.sent
		}
		d.handlePaletteInput(ac, []byte("YLN\r"), handler.roleToken.effect)

		require.False(t, ac.overlays.paletteActive())
		require.Same(t, proxy, ac.currentAttachmentSession())
		frame := awaitTestValue(t, client.sent, "local notification yank did not reach the client clipboard")
		require.Equal(t, ports.MsgOutput, frame.Type)
		output, err := ports.UnmarshalOutput(frame.Payload)
		require.NoError(t, err)
		require.Contains(t, string(output.Data), "\x1b]52;")
		requireNoProxyFrame(t, link)
	})
}

type proxyCommandTestTimer struct {
	ch   chan time.Time
	once sync.Once
}

func newProxyCommandTestTimer() *proxyCommandTestTimer {
	return &proxyCommandTestTimer{ch: make(chan time.Time, 1)}
}

func (t *proxyCommandTestTimer) C() <-chan time.Time      { return t.ch }
func (t *proxyCommandTestTimer) Reset(time.Duration) bool { return false }
func (t *proxyCommandTestTimer) Stop() bool               { return true }
func (t *proxyCommandTestTimer) Fire() {
	t.once.Do(func() { t.ch <- time.Time{} })
}

type proxyCommandTestClock struct{ timers chan *proxyCommandTestTimer }

func newProxyCommandTestClock() *proxyCommandTestClock {
	return &proxyCommandTestClock{timers: make(chan *proxyCommandTestTimer, 8)}
}

func (*proxyCommandTestClock) Now() time.Time { return time.Time{} }
func (c *proxyCommandTestClock) NewTimer(delay time.Duration) ports.Timer {
	if delay != proxyAttachedCommandTimeout {
		return stubTimer{}
	}
	timer := newProxyCommandTestTimer()
	c.timers <- timer
	return timer
}

func requireProxyCommandRequest(t *testing.T, link *proxyTestTransport) ports.CommandRequest {
	t.Helper()
	frame := awaitTestValue(t, link.sent, "proxy command was not sent")
	require.Equal(t, ports.MsgCommand, frame.Type)
	request, err := ports.UnmarshalCommandRequest(frame.Payload)
	require.NoError(t, err)
	return request
}

func commandResultFrame(result ports.CommandResult) ports.Frame {
	return ports.Frame{Type: ports.MsgCommandResult, Payload: ports.MarshalCommandResult(result)}
}

func TestProxyAttachedCommandCorrelatesInterleavedWrongAndLateResults(t *testing.T) {
	d, proxy, _, link, _ := newProxyInputHarness(t)
	clock := newProxyCommandTestClock()

	firstDone := make(chan ports.CommandResult, 1)
	firstErr := make(chan error, 1)
	go func() {
		result, err := proxy.sendCommand(context.Background(), clock, "next-tab", nil)
		firstDone <- result
		firstErr <- err
	}()
	first := requireProxyCommandRequest(t, link)
	require.Equal(t, uint64(1), first.RequestID)
	require.True(t, first.Attached)
	_ = awaitTestValue(t, clock.timers, "first command did not arm timeout")

	generation := proxy.linkGeneration
	result, err := d.handleLinkFrame(proxy, generation, ports.Frame{Type: ports.MsgOutput, Payload: ports.MarshalOutput(ports.Output{BaseStateNum: 0, NewStateNum: 1, Data: []byte("remote")})})
	require.NoError(t, err)
	require.Equal(t, proxyLinkResume, result)
	require.Equal(t, ports.MsgAck, awaitTestValue(t, link.sent, "interleaved output was not acknowledged").Type)
	result, err = d.handleLinkFrame(proxy, generation, proxyMeta(proxy.key.Name))
	require.NoError(t, err)
	require.Equal(t, proxyLinkResume, result)
	result, err = d.handleLinkFrame(proxy, generation, ports.Frame{Type: ports.MsgPong, Payload: ports.MarshalPong(ports.Pong{})})
	require.NoError(t, err)
	require.Equal(t, proxyLinkResume, result)
	result, err = d.handleLinkFrame(proxy, generation, commandResultFrame(ports.CommandResult{RequestID: 999, OK: true}))
	require.NoError(t, err)
	require.Equal(t, proxyLinkResume, result)
	select {
	case <-firstDone:
		t.Fatal("wrong request ID completed command")
	default:
	}
	require.NoError(t, proxy.sendInput([]byte("x")))
	requireProxyInputFrame(t, link, 1, []byte("x"))

	_, err = d.handleLinkFrame(proxy, generation, commandResultFrame(ports.CommandResult{RequestID: first.RequestID, OK: true}))
	require.NoError(t, err)
	require.True(t, (<-firstDone).OK)
	require.NoError(t, <-firstErr)

	secondDone := make(chan ports.CommandResult, 1)
	go func() {
		result, _ := proxy.sendCommand(context.Background(), clock, "previous-tab", nil)
		secondDone <- result
	}()
	second := requireProxyCommandRequest(t, link)
	require.Equal(t, uint64(2), second.RequestID)
	_ = awaitTestValue(t, clock.timers, "second command did not arm timeout")
	_, err = d.handleLinkFrame(proxy, generation, commandResultFrame(ports.CommandResult{RequestID: first.RequestID, OK: true}))
	require.NoError(t, err)
	select {
	case <-secondDone:
		t.Fatal("late request ID completed replacement command")
	default:
	}
	_, err = d.handleLinkFrame(proxy, generation, commandResultFrame(ports.CommandResult{RequestID: second.RequestID, Code: ports.ErrInternal, Text: "remote failed"}))
	require.NoError(t, err)
	remoteFailure := <-secondDone
	require.False(t, remoteFailure.OK)
	require.Equal(t, "remote failed", remoteFailure.Text)
}

func TestProxyAttachedCommandSharesSerializedSender(t *testing.T) {
	d, proxy, _, link, _ := newProxyInputHarness(t)
	clock := newProxyCommandTestClock()
	gate := make(chan struct{})
	entered := make(chan struct{}, 2)
	link.sendGate = gate
	link.sendEntered = entered

	commandDone := make(chan error, 1)
	go func() {
		_, err := proxy.sendCommand(context.Background(), clock, "next-tab", nil)
		commandDone <- err
	}()
	awaitTestCompletion(t, entered, "command sender did not enter transport")
	inputDone := make(chan error, 1)
	go func() { inputDone <- proxy.sendInput([]byte("x")) }()
	select {
	case <-entered:
		t.Fatal("input entered Transport.Send while command send held the shared sender")
	default:
	}
	close(gate)
	request := requireProxyCommandRequest(t, link)
	require.NoError(t, awaitTestValue(t, inputDone, "input send did not finish"))
	requireProxyInputFrame(t, link, 1, []byte("x"))
	_ = awaitTestValue(t, clock.timers, "command did not arm timeout")
	_, err := d.handleLinkFrame(proxy, proxy.linkGeneration, commandResultFrame(ports.CommandResult{RequestID: request.RequestID, OK: true}))
	require.NoError(t, err)
	require.NoError(t, awaitTestValue(t, commandDone, "command did not finish"))
	require.False(t, link.concurrent.Load(), "transport observed concurrent Send calls")
}

func TestProxyAttachedCommandAllowsOnlyOneOutstandingInteractiveRequest(t *testing.T) {
	d, proxy, _, link, _ := newProxyInputHarness(t)
	clock := newProxyCommandTestClock()
	firstDone := make(chan struct{})
	go func() {
		_, _ = proxy.sendCommand(context.Background(), clock, "next-tab", nil)
		close(firstDone)
	}()
	first := requireProxyCommandRequest(t, link)
	_ = awaitTestValue(t, clock.timers, "first command did not arm timeout")

	secondDone := make(chan struct{})
	go func() {
		_, _ = proxy.sendCommand(context.Background(), clock, "previous-tab", nil)
		close(secondDone)
	}()
	requireNoProxyFrame(t, link)
	_, err := d.handleLinkFrame(proxy, proxy.linkGeneration, commandResultFrame(ports.CommandResult{RequestID: first.RequestID, OK: true}))
	require.NoError(t, err)
	awaitTestCompletion(t, firstDone, "first command did not complete")
	second := requireProxyCommandRequest(t, link)
	_ = awaitTestValue(t, clock.timers, "second command did not arm timeout")
	_, err = d.handleLinkFrame(proxy, proxy.linkGeneration, commandResultFrame(ports.CommandResult{RequestID: second.RequestID, OK: true}))
	require.NoError(t, err)
	awaitTestCompletion(t, secondDone, "second command did not complete")
}

func TestProxyAttachedCommandTimeoutCancelAndGenerationReplacement(t *testing.T) {
	t.Run("timeout", func(t *testing.T) {
		_, proxy, _, link, _ := newProxyInputHarness(t)
		clock := newProxyCommandTestClock()
		done := make(chan error, 1)
		go func() {
			_, err := proxy.sendCommand(context.Background(), clock, "next-tab", nil)
			done <- err
		}()
		_ = requireProxyCommandRequest(t, link)
		timer := awaitTestValue(t, clock.timers, "command did not arm timeout")
		timer.Fire()
		require.ErrorIs(t, awaitTestValue(t, done, "command did not time out"), errProxyCommandTimeout)
	})

	t.Run("context cancellation", func(t *testing.T) {
		_, proxy, _, link, _ := newProxyInputHarness(t)
		clock := newProxyCommandTestClock()
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() {
			_, err := proxy.sendCommand(ctx, clock, "next-tab", nil)
			done <- err
		}()
		_ = requireProxyCommandRequest(t, link)
		_ = awaitTestValue(t, clock.timers, "command did not arm timeout")
		cancel()
		require.ErrorIs(t, awaitTestValue(t, done, "command did not cancel"), context.Canceled)
	})

	t.Run("link generation replacement", func(t *testing.T) {
		d, proxy, _, link, _ := newProxyInputHarness(t)
		clock := newProxyCommandTestClock()
		done := make(chan error, 1)
		go func() {
			_, err := proxy.sendCommand(context.Background(), clock, "next-tab", nil)
			done <- err
		}()
		request := requireProxyCommandRequest(t, link)
		_ = awaitTestValue(t, clock.timers, "command did not arm timeout")
		oldGeneration := proxy.linkGeneration
		newGeneration, _ := proxy.installTransport(newProxyTestTransport())
		require.Greater(t, newGeneration, oldGeneration)
		require.Error(t, awaitTestValue(t, done, "replacement did not cancel command"))
		_, err := d.handleLinkFrame(proxy, oldGeneration, commandResultFrame(ports.CommandResult{RequestID: request.RequestID, OK: true}))
		require.NoError(t, err, "late old-generation result must be harmless")
	})
}

func TestProxyAttachedCommandRejectsTargetsAndExecutesExactActiveSession(t *testing.T) {
	d, sess, ac, sends := newManualSessionWithPTYs(t, newQuietPTY())
	ac.proxied = true
	second := newTab(newQuietPTY(), domain.Size{Cols: 80, Rows: 22})
	publishTiledPaneOwners(sess, second)
	sess.mu.Lock()
	sess.tabs = append(sess.tabs, second)
	sess.active = 0
	sess.mu.Unlock()
	d.attachCoordinator(sess, nil, ac, true)

	send := func(request ports.CommandRequest) ports.CommandResult {
		payload, err := ports.MarshalCommandRequest(request)
		require.NoError(t, err)
		token := sess.attachmentToken(ac, ac.transport())
		require.False(t, d.handleActiveClientFrame(token, ports.Frame{Type: ports.MsgCommand, Payload: payload}))
		frame := awaitFrame(t, sends, ports.MsgCommandResult)
		result, err := ports.UnmarshalCommandResult(frame.Payload)
		require.NoError(t, err)
		return result
	}

	result := send(ports.CommandRequest{Version: ports.ProtocolVersion, RequestID: 1, Attached: true, Slug: "next-tab"})
	require.True(t, result.OK)
	sess.mu.Lock()
	require.Equal(t, 1, sess.active)
	sess.mu.Unlock()

	result = send(ports.CommandRequest{Version: ports.ProtocolVersion, RequestID: 2, Attached: true, Slug: "previous-tab", TargetSession: "other"})
	require.False(t, result.OK)
	sess.mu.Lock()
	require.Equal(t, 1, sess.active, "target override must not execute against any session")
	sess.mu.Unlock()

	result = send(ports.CommandRequest{Version: ports.ProtocolVersion, RequestID: 3, Attached: true, Slug: "rename-tab"})
	require.True(t, result.OK)
	require.True(t, ac.overlays.promptActive(), "attached interactive command must be able to open the remote prompt")
}

func TestProxyAttachedCommandRemoteErrorCreatesLocalNotice(t *testing.T) {
	d, proxy, ac, link, handler := newProxyInputHarness(t)
	clock := newProxyCommandTestClock()
	d.clock = clock
	handler.enterPalette()
	done := make(chan struct{})
	go func() {
		d.handlePaletteInput(ac, []byte("NXT\r"), handler.roleToken.effect)
		close(done)
	}()
	request := requireProxyCommandRequest(t, link)
	_ = awaitTestValue(t, clock.timers, "palette command did not arm timeout")
	_, err := d.handleLinkFrame(proxy, proxy.linkGeneration, commandResultFrame(ports.CommandResult{RequestID: request.RequestID, Code: ports.ErrInternal, Text: "remote failed"}))
	require.NoError(t, err)
	awaitTestCompletion(t, done, "palette command did not finish")
	notice, ok := d.notices.latest()
	require.True(t, ok)
	require.Equal(t, domain.NoticeSessionUnavailable, notice.Code)
	require.Contains(t, notice.Message, "remote failed")
}

func TestProxyAttachedCommandMalformedAndUnavailableResultsAreSafe(t *testing.T) {
	d, proxy, _, _, _ := newProxyInputHarness(t)
	result, err := d.handleLinkFrame(proxy, proxy.linkGeneration, ports.Frame{Type: ports.MsgCommandResult, Payload: []byte{1}})
	require.Equal(t, proxyLinkStop, result)
	require.Error(t, err)

	proxy.mu.Lock()
	proxy.transport = nil
	proxy.mu.Unlock()
	_, err = proxy.sendCommand(context.Background(), stubClock{}, "next-tab", nil)
	require.True(t, errors.Is(err, errProxyCommandUnavailable))
}

func TestProxyOwnershipMakesCompatibleRemoteRowsSelectable(t *testing.T) {
	key := domain.RemoteSessionKey{Host: "arch", Name: "work"}
	for _, status := range []remoteHostStatus{remoteHostCached, remoteHostRefreshing, remoteHostFresh, remoteHostUnreachable} {
		view := remotePickerView(key, ports.RemoteCatalogSession{Name: key.Name, State: "running", Tabs: 2}, status, time.Unix(1, 0))
		require.True(t, view.RemoteAttachReady, "compatible status %d must be enabled after ownership is complete", status)
		model := picker.New([]picker.SessionView{view}, picker.SelectionConfig{Mode: picker.SelectNavigationTab})
		target, selectable := model.Selected()
		require.True(t, selectable)
		require.Equal(t, key, *target.RemoteKey)
	}
	mismatch := remotePickerView(key, ports.RemoteCatalogSession{Name: key.Name, State: "running"}, remoteHostVersionMismatch, time.Time{})
	require.False(t, mismatch.RemoteAttachReady)
	_, selectable := picker.New([]picker.SessionView{mismatch}, picker.SelectionConfig{Mode: picker.SelectNavigationTab}).Selected()
	require.False(t, selectable)
}

func TestProxyPickerSelectionDialsOutsideLocksAndRevalidatesExactRoleAndKey(t *testing.T) {
	d, source, ac, _ := newManualSessionWithPTYs(t, newQuietPTY())
	key := domain.RemoteSessionKey{Host: "arch", Name: "work"}
	transport := newProxyTestTransport()
	transport.recv <- proxyRecv{frame: proxyWelcome(key.Name, 1, ports.CapabilityProxied)}
	transport.recv <- proxyRecv{frame: proxyMeta(key.Name)}
	factory := portsmocks.NewMockRemoteDialerFactory(t)
	dialer := portsmocks.NewMockDialer(t)
	var daemonLockAvailable, sourceLockAvailable bool
	factory.EXPECT().DialerForRemote(key.Host, key.Name, ports.RemoteTransportUDP, mock.Anything).
		Run(func(string, string, ports.RemoteTransportMode, *slog.Logger) {
			// If picker selection retained either architecture lock while dialing,
			// this synchronous callback would deadlock.
			d.mu.Lock()
			daemonLockAvailable = true
			d.mu.Unlock()
			source.mu.Lock()
			sourceLockAvailable = true
			source.mu.Unlock()
		}).Return(dialer, nil).Once()
	dialer.EXPECT().Dial(mock.Anything).Return(transport, nil).Once()
	d.remoteDialerFactory = factory
	d.remoteTransportMode = ports.RemoteTransportUDP
	d.attachCoordinator(source, nil, ac, true)

	token := source.attachmentToken(ac, ac.transport())
	effect, admitted := ac.beginRoleEffect(token)
	require.True(t, admitted)
	token.effect = effect
	err := d.switchToTargetForRole(token, picker.Target{Session: key.ID(), RemoteKey: &key, TabIndex: -1}, sessionHandoffGuard{}, "picker-select")
	require.NoError(t, err)
	require.True(t, daemonLockAvailable)
	require.True(t, sourceLockAvailable)
	proxy, ok := ac.currentAttachmentSession().(*proxySession)
	require.True(t, ok)
	require.Equal(t, key, proxy.key)
	t.Cleanup(func() { stopProxy(t, proxy) })

	wrong := domain.RemoteSessionKey{Host: "arch", Name: "other"}
	currentToken := attachmentToken(proxy, ac, ac.transport())
	currentEffect, admitted := ac.beginRoleEffect(currentToken)
	require.True(t, admitted)
	currentToken.effect = currentEffect
	err = d.switchToTargetForRole(currentToken, picker.Target{Session: key.ID(), RemoteKey: &wrong, TabIndex: -1}, sessionHandoffGuard{}, "picker-select")
	require.ErrorIs(t, err, errAttachmentTransition)
	require.Same(t, proxy, ac.currentAttachmentSession(), "mismatched structured key must not redirect attachment")

	staleToken := attachmentToken(proxy, ac, ac.transport())
	staleEffect, admitted := ac.beginRoleEffect(staleToken)
	require.True(t, admitted)
	staleToken.effect = staleEffect
	ac.roleGeneration.Add(1) // model a concurrent role replacement after selection
	err = d.switchToTargetForRole(staleToken, picker.Target{Session: key.ID(), RemoteKey: &key, TabIndex: -1}, sessionHandoffGuard{}, "picker-select")
	require.ErrorIs(t, err, errAttachmentTransition)
	require.Same(t, proxy, ac.currentAttachmentSession(), "stale initiating role must not republish attachment")
}

func TestRemotePickerEnterRoutesStructuredKeyThroughProxyOwnership(t *testing.T) {
	d, local, ac, _ := newManualSessionWithPTYs(t, newQuietPTY())
	d.attachCoordinator(local, nil, ac, true)
	key := domain.RemoteSessionKey{Host: "arch", Name: "enter"}
	transport := newProxyTestTransport()
	transport.recv <- proxyRecv{frame: proxyWelcome(key.Name, 1, ports.CapabilityProxied)}
	transport.recv <- proxyRecv{frame: proxyMeta(key.Name)}
	d.remoteDialerFactory = newProxyConstructionFactory(transport)
	d.remoteTransportMode = ports.RemoteTransportUDP
	d.remoteCatalog.replaceCache([]ports.RemoteCatalogCacheEntry{{
		Host: key.Host, FetchedAt: time.Unix(1, 0),
		Sessions: []ports.RemoteCatalogSession{{Name: key.Name, State: "running"}},
	}})

	model := d.newPickerModel(local, pickerNavigate, moveSourceLocator{}, picker.SourceFilter{Session: key.ID(), RemoteKey: &key})
	d.publishPicker(local, ac, model, pickerNavigate, moveSourceLocator{})
	target, selectable := model.Selected()
	require.True(t, selectable)
	require.Equal(t, key, *target.RemoteKey)

	token := local.attachmentToken(ac, ac.transport())
	effect, admitted := ac.beginRoleEffect(token)
	require.True(t, admitted)
	d.handlePickerInput(ac, []byte("\r"), effect)
	proxy, ok := ac.currentAttachmentSession().(*proxySession)
	require.True(t, ok, "Enter must route the row instead of only marking it selectable")
	require.Equal(t, key, proxy.key)
	t.Cleanup(func() { stopProxy(t, proxy) })
}
