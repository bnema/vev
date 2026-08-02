package daemon

import (
	"errors"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/usecase/keys"
	"github.com/bnema/vev/internal/usecase/layout"
	"github.com/bnema/vev/internal/usecase/mouse"
	"github.com/bnema/vev/internal/usecase/palette"
)

// sendInput is the sole proxy input sender. Sequence assignment and transport
// Send are serialized with every other proxy link write; raw is never rebuilt
// from an interpreted key action.
func (p *proxySession) sendInput(raw []byte) error {
	if p == nil || len(raw) == 0 {
		return nil
	}
	data := append([]byte(nil), raw...)
	p.sendMu.Lock()
	defer p.sendMu.Unlock()

	p.mu.Lock()
	transport := p.transport
	p.mu.Unlock()
	if transport == nil {
		return errors.New("proxy session: input link is unavailable")
	}
	p.inputNext++
	return transport.Send(ports.Frame{
		Type: ports.MsgInput,
		Payload: ports.MarshalInput(ports.Input{
			InputSeq: p.inputNext,
			Data:     data,
		}),
	})
}

// proxyKeyHandler pins input ownership while a local client is attached to a
// proxy. Local UI actions never reach the link. Every remote-owned action is
// sent using the exact bytes supplied by keys.Router.
type proxyKeyHandler struct {
	d         *Daemon
	proxy     *proxySession
	ac        *attachedClient
	roleToken attachmentRoleToken
}

func (h proxyKeyHandler) acquireRoleEffect() (*roleEffectTicket, bool, bool) {
	if h.proxy == nil || h.ac == nil {
		return nil, false, false
	}
	if h.roleToken.ac == nil {
		return nil, false, false
	}
	if h.roleToken.sess != h.proxy || h.roleToken.ac != h.ac {
		return nil, false, false
	}
	if effect := h.roleToken.effect; effect != nil && !effect.ended.Load() {
		return effect, false, true
	}
	effect, admitted := h.roleToken.ac.beginRoleEffect(h.roleToken)
	if h.d != nil && h.d.afterDelayedKeyEffectAttempt != nil {
		h.d.afterDelayedKeyEffectAttempt(admitted)
	}
	if !admitted {
		return nil, false, false
	}
	if h.d != nil && h.d.afterRoleEffectAdmitted != nil {
		token := h.roleToken
		token.effect = effect
		h.d.afterRoleEffectAdmitted(token)
	}
	return effect, true, true
}

func (h proxyKeyHandler) sendOwned(raw []byte) {
	if len(raw) == 0 {
		return
	}
	if err := h.proxy.sendInput(raw); err != nil && h.d != nil {
		h.d.log.Warn("proxy input send failed", "host", h.proxy.key.Host, "session", h.proxy.key.Name, "err", err)
	}
}

func (h proxyKeyHandler) send(raw []byte) {
	effect, owned, ok := h.acquireRoleEffect()
	if !ok {
		return
	}
	if owned {
		defer effect.End()
	}
	h.sendOwned(raw)
}

func (h proxyKeyHandler) Forward(raw []byte) { h.send(raw) }

func (h proxyKeyHandler) Mouse(event mouse.Event) { h.send(event.Raw) }

func (h proxyKeyHandler) Action(action keys.Action, raw []byte) {
	effect, owned, ok := h.acquireRoleEffect()
	if !ok {
		return
	}
	if owned {
		defer effect.End()
	}
	h.roleToken.effect = effect
	switch action {
	case keys.ActionOpenPalette:
		h.enterPalette()
	case keys.ActionJumpAttention:
		if h.proxy.hasRemoteAttention() {
			h.sendOwned(raw)
			return
		}
		h.jumpLocalAttention()
	case keys.ActionSwitchTab1, keys.ActionSwitchTab2, keys.ActionSwitchTab3,
		keys.ActionSwitchTab4, keys.ActionSwitchTab5, keys.ActionSwitchTab6,
		keys.ActionSwitchTab7, keys.ActionSwitchTab8, keys.ActionSwitchTab9,
		keys.ActionFocusPaneLeft, keys.ActionFocusPaneRight, keys.ActionFocusPaneUp, keys.ActionFocusPaneDown,
		keys.ActionToggleFloatingPane,
		keys.ActionGrowPaneWidth, keys.ActionShrinkPaneWidth,
		keys.ActionGrowPaneHeight, keys.ActionShrinkPaneHeight,
		keys.ActionEqualizePanes,
		keys.ActionConsumeOrExpelPaneLeft, keys.ActionConsumeOrExpelPaneRight:
		h.sendOwned(raw)
	}
}

func (p *proxySession) hasRemoteAttention() bool {
	if p == nil {
		return false
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, tab := range p.meta.Tabs {
		if tab.Attention {
			return true
		}
	}
	return false
}

func (h proxyKeyHandler) enterPalette() {
	if h.d == nil || h.ac == nil || h.proxy == nil {
		return
	}
	// paletteResults still accepts a local current session. Build its immutable
	// snapshot without one and remove this proxy by its structured opaque ID.
	recent := h.d.recentSessions(h.proxy)
	commands := h.d.paletteCommands()
	results := h.d.paletteResults(nil, commands)
	filtered := results[:0]
	for _, result := range results {
		if id, ok := result.SessionID(); ok && id == h.proxy.id {
			continue
		}
		filtered = append(filtered, result)
	}
	h.ac.overlays.paletteMu.Lock()
	h.ac.overlays.paletteGeneration++
	h.ac.overlays.palette = palette.New(filtered)
	h.ac.overlays.paletteRecent = recent
	h.ac.overlays.paletteHints = palette.ContextualHints{}
	h.ac.overlays.paletteFeedback = ""
	h.ac.overlays.palettePending = nil
	h.ac.overlays.paletteMu.Unlock()
	h.d.invalidateRender(h.proxy, h.ac, true, "proxy_input.go")
}

// jumpLocalAttention handles only the selected-session part of attention
// navigation. A pending tab on the proxy remains remote-owned; without one,
// the local daemon may move the client to another local session.
func (h proxyKeyHandler) jumpLocalAttention() {
	if h.d == nil || h.roleToken.ac == nil || h.roleToken.effect == nil {
		return
	}
	target, ok := h.d.oldestOtherSessionAttention(nil)
	if !ok {
		return
	}
	h.d.mu.Lock()
	targetSession, ok := localSession(h.d.sessions[target.sessionID])
	h.d.mu.Unlock()
	if !ok {
		return
	}

	token := h.roleToken
	token.effect.bindActionEnd(h.d, "proxy-jump-attention")
	token.effect.End()
	transition, err := h.d.transitionAttachment(attachmentTransitionRequest{
		source: h.proxy, target: targetSession, next: h.ac,
		expectedRole: attachmentActive, targetRole: attachmentActive,
		expectedTransport: token.transport, sourceToken: &token,
		action: "proxy-jump-attention", activateTargetTab: true,
		targetTabIndex: target.tabIndex, ready: true,
	})
	if err != nil {
		return
	}
	h.d.touchMRU(targetSession)
	h.ac.recordPreviousSession(h.proxy)
	h.d.deferAttachmentTransitionCleanups(transition)
	h.d.firstPaintForTransition(transition.published)
}

func proxyMouseOwnedLocally(rt *overlayRuntime) bool {
	// copyActive cannot be reached for proxy attachments through production input
	// routing: copy mode is local-session-only and proxy keys never enter it.
	return rt != nil && (rt.promptActive() || rt.paletteActive() || rt.pickerActive() ||
		rt.noticesActive() || rt.resizeModeActive())
}

func proxiedNavConfig(cfg domain.NavConfig) domain.NavConfig {
	cfg.OverflowSessions = false
	return cfg
}

func proxiedJumpSearchesOtherSessions(proxied bool) bool { return !proxied }

// focusDirProxied performs ordinary pane and tab overflow on the remote daemon,
// but never follows OverflowSessions. The local proxy daemon is the only owner
// allowed to change the selected session.
func (d *Daemon) focusDirProxied(sess *session, ac *attachedClient, dir layout.Direction) error {
	target := resolveDaemonActionTargetForAttachment(sess, ac)
	oldFocus := layout.PaneID("")
	if target.pane != nil {
		oldFocus = target.pane.id
	}
	span, err := d.focusDirAt(sess, target.tab, target.pane, dir)
	if err == nil {
		if ac != nil {
			d.finishPaneFocusForClient(sess, ac, target.tab, oldFocus, "proxy_input.go")
		}
		return nil
	}
	if !errors.Is(err, errNoNeighbor) || target.tab == nil || !overflowSourceEligible(sess, target.tab) {
		return err
	}

	sess.mu.Lock()
	position, count := sess.active, len(sess.tabs)
	sess.mu.Unlock()
	step := resolveOverflow(dir, proxiedNavConfig(d.currentNavConfig()), position, count)
	if step.kind != overflowTabs {
		return errNoNeighbor
	}
	candidate, ok := d.prepareTabOverflow(sess, target.tab, dir, span, step.delta)
	if !ok || !d.commitTabOverflow(sess, candidate) {
		return errNoNeighbor
	}
	d.activateTab(sess, candidate.target)
	if ac != nil {
		d.finishPaneFocusForClient(sess, ac, candidate.target, candidate.targetOldFocus, "proxy_input.go")
	}
	return nil
}
