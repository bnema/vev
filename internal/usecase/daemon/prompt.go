package daemon

import (
	"strings"

	"github.com/bnema/vev/internal/domain"
	promptui "github.com/bnema/vev/internal/usecase/prompt"
	"github.com/bnema/vev/internal/usecase/ui"
	"github.com/bnema/vev/pkg/renderer"
)

func promptModalFor(title string) ui.Modal {
	return ui.Modal{WidthPct: 100, MinWidth: 32, FixedHeight: 4, Title: title, Anchor: ui.AnchorBottom, BottomMargin: 1}
}

func (d *Daemon) enterPrompt(sess *session, ac *attachedClient, title, initial string, submit func(string) error) {
	d.closePrompt(ac)
	ac.overlays.promptMu.Lock()
	ac.overlays.prompt = promptui.New(title, initial)
	ac.overlays.promptSubmit = submit
	ac.overlays.promptPending = nil
	ac.overlays.promptMu.Unlock()
	d.paint(sess, ac, true)
}

func (d *Daemon) closePrompt(ac *attachedClient) {
	ac.overlays.promptMu.Lock()
	ac.overlays.prompt = nil
	ac.overlays.promptSubmit = nil
	ac.overlays.promptPending = nil
	ac.overlays.promptMu.Unlock()
}

func (d *Daemon) handlePromptInput(ac *attachedClient, data []byte) {
	sess := ac.currentSession()
	if sess == nil {
		return
	}

	ac.overlays.promptMu.Lock()
	if ac.overlays.prompt == nil {
		ac.overlays.promptPending = nil
		ac.overlays.promptMu.Unlock()
		return
	}

	changed := false
	exit := false
	var submit func(string) error
	var submittedPrompt *promptui.Model
	var value string

	routeOverlayBytes(data, &ac.overlays.promptPending, overlayEvents{
		rune: func(r rune) {
			ac.overlays.prompt.Insert(r)
			changed = true
		},
		backspace: func() {
			ac.overlays.prompt.Backspace()
			changed = true
		},
		enter: func() {
			submit = ac.overlays.promptSubmit
			submittedPrompt = ac.overlays.prompt
			value = strings.TrimSpace(ac.overlays.prompt.Value())
		},
		cancel: func() { exit = true },
		up:     func() {},
		down:   func() {},
	})
	ac.overlays.promptMu.Unlock()

	if submit != nil {
		if err := submit(value); err != nil {
			ac.overlays.promptMu.Lock()
			if ac.overlays.prompt == submittedPrompt {
				ac.overlays.prompt.SetError(err.Error())
			}
			ac.overlays.promptMu.Unlock()
			d.paint(sess, ac, true)
			return
		}
		d.closePrompt(ac)
		if current := ac.currentSession(); current != nil {
			d.paint(current, ac, true)
		}
		return
	}
	if exit {
		d.closePrompt(ac)
		d.paint(sess, ac, true)
		return
	}
	if changed {
		d.paint(sess, ac, true)
	}
}

func composePromptClientFrame(model *promptui.Model, base renderer.Frame, styles ...themeStyles) (renderer.Frame, []renderer.Damage) {
	styleSet := resolveThemeStyles(styles)
	inner := promptModalFor(model.Title()).Composite(base, styleSet.border)
	modalFrame := model.Render(domain.Size{Cols: inner.Width, Rows: inner.Height}, styleSet.accent)
	for y := range min(inner.Height, modalFrame.Height) {
		for x := range min(inner.Width, modalFrame.Width) {
			base.Set(inner.X+x, inner.Y+y, modalFrame.At(x, y))
		}
	}
	return base, []renderer.Damage{renderer.FullRedraw()}
}
