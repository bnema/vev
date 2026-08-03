package daemon

import (
	"errors"
	"strings"

	"github.com/bnema/vev/internal/domain"
	promptui "github.com/bnema/vev/internal/usecase/prompt"
	"github.com/bnema/vev/internal/usecase/ui"
)

func promptModalFor(title string) ui.Modal {
	return ui.Modal{WidthPct: 100, MinWidth: 32, FixedHeight: 4, Title: title, Anchor: domain.AnchorBottom, Margins: ui.Margins{Bottom: 1}}
}

func (d *Daemon) enterPrompt(sess attachmentSession, ac *attachedClient, title, initial string, submit func(string) error) {
	d.closePrompt(ac)
	ac.overlays.promptMu.Lock()
	ac.overlays.prompt = promptui.New(title, initial)
	ac.overlays.promptSubmit = submit
	ac.overlays.promptPending = nil
	ac.overlays.promptMu.Unlock()
	d.invalidateRender(sess, ac, true, "prompt.go")
}

func (d *Daemon) enterTransitionPrompt(sess attachmentSession, ac *attachedClient, title, initial string, submit func(string, attachmentConnectionToken) error) {
	d.closePrompt(ac)
	ac.overlays.promptMu.Lock()
	ac.overlays.prompt = promptui.New(title, initial)
	ac.overlays.promptTransitionSubmit = submit
	ac.overlays.promptPending = nil
	ac.overlays.promptMu.Unlock()
	d.invalidateRender(sess, ac, true, "prompt.go")
}

func (d *Daemon) closePrompt(ac *attachedClient) {
	ac.overlays.promptMu.Lock()
	ac.overlays.prompt = nil
	ac.overlays.promptSubmit = nil
	ac.overlays.promptTransitionSubmit = nil
	ac.overlays.promptPending = nil
	ac.overlays.promptMu.Unlock()
}

// promptValidationError identifies expected prompt feedback that should stay
// inline rather than creating a history entry or toast.
func promptValidationError(err error) bool {
	return errors.Is(err, domain.ErrInvalidSessionName) ||
		errors.Is(err, errSessionNameRequired) ||
		errors.Is(err, errSessionNameInUse)
}

func (d *Daemon) handlePromptInput(ac *attachedClient, data []byte, effects ...*attachmentEffectTicket) {
	sess := ac.currentAttachmentSession()
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
	var transitionSubmit func(string, attachmentConnectionToken) error
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
			transitionSubmit = ac.overlays.promptTransitionSubmit
			submittedPrompt = ac.overlays.prompt
			value = strings.TrimSpace(ac.overlays.prompt.Value())
		},
		cancel: func() { exit = true },
		up:     func() {},
		down:   func() {},
	})
	ac.overlays.promptMu.Unlock()

	if submit != nil || transitionSubmit != nil {
		var err error
		if transitionSubmit != nil {
			var token attachmentConnectionToken
			if len(effects) != 0 && effects[0] != nil {
				token = effects[0].connectionToken()
			}
			err = transitionSubmit(value, token)
		} else {
			err = submit(value)
		}
		if errors.Is(err, errAttachmentTransition) {
			return
		}
		if err != nil {
			ac.overlays.promptMu.Lock()
			if ac.overlays.prompt == submittedPrompt {
				ac.overlays.prompt.SetError(err.Error())
			}
			ac.overlays.promptMu.Unlock()
			if !promptValidationError(err) {
				d.reportAttachmentError(sess, err)
			}
			d.invalidateRender(sess, ac, true, "prompt.go")
			return
		}
		d.closePrompt(ac)
		if current := ac.currentAttachmentSession(); current != nil {
			d.invalidateRender(current, ac, true, "prompt.go")
		}
		return
	}
	if exit {
		d.closePrompt(ac)
		d.invalidateRender(sess, ac, true, "prompt.go")
		return
	}
	if changed {
		d.invalidateRender(sess, ac, true, "prompt.go")
	}
}
