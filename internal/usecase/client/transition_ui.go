package client

import (
	"fmt"
	"io"
	"time"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol"
)

const (
	transitionSpinnerDelay    = 120 * time.Millisecond
	transitionSpinnerInterval = 80 * time.Millisecond
)

var transitionSpinnerFrames = [...]rune{'⠋', '⠙', '⠹', '⠸', '⠼', '⠴', '⠦', '⠧', '⠇', '⠏'}

func transitionMessage(target protocol.AttachTarget) string {
	origin := target.Endpoint
	if target.RemoteTarget != nil && target.RemoteTarget.DisplayOrigin != "" {
		origin = target.RemoteTarget.DisplayOrigin
	}
	label := target.Session
	if origin != "" {
		label = domain.RemoteSessionDisplay(target.Session, origin)
	}
	verb := "Switching to "
	if target.RemoteTarget != nil && target.RemoteTarget.Stopped {
		verb = "Starting "
	}
	return verb + label + "…"
}

func drawTransitionToast(out io.Writer, size domain.Size, frame int, message string) (domain.Rect, error) {
	spinner := transitionSpinnerFrames[frame%len(transitionSpinnerFrames)]
	return drawClientToast(out, size, fmt.Sprintf("%c %s", spinner, message))
}

// transitionUI owns the client-local handoff presentation. Every method is
// called by the Runner or active attach loop, preserving the terminal's single
// writer rule while network operations expose timer/resize events.
type transitionUI struct {
	term       ports.Terminal
	clock      ports.Clock
	rawEntered *bool
	timer      ports.Timer
	active     bool
	showing    bool
	frame      int
	message    string
}

func newTransitionUI(term ports.Terminal, clock ports.Clock, rawEntered *bool) *transitionUI {
	return &transitionUI{term: term, clock: clock, rawEntered: rawEntered}
}

func (u *transitionUI) start(target protocol.AttachTarget) {
	if u == nil {
		return
	}
	u.stop()
	u.active = true
	u.frame = 0
	u.message = transitionMessage(target)
	if u.clock != nil {
		u.timer = u.clock.NewTimer(transitionSpinnerDelay)
	}
}

func (u *transitionUI) tickC() <-chan time.Time {
	if u == nil || !u.active || u.timer == nil {
		return nil
	}
	return u.timer.C()
}

func (u *transitionUI) advance() error {
	if u == nil || !u.active {
		return nil
	}
	geometry, err := u.term.Geometry()
	if err != nil {
		return fmt.Errorf("reading terminal geometry for transition toast: %w", err)
	}
	if err := u.redraw(geometry.Size); err != nil {
		return err
	}
	u.frame = (u.frame + 1) % len(transitionSpinnerFrames)
	if u.timer != nil {
		u.timer.Reset(transitionSpinnerInterval)
	}
	return nil
}

func (u *transitionUI) redraw(size domain.Size) error {
	if u == nil || !u.active || u.rawEntered == nil || !*u.rawEntered {
		return nil
	}
	bounds, err := drawTransitionToast(u.term.Out(), size, u.frame, u.message)
	if err != nil {
		return fmt.Errorf("drawing transition toast: %w", err)
	}
	if bounds.Width == 0 || bounds.Height == 0 {
		return nil
	}
	if err := u.term.Flush(); err != nil {
		return fmt.Errorf("flushing transition toast: %w", err)
	}
	u.showing = true
	return nil
}

func (u *transitionUI) stop() {
	if u == nil {
		return
	}
	if u.timer != nil {
		u.timer.Stop()
		u.timer = nil
	}
	u.active = false
	u.showing = false
	u.frame = 0
	u.message = ""
}
