package daemon

import "github.com/bnema/vev/internal/domain"

// floatingState describes the lifecycle of a tab's independent floating pane.
// A floating slot is always guarded by its owning tab.mu.
type floatingState uint8

const (
	floatingUninitialized floatingState = iota
	floatingWarming
	floatingHidden
	floatingVisible
)

// floatingSlot holds the runtime which is deliberately outside tab.tree and
// tab.panes. generation identifies an asynchronous launch and its installed
// pane, preventing stale launch completions and exits from changing new state.
type floatingSlot struct {
	state          floatingState
	pane           *pane
	desiredVisible bool
	generation     uint64
	launch         domain.FloatingConfig
}

// beginFloatingWarmLocked starts a single background launch. The caller must
// hold tb.mu and must only call it for an uninitialized slot.
func (tb *tab) beginFloatingWarmLocked(launch domain.FloatingConfig, desiredVisible bool) uint64 {
	if tb == nil || tb.floating.state != floatingUninitialized {
		return 0
	}
	tb.floating.generation++
	tb.floating.state = floatingWarming
	tb.floating.desiredVisible = desiredVisible
	tb.floating.launch = launch
	return tb.floating.generation
}

// toggleFloatingLocked applies the user action. Its result says whether a new
// PTY launch is required and returns the generation associated with that launch
// or current slot.
func (tb *tab) toggleFloatingLocked(launch domain.FloatingConfig) (bool, uint64) {
	if tb == nil {
		return false, 0
	}
	switch tb.floating.state {
	case floatingUninitialized:
		generation := tb.beginFloatingWarmLocked(launch, true)
		return generation != 0, generation
	case floatingWarming:
		tb.floating.desiredVisible = !tb.floating.desiredVisible
	case floatingHidden:
		tb.floating.state = floatingVisible
	case floatingVisible:
		tb.floating.state = floatingHidden
	}
	return false, tb.floating.generation
}

// installFloatingLocked installs a completed launch only when it is still the
// current warming generation. Stale completions must close their PTY elsewhere.
func (tb *tab) installFloatingLocked(p *pane, generation uint64) bool {
	if tb == nil || p == nil || tb.floating.state != floatingWarming || tb.floating.generation != generation {
		return false
	}
	tb.floating.pane = p
	if tb.floating.desiredVisible {
		tb.floating.state = floatingVisible
	} else {
		tb.floating.state = floatingHidden
	}
	return true
}

// failFloatingLocked clears only the current warming launch. It deliberately
// leaves generation intact; the next launch advances it.
func (tb *tab) failFloatingLocked(generation uint64) bool {
	if tb == nil || tb.floating.state != floatingWarming || tb.floating.generation != generation {
		return false
	}
	tb.floating.state = floatingUninitialized
	tb.floating.pane = nil
	tb.floating.desiredVisible = false
	tb.floating.launch = domain.FloatingConfig{}
	return true
}

// clearFloatingLocked clears a matching installed runtime. Matching both pane
// identity and generation prevents an old reader from reaping a newer pane.
func (tb *tab) clearFloatingLocked(p *pane, generation uint64) bool {
	if tb == nil || p == nil || tb.floating.pane != p || tb.floating.generation != generation ||
		(tb.floating.state != floatingHidden && tb.floating.state != floatingVisible) {
		return false
	}
	tb.floating.generation++
	tb.floating.state = floatingUninitialized
	tb.floating.pane = nil
	tb.floating.desiredVisible = false
	tb.floating.launch = domain.FloatingConfig{}
	return true
}

// terminalTargetLocked chooses the visible floating terminal ahead of the
// normal layout target. The caller must hold tb.mu.
func (tb *tab) terminalTargetLocked() *pane {
	if tb != nil && tb.floating.state == floatingVisible && tb.floating.pane != nil {
		return tb.floating.pane
	}
	return tb.focusedPane()
}

// floatingInnerSize returns the terminal area inside a percentage-sized popup.
// For each axis, a popup with at least three cells spends one cell on each
// border; smaller popups omit that axis's border. A valid tab always yields a
// valid PTY size.
func floatingInnerSize(tabSize domain.Size, cfg domain.FloatingConfig) domain.Size {
	if !tabSize.Valid() {
		return domain.Size{}
	}
	return domain.Size{
		Cols: floatingInnerAxis(tabSize.Cols, cfg.Width),
		Rows: floatingInnerAxis(tabSize.Rows, cfg.Height),
	}
}

func floatingInnerAxis(available, percent int) int {
	percent = min(max(percent, 1), 100)
	bounds := min(max(available*percent/100, 1), available)
	if bounds >= 3 {
		return bounds - 2
	}
	return bounds
}
