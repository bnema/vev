package client

import (
	"bytes"
	"sync"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/protocol"
)

// Terminal focus.
//
// The client turns on focus reporting (DEC mode 1004) with its other terminal
// modes. The terminal then sends ESC [ I when its window gains focus and
// ESC [ O when it loses it. terminalFocusScanner strips both reports from the
// input stream and keeps the latest state, so any client feature can read
// Focus() and the attached loop tells the daemon about each change.
//
// A terminal without focus reporting sends nothing: focus stays unknown and
// every feature keeps its pre-focus behavior.

var (
	terminalFocusInReport  = []byte("\x1b[I")
	terminalFocusOutReport = []byte("\x1b[O")
)

// TerminalFocusState is the client's one terminal focus. The terminal input
// pump is its only writer; any client feature may read it or wait for a
// change. It outlives attachments, so a new attachment starts from the focus
// the terminal last reported.
type TerminalFocusState struct {
	mu      sync.Mutex
	focus   domain.TerminalFocus
	changed chan struct{}
}

func newTerminalFocusState() *TerminalFocusState {
	return &TerminalFocusState{changed: make(chan struct{})}
}

// Focus returns the latest reported focus. A nil state is unknown.
func (s *TerminalFocusState) Focus() domain.TerminalFocus {
	if s == nil {
		return domain.TerminalFocusUnknown
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.focus
}

// Watch returns the current focus and a channel closed on the next change.
func (s *TerminalFocusState) Watch() (domain.TerminalFocus, <-chan struct{}) {
	if s == nil {
		return domain.TerminalFocusUnknown, nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.focus, s.changed
}

func (s *TerminalFocusState) set(focus domain.TerminalFocus) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if focus == s.focus {
		return
	}
	s.focus = focus
	close(s.changed)
	s.changed = make(chan struct{})
}

// attachmentFocusForeground is the optional seam through which an attached
// loop reads the terminal focus.
type attachmentFocusForeground interface {
	terminalFocus() *TerminalFocusState
}

func (f *attachmentForeground) terminalFocus() *TerminalFocusState {
	if f == nil || f.input == nil {
		return nil
	}
	return f.input.focus
}

// attachmentTerminalFocus returns the focus state of fg, or nil.
func attachmentTerminalFocus(fg AttachmentForeground) *TerminalFocusState {
	if focus, ok := fg.(attachmentFocusForeground); ok {
		return focus.terminalFocus()
	}
	return nil
}

// focusReporter sends the terminal focus to the daemon: the current state once
// the attachment starts, then every change. Unknown focus is never sent.
type focusReporter struct {
	state   *TerminalFocusState
	sent    domain.TerminalFocus
	changed <-chan struct{}
}

// next returns the message to send now, if any, and re-arms the change wake.
func (r *focusReporter) next() (protocol.TerminalFocus, bool) {
	focus, changed := r.state.Watch()
	r.changed = changed
	if focus == domain.TerminalFocusUnknown || focus == r.sent {
		return protocol.TerminalFocus{}, false
	}
	r.sent = focus
	return protocol.TerminalFocus{Focus: focus}, true
}

// stripTerminalFocusReports removes complete focus reports from data and
// records the last one in state. It never withholds a prefix: a terminal
// writes each report whole, while a lone Escape or Alt+[ typed by the user
// must reach the session without delay. Other bytes keep their order. A
// pasted literal report, or one split across reads, is accepted as rare.
func stripTerminalFocusReports(state *TerminalFocusState, data []byte) []byte {
	at, focus := nextTerminalFocusReport(data)
	if at < 0 {
		return data
	}
	out := make([]byte, 0, len(data))
	for at >= 0 {
		out = append(out, data[:at]...)
		state.set(focus)
		data = data[at+len(terminalFocusInReport):]
		at, focus = nextTerminalFocusReport(data)
	}
	return append(out, data...)
}

// nextTerminalFocusReport returns the offset of the first complete report in
// data, or -1.
func nextTerminalFocusReport(data []byte) (int, domain.TerminalFocus) {
	in := bytes.Index(data, terminalFocusInReport)
	out := bytes.Index(data, terminalFocusOutReport)
	switch {
	case in < 0 && out < 0:
		return -1, domain.TerminalFocusUnknown
	case out < 0 || (in >= 0 && in < out):
		return in, domain.TerminalFocusFocused
	default:
		return out, domain.TerminalFocusUnfocused
	}
}
