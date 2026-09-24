package daemon

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/usecase/ui"
)

// TestPaintAcksAttentionOnlyWhenTheClientMaySeeIt pins the focus rule: a
// client whose terminal window lost focus shows the bell of its own tab but
// never clears it, while a focused client, or one whose terminal never
// reports focus, clears it on its visible paint as before.
func TestPaintAcksAttentionOnlyWhenTheClientMaySeeIt(t *testing.T) {
	tests := []struct {
		name      string
		focus     domain.TerminalFocus
		wantAcked bool
	}{
		{name: "unknown focus acknowledges as before", focus: domain.TerminalFocusUnknown, wantAcked: true},
		{name: "focused client acknowledges", focus: domain.TerminalFocusFocused, wantAcked: true},
		{name: "unfocused client leaves the bell", focus: domain.TerminalFocusUnfocused, wantAcked: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d, sess, ac, sends, releases := newManualTabSession(t, 1)
			defer releases[0]()
			selectTestAttachmentTab(sess, 0)
			ac.setTerminalFocus(tt.focus)
			d.noteAttention(sess, sess.tabs[0])

			d.setAttentionFrame(1)
			d.paint(sess, ac, true, nil)
			require.Contains(t, string(mustOutputData(t, sends)), string(ui.AttentionGlyph), "the tab bar shows the bell")

			sess.mu.Lock()
			defer sess.mu.Unlock()
			require.Equal(t, !tt.wantAcked, sess.tabs[0].attention)
		})
	}
}

// TestRegainedFocusAcknowledgesTheVisibleTab pins the other half: the bell a
// hidden client left pending clears once that client's window is focused and
// paints again.
func TestRegainedFocusAcknowledgesTheVisibleTab(t *testing.T) {
	d, sess, ac, sends, releases := newManualTabSession(t, 1)
	defer releases[0]()
	selectTestAttachmentTab(sess, 0)
	ac.setTerminalFocus(domain.TerminalFocusUnfocused)
	d.noteAttention(sess, sess.tabs[0])
	d.setAttentionFrame(1)
	d.paint(sess, ac, true, nil)
	mustOutputData(t, sends)
	require.True(t, sess.anyAttention(), "hidden client keeps the bell")

	require.True(t, ac.setTerminalFocus(domain.TerminalFocusFocused))
	require.False(t, ac.setTerminalFocus(domain.TerminalFocusFocused), "a repeated report is no change")
	d.paint(sess, ac, true, nil)
	mustOutputData(t, sends)
	require.False(t, sess.anyAttention(), "focused client clears the bell")
}

// TestAttachDeclaredFocusGuardsTheFirstPaint pins that the focus declared in
// Hello is the attachment's focus from its first paint, so a hidden window
// that opens an attachment never clears the bell of the tab it lands on.
func TestAttachDeclaredFocusGuardsTheFirstPaint(t *testing.T) {
	tests := []struct {
		name      string
		focus     domain.TerminalFocus
		wantAcked bool
	}{
		{name: "unknown focus acknowledges as before", focus: domain.TerminalFocusUnknown, wantAcked: true},
		{name: "focused window acknowledges", focus: domain.TerminalFocusFocused, wantAcked: true},
		{name: "unfocused window leaves the bell", focus: domain.TerminalFocusUnfocused, wantAcked: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d, sess, first, _, releases := newManualTabSession(t, 1)
			defer releases[0]()
			first.setTerminalFocus(domain.TerminalFocusUnfocused)
			d.noteAttention(sess, sess.tabs[0])

			tr, sends := newCapturingTransport(t)
			peer, err := d.attachClient(sess, tr, domain.Size{Cols: 80, Rows: 24}, attachClientOptions{terminalFocus: tt.focus})
			require.NoError(t, err)
			require.Equal(t, tt.focus, peer.terminalFocus())
			require.True(t, sess.switchAttachmentTab(peer, 0))
			d.setAttentionFrame(1)
			d.paint(sess, peer, true, nil)
			mustOutputData(t, sends)
			require.Equal(t, !tt.wantAcked, sess.anyAttention())
		})
	}
}

// TestFocusReportDrawsTheBellBeforeAcknowledging pins the routed focus report:
// only a gain of focus repaints, and the bell a focused window finds on its
// tab is drawn for one pulse before a later paint acknowledges it.
func TestFocusReportDrawsTheBellBeforeAcknowledging(t *testing.T) {
	d, sess, ac, sends, releases := newManualTabSession(t, 1)
	defer releases[0]()
	selectTestAttachmentTab(sess, 0)
	effect := &attachmentEffect{attachmentCapability: attachmentCapability{sess: sess, ac: ac}}

	d.applyTerminalFocusForAttachment(effect, protocol.TerminalFocus{Focus: domain.TerminalFocusUnfocused})
	require.Equal(t, domain.TerminalFocusUnfocused, ac.terminalFocus())
	require.Empty(t, sends, "losing focus does not repaint")
	d.noteAttention(sess, sess.tabs[0])

	d.setAttentionFrame(0)
	d.applyTerminalFocusForAttachment(effect, protocol.TerminalFocus{Focus: domain.TerminalFocusFocused})
	require.Equal(t, domain.TerminalFocusFocused, ac.terminalFocus())
	mustOutputData(t, sends)
	require.True(t, sess.anyAttention(), "the bell survives the blank pulse frame")

	d.setAttentionFrame(1)
	d.paint(sess, ac, false, nil)
	require.Contains(t, string(mustOutputData(t, sends)), string(ui.AttentionGlyph), "the focused window draws the bell")
	require.False(t, sess.anyAttention(), "the drawn bell is acknowledged")

	// The acknowledgement repaints every client; drain those frames.
	for len(sends) > 0 {
		<-sends
	}
	d.applyTerminalFocusForAttachment(effect, protocol.TerminalFocus{Focus: domain.TerminalFocusFocused})
	require.Empty(t, sends, "an unchanged focus does not repaint")
}
