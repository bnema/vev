package daemon

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
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
