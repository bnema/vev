package daemon

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	vt "github.com/bnema/vev-vt"
	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/protocol/wire"
)

// replayOutputFrames decodes and writes each MsgOutput frame's terminal bytes
// into screen, growing its shadow the same way a real client would.
func replayOutputFrames(t *testing.T, screen *vt.Screen, frames []wire.Frame) {
	t.Helper()
	for _, f := range frames {
		require.Equal(t, wire.MsgOutput, f.Type)
		out, err := wire.UnmarshalOutput(f.Payload)
		require.NoError(t, err)
		screen.Write(out.Data)
	}
}

// drainAllFrames collects every frame already queued on sends without blocking.
func drainAllFrames(sends chan wire.Frame) []wire.Frame {
	var out []wire.Frame
	for {
		select {
		case f := <-sends:
			out = append(out, f)
		default:
			return out
		}
	}
}

// runeColumn returns the rune-indexed column at which text first occurs in
// row, or -1. Rows are exactly one rune per terminal column, so this must not
// use strings.Index: box-drawing borders are multi-byte and would inflate a
// byte offset past the real column.
func runeColumn(row, text string) int {
	runes, sub := []rune(row), []rune(text)
	for i := 0; i+len(sub) <= len(runes); i++ {
		match := true
		for j, r := range sub {
			if runes[i+j] != r {
				match = false
				break
			}
		}
		if match {
			return i
		}
	}
	return -1
}

// containsTopRight reports whether text appears anywhere at or past minCol in
// the frame's top half.
func containsTopRight(rows []string, text string, minCol int) bool {
	for y, row := range rows {
		if y > len(rows)/2 {
			break
		}
		if runeColumn(row, text) >= minCol {
			return true
		}
	}
	return false
}

// TestPaintComposesNoticeToastTopRightAndExpiresOnTTL drives an actual
// notify -> showToast -> paint -> emit round trip and decodes the resulting
// wire bytes through a terminal emulator, the same way a real client would,
// so this proves the composed frame (not just internal toast state) carries
// the message top-right, and that it disappears once the fake clock crosses
// the toast's TTL.
func TestPaintComposesNoticeToastTopRightAndExpiresOnTTL(t *testing.T) {
	clk := newNoticeClock()
	d, sess, ac, sends := newNoticeFixture(t, clk)
	screen := vt.NewScreen(ac.size.Cols, ac.size.Rows)

	d.notify(sess, domain.NoticeError, domain.NoticePaneSpawn, "couldn't open pane", nil)
	awaitToastCount(t, ac, 1)
	replayOutputFrames(t, screen, drainAllFrames(sends))

	rows := frameRows(captureTestFrame(screen))
	if !containsTopRight(rows, "couldn't open pane", ac.size.Cols/2) {
		t.Fatalf("composed frame missing toast message top-right:\n%s", strings.Join(rows, "\n"))
	}

	clk.advance(noticeTTL(domain.NoticeError))
	awaitToastCount(t, ac, 0)
	select {
	case f := <-sends:
		replayOutputFrames(t, screen, append([]wire.Frame{f}, drainAllFrames(sends)...))
	case <-time.After(2 * time.Second):
		t.Fatal("expiry did not invalidate the render")
	}

	rows = frameRows(captureTestFrame(screen))
	if containsTopRight(rows, "couldn't open pane", ac.size.Cols/2) {
		t.Fatalf("toast message still present after TTL expired:\n%s", strings.Join(rows, "\n"))
	}
}
