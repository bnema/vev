package ui

import (
	"fmt"
	"strings"
	"testing"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/pkg/renderer"
)

// frameText joins every row of frame into one newline-separated string so
// tests can search for substrings without caring which cells they land on.
func frameText(f renderer.Frame) string {
	rows := make([]string, f.Height)
	for y := 0; y < f.Height; y++ {
		var b strings.Builder
		for x := 0; x < f.Width; x++ {
			cell := f.At(x, y)
			if cell.Continuation {
				continue
			}
			if cell.Rune == 0 {
				b.WriteByte(' ')
				continue
			}
			b.WriteRune(cell.Rune)
		}
		rows[y] = b.String()
	}
	return strings.Join(rows, "\n")
}

// firstRowContaining returns the index of the first row in f whose rendered
// text contains s, or -1 if no row does.
func firstRowContaining(f renderer.Frame, s string) int {
	for y, row := range strings.Split(frameText(f), "\n") {
		if strings.Contains(row, s) {
			return y
		}
	}
	return -1
}

func TestComposeNotices(t *testing.T) {
	styles := NoticeStyles{
		Text:     renderer.Style{Foreground: 7, Background: -1},
		BoxInfo:  renderer.Style{Foreground: 4, Background: -1},
		BoxWarn:  renderer.Style{Foreground: 3, Background: -1},
		BoxError: renderer.Style{Foreground: 1, Background: -1},
	}
	isBorderRune := func(r rune) bool {
		switch r {
		case '┌', '┐', '└', '┘', '─', '│':
			return true
		default:
			return false
		}
	}

	tests := []struct {
		name     string
		notices  []NoticeView
		overflow int
		check    func(t *testing.T, f renderer.Frame)
	}{
		{
			name: "single error notice draws a bordered box top-right with title and message",
			notices: []NoticeView{
				{Severity: domain.NoticeError, Title: "pane-spawn", Message: "couldn't open pane", Count: 3},
			},
			check: func(t *testing.T, f renderer.Frame) {
				found := false
				for y := 0; y <= 8 && !found; y++ {
					for x := 40; x < f.Width; x++ {
						if isBorderRune(f.At(x, y).Rune) {
							found = true
							break
						}
					}
				}
				if !found {
					t.Fatalf("no box border found in top-right quadrant (x>=40, y<=8):\n%s", frameText(f))
				}
				text := frameText(f)
				if !strings.Contains(text, "pane-spawn ×3") {
					t.Fatalf("frame missing title %q:\n%s", "pane-spawn ×3", text)
				}
				if !strings.Contains(text, "couldn't open pane") {
					t.Fatalf("frame missing message %q:\n%s", "couldn't open pane", text)
				}
			},
		},
		{
			name: "three notices with overflow renders a +N more line",
			notices: []NoticeView{
				{Severity: domain.NoticeError, Title: "pane-spawn", Message: "one"},
				{Severity: domain.NoticeWarn, Title: "tab-spawn", Message: "two"},
				{Severity: domain.NoticeInfo, Title: "clipboard", Message: "three"},
			},
			overflow: 2,
			check: func(t *testing.T, f renderer.Frame) {
				text := frameText(f)
				if !strings.Contains(text, "+2 more") {
					t.Fatalf("frame missing overflow line %q:\n%s", "+2 more", text)
				}
				rowOne := firstRowContaining(f, "one")
				rowTwo := firstRowContaining(f, "two")
				rowThree := firstRowContaining(f, "three")
				if rowOne < 0 || rowTwo < 0 || rowThree < 0 {
					t.Fatalf("could not locate all three notice messages in frame: one=%d two=%d three=%d\n%s", rowOne, rowTwo, rowThree, text)
				}
				if rowOne >= rowTwo || rowTwo >= rowThree {
					t.Fatalf("notices not stacked newest-first top-down: one=%d two=%d three=%d\n%s", rowOne, rowTwo, rowThree, text)
				}
			},
		},
		{
			name:     "no notices and no overflow draws nothing",
			notices:  nil,
			overflow: 0,
			check: func(t *testing.T, f renderer.Frame) {
				want := renderer.NewFrame(f.Width, f.Height)
				if frameText(f) != frameText(want) {
					t.Fatalf("frame changed with no notices and no overflow:\n%s", frameText(f))
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := renderer.NewFrame(80, 24)
			ComposeNotices(f, tt.notices, tt.overflow, styles)
			tt.check(t, f)
		})
	}
}

func TestComposeNoticesSeverityStylingIsDistinguishable(t *testing.T) {
	styles := NoticeStyles{
		Text:     renderer.Style{Foreground: 7, Background: -1},
		BoxInfo:  renderer.Style{Foreground: 4, Background: -1},
		BoxWarn:  renderer.Style{Foreground: 3, Background: -1},
		BoxError: renderer.Style{Foreground: 1, Background: -1},
	}

	tests := []struct {
		name string
		sev  domain.NoticeSeverity
		want renderer.Style
	}{
		{"info", domain.NoticeInfo, styles.BoxInfo},
		{"warn", domain.NoticeWarn, styles.BoxWarn},
		{"error", domain.NoticeError, styles.BoxError},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := renderer.NewFrame(80, 24)
			ComposeNotices(f, []NoticeView{{Severity: tt.sev, Title: "x", Message: "y"}}, 0, styles)
			corner := findRune(f, '┌')
			if corner == nil {
				t.Fatalf("no box top-left corner found:\n%s", frameText(f))
			}
			if !corner.Style.Equal(tt.want) {
				t.Fatalf("box border style = %+v, want %s style %+v", corner.Style, tt.name, tt.want)
			}
		})
	}
}

// findRune returns the first cell in f carrying r, or nil if none does.
func findRune(f renderer.Frame, r rune) *renderer.Cell {
	for y := 0; y < f.Height; y++ {
		for x := 0; x < f.Width; x++ {
			if cell := f.At(x, y); cell.Rune == r {
				return &cell
			}
		}
	}
	return nil
}

// findRunePos returns the coordinates of the first cell in f carrying r, or
// (-1, -1) if no cell does.
func findRunePos(f renderer.Frame, r rune) (int, int) {
	for y := 0; y < f.Height; y++ {
		for x := 0; x < f.Width; x++ {
			if f.At(x, y).Rune == r {
				return x, y
			}
		}
	}
	return -1, -1
}

// TestComposeNoticesWidth pins the box's actual width and left edge across
// representative terminal widths, so the test fails if the *2/5 ratio, the
// 60-column cap, the 24-column floor, or the final frame-width clamp changes.
// Each expected x and width below is worked out by hand from the formula
// documented on ComposeNotices, not recomputed from the implementation.
func TestComposeNoticesWidth(t *testing.T) {
	styles := NoticeStyles{
		Text:     renderer.Style{Foreground: 7, Background: -1},
		BoxInfo:  renderer.Style{Foreground: 4, Background: -1},
		BoxWarn:  renderer.Style{Foreground: 3, Background: -1},
		BoxError: renderer.Style{Foreground: 1, Background: -1},
	}

	tests := []struct {
		name       string
		frameWidth int
		wantWidth  int
		wantX      int // -1 means the box's left edge falls off-frame (x < 0)
	}{
		// 200 cols: 200*2/5=80, capped at 60. x = 200 - 1(margin) - 60 = 139.
		{name: "60-column cap binds on a wide terminal", frameWidth: 200, wantWidth: 60, wantX: 139},
		// 100 cols: 100*2/5=40, under the cap and floor. x = 100 - 1 - 40 = 59.
		{name: "cols*2/5 ratio binds on a mid terminal", frameWidth: 100, wantWidth: 40, wantX: 59},
		// 40 cols: 40*2/5=16, floored to 24. x = 40 - 1 - 24 = 15.
		{name: "24-column floor binds on a narrow terminal", frameWidth: 40, wantWidth: 24, wantX: 15},
		// 20 cols: 20*2/5=8, floored to 24, then clamped down to frame.Width
		// (20) since 24 > 20. x = 20 - 1(margin) - 20 = -1, so the left edge
		// (and its corner rune) fall one column off-frame.
		{name: "frame-width clamp overrides the floor on a tiny terminal", frameWidth: 20, wantWidth: 20, wantX: -1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := renderer.NewFrame(tt.frameWidth, 10)
			ComposeNotices(f, []NoticeView{{Severity: domain.NoticeInfo, Title: "x", Message: "y"}}, 0, styles)

			wantRight := tt.wantX + tt.wantWidth - 1
			rightX, rightY := findRunePos(f, '┐')
			if rightX < 0 {
				t.Fatalf("no top-right corner found:\n%s", frameText(f))
			}
			if rightY != noticeMargin || rightX != wantRight {
				t.Fatalf("top-right corner at (%d,%d), want (%d,%d):\n%s", rightX, rightY, wantRight, noticeMargin, frameText(f))
			}

			// The message body starts one column right of the box's left
			// edge (wantX+1) and one row below its top edge. Pinning this
			// rune's exact column catches width/x regressions that the
			// top-right corner can't: right = x+width-1 = frame.Width-margin-1
			// no matter what width is, so the right corner alone can't tell
			// a correctly clamped box from one whose x drifted further
			// off-frame (both still land off-screen).
			msgX, msgY := tt.wantX+1, noticeMargin+1
			if got := f.At(msgX, msgY).Rune; got != 'y' {
				t.Fatalf("message rune at (%d,%d) = %q, want 'y':\n%s", msgX, msgY, got, frameText(f))
			}

			leftX, leftY := findRunePos(f, '┌')
			if tt.wantX < 0 {
				if leftX >= 0 {
					t.Fatalf("expected left corner to be clipped off-frame, found one at (%d,%d):\n%s", leftX, leftY, frameText(f))
				}
				return
			}
			if leftX != tt.wantX || leftY != noticeMargin {
				t.Fatalf("top-left corner at (%d,%d), want (%d,%d):\n%s", leftX, leftY, tt.wantX, noticeMargin, frameText(f))
			}
			if gotWidth := rightX - leftX + 1; gotWidth != tt.wantWidth {
				t.Fatalf("box width = %d, want %d:\n%s", gotWidth, tt.wantWidth, frameText(f))
			}
		})
	}
}

func TestComposeNoticesHandlesTinyFrames(t *testing.T) {
	styles := NoticeStyles{Text: renderer.DefaultStyle(), BoxInfo: renderer.DefaultStyle(), BoxWarn: renderer.DefaultStyle(), BoxError: renderer.DefaultStyle()}
	for _, size := range []domain.Size{{}, {Cols: 1, Rows: 1}, {Cols: 2, Rows: 1}, {Cols: 1, Rows: 2}, {Cols: 24, Rows: 1}} {
		t.Run(fmt.Sprintf("%dx%d", size.Cols, size.Rows), func(t *testing.T) {
			f := renderer.NewFrame(size.Cols, size.Rows)
			notices := []NoticeView{{Severity: domain.NoticeError, Title: "pane-spawn", Message: "a very long message that should wrap across several lines of text"}}
			ComposeNotices(f, notices, 5, styles)
		})
	}
}
