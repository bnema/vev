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
				{Severity: uint8(domain.NoticeError), Title: "pane-spawn", Message: "couldn't open pane", Count: 3},
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
				{Severity: uint8(domain.NoticeError), Title: "pane-spawn", Message: "one"},
				{Severity: uint8(domain.NoticeWarn), Title: "tab-spawn", Message: "two"},
				{Severity: uint8(domain.NoticeInfo), Title: "clipboard", Message: "three"},
			},
			overflow: 2,
			check: func(t *testing.T, f renderer.Frame) {
				text := frameText(f)
				if !strings.Contains(text, "+2 more") {
					t.Fatalf("frame missing overflow line %q:\n%s", "+2 more", text)
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
			ComposeNotices(f, []NoticeView{{Severity: uint8(tt.sev), Title: "x", Message: "y"}}, 0, styles)
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

func TestComposeNoticesHandlesTinyFrames(t *testing.T) {
	styles := NoticeStyles{Text: renderer.DefaultStyle(), BoxInfo: renderer.DefaultStyle(), BoxWarn: renderer.DefaultStyle(), BoxError: renderer.DefaultStyle()}
	for _, size := range []domain.Size{{}, {Cols: 1, Rows: 1}, {Cols: 2, Rows: 1}, {Cols: 1, Rows: 2}, {Cols: 24, Rows: 1}} {
		t.Run(fmt.Sprintf("%dx%d", size.Cols, size.Rows), func(t *testing.T) {
			f := renderer.NewFrame(size.Cols, size.Rows)
			notices := []NoticeView{{Severity: uint8(domain.NoticeError), Title: "pane-spawn", Message: "a very long message that should wrap across several lines of text"}}
			ComposeNotices(f, notices, 5, styles)
		})
	}
}
