package notices

import (
	"fmt"
	"strings"
	"testing"
	"time"

	renderer "github.com/bnema/vev-vt"
	"github.com/bnema/vev/internal/domain"
)

func testStyles() RenderStyles {
	selection := renderer.DefaultStyle()
	selection.Inverse = true
	base := renderer.DefaultStyle()
	return RenderStyles{Background: base, Base: base, Selection: selection, Text: base, SelectionText: selection, Muted: base, SelectionMuted: selection}
}

// rowText flattens one rendered row back to text, skipping wide-rune
// continuation cells, so assertions can check substrings.
func rowText(f renderer.Frame, y int) string {
	var b strings.Builder
	for _, c := range f.Row(y) {
		if c.Continuation {
			continue
		}
		if c.Rune == 0 {
			b.WriteRune(' ')
			continue
		}
		b.WriteRune(c.Rune)
	}
	return b.String()
}

func TestNewSelectsNewestFirstEntry(t *testing.T) {
	now := time.Unix(1000, 0)
	entries := []domain.Notification{
		{Code: domain.NoticePaneSpawn, Message: "newest", Time: now},
		{Code: domain.NoticeTabSpawn, Message: "oldest", Time: now.Add(-time.Hour)},
	}
	m := New(entries, now)
	got, ok := m.Selected()
	if !ok || got.Message != "newest" {
		t.Fatalf("Selected() = %+v, %v, want newest, true", got, ok)
	}
}

func TestNavigationClampsAtBothEnds(t *testing.T) {
	tests := []struct {
		name  string
		moves func(m *Model)
		want  string
	}{
		{"up from top stays at top", func(m *Model) { m.Up(); m.Up() }, "a"},
		{"down past bottom stays at bottom", func(m *Model) { m.Down(); m.Down(); m.Down() }, "c"},
		{"down then up returns to top", func(m *Model) { m.Down(); m.Down(); m.Up(); m.Up() }, "a"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			now := time.Unix(1000, 0)
			m := New([]domain.Notification{{Message: "a", Time: now}, {Message: "b", Time: now}, {Message: "c", Time: now}}, now)
			tt.moves(m)
			got, ok := m.Selected()
			if !ok || got.Message != tt.want {
				t.Fatalf("Selected() = %q, %v, want %q, true", got.Message, ok, tt.want)
			}
		})
	}
}

func TestSelectedAndNavigationOnEmptyHistory(t *testing.T) {
	m := New(nil, time.Unix(1000, 0))
	if _, ok := m.Selected(); ok {
		t.Fatal("Selected() ok = true on empty history, want false")
	}
	m.Up()
	m.Down()
	if _, ok := m.Selected(); ok {
		t.Fatal("Selected() ok = true after navigating empty history, want false")
	}
}

func TestRelativeTime(t *testing.T) {
	now := time.Unix(10000, 0)
	tests := []struct {
		name string
		at   time.Time
		want string
	}{
		{"seconds ago", now.Add(-30 * time.Second), "just now"},
		{"two minutes ago", now.Add(-2 * time.Minute), "2m ago"},
		{"fifty nine minutes ago", now.Add(-59 * time.Minute), "59m ago"},
		{"two hours ago", now.Add(-2 * time.Hour), "2h ago"},
		{"two days ago", now.Add(-48 * time.Hour), "2d ago"},
		{"future clock skew", now.Add(time.Minute), "just now"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := relativeTime(now, tt.at); got != tt.want {
				t.Errorf("relativeTime(now, %v) = %q, want %q", tt.at, got, tt.want)
			}
		})
	}
}

func TestRenderRowContainsSeverityCodeSlugAndCount(t *testing.T) {
	now := time.Unix(10000, 0)
	entries := []domain.Notification{
		{Code: domain.NoticePaneSpawn, Severity: domain.NoticeError, Message: "couldn't open pane", Count: 3, Time: now.Add(-2 * time.Minute)},
	}
	m := New(entries, now)
	frame := m.Render(domain.Size{Cols: 60, Rows: 5}, testStyles())
	row := rowText(frame, 0)
	for _, want := range []string{"[E]", "2m ago", "pane-spawn", "couldn't open pane", "×3"} {
		if !strings.Contains(row, want) {
			t.Errorf("row %q does not contain %q", row, want)
		}
	}
}

func TestRenderSeverityLetterPerSeverity(t *testing.T) {
	now := time.Unix(10000, 0)
	tests := []struct {
		sev  domain.NoticeSeverity
		want string
	}{
		{domain.NoticeInfo, "[I]"},
		{domain.NoticeWarn, "[W]"},
		{domain.NoticeError, "[E]"},
	}
	for _, tt := range tests {
		m := New([]domain.Notification{{Severity: tt.sev, Message: "m", Time: now}}, now)
		frame := m.Render(domain.Size{Cols: 40, Rows: 3}, testStyles())
		row := rowText(frame, 0)
		if !strings.Contains(row, tt.want) {
			t.Errorf("severity %v: row %q does not contain %q", tt.sev, row, tt.want)
		}
	}
}

func TestRenderEmptyHistoryShowsPlaceholderNotBlank(t *testing.T) {
	m := New(nil, time.Unix(0, 0))
	frame := m.Render(domain.Size{Cols: 40, Rows: 10}, testStyles())
	blank := true
	for y := 0; y < frame.Height; y++ {
		if strings.TrimSpace(rowText(frame, y)) != "" {
			blank = false
			break
		}
	}
	if blank {
		t.Fatal("Render() on empty history drew nothing, want a placeholder")
	}
}

func TestRenderDegenerateDimensionsDoesNotPanic(t *testing.T) {
	m := New([]domain.Notification{{Message: "x", Time: time.Unix(0, 0)}}, time.Unix(0, 0))
	tests := []struct {
		size       domain.Size
		wantWidth  int
		wantHeight int
	}{
		// Render clamps each negative dimension to 0 independently via
		// max(inner.Cols, 0) / max(inner.Rows, 0); it never clamps the pair
		// together, so a negative Cols with a valid Rows still yields a
		// zero-width, full-height frame (and vice versa).
		{domain.Size{Cols: 0, Rows: 0}, 0, 0},
		{domain.Size{Cols: -1, Rows: 5}, 0, 5},
		{domain.Size{Cols: 5, Rows: -1}, 5, 0},
		{domain.Size{Cols: 0, Rows: 5}, 0, 5},
		{domain.Size{Cols: 1, Rows: 1}, 1, 1},
	}
	for _, tt := range tests {
		frame := m.Render(tt.size, testStyles())
		if frame.Width != tt.wantWidth || frame.Height != tt.wantHeight {
			t.Fatalf("Render(%+v) = frame{Width: %d, Height: %d}, want {Width: %d, Height: %d}", tt.size, frame.Width, frame.Height, tt.wantWidth, tt.wantHeight)
		}
	}
}

func TestRenderScrollsToKeepSelectionVisible(t *testing.T) {
	now := time.Unix(10000, 0)
	entries := make([]domain.Notification, 200)
	for i := range entries {
		entries[i] = domain.Notification{Message: fmt.Sprintf("n%d", i), Time: now}
	}
	m := New(entries, now)
	for range 150 {
		m.Down()
	}
	frame := m.Render(domain.Size{Cols: 40, Rows: 10}, testStyles())
	found := false
	for y := 0; y < frame.Height; y++ {
		if strings.Contains(rowText(frame, y), "n150") {
			found = true
			break
		}
	}
	if !found {
		t.Fatal("Render() scrolled the selected row out of view")
	}
}

func TestCloneIsIndependentOfOriginal(t *testing.T) {
	now := time.Unix(10000, 0)
	m := New([]domain.Notification{{Message: "a", Time: now}, {Message: "b", Time: now}}, now)
	clone := m.Clone()
	m.Down()
	if got, _ := clone.Selected(); got.Message != "a" {
		t.Fatalf("clone.Selected() = %q, want a (clone must not observe later mutation)", got.Message)
	}
	if got, _ := m.Selected(); got.Message != "b" {
		t.Fatalf("m.Selected() = %q, want b", got.Message)
	}
}

func TestCloneOfNilIsNil(t *testing.T) {
	var m *Model
	if clone := m.Clone(); clone != nil {
		t.Fatalf("Clone() of nil = %v, want nil", clone)
	}
}
