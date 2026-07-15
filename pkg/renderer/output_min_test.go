package renderer

import (
	"bytes"
	"strings"
	"testing"
)

// ---------------------------------------------------------------------------
// Cursor-motion minimization (drawState.moveTo)
// ---------------------------------------------------------------------------

func TestMoveToChoosesCheapestSequence(t *testing.T) {
	tests := []struct {
		name    string
		known   bool
		fromRow int
		fromCol int
		toRow   int
		toCol   int
		want    string
	}{
		{name: "unknown position uses absolute CUP", known: false, toRow: 2, toCol: 3, want: "\x1b[3;4H"},
		{name: "same position emits nothing", known: true, fromRow: 2, fromCol: 3, toRow: 2, toCol: 3, want: ""},
		{name: "same row col0 uses CR", known: true, fromRow: 2, fromCol: 5, toRow: 2, toCol: 0, want: "\r"},
		{name: "same row forward uses CUF", known: true, fromRow: 2, fromCol: 2, toRow: 2, toCol: 7, want: "\x1b[5C"},
		{name: "same row backward uses CUB", known: true, fromRow: 2, fromCol: 7, toRow: 2, toCol: 2, want: "\x1b[5D"},
		{name: "same row forward by one omits count", known: true, fromRow: 2, fromCol: 3, toRow: 2, toCol: 4, want: "\x1b[C"},
		{name: "same col up uses CUU", known: true, fromRow: 5, fromCol: 3, toRow: 2, toCol: 3, want: "\x1b[3A"},
		{name: "same col down uses CUD", known: true, fromRow: 2, fromCol: 3, toRow: 5, toCol: 3, want: "\x1b[3B"},
		{name: "same col up by one omits count", known: true, fromRow: 5, fromCol: 3, toRow: 4, toCol: 3, want: "\x1b[A"},
		{name: "diagonal move uses absolute CUP", known: true, fromRow: 2, fromCol: 2, toRow: 5, toCol: 7, want: "\x1b[6;8H"},
		{name: "short forward beats absolute", known: true, fromRow: 2, fromCol: 0, toRow: 2, toCol: 1, want: "\x1b[C"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			st := newDrawState()
			if tt.known {
				st.setCursor(tt.fromRow, tt.fromCol)
			}
			var buf bytes.Buffer
			st.moveTo(&buf, tt.toRow, tt.toCol)
			if buf.String() != tt.want {
				t.Errorf("moveTo() = %q, want %q", buf.String(), tt.want)
			}
			if !st.curKnown || st.curRow != tt.toRow || st.curCol != tt.toCol {
				t.Errorf("cursor not tracked to (%d,%d): got known=%v (%d,%d)", tt.toRow, tt.toCol, st.curKnown, st.curRow, st.curCol)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// End-to-end golden byte output (exact bytes emitted by Draw)
// ---------------------------------------------------------------------------

func drawGolden(t *testing.T, r *Renderer, frame Frame, damage []Damage) string {
	t.Helper()
	out, err := r.Draw(frame, damage)
	if err != nil {
		t.Fatal(err)
	}
	return string(out)
}

func setRow(f Frame, y int, s string, st Style) {
	col := 0
	for _, ru := range s {
		f.Set(col, y, Cell{Rune: ru, Style: st})
		col++
	}
}

func hasCUB(out string) bool {
	for i := 0; i+2 < len(out); i++ {
		if out[i] != '\x1b' || out[i+1] != '[' {
			continue
		}
		j := i + 2
		for j < len(out) && out[j] >= '0' && out[j] <= '9' {
			j++
		}
		if j < len(out) && out[j] == 'D' {
			return true
		}
	}
	return false
}

func TestBackwardOverlappingDamageRendersFinalSpanOnce(t *testing.T) {
	r := New(Capabilities{})
	base := NewFrame(10, 1)
	if _, err := r.Draw(base, []Damage{FullRedraw()}); err != nil {
		t.Fatal(err)
	}

	frame := NewFrame(10, 1)
	setRow(frame, 0, "abcdefghi", DefaultStyle())
	got := drawGolden(t, r, frame, []Damage{
		{Kind: DamageText, X: 4, Y: 0, Width: 5, Height: 1, Count: 1},
		{Kind: DamageText, X: 0, Y: 0, Width: 5, Height: 1, Count: 1},
	})
	want := "\x1b[1;1Habcdefghi\x1b[0m"
	if got != want {
		t.Fatalf("output = %q, want %q", got, want)
	}
}

func TestDamageOrderPermutationProducesCanonicalOutput(t *testing.T) {
	tests := []struct {
		name  string
		spans []Damage
	}{
		{
			name: "overlapping spans",
			spans: []Damage{
				{Kind: DamageText, X: 0, Y: 0, Width: 5, Height: 1, Count: 1},
				{Kind: DamageText, X: 4, Y: 0, Width: 5, Height: 1, Count: 1},
			},
		},
		{
			name: "adjacent spans",
			spans: []Damage{
				{Kind: DamageText, X: 0, Y: 0, Width: 5, Height: 1, Count: 1},
				{Kind: DamageText, X: 5, Y: 0, Width: 4, Height: 1, Count: 1},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			render := func(spans []Damage) string {
				t.Helper()
				r := New(Capabilities{})
				if _, err := r.Draw(NewFrame(10, 1), []Damage{FullRedraw()}); err != nil {
					t.Fatal(err)
				}
				frame := NewFrame(10, 1)
				setRow(frame, 0, "abcdefghi", DefaultStyle())
				return drawGolden(t, r, frame, spans)
			}

			forward := render(tt.spans)
			reverse := render([]Damage{tt.spans[1], tt.spans[0]})
			if forward != reverse {
				t.Fatalf("forward output = %q, reverse output = %q", forward, reverse)
			}
			if hasCUB(forward) {
				t.Fatalf("canonical output contains CUB: %q", forward)
			}
			want := "\x1b[1;1Habcdefghi\x1b[0m"
			if forward != want {
				t.Fatalf("output = %q, want canonical output %q", forward, want)
			}
		})
	}
}

func TestNativeClearGolden(t *testing.T) {
	tests := []struct {
		name  string
		build func() Frame
		want  string
	}{
		{
			name: "trailing blank span becomes EL",
			build: func() Frame {
				f := NewFrame(10, 1)
				setRow(f, 0, "Hi", DefaultStyle())
				return f
			},
			// content, then EL to clear the 8 trailing blanks, then reset
			want: "\x1b[1;1HHi\x1b[K\x1b[0m",
		},
		{
			name: "whole blank line cleared natively",
			build: func() Frame {
				f := NewFrame(10, 2)
				setRow(f, 0, "AB", DefaultStyle())
				// row 1 stays entirely blank
				return f
			},
			// Row 0 leaves the cursor tracked at (0,2); the cheapest whole-line
			// clear for row 1 is a one-row CUD in the same column plus 2K
			// (3+4 bytes) rather than CUP to col 0 plus EL (6+3 bytes).
			want: "\x1b[1;1HAB\x1b[K\x1b[B\x1b[2K\x1b[0m",
		},
		{
			name: "short trailing blank span kept as spaces",
			build: func() Frame {
				f := NewFrame(5, 1)
				setRow(f, 0, "ABC", DefaultStyle())
				// two trailing blanks (< 3) → cheaper as spaces
				return f
			},
			want: "\x1b[1;1HABC  \x1b[0m",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := New(Capabilities{})
			frame := tt.build()
			got := drawGolden(t, r, frame, []Damage{FullRedraw()})
			if got != tt.want {
				t.Fatalf("output = %q, want %q", got, tt.want)
			}
			// Shadow consistency: an identical redraw must be a no-op.
			out2, err := r.Draw(frame, nil)
			if err != nil {
				t.Fatal(err)
			}
			if len(out2) != 0 {
				t.Fatalf("second draw not a no-op: %q", string(out2))
			}
		})
	}
}

func TestSGRPersistsAcrossDamageRects(t *testing.T) {
	// Two same-styled rects on different rows: the SGR sequence must be emitted
	// exactly once (the pen persists across rects for the whole Draw).
	r := New(Capabilities{})
	base := NewFrame(4, 3)
	if _, err := r.Draw(base, []Damage{FullRedraw()}); err != nil {
		t.Fatal(err)
	}

	styled := Style{Bold: true, Foreground: 1, Background: -1}
	frame := NewFrame(4, 3)
	setRow(frame, 0, "XXXX", styled)
	setRow(frame, 2, "YYYY", styled)
	damage := []Damage{
		{Kind: DamageText, X: 0, Y: 0, Width: 4, Height: 1, Count: 1},
		{Kind: DamageText, X: 0, Y: 2, Width: 4, Height: 1, Count: 1},
	}
	got := drawGolden(t, r, frame, damage)
	// Row0 reaches the right edge (no trailing blanks) so tracking invalidates;
	// row2 is therefore positioned with an absolute CUP. The SGR appears once.
	want := "\x1b[1;1H\x1b[0;1;38;5;1mXXXX\x1b[3;1HYYYY\x1b[0m"
	if got != want {
		t.Fatalf("output = %q, want %q", got, want)
	}
}

func TestRightEdgeRunInvalidatesTracking(t *testing.T) {
	// Adversarial: a run printed up to the terminal's right edge leaves the
	// cursor in a terminal-dependent "wrap pending" state. The renderer must
	// NOT assume the cursor wrapped to the next row; the following row must be
	// positioned with an absolute CUP, never an implicit newline/relative move.
	r := New(Capabilities{})
	frame := NewFrame(5, 2)
	setRow(frame, 0, "ABCDE", DefaultStyle())
	setRow(frame, 1, "FGHIJ", DefaultStyle())
	got := drawGolden(t, r, frame, []Damage{FullRedraw()})
	want := "\x1b[1;1HABCDE\x1b[2;1HFGHIJ\x1b[0m"
	if got != want {
		t.Fatalf("output = %q, want %q", got, want)
	}
}

func TestWideRunCursorTrackingAndEL(t *testing.T) {
	t.Run("cursor advances two columns per wide rune", func(t *testing.T) {
		// Rect A ends with a wide pair (not at the edge); rect B starts one
		// column later. The gap between them must be crossed with the cursor
		// position that reflects the terminal advancing two columns for the
		// wide rune — otherwise the relative move would be wrong.
		r := New(Capabilities{})
		base := NewFrame(8, 1)
		if _, err := r.Draw(base, []Damage{FullRedraw()}); err != nil {
			t.Fatal(err)
		}
		frame := NewFrame(8, 1)
		frame.Set(0, 0, Cell{Rune: '你', Style: DefaultStyle()})
		frame.Set(1, 0, Cell{Continuation: true, Style: DefaultStyle()})
		frame.Set(2, 0, Cell{Rune: 'A', Style: DefaultStyle()})
		frame.Set(3, 0, Cell{Rune: 'B', Style: DefaultStyle()})
		// column 4 stays blank (undamaged gap)
		frame.Set(5, 0, Cell{Rune: 'C', Style: DefaultStyle()})
		frame.Set(6, 0, Cell{Rune: 'D', Style: DefaultStyle()})
		frame.Set(7, 0, Cell{Rune: 'E', Style: DefaultStyle()})
		damage := []Damage{
			{Kind: DamageText, X: 0, Y: 0, Width: 4, Height: 1, Count: 1},
			{Kind: DamageText, X: 5, Y: 0, Width: 3, Height: 1, Count: 1},
		}
		got := drawGolden(t, r, frame, damage)
		// After 你AB the cursor is at col 4; moving to col 5 is a one-column CUF.
		want := "\x1b[1;1H你AB\x1b[CCDE\x1b[0m"
		if got != want {
			t.Fatalf("output = %q, want %q", got, want)
		}
	})

	t.Run("rect splitting a wide pair invalidates tracking", func(t *testing.T) {
		// A damage rect that ends on a wide head whose continuation lies
		// outside the rect makes the terminal advance one column further than
		// the rect width accounts for. Tracking must be dropped: the next
		// rect must be positioned with absolute CUP, not a relative move.
		r := New(Capabilities{})
		base := NewFrame(8, 1)
		if _, err := r.Draw(base, []Damage{FullRedraw()}); err != nil {
			t.Fatal(err)
		}
		frame := NewFrame(8, 1)
		frame.Set(0, 0, Cell{Rune: 'A', Style: DefaultStyle()})
		frame.Set(1, 0, Cell{Rune: '你', Style: DefaultStyle()})
		frame.Set(2, 0, Cell{Continuation: true, Style: DefaultStyle()})
		frame.Set(4, 0, Cell{Rune: 'B', Style: DefaultStyle()})
		damage := []Damage{
			// Ends on the wide head at col 1; its continuation (col 2) is outside.
			{Kind: DamageText, X: 0, Y: 0, Width: 2, Height: 1, Count: 1},
			{Kind: DamageText, X: 4, Y: 0, Width: 1, Height: 1, Count: 1},
		}
		got := drawGolden(t, r, frame, damage)
		// After A你 the real cursor is at col 3, not col 2: a relative CUF
		// would be off by one, so the second rect must use absolute CUP.
		want := "\x1b[1;1HA你\x1b[1;5HB\x1b[0m"
		if got != want {
			t.Fatalf("output = %q, want %q", got, want)
		}
	})

	t.Run("EL after a wide pair", func(t *testing.T) {
		r := New(Capabilities{})
		frame := NewFrame(7, 1)
		frame.Set(0, 0, Cell{Rune: '你', Style: DefaultStyle()})
		frame.Set(1, 0, Cell{Continuation: true, Style: DefaultStyle()})
		frame.Set(2, 0, Cell{Rune: '好', Style: DefaultStyle()})
		frame.Set(3, 0, Cell{Continuation: true, Style: DefaultStyle()})
		// columns 4,5,6 blank → EL
		got := drawGolden(t, r, frame, []Damage{FullRedraw()})
		want := "\x1b[1;1H你好\x1b[K\x1b[0m"
		if got != want {
			t.Fatalf("output = %q, want %q", got, want)
		}
		out2, err := r.Draw(frame, nil)
		if err != nil {
			t.Fatal(err)
		}
		if len(out2) != 0 {
			t.Fatalf("second draw not a no-op: %q", string(out2))
		}
	})
}

func TestScrollFastPathEmitsCanonicalTextDamage(t *testing.T) {
	render := func(damage []Damage) string {
		t.Helper()
		r := New(Capabilities{})
		base := NewFrame(10, 3)
		setRow(base, 0, "aaaaaaaaaa", DefaultStyle())
		setRow(base, 1, "bbbbbbbbbb", DefaultStyle())
		setRow(base, 2, "cccccccccc", DefaultStyle())
		if _, err := r.Draw(base, []Damage{FullRedraw()}); err != nil {
			t.Fatal(err)
		}

		frame := NewFrame(10, 3)
		setRow(frame, 0, "bbbbbbbbbb", DefaultStyle())
		setRow(frame, 1, "cccccccccc", DefaultStyle())
		setRow(frame, 2, "abcdefghij", DefaultStyle())
		out, err := r.Draw(frame, damage)
		if err != nil {
			t.Fatal(err)
		}
		return string(out)
	}

	scroll := Damage{Kind: DamageScrollUp, X: 0, Y: 0, Width: 10, Height: 3, Count: 1}
	forward := render([]Damage{
		scroll,
		{Kind: DamageText, X: 0, Y: 2, Width: 5, Height: 1},
		{Kind: DamageText, X: 4, Y: 2, Width: 6, Height: 1},
	})
	reverse := render([]Damage{
		{Kind: DamageText, X: 4, Y: 2, Width: 6, Height: 1},
		scroll,
		{Kind: DamageText, X: 0, Y: 2, Width: 5, Height: 1},
	})
	if forward != reverse {
		t.Fatalf("scroll output depends on text damage order: forward %q, reverse %q", forward, reverse)
	}
	if strings.Contains(forward, "\r") || !strings.Contains(forward, "abcdefghij") {
		t.Fatalf("scroll output did not emit one forward canonical span: %q", forward)
	}
}

func TestCanonicalDamageMergesClearAndText(t *testing.T) {
	r := New(Capabilities{})
	base := NewFrame(10, 1)
	if _, err := r.Draw(base, []Damage{FullRedraw()}); err != nil {
		t.Fatal(err)
	}
	frame := NewFrame(10, 1)
	setRow(frame, 0, "abcdefghi", DefaultStyle())
	out, err := r.Draw(frame, []Damage{
		{Kind: DamageClear, X: 5, Y: 0, Width: 4, Height: 1},
		{Kind: DamageText, X: 0, Y: 0, Width: 5, Height: 1},
	})
	if err != nil {
		t.Fatal(err)
	}
	if got, want := string(out), "\x1b[1;1Habcdefghi\x1b[0m"; got != want {
		t.Fatalf("output = %q, want %q", got, want)
	}
}

func TestCanonicalDamagePreservesWideHeadTracking(t *testing.T) {
	r := New(Capabilities{})
	base := NewFrame(8, 1)
	if _, err := r.Draw(base, []Damage{FullRedraw()}); err != nil {
		t.Fatal(err)
	}
	frame := NewFrame(8, 1)
	frame.Set(0, 0, Cell{Rune: 'A', Style: DefaultStyle()})
	frame.Set(1, 0, Cell{Rune: '你', Style: DefaultStyle()})
	frame.Set(2, 0, Cell{Continuation: true, Style: DefaultStyle()})
	frame.Set(4, 0, Cell{Rune: 'B', Style: DefaultStyle()})
	out, err := r.Draw(frame, []Damage{
		{Kind: DamageText, X: 4, Y: 0, Width: 1, Height: 1},
		{Kind: DamageText, X: 0, Y: 0, Width: 2, Height: 1},
	})
	if err != nil {
		t.Fatal(err)
	}
	if got, want := string(out), "\x1b[1;1HA你\x1b[1;5HB\x1b[0m"; got != want {
		t.Fatalf("output = %q, want %q", got, want)
	}
}

func TestDamagePlannerFallbackRedrawsAndSynchronizesShadow(t *testing.T) {
	r := New(Capabilities{})
	frame := NewFrame(1, maxPlannedDamageSpans+1)
	if _, err := r.Draw(frame, []Damage{FullRedraw()}); err != nil {
		t.Fatal(err)
	}
	frame.Set(0, frame.Height-1, Cell{Rune: 'X', Style: DefaultStyle()})
	out, err := r.Draw(frame, []Damage{{Kind: DamageText, X: 0, Y: 0, Width: 1, Height: frame.Height}})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(out), "X") {
		t.Fatalf("fallback output did not redraw the frame: %q", string(out))
	}
	out, err = r.Draw(frame, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(out) != 0 {
		t.Fatalf("fallback did not synchronize shadow; follow-up output = %q", string(out))
	}
}
