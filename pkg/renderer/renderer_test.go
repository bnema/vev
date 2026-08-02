package renderer

import (
	"bytes"
	"strings"
	"testing"
)

// markFrame writes uniform printable content so each row is distinguishable.
func markFrame(f *Frame) {
	for y := range f.Height {
		for x := range f.Width {
			f.Set(x, y, Cell{Rune: rune('A' + (y*f.Width+x)%26), Style: DefaultStyle()})
		}
	}
}

// outputContains checks that the output contains all the given substrings.
func outputContains(t *testing.T, data []byte, subs ...string) {
	t.Helper()
	s := string(data)
	for _, sub := range subs {
		if !strings.Contains(s, sub) {
			t.Errorf("output should contain %q", sub)
		}
	}
}

func outputEndsWith(t *testing.T, data []byte, suffix string) {
	t.Helper()
	s := string(data)
	if !strings.HasSuffix(s, suffix) {
		t.Errorf("output should end with %q, got %q", suffix, s)
	}
}

// ---------------------------------------------------------------------------
// First draw
// ---------------------------------------------------------------------------

func TestFirstDraw(t *testing.T) {
	tests := []struct {
		name        string
		damage      []Damage
		minCSICount int
	}{
		{
			name:        "explicit text damage",
			damage:      []Damage{{Kind: DamageText, X: 0, Y: 0, Width: 5, Height: 3}},
			minCSICount: 3,
		},
		{
			name:   "full redraw damage",
			damage: []Damage{FullRedraw()},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := New(Capabilities{})
			frame := NewFrame(5, 3)
			markFrame(&frame)

			out, err := r.Draw(frame, tt.damage)
			if err != nil {
				t.Fatal(err)
			}
			if len(out) == 0 {
				t.Fatal("expected non-empty output on first draw")
			}
			outputContains(t, out, "\x1b[0m")
			if tt.minCSICount > 0 {
				// Should have cursor-position sequences for all three rows.
				if c := strings.Count(string(out), "\x1b["); c < tt.minCSICount {
					t.Fatalf("expected at least %d CSI sequences, got %d", tt.minCSICount, c)
				}
			}
		})
	}
}

// ---------------------------------------------------------------------------
// No-op
// ---------------------------------------------------------------------------

func TestNoOp(t *testing.T) {
	tests := []struct {
		name         string
		width        int
		height       int
		firstDamage  []Damage
		secondDamage []Damage
	}{
		{
			name:         "nil damage on unchanged frame",
			width:        5,
			height:       3,
			firstDamage:  []Damage{{Kind: DamageText, X: 0, Y: 0, Width: 5, Height: 3}},
			secondDamage: nil,
		},
		{
			name:         "empty damage slice on unchanged frame",
			width:        3,
			height:       2,
			firstDamage:  []Damage{{Kind: DamageText, X: 0, Y: 0, Width: 3, Height: 2}},
			secondDamage: []Damage{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := New(Capabilities{})
			frame := NewFrame(tt.width, tt.height)
			markFrame(&frame)

			// First draw – populate shadow.
			out1, err := r.Draw(frame, tt.firstDamage)
			if err != nil {
				t.Fatal(err)
			}
			if len(out1) == 0 {
				t.Fatal("first draw must produce output")
			}

			// Second draw with no changes.
			out2, err := r.Draw(frame, tt.secondDamage)
			if err != nil {
				t.Fatal(err)
			}
			if len(out2) != 0 {
				t.Fatalf("expected no-op (empty output), got %q", string(out2))
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Style reset discipline
// ---------------------------------------------------------------------------

func TestStyleResetDiscipline(t *testing.T) {
	tests := []struct {
		name string
		run  func(t *testing.T)
	}{
		{
			name: "single styled cell among default cells",
			run: func(t *testing.T) {
				r := New(Capabilities{})
				frame := NewFrame(4, 2)

				// One styled cell, rest default.
				frame.Set(0, 0, Cell{Rune: 'X', Style: Style{Bold: true, Foreground: 2}})

				out, err := r.Draw(frame, []Damage{{Kind: DamageText, X: 0, Y: 0, Width: 4, Height: 2}})
				if err != nil {
					t.Fatal(err)
				}
				// Must end with style reset.
				outputEndsWith(t, out, "\x1b[0m")
			},
		},
		{
			name: "after scroll damage",
			run: func(t *testing.T) {
				r := New(Capabilities{})
				frame := NewFrame(4, 3)
				markFrame(&frame)

				// Populate shadow.
				_, err := r.Draw(frame, []Damage{{Kind: DamageText, X: 0, Y: 0, Width: 4, Height: 3}})
				if err != nil {
					t.Fatal(err)
				}

				// Cause a scroll-up via VT model simulation: shift cells and emit scroll damage.
				frame2 := NewFrame(4, 3)
				copy(frame2.Cells[4:], frame.Cells[:8]) // row 0←row 1, row 1←row 2
				for i := 8; i < 12; i++ {
					frame2.Cells[i] = BlankCell() // bottom row blank
				}
				frame2.Set(0, 2, Cell{Rune: 'N', Style: DefaultStyle()}) // new char on bottom row

				damage := []Damage{
					{Kind: DamageScrollUp, X: 0, Y: 0, Width: 4, Height: 3, Count: 1},
					{Kind: DamageText, X: 0, Y: 2, Width: 4, Height: 1},
				}

				out, err := r.Draw(frame2, damage)
				if err != nil {
					t.Fatal(err)
				}
				// The output must end with a style reset.
				outputEndsWith(t, out, "\x1b[0m")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, tt.run)
	}
}

func TestStyleEqualUsesActiveColorMode(t *testing.T) {
	tests := []struct {
		name      string
		left      Style
		right     Style
		wantEqual bool
	}{
		{
			name:      "ignores inactive indexed foreground",
			left:      Style{Foreground: 1, HasForegroundRGB: true, ForegroundRGB: RGB{R: 12, G: 34, B: 56}, Background: -1},
			right:     Style{Foreground: 2, HasForegroundRGB: true, ForegroundRGB: RGB{R: 12, G: 34, B: 56}, Background: -1},
			wantEqual: true,
		},
		{
			name:      "compares active RGB values",
			left:      Style{Foreground: 1, HasForegroundRGB: true, ForegroundRGB: RGB{R: 12, G: 34, B: 56}, Background: -1},
			right:     Style{Foreground: 2, HasForegroundRGB: true, ForegroundRGB: RGB{R: 13, G: 34, B: 56}, Background: -1},
			wantEqual: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.left.Equal(tt.right); got != tt.wantEqual {
				t.Fatalf("Equal() = %v, want %v", got, tt.wantEqual)
			}
		})
	}
}

func TestRendererEmitsSGR(t *testing.T) {
	tests := []struct {
		name    string
		width   int
		height  int
		setup   func(frame Frame)
		damage  []Damage
		wantAll []string
	}{
		{
			name:   "truecolor foreground and background",
			width:  1,
			height: 1,
			setup: func(frame Frame) {
				frame.Set(0, 0, Cell{
					Rune: 'X',
					Style: Style{
						Foreground:       -1,
						Background:       -1,
						HasForegroundRGB: true,
						ForegroundRGB:    RGB{R: 12, G: 34, B: 56},
						HasBackgroundRGB: true,
						BackgroundRGB:    RGB{R: 200, G: 100, B: 50},
					},
				})
			},
			damage:  []Damage{{Kind: DamageText, X: 0, Y: 0, Width: 1, Height: 1}},
			wantAll: []string{"\x1b[0;38;2;12;34;56;48;2;200;100;50m", "X"},
		},
		{
			name:   "style changes across adjacent color modes",
			width:  3,
			height: 1,
			setup: func(frame Frame) {
				frame.Set(0, 0, Cell{Rune: 'R', Style: Style{Foreground: -1, Background: -1, HasForegroundRGB: true, ForegroundRGB: RGB{R: 1, G: 2, B: 3}}})
				frame.Set(1, 0, Cell{Rune: 'I', Style: Style{Foreground: 82, Background: -1}})
				frame.Set(2, 0, Cell{Rune: 'D', Style: DefaultStyle()})
			},
			damage:  []Damage{{Kind: DamageText, X: 0, Y: 0, Width: 3, Height: 1}},
			wantAll: []string{"\x1b[0;38;2;1;2;3m", "R", "\x1b[0;38;5;82m", "I", "\x1b[0mD"},
		},
		{
			name:   "bold italic inverse",
			width:  1,
			height: 1,
			setup: func(frame Frame) {
				frame.Set(0, 0, Cell{Rune: 'X', Style: Style{Bold: true, Italic: true, Inverse: true, Foreground: -1, Background: -1}})
			},
			damage:  []Damage{{Kind: DamageText, X: 0, Y: 0, Width: 1, Height: 1}},
			wantAll: []string{"\x1b[0;1;3;7m", "X"},
		},
		{
			name:   "extended SGR attributes and underline color",
			width:  1,
			height: 1,
			setup: func(frame Frame) {
				frame.Set(0, 0, Cell{Rune: 'X', Style: Style{Foreground: -1, Background: -1, Attrs: AttrDim | AttrUnderline | AttrBlink | AttrStrikethrough, UnderlineStyle: UnderlineCurly, HasUnderlineColor: true, UnderlineColor: 9}})
			},
			damage:  []Damage{{Kind: DamageText, X: 0, Y: 0, Width: 1, Height: 1}},
			wantAll: []string{"\x1b[0;2;4:3;5;9;58;5;9m", "X"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := New(Capabilities{})
			frame := NewFrame(tt.width, tt.height)
			tt.setup(frame)

			out, err := r.Draw(frame, tt.damage)
			if err != nil {
				t.Fatal(err)
			}

			outputContains(t, out, tt.wantAll...)
		})
	}
}

// ---------------------------------------------------------------------------
// Scroll fast path
// ---------------------------------------------------------------------------

func TestScrollFastPath(t *testing.T) {
	tests := []struct {
		name string
	}{
		{name: "scroll up by one row exposes new bottom row via scroll region"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := New(Capabilities{})
			frame := NewFrame(5, 4)
			markFrame(&frame)

			// Populate shadow.
			_, err := r.Draw(frame, []Damage{{Kind: DamageText, X: 0, Y: 0, Width: 5, Height: 4}})
			if err != nil {
				t.Fatal(err)
			}

			// Build a new frame that has been scrolled up by 1 (like the VT model does).
			scrolled := NewFrame(5, 4)
			copy(scrolled.Cells[0:15], frame.Cells[5:20]) // rows 0,1,2 ← rows 1,2,3
			for i := 15; i < 20; i++ {
				scrolled.Cells[i] = BlankCell() // row 3 blanked
			}
			scrolled.Set(0, 3, Cell{Rune: 'N', Style: DefaultStyle()})
			scrolled.Set(1, 3, Cell{Rune: 'e', Style: DefaultStyle()})
			scrolled.Set(2, 3, Cell{Rune: 'w', Style: DefaultStyle()})

			damage := []Damage{
				{Kind: DamageScrollUp, X: 0, Y: 0, Width: 5, Height: 4, Count: 1},
				{Kind: DamageText, X: 0, Y: 3, Width: 5, Height: 1},
			}

			out, err := r.Draw(scrolled, damage)
			if err != nil {
				t.Fatal(err)
			}
			if len(out) == 0 {
				t.Fatal("expected non-empty output for scroll")
			}
			// Should contain scroll-region sequences.
			outputContains(t, out, "\x1b[1;4r") // scroll region rows 1-4
			outputContains(t, out, "\x1b[r")    // restore scroll region
			// Should contain the new text on the exposed row.
			outputContains(t, out, "N")
			outputContains(t, out, "e")
			outputContains(t, out, "w")
		})
	}
}

// ---------------------------------------------------------------------------
// Synchronized output wrapper
// ---------------------------------------------------------------------------

func TestSynchronizedOutput(t *testing.T) {
	tests := []struct {
		name string
		run  func(t *testing.T)
	}{
		{
			name: "wraps output in sync CSI",
			run: func(t *testing.T) {
				r := New(Capabilities{SynchronizedOutput: true})
				frame := NewFrame(3, 2)
				markFrame(&frame)

				out, err := r.Draw(frame, []Damage{{Kind: DamageText, X: 0, Y: 0, Width: 3, Height: 2}})
				if err != nil {
					t.Fatal(err)
				}
				if !strings.HasPrefix(string(out), SyncStartCSI) {
					t.Errorf("expected output to start with sync start CSI")
				}
				if !strings.HasSuffix(string(out), SyncEndCSI) {
					t.Errorf("expected output to end with sync end CSI, got %q", string(out))
				}
			},
		},
		{
			name: "no-op returns empty output, not wrapped empty output",
			run: func(t *testing.T) {
				r := New(Capabilities{SynchronizedOutput: true})
				frame := NewFrame(3, 2)
				markFrame(&frame)

				// First draw.
				_, err := r.Draw(frame, []Damage{{Kind: DamageText, X: 0, Y: 0, Width: 3, Height: 2}})
				if err != nil {
					t.Fatal(err)
				}

				// No-op – must return nil, not wrapped empty output.
				out, err := r.Draw(frame, nil)
				if err != nil {
					t.Fatal(err)
				}
				if len(out) != 0 {
					t.Fatalf("expected empty output for no-op, got %q", string(out))
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, tt.run)
	}
}

// ---------------------------------------------------------------------------
// WrapSynchronized standalone function
// ---------------------------------------------------------------------------

func TestWrapSynchronized(t *testing.T) {
	content := []byte("\x1b[0mhello")

	tests := []struct {
		name    string
		content []byte
		enabled bool
		check   func(t *testing.T, got []byte)
	}{
		{
			name:    "enabled wraps with sync start/end and preserves content",
			content: content,
			enabled: true,
			check: func(t *testing.T, got []byte) {
				if !strings.HasPrefix(string(got), SyncStartCSI) {
					t.Errorf("missing sync start")
				}
				if !strings.HasSuffix(string(got), SyncEndCSI) {
					t.Errorf("missing sync end")
				}
				if !strings.Contains(string(got), "hello") {
					t.Errorf("missing original content")
				}
			},
		},
		{
			name:    "disabled returns content unchanged",
			content: content,
			enabled: false,
			check: func(t *testing.T, got []byte) {
				if string(got) != string(content) {
					t.Errorf("disabled wrapping should return content unchanged")
				}
			},
		},
		{
			name:    "empty input returns empty",
			content: nil,
			enabled: true,
			check: func(t *testing.T, got []byte) {
				if len(got) != 0 {
					t.Errorf("empty input should return empty")
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := WrapSynchronized(tt.content, tt.enabled)
			tt.check(t, got)
		})
	}
}

// ---------------------------------------------------------------------------
// Partial damage draw
// ---------------------------------------------------------------------------

func TestPartialDamage(t *testing.T) {
	tests := []struct {
		name string
	}{
		{name: "single changed cell repositions cursor and resets style"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := New(Capabilities{})
			frame := NewFrame(4, 2)
			markFrame(&frame)

			// Populate shadow.
			_, err := r.Draw(frame, []Damage{{Kind: DamageText, X: 0, Y: 0, Width: 4, Height: 2}})
			if err != nil {
				t.Fatal(err)
			}

			// Change one cell.
			frame.Set(2, 1, Cell{Rune: 'Z', Style: DefaultStyle()})
			damage := []Damage{{Kind: DamageText, X: 2, Y: 1, Width: 1, Height: 1}}

			out, err := r.Draw(frame, damage)
			if err != nil {
				t.Fatal(err)
			}
			if len(out) == 0 {
				t.Fatal("expected non-empty output for partial damage")
			}
			// Should position cursor at (2,1) — 1-indexed (3;2).
			outputContains(t, out, "\x1b[2;3H")
			outputContains(t, out, "Z")
			outputEndsWith(t, out, "\x1b[0m")
		})
	}
}

// ---------------------------------------------------------------------------
// Reset
// ---------------------------------------------------------------------------

func TestReset(t *testing.T) {
	tests := []struct {
		name string
	}{
		{name: "clears shadow state and forces a full draw next time"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := New(Capabilities{})
			frame := NewFrame(3, 2)
			markFrame(&frame)

			_, err := r.Draw(frame, []Damage{{Kind: DamageText, X: 0, Y: 0, Width: 3, Height: 2}})
			if err != nil {
				t.Fatal(err)
			}

			r.Reset()

			if r.width != 0 || r.height != 0 || r.shadow != nil {
				t.Fatal("Reset should clear width/height/shadow")
			}

			// After reset, the next draw should be a full draw.
			out, err := r.Draw(frame, nil)
			if err != nil {
				t.Fatal(err)
			}
			if len(out) == 0 {
				t.Fatal("expected non-empty output after reset")
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Damage helper
// ---------------------------------------------------------------------------

func TestFullRedraw(t *testing.T) {
	tests := []struct {
		name string
		run  func(t *testing.T)
	}{
		{
			name: "known same-size full redraw diffs against shadow",
			run: func(t *testing.T) {
				r := New(Capabilities{})
				frame := NewFrame(3, 2)
				markFrame(&frame)

				if _, err := r.Draw(frame, []Damage{FullRedraw()}); err != nil {
					t.Fatal(err)
				}

				changed := frame
				changed.Set(0, 1, Cell{Rune: 'Z', Style: DefaultStyle()})
				out, err := r.Draw(changed, []Damage{FullRedraw()})
				if err != nil {
					t.Fatal(err)
				}
				got := string(out)
				if strings.Contains(got, "ABC") {
					t.Fatalf("unchanged row was re-emitted on dimension-stable FullRedraw: %q", got)
				}
				if !strings.Contains(got, "ZEF") {
					t.Fatalf("changed row was not emitted on dimension-stable FullRedraw: %q", got)
				}
				outputEndsWith(t, out, "\x1b[0m")
			},
		},
		{
			name: "unchanged known same-size full redraw is a no-op",
			run: func(t *testing.T) {
				r := New(Capabilities{})
				frame := NewFrame(3, 2)
				markFrame(&frame)

				if _, err := r.Draw(frame, []Damage{FullRedraw()}); err != nil {
					t.Fatal(err)
				}
				out, err := r.Draw(frame, []Damage{FullRedraw()})
				if err != nil {
					t.Fatal(err)
				}
				if len(out) != 0 {
					t.Fatalf("unchanged dimension-stable FullRedraw emitted output: %q", string(out))
				}
			},
		},
		{
			name: "reset full redraw emits complete frame",
			run: func(t *testing.T) {
				r := New(Capabilities{})
				frame := NewFrame(3, 2)
				markFrame(&frame)

				if _, err := r.Draw(frame, []Damage{FullRedraw()}); err != nil {
					t.Fatal(err)
				}
				r.Reset()

				out, err := r.Draw(frame, []Damage{FullRedraw()})
				if err != nil {
					t.Fatal(err)
				}
				got := string(out)
				if !strings.Contains(got, "ABC") || !strings.Contains(got, "DEF") {
					t.Fatalf("reset FullRedraw did not emit complete frame: %q", got)
				}
				outputEndsWith(t, out, "\x1b[0m")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, tt.run)
	}
}

// ---------------------------------------------------------------------------
// Scroll with no remaining damage
// ---------------------------------------------------------------------------

func TestScrollOnlyDamage(t *testing.T) {
	tests := []struct {
		name string
	}{
		{name: "scroll damage plus new-row text damage from the VT model"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := New(Capabilities{})
			frame := NewFrame(4, 3)
			markFrame(&frame)

			_, err := r.Draw(frame, []Damage{{Kind: DamageText, X: 0, Y: 0, Width: 4, Height: 3}})
			if err != nil {
				t.Fatal(err)
			}

			// Scroll up 1 but the VT model would also emit DamageText for the new bottom row.
			scrolled := NewFrame(4, 3)
			copy(scrolled.Cells[0:8], frame.Cells[4:12])
			for i := 8; i < 12; i++ {
				scrolled.Cells[i] = BlankCell()
			}
			scrolled.Set(0, 2, Cell{Rune: 'X', Style: DefaultStyle()})

			damage := []Damage{
				{Kind: DamageScrollUp, X: 0, Y: 0, Width: 4, Height: 3, Count: 1},
				{Kind: DamageText, X: 0, Y: 2, Width: 4, Height: 1},
			}

			out, err := r.Draw(scrolled, damage)
			if err != nil {
				t.Fatal(err)
			}
			if len(out) == 0 {
				t.Fatal("expected output for scroll")
			}
			// Should include scroll region
			outputContains(t, out, "\x1b[1;3r")
			outputContains(t, out, "\x1b[r")
			outputEndsWith(t, out, "\x1b[0m")
		})
	}
}

// ---------------------------------------------------------------------------
// writeCursor output format
// ---------------------------------------------------------------------------

func TestWriteCursor(t *testing.T) {
	tests := []struct {
		name string
		y, x int
		want string
	}{
		{name: "origin", y: 0, x: 0, want: "\x1b[1;1H"},
		{name: "offset", y: 2, x: 5, want: "\x1b[3;6H"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			writeCursor(&buf, tt.y, tt.x)
			if buf.String() != tt.want {
				t.Errorf("unexpected cursor positioning: %q", buf.String())
			}
		})
	}
}

// ---------------------------------------------------------------------------
// isSafeScroll edge cases
// ---------------------------------------------------------------------------

func TestIsSafeScroll(t *testing.T) {
	frame := NewFrame(10, 8)

	tests := []struct {
		name string
		d    Damage
		want bool
	}{
		{"full width, partial height", Damage{X: 0, Y: 1, Width: 10, Height: 5, Count: 2}, true},
		{"not full width", Damage{X: 1, Y: 1, Width: 8, Height: 5, Count: 2}, false},
		{"count zero", Damage{X: 0, Y: 0, Width: 10, Height: 8, Count: 0}, false},
		{"count equals height", Damage{X: 0, Y: 0, Width: 10, Height: 8, Count: 8}, false},
		{"extends past frame", Damage{X: 0, Y: 6, Width: 10, Height: 6, Count: 2}, false},
		{"negative Y", Damage{X: 0, Y: -1, Width: 10, Height: 5, Count: 1}, false},
		{"height zero", Damage{X: 0, Y: 0, Width: 10, Height: 0, Count: 1}, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := isSafeScroll(frame, tt.d)
			if got != tt.want {
				t.Errorf("isSafeScroll(%+v) = %v, want %v", tt.d, got, tt.want)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// BlankCell / Cell.Equal
// ---------------------------------------------------------------------------

func TestCellEqual(t *testing.T) {
	tests := []struct {
		name string
		a    Cell
		b    Cell
		want bool
	}{
		{
			name: "blank cells should be equal",
			a:    BlankCell(),
			b:    BlankCell(),
			want: true,
		},
		{
			name: "different runes should not be equal",
			a:    Cell{Rune: 'A', Style: DefaultStyle()},
			b:    BlankCell(),
			want: false,
		},
		{
			name: "different styles should not be equal",
			a:    Cell{Rune: ' ', Style: Style{Bold: true, Foreground: -1, Background: -1}},
			b:    BlankCell(),
			want: false,
		},
		{
			name: "continuation flag participates in equality",
			a:    Cell{Continuation: true},
			b:    Cell{},
			want: false,
		},
		{
			name: "two continuation cells are equal",
			a:    Cell{Continuation: true},
			b:    Cell{Continuation: true},
			want: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.a.Equal(tt.b); got != tt.want {
				t.Errorf("Equal() = %v, want %v", got, tt.want)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Frame helpers
// ---------------------------------------------------------------------------

func TestFrameValidate(t *testing.T) {
	tests := []struct {
		name    string
		frame   Frame
		wantErr bool
	}{
		{name: "valid frame", frame: NewFrame(5, 3), wantErr: false},
		{name: "invalid dimensions (zero width)", frame: Frame{Width: 0, Height: 5}, wantErr: true},
		{name: "wrong cell count", frame: Frame{Width: 2, Height: 2, Cells: make([]Cell, 3)}, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.frame.Validate()
			if tt.wantErr && err == nil {
				t.Fatal("expected error, got nil")
			}
			if !tt.wantErr && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

func TestFrameAtSet(t *testing.T) {
	tests := []struct {
		name string
	}{
		{name: "At/Set round-trip and unaffected cells stay blank"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := NewFrame(3, 2)
			cell := Cell{Rune: 'X', Style: Style{Bold: true}}
			f.Set(1, 1, cell)

			got := f.At(1, 1)
			if got.Rune != 'X' || !got.Style.Bold {
				t.Errorf("At/Set round-trip failed: got %+v", got)
			}

			// Unaffected cells are blank.
			got = f.At(0, 0)
			if got != BlankCell() {
				t.Errorf("unexpected cell at (0,0): %+v", got)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Damage helpers
// ---------------------------------------------------------------------------

func TestSameDamage(t *testing.T) {
	tests := []struct {
		name string
		a    Damage
		b    Damage
		want bool
	}{
		{
			name: "identical damages should be equal",
			a:    Damage{Kind: DamageText, X: 1, Y: 2, Width: 3, Height: 4, Count: 5},
			b:    Damage{Kind: DamageText, X: 1, Y: 2, Width: 3, Height: 4, Count: 5},
			want: true,
		},
		{
			name: "different counts should not be equal",
			a:    Damage{Kind: DamageText, X: 1, Y: 2, Width: 3, Height: 4, Count: 5},
			b:    Damage{Kind: DamageText, X: 1, Y: 2, Width: 3, Height: 4, Count: 0},
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := sameDamage(tt.a, tt.b); got != tt.want {
				t.Errorf("sameDamage() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDamageCoversCell(t *testing.T) {
	damage := []Damage{
		{Kind: DamageText, X: 2, Y: 3, Width: 5, Height: 2},
	}

	tests := []struct {
		name string
		x, y int
		want bool
	}{
		{name: "should cover (3,3)", x: 3, y: 3, want: true},
		{name: "should cover (6,4)", x: 6, y: 4, want: true},
		{name: "should not cover (1,3)", x: 1, y: 3, want: false},
		{name: "should not cover (3,5)", x: 3, y: 5, want: false},
		{name: "should not cover (7,3)", x: 7, y: 3, want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := damageCoversCell(damage, tt.x, tt.y); got != tt.want {
				t.Errorf("damageCoversCell(%d,%d) = %v, want %v", tt.x, tt.y, got, tt.want)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// needsFull
// ---------------------------------------------------------------------------

func TestNeedsFull(t *testing.T) {
	tests := []struct {
		name   string
		damage []Damage
		want   bool
	}{
		{name: "nil damage should not need full", damage: nil, want: false},
		{name: "empty damage should not need full", damage: []Damage{}, want: false},
		{name: "text damage should not need full", damage: []Damage{{Kind: DamageText}}, want: false},
		{name: "FullRedraw should need full", damage: []Damage{FullRedraw()}, want: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := needsFull(tt.damage); got != tt.want {
				t.Errorf("needsFull() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDrawFallbackAndClampingEdgeCases(t *testing.T) {
	tests := []struct {
		name string
		run  func(t *testing.T)
	}{
		{
			name: "falls back to full redraw when scroll fast path is unsafe",
			run: func(t *testing.T) {
				r := New(Capabilities{})
				frame := NewFrame(4, 3)
				markFrame(&frame)
				if _, err := r.Draw(frame, []Damage{{Kind: DamageText, X: 0, Y: 0, Width: 4, Height: 3}}); err != nil {
					t.Fatal(err)
				}

				changed := NewFrame(4, 3)
				markFrame(&changed)
				changed.Set(0, 0, Cell{Rune: 'x', Style: DefaultStyle()}) // invalidates scroll relationship
				damage := []Damage{
					{Kind: DamageScrollUp, X: 0, Y: 0, Width: 4, Height: 3, Count: 1},
					{Kind: DamageText, X: 0, Y: 2, Width: 4, Height: 1},
				}
				out, err := r.Draw(changed, damage)
				if err != nil {
					t.Fatal(err)
				}
				if rows := strings.Count(string(out), "H"); rows < changed.Height {
					t.Fatalf("expected full redraw after unsafe scroll fallback, output %q", string(out))
				}
				for i := range changed.Cells {
					if !r.shadow[i].Equal(changed.Cells[i]) {
						t.Fatalf("shadow[%d] = %+v, want %+v", i, r.shadow[i], changed.Cells[i])
					}
				}
			},
		},
		{
			name: "out-of-bounds damage rectangles are clamped without panicking",
			run: func(t *testing.T) {
				r := New(Capabilities{})
				frame := NewFrame(3, 2)
				markFrame(&frame)
				if _, err := r.Draw(frame, []Damage{FullRedraw()}); err != nil {
					t.Fatal(err)
				}
				frame.Set(0, 0, Cell{Rune: 'Z', Style: DefaultStyle()})
				defer func() {
					if r := recover(); r != nil {
						t.Fatalf("Draw panicked for out-of-bounds damage: %v", r)
					}
				}()
				if _, err := r.Draw(frame, []Damage{{Kind: DamageText, X: -2, Y: -1, Width: 4, Height: 3}}); err != nil {
					t.Fatal(err)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, tt.run)
	}
}

// ---------------------------------------------------------------------------
// Wide (double-width) character rendering
// ---------------------------------------------------------------------------

func TestWideCharRendering(t *testing.T) {
	// A wide left cell {Rune: r} followed by a continuation cell
	// {Continuation: true}. The renderer must emit the rune once and emit
	// nothing at all for the continuation cell (the terminal advances two
	// columns by itself). It must never emit a space for a continuation.
	tests := []struct {
		name  string
		build func() Frame
		want  string
	}{
		{
			name: "single wide char followed by ascii",
			build: func() Frame {
				f := NewFrame(4, 1)
				f.Set(0, 0, Cell{Rune: '你', Style: DefaultStyle()})
				f.Set(1, 0, Cell{Continuation: true, Style: DefaultStyle()})
				f.Set(2, 0, Cell{Rune: 'A', Style: DefaultStyle()})
				return f
			},
			// row 0: cursor home, 你, (skip continuation), A, trailing space, reset
			want: "\x1b[1;1H你A \x1b[0m",
		},
		{
			name: "two adjacent wide chars then ascii",
			build: func() Frame {
				f := NewFrame(6, 1)
				f.Set(0, 0, Cell{Rune: '你', Style: DefaultStyle()})
				f.Set(1, 0, Cell{Continuation: true, Style: DefaultStyle()})
				f.Set(2, 0, Cell{Rune: '好', Style: DefaultStyle()})
				f.Set(3, 0, Cell{Continuation: true, Style: DefaultStyle()})
				f.Set(4, 0, Cell{Rune: 'X', Style: DefaultStyle()})
				return f
			},
			want: "\x1b[1;1H你好X \x1b[0m",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := New(Capabilities{})
			frame := tt.build()
			out, err := r.Draw(frame, []Damage{FullRedraw()})
			if err != nil {
				t.Fatal(err)
			}
			if string(out) != tt.want {
				t.Errorf("output = %q, want %q", string(out), tt.want)
			}
			// No continuation must ever be rendered as a space: the visible
			// glyph count is exactly the number of non-continuation cells.
			// A second draw with no damage must be a no-op, proving the shadow
			// matches the frame (continuation cells included).
			out2, err := r.Draw(frame, nil)
			if err != nil {
				t.Fatal(err)
			}
			if len(out2) != 0 {
				t.Errorf("second draw not a no-op: %q (shadow inconsistent)", string(out2))
			}
		})
	}
}

func TestRendererPrepare(t *testing.T) {
	fullFrame := testFrame("ABC", "DEF")
	fullDamage := []Damage{FullRedraw()}

	tests := []struct {
		name string
		run  func(*testing.T)
	}{
		{name: "zero commit is safe", run: func(t *testing.T) {
			var prepared PreparedDraw
			prepared.Commit()
		}},

		{name: "discard and reprepare returns identical owned bytes", run: func(t *testing.T) {
			r := New(Capabilities{})
			first, err := r.Prepare(fullFrame, fullDamage, false)
			if err != nil {
				t.Fatal(err)
			}
			want := append([]byte(nil), first.Bytes()...)

			second, err := r.Prepare(fullFrame, fullDamage, false)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(second.Bytes(), want) {
				t.Fatalf("reprepared bytes = %q, want %q", second.Bytes(), want)
			}

			other := testFrame("12345678", "abcdefgh", "ABCDEFGH")
			if _, err := r.Prepare(other, fullDamage, false); err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(first.Bytes(), want) {
				t.Fatalf("prepared bytes changed after pooled buffer reuse: got %q, want %q", first.Bytes(), want)
			}
		}},

		{name: "commit then no-op", run: func(t *testing.T) {
			r := New(Capabilities{})
			prepared, err := r.Prepare(fullFrame, fullDamage, false)
			if err != nil {
				t.Fatal(err)
			}
			prepared.Commit()

			next, err := r.Prepare(fullFrame, nil, false)
			if err != nil {
				t.Fatal(err)
			}
			if len(next.Bytes()) != 0 {
				t.Fatalf("unchanged frame emitted %q", next.Bytes())
			}
		}},

		{name: "reset preparation is transactional", run: func(t *testing.T) {
			r := New(Capabilities{})
			initial, err := r.Prepare(fullFrame, fullDamage, false)
			if err != nil {
				t.Fatal(err)
			}
			initial.Commit()

			resetFrame := testFrame("ABC", "XYZ")
			resetPrepared, err := r.Prepare(resetFrame, nil, true)
			if err != nil {
				t.Fatal(err)
			}
			want, err := New(Capabilities{}).Draw(resetFrame, nil)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(resetPrepared.Bytes(), want) {
				t.Fatalf("reset preparation = %q, want full draw %q", resetPrepared.Bytes(), want)
			}

			unchanged, err := r.Prepare(fullFrame, nil, false)
			if err != nil {
				t.Fatal(err)
			}
			if len(unchanged.Bytes()) != 0 {
				t.Fatalf("discarded reset changed committed shadow: %q", unchanged.Bytes())
			}

			resetPrepared, err = r.Prepare(resetFrame, nil, true)
			if err != nil {
				t.Fatal(err)
			}
			resetPrepared.Commit()
			afterCommit, err := r.Prepare(resetFrame, nil, false)
			if err != nil {
				t.Fatal(err)
			}
			if len(afterCommit.Bytes()) != 0 {
				t.Fatalf("committed reset was not retained: %q", afterCommit.Bytes())
			}
		}},

		{name: "successful no-byte commit", run: func(t *testing.T) {
			r := New(Capabilities{})
			initial, err := r.Prepare(fullFrame, fullDamage, false)
			if err != nil {
				t.Fatal(err)
			}
			initial.Commit()

			noOp, err := r.Prepare(fullFrame, nil, false)
			if err != nil {
				t.Fatal(err)
			}
			if len(noOp.Bytes()) != 0 {
				t.Fatalf("no-op emitted %q", noOp.Bytes())
			}
			noOp.Commit()

			again, err := r.Prepare(fullFrame, nil, false)
			if err != nil {
				t.Fatal(err)
			}
			if len(again.Bytes()) != 0 {
				t.Fatalf("committed no-op changed renderer state: %q", again.Bytes())
			}
		}},

		{name: "double commit is idempotent", run: func(t *testing.T) {
			base := testFrame("0000", "1111", "2222", "3333")
			scrolled := testFrame("1111", "2222", "3333", "new!")
			damage := []Damage{
				{Kind: DamageScrollUp, X: 0, Y: 0, Width: 4, Height: 4, Count: 1},
				{Kind: DamageText, X: 0, Y: 3, Width: 4, Height: 1},
			}
			r := New(Capabilities{})
			if _, err := r.Draw(base, fullDamage); err != nil {
				t.Fatal(err)
			}

			prepared, err := r.Prepare(scrolled, damage, false)
			if err != nil {
				t.Fatal(err)
			}
			prepared.Commit()
			prepared.Commit()

			next, err := r.Prepare(scrolled, nil, false)
			if err != nil {
				t.Fatal(err)
			}
			if len(next.Bytes()) != 0 {
				t.Fatalf("double commit advanced state twice: %q", next.Bytes())
			}
		}},

		{name: "copied prepared draw commits scroll once", run: func(t *testing.T) {
			base := testFrame("0000", "1111", "2222", "3333")
			scrolled := testFrame("1111", "2222", "3333", "new!")
			damage := []Damage{
				{Kind: DamageScrollUp, X: 0, Y: 0, Width: 4, Height: 4, Count: 1},
				{Kind: DamageText, X: 0, Y: 3, Width: 4, Height: 1},
			}
			r := New(Capabilities{})
			if _, err := r.Draw(base, fullDamage); err != nil {
				t.Fatal(err)
			}

			prepared, err := r.Prepare(scrolled, damage, false)
			if err != nil {
				t.Fatal(err)
			}
			copied := prepared
			prepared.Commit()
			copied.Commit()

			next, err := r.Prepare(scrolled, nil, false)
			if err != nil {
				t.Fatal(err)
			}
			if len(next.Bytes()) != 0 {
				t.Fatalf("copied commit advanced state twice: %q", next.Bytes())
			}
		}},

		{name: "commit uses immutable frame snapshot", run: func(t *testing.T) {
			r := New(Capabilities{})
			base := testFrame("ABC", "DEF")
			if _, err := r.Draw(base, fullDamage); err != nil {
				t.Fatal(err)
			}

			changed := base.Clone()
			changed.Set(1, 1, Cell{Rune: 'X', Style: DefaultStyle()})
			want := changed.Clone()
			prepared, err := r.Prepare(changed, []Damage{{Kind: DamageText, X: 1, Y: 1, Width: 1, Height: 1}}, false)
			if err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(string(prepared.Bytes()), "X") {
				t.Fatalf("prepared output = %q, want changed cell", prepared.Bytes())
			}

			changed.Set(1, 1, Cell{Rune: 'Y', Style: DefaultStyle()})
			prepared.Commit()

			next, err := r.Prepare(want, nil, false)
			if err != nil {
				t.Fatal(err)
			}
			if len(next.Bytes()) != 0 {
				t.Fatalf("snapshot did not preserve prepared frame: %q", next.Bytes())
			}
		}},

		{name: "matches concrete ANSI fixtures", run: func(t *testing.T) {
			tests := []struct {
				name    string
				prepare func(t *testing.T) PreparedDraw
				want    string
			}{
				{
					name: "full draw",
					prepare: func(t *testing.T) PreparedDraw {
						prepared, err := New(Capabilities{}).Prepare(fullFrame, fullDamage, false)
						if err != nil {
							t.Fatal(err)
						}
						return prepared
					},
					want: "\x1b[1;1HABC\x1b[2;1HDEF\x1b[0m",
				},
				{
					name: "fragmented damage",
					prepare: func(t *testing.T) PreparedDraw {
						base := testFrame("abcdefgh", "ijklmnop", "qrstuvwx")
						frame := base.Clone()
						frame.Set(1, 0, Cell{Rune: 'X', Style: DefaultStyle()})
						frame.Set(5, 0, Cell{Rune: 'Y', Style: DefaultStyle()})
						frame.Set(2, 2, Cell{Rune: 'Z', Style: DefaultStyle()})
						damage := []Damage{
							{Kind: DamageText, X: 5, Y: 0, Width: 1, Height: 1},
							{Kind: DamageText, X: 1, Y: 0, Width: 1, Height: 1},
							{Kind: DamageText, X: 2, Y: 2, Width: 1, Height: 1},
						}
						r := New(Capabilities{})
						if _, err := r.Draw(base, fullDamage); err != nil {
							t.Fatal(err)
						}
						prepared, err := r.Prepare(frame, damage, false)
						if err != nil {
							t.Fatal(err)
						}
						return prepared
					},
					want: "\x1b[1;2HX\x1b[3CY\x1b[3;3HZ\x1b[0m",
				},
				{
					name: "scroll",
					prepare: func(t *testing.T) PreparedDraw {
						base := testFrame("00000", "11111", "22222", "33333")
						frame := testFrame("11111", "22222", "33333", "new!!")
						damage := []Damage{
							{Kind: DamageScrollUp, X: 0, Y: 0, Width: 5, Height: 4, Count: 1},
							{Kind: DamageText, X: 0, Y: 3, Width: 5, Height: 1},
						}
						r := New(Capabilities{})
						if _, err := r.Draw(base, fullDamage); err != nil {
							t.Fatal(err)
						}
						prepared, err := r.Prepare(frame, damage, false)
						if err != nil {
							t.Fatal(err)
						}
						return prepared
					},
					want: "\x1b[0m\x1b[1;4r\x1b[4;1H\n\x1b[r\x1b[4;1Hnew!!\x1b[0m",
				},
			}

			for _, tt := range tests {
				t.Run(tt.name, func(t *testing.T) {
					if got := string(tt.prepare(t).Bytes()); got != tt.want {
						t.Fatalf("prepared ANSI = %q, want %q", got, tt.want)
					}
				})
			}
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, tt.run)
	}
}

func TestPlanDeltaRejectsInvalidCommittedFrame(t *testing.T) {
	frame := NewFrame(2, 1)
	committed := NewFrame(2, 1)
	committed.lineOffset = nil

	if _, err := PlanDelta(frame, nil, committed, false); err == nil {
		t.Fatal("PlanDelta accepted an invalid committed frame")
	}
}

func TestWideCharDamageEmission(t *testing.T) {
	// Populate the shadow with a blank frame, then place a wide char and draw
	// with a text-damage rect covering the pair. The continuation cell must
	// not produce a space.
	r := New(Capabilities{})
	blank := NewFrame(4, 1)
	if _, err := r.Draw(blank, []Damage{FullRedraw()}); err != nil {
		t.Fatal(err)
	}

	frame := NewFrame(4, 1)
	frame.Set(0, 0, Cell{Rune: '好', Style: DefaultStyle()})
	frame.Set(1, 0, Cell{Continuation: true, Style: DefaultStyle()})

	out, err := r.Draw(frame, []Damage{{Kind: DamageText, X: 0, Y: 0, Width: 2, Height: 1, Count: 1}})
	if err != nil {
		t.Fatal(err)
	}
	got := string(out)
	if !strings.Contains(got, "好") {
		t.Errorf("output %q missing wide rune", got)
	}
	if strings.Contains(got, "好 ") {
		t.Errorf("output %q emitted a space for the continuation cell", got)
	}
}
