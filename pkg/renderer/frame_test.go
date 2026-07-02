package renderer

import "testing"

// rowRunes renders a logical row to a string for readable assertions.
func rowRunes(f Frame, y int) string {
	out := make([]rune, f.Width)
	for x := range f.Width {
		out[x] = f.At(x, y).Rune
	}
	return string(out)
}

// fillFrame writes one distinct rune per logical row so rotation is observable.
func fillFrame(f Frame, rows []string) {
	for y, s := range rows {
		for x, r := range s {
			f.Set(x, y, Cell{Rune: r, Style: DefaultStyle()})
		}
	}
}

func TestNewFrameCanonicalOffsets(t *testing.T) {
	f := NewFrame(4, 3)
	for y := 0; y < f.Height; y++ {
		if f.lineOffset[y] != y*f.Width {
			t.Fatalf("lineOffset[%d] = %d, want %d", y, f.lineOffset[y], y*f.Width)
		}
	}
	if err := f.CheckInvariants(); err != nil {
		t.Fatalf("fresh frame invariants: %v", err)
	}
}

// TestCheckInvariantsDetectsBrokenRotation is the RED guard: a rotation that
// duplicates a physical offset (the classic off-by-one bug) must be rejected.
func TestCheckInvariantsDetectsBrokenRotation(t *testing.T) {
	f := NewFrame(4, 3)
	// Corrupt: two logical rows map to the same physical row.
	f.lineOffset[1] = f.lineOffset[0]
	if err := f.CheckInvariants(); err == nil {
		t.Fatal("expected invariant violation for duplicate physical row, got nil")
	}

	f = NewFrame(4, 3)
	f.lineOffset[0] = 1 // not a multiple of width
	if err := f.CheckInvariants(); err == nil {
		t.Fatal("expected invariant violation for non-multiple offset, got nil")
	}

	f = NewFrame(4, 3)
	f.lineOffset[2] = 99 // out of range
	if err := f.CheckInvariants(); err == nil {
		t.Fatal("expected invariant violation for out-of-range offset, got nil")
	}
}

func TestScrollUpEquivalence(t *testing.T) {
	tests := []struct {
		name          string
		width, height int
		rows          []string
		top, bottom   int
		n             int
		want          []string
	}{
		{
			name: "scroll up 1 full screen", width: 3, height: 4,
			rows: []string{"aaa", "bbb", "ccc", "ddd"},
			top:  0, bottom: 3, n: 1,
			want: []string{"bbb", "ccc", "ddd", "   "},
		},
		{
			name: "scroll up 2 full screen", width: 3, height: 4,
			rows: []string{"aaa", "bbb", "ccc", "ddd"},
			top:  0, bottom: 3, n: 2,
			want: []string{"ccc", "ddd", "   ", "   "},
		},
		{
			name: "scroll up region subset", width: 3, height: 5,
			rows: []string{"aaa", "bbb", "ccc", "ddd", "eee"},
			top:  1, bottom: 3, n: 1,
			want: []string{"aaa", "ccc", "ddd", "   ", "eee"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := NewFrame(tt.width, tt.height)
			fillFrame(f, tt.rows)
			f.ScrollUp(tt.top, tt.bottom, tt.n)
			for y := 0; y < f.Height; y++ {
				if got := rowRunes(f, y); got != tt.want[y] {
					t.Errorf("row %d = %q, want %q", y, got, tt.want[y])
				}
			}
			if err := f.CheckInvariants(); err != nil {
				t.Errorf("invariants after scroll up: %v", err)
			}
		})
	}
}

func TestScrollDownEquivalence(t *testing.T) {
	tests := []struct {
		name          string
		width, height int
		rows          []string
		top, bottom   int
		n             int
		want          []string
	}{
		{
			name: "scroll down 1 full screen", width: 3, height: 4,
			rows: []string{"aaa", "bbb", "ccc", "ddd"},
			top:  0, bottom: 3, n: 1,
			want: []string{"   ", "aaa", "bbb", "ccc"},
		},
		{
			name: "scroll down 2 region subset", width: 3, height: 5,
			rows: []string{"aaa", "bbb", "ccc", "ddd", "eee"},
			top:  1, bottom: 4, n: 2,
			want: []string{"aaa", "   ", "   ", "bbb", "ccc"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := NewFrame(tt.width, tt.height)
			fillFrame(f, tt.rows)
			f.ScrollDown(tt.top, tt.bottom, tt.n)
			for y := 0; y < f.Height; y++ {
				if got := rowRunes(f, y); got != tt.want[y] {
					t.Errorf("row %d = %q, want %q", y, got, tt.want[y])
				}
			}
			if err := f.CheckInvariants(); err != nil {
				t.Errorf("invariants after scroll down: %v", err)
			}
		})
	}
}

// TestRotationIsPermutationUnderRandomOps drives an arbitrary sequence of
// scrolls and asserts lineOffset stays a valid permutation throughout.
func TestRotationIsPermutationUnderRandomOps(t *testing.T) {
	f := NewFrame(5, 6)
	fillFrame(f, []string{"aaaaa", "bbbbb", "ccccc", "ddddd", "eeeee", "fffff"})
	ops := []struct {
		up          bool
		top, bottom int
		n           int
	}{
		{true, 0, 5, 1}, {false, 1, 4, 2}, {true, 2, 5, 3},
		{false, 0, 5, 1}, {true, 0, 2, 2}, {false, 3, 5, 1},
	}
	for i, op := range ops {
		if op.up {
			f.ScrollUp(op.top, op.bottom, op.n)
		} else {
			f.ScrollDown(op.top, op.bottom, op.n)
		}
		if err := f.CheckInvariants(); err != nil {
			t.Fatalf("op %d (%+v): invariants broken: %v", i, op, err)
		}
	}
}
