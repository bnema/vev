package vtcore

import "testing"

func TestFrameCanonicalOffsetsAndInvariants(t *testing.T) {
	f := NewFrame(4, 3)
	for y := range f.Height {
		if f.lineOffset[y] != y*f.Width {
			t.Fatalf("lineOffset[%d] = %d, want %d", y, f.lineOffset[y], y*f.Width)
		}
	}
	if err := f.CheckInvariants(); err != nil {
		t.Fatalf("fresh frame invariants: %v", err)
	}
}

func TestCheckInvariantsDetectsBrokenRotation(t *testing.T) {
	f := NewFrame(4, 3)
	f.lineOffset[1] = f.lineOffset[0]
	if err := f.CheckInvariants(); err == nil {
		t.Fatal("expected duplicate physical row to be rejected")
	}

	f = NewFrame(4, 3)
	f.lineOffset[0] = 1
	if err := f.CheckInvariants(); err == nil {
		t.Fatal("expected non-multiple offset to be rejected")
	}

	f = NewFrame(4, 3)
	f.lineOffset[2] = 99
	if err := f.CheckInvariants(); err == nil {
		t.Fatal("expected out-of-range offset to be rejected")
	}
}

func TestReplacePreservesLogicalRows(t *testing.T) {
	dst := NewFrame(3, 3)
	dst.Set(0, 0, Cell{Rune: 'x'})
	dst.ScrollUp(0, 2, 1)

	src := NewFrame(3, 3)
	for y, row := range []string{"abc", "def", "ghi"} {
		for x, r := range row {
			src.Set(x, y, Cell{Rune: r})
		}
	}
	src.ScrollUp(0, 2, 1)

	dst.Replace(src)
	for y, want := range []string{"def", "ghi", "   "} {
		for x, r := range want {
			if got := dst.At(x, y).Rune; got != r {
				t.Fatalf("row %d col %d = %q, want %q", y, x, got, r)
			}
		}
	}
	if err := dst.CheckInvariants(); err != nil {
		t.Fatalf("replaced frame invariants: %v", err)
	}
}
