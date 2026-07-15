package vt

import (
	"reflect"
	"testing"

	"github.com/bnema/vev/pkg/renderer"
)

func damageKinds(d []renderer.Damage) []renderer.DamageKind {
	ks := make([]renderer.DamageKind, len(d))
	for i, dd := range d {
		ks[i] = dd.Kind
	}
	return ks
}

func hasDamageKind(d []renderer.Damage, kind renderer.DamageKind) bool {
	for _, dd := range d {
		if dd.Kind == kind {
			return true
		}
	}
	return false
}

func cellAt(s *Screen, x, y int) renderer.Cell {
	return s.Frame.At(x, y)
}

func assertFramesEqual(t *testing.T, got, want renderer.Frame) {
	t.Helper()
	if !reflect.DeepEqual(got, want) {
		t.Fatal("frame changed unexpectedly")
	}
}

func assertCell(t *testing.T, s *Screen, x, y int, expected rune) {
	t.Helper()
	c := cellAt(s, x, y)
	if c.Rune != expected {
		t.Errorf("cell(%d,%d) rune = %q, want %q", x, y, c.Rune, expected)
	}
	if c.Continuation {
		t.Errorf("cell(%d,%d) unexpectedly marked as continuation", x, y)
	}
}

// assertContinuation asserts the cell at (x,y) is the right half of a
// wide-character pair (Continuation set, Rune 0).
func assertContinuation(t *testing.T, s *Screen, x, y int) {
	t.Helper()
	c := cellAt(s, x, y)
	if !c.Continuation {
		t.Errorf("cell(%d,%d) expected continuation, got rune=%q continuation=%v", x, y, c.Rune, c.Continuation)
	}
	if c.Rune != 0 {
		t.Errorf("cell(%d,%d) continuation rune = %q, want 0", x, y, c.Rune)
	}
}

// assertBlank asserts the cell at (x,y) is a blank default cell.
func assertBlank(t *testing.T, s *Screen, x, y int) {
	t.Helper()
	c := cellAt(s, x, y)
	if c.Rune != ' ' || c.Continuation {
		t.Errorf("cell(%d,%d) = {rune:%q cont:%v}, want blank space", x, y, c.Rune, c.Continuation)
	}
}

func assertNoOrphanWideCells(t *testing.T, s *Screen) {
	t.Helper()
	for y := range s.Frame.Height {
		for x := range s.Frame.Width {
			c := cellAt(s, x, y)
			if c.Continuation {
				if x == 0 || cellAt(s, x-1, y).Continuation || renderer.RuneWidth(cellAt(s, x-1, y).Rune) != 2 {
					t.Fatalf("orphan continuation at (%d,%d)", x, y)
				}
				continue
			}
			if renderer.RuneWidth(c.Rune) == 2 && (x+1 >= s.Frame.Width || !cellAt(s, x+1, y).Continuation) {
				t.Fatalf("orphan wide head at (%d,%d) rune %q", x, y, c.Rune)
			}
		}
	}
}

func lineText(s *Screen, y int) string {
	out := make([]rune, s.Frame.Width)
	for x := range s.Frame.Width {
		out[x] = s.Frame.At(x, y).Rune
	}
	return string(out)
}

func rowString(row []renderer.Cell) string {
	out := make([]rune, len(row))
	for i, cell := range row {
		out[i] = cell.Rune
	}
	return string(out)
}
