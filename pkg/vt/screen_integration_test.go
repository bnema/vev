package vt

import (
	"testing"

	"github.com/bnema/vev/pkg/renderer"
)

func TestZshAutosuggestionRepaintEmitsForwardDamage(t *testing.T) {
	s := NewScreen(20, 2)
	r := renderer.New(renderer.Capabilities{})

	s.Write([]byte("❯ "))
	if _, err := r.Draw(s.Frame, s.Damage()); err != nil {
		t.Fatal(err)
	}
	s.ClearDamage()

	s.Write([]byte("h"))
	if _, err := r.Draw(s.Frame, s.Damage()); err != nil {
		t.Fatal(err)
	}
	s.ClearDamage()

	s.Write([]byte("\x1b[31mh\x1b[39m"))
	s.Write([]byte("\x1b[38mello\x1b[39m\r \x1b[31mh\x1b[39m"))

	for y := range s.Frame.Height {
		for x := range s.Frame.Width {
			r := s.Frame.At(x, y).Rune
			if r == '\r' || r == '\n' {
				t.Fatalf("cell(%d,%d) contains control rune %q", x, y, r)
			}
		}
	}

	out, err := r.Draw(s.Frame, s.Damage())
	if err != nil {
		t.Fatal(err)
	}
	for _, b := range out {
		if b == '\r' || b == '\n' {
			t.Fatalf("incremental output contains raw control byte %q: %q", b, string(out))
		}
	}
}

func TestIntegrationScenarios(t *testing.T) {
	tests := []struct {
		name string
		run  func(t *testing.T)
	}{
		{
			name: "renderer stays in sync after VT edit damage",
			run: func(t *testing.T) {
				s := NewScreen(8, 2)
				r := renderer.New(renderer.Capabilities{})
				s.Write([]byte("abcdef"))
				if _, err := r.Draw(s.Frame, s.Damage()); err != nil {
					t.Fatal(err)
				}
				s.ClearDamage()

				s.Write([]byte("\x1b[1;3H\x1b[2@XY"))
				if _, err := r.Draw(s.Frame, s.Damage()); err != nil {
					t.Fatal(err)
				}
				s.ClearDamage()

				out, err := r.Draw(s.Frame, nil)
				if err != nil {
					t.Fatal(err)
				}
				if len(out) != 0 {
					t.Fatalf("renderer emitted stale follow-up output after VT edit damage: %q", string(out))
				}
			},
		},
		{
			name: "DCS sequence terminated by ST is ignored",
			run: func(t *testing.T) {
				s := NewScreen(20, 2)
				s.Write([]byte("\x1bP+q696e646e\x1b\\prompt"))

				assertCell(t, s, 0, 0, 'p')
				assertCell(t, s, 1, 0, 'r')
				assertCell(t, s, 2, 0, 'o')
				assertCell(t, s, 3, 0, 'm')
				assertCell(t, s, 4, 0, 'p')
				assertCell(t, s, 5, 0, 't')
				if s.Col != 6 || s.Row != 0 {
					t.Errorf("cursor at row=%d col=%d, want row=0 col=6", s.Row, s.Col)
				}
			},
		},
		{
			name: "fish-like prompt redraw preserves typed characters",
			run: func(t *testing.T) {
				s := NewScreen(10, 2)
				s.Write([]byte("> "))
				s.Write([]byte("a\r\x1b[3C"))
				s.Write([]byte("\x1b[91mb\x1b[39m\x1b[K\r\x1b[4C"))

				assertCell(t, s, 0, 0, '>')
				assertCell(t, s, 1, 0, ' ')
				assertCell(t, s, 2, 0, 'a')
				assertCell(t, s, 3, 0, 'b')
				if s.Row != 0 || s.Col != 4 {
					t.Errorf("cursor at row=%d col=%d, want row=0 col=4", s.Row, s.Col)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, tt.run)
	}
}
