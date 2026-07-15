package vt

import (
	"testing"

	"github.com/bnema/vev/pkg/renderer"
	"github.com/stretchr/testify/require"
)

func TestScrollDamage(t *testing.T) {
	tests := []struct {
		name string
		run  func(t *testing.T)
	}{
		{
			name: "scroll produces both DamageScrollUp and DamageText",
			run: func(t *testing.T) {
				s := NewScreen(5, 2)
				// Fill screen then cause a scroll.
				s.Write([]byte("AAAAABBBBB")) // both rows filled
				s.ClearDamage()

				s.Write([]byte("CCCCC")) // forces a scroll up

				d := s.Damage()
				if len(d) == 0 {
					t.Fatal("expected damage after scroll")
				}

				if !hasDamageKind(d, renderer.DamageScrollUp) {
					t.Errorf("expected DamageScrollUp in %v", damageKinds(d))
				}
				if !hasDamageKind(d, renderer.DamageText) {
					t.Errorf("expected DamageText in %v", damageKinds(d))
				}
			},
		},
		{
			name: "scroll damage coordinates match the full-width scrolled region",
			run: func(t *testing.T) {
				s := NewScreen(5, 3)
				s.Write([]byte("AAAAABBBBBCCCCC")) // fill all 3 rows
				s.ClearDamage()

				// Write another char to force scroll.
				s.Write([]byte("D"))

				d := s.Damage()
				var scrollDamage *renderer.Damage
				for i, dd := range d {
					if dd.Kind == renderer.DamageScrollUp {
						scrollDamage = &d[i]
						break
					}
				}
				if scrollDamage == nil {
					t.Fatal("expected scroll damage")
				}
				if scrollDamage.X != 0 || scrollDamage.Y != 0 {
					t.Errorf("scroll position: (%d,%d), want (0,0)", scrollDamage.X, scrollDamage.Y)
				}
				if scrollDamage.Width != 5 || scrollDamage.Height != 3 {
					t.Errorf("scroll size: %dx%d, want 5x3", scrollDamage.Width, scrollDamage.Height)
				}
				if scrollDamage.Count != 1 {
					t.Errorf("scroll count = %d, want 1", scrollDamage.Count)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, tt.run)
	}
}

func TestDamageCoalescingBehavior(t *testing.T) {
	tests := []struct {
		name string
		run  func(t *testing.T)
	}{
		{
			name: "adjacent single-cell writes coalesce into one damage entry",
			run: func(t *testing.T) {
				s := NewScreen(20, 3)
				s.Write([]byte("ABC")) // Three adjacent chars on one line

				d := s.Damage()
				// Should be coalesced into a single DamageText.
				if len(d) != 1 {
					t.Fatalf("expected 1 coalesced damage, got %d: %+v", len(d), d)
				}
				if d[0].Kind != renderer.DamageText || d[0].X != 0 || d[0].Y != 0 || d[0].Width != 3 || d[0].Height != 1 {
					t.Errorf("unexpected coalesced damage: %+v", d[0])
				}
			},
		},
		{
			name: "newline breaks coalescing across lines",
			run: func(t *testing.T) {
				s := NewScreen(10, 3)
				s.Write([]byte("AB\nCD"))

				d := s.Damage()
				// "AB" on line 0, "CD" on line 1 → two separate DamageText items.
				textCount := 0
				for _, dd := range d {
					if dd.Kind == renderer.DamageText {
						textCount++
					}
				}
				if textCount < 2 {
					t.Errorf("expected at least 2 DamageText items (one per line), got %d", textCount)
				}
			},
		},
		{
			name: "writing replaces the initial FullRedraw with text damage",
			run: func(t *testing.T) {
				s := NewScreen(5, 3)
				// NewScreen already has FullRedraw in damage.
				d := s.Damage()
				if len(d) != 1 || d[0].Kind != renderer.DamageFullRedraw {
					t.Fatalf("expected single FullRedraw, got %+v", d)
				}

				// Writing should replace it with text damage.
				s.Write([]byte("X"))
				d = s.Damage()
				if len(d) == 0 {
					t.Fatal("expected damage after write")
				}
				if d[0].Kind == renderer.DamageFullRedraw {
					t.Error("FullRedraw should have been replaced by text damage")
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, tt.run)
	}
}

func TestDamageSaturationBoundsPendingRecords(t *testing.T) {
	s := NewScreen(2, 2)
	s.ClearDamage()

	for i := range 1_100 {
		s.record(renderer.Damage{Kind: renderer.DamageText, X: 0, Y: i % 2, Width: 1, Height: 1, Count: 1})
	}
	require.Equal(t, []renderer.Damage{renderer.FullRedraw()}, s.Damage())

	// Saturation is sticky until the render owner acknowledges or clears it.
	s.record(renderer.Damage{Kind: renderer.DamageText, X: 0, Y: 0, Width: 1, Height: 1, Count: 1})
	require.Equal(t, []renderer.Damage{renderer.FullRedraw()}, s.Damage())

	s.ClearDamage()
	s.record(renderer.Damage{Kind: renderer.DamageText, X: 0, Y: 0, Width: 1, Height: 1, Count: 1})
	require.Equal(t, []renderer.Damage{{Kind: renderer.DamageText, X: 0, Y: 0, Width: 1, Height: 1, Count: 1}}, s.Damage())
}

func TestClearDamage(t *testing.T) {
	tests := []struct {
		name string
	}{
		{name: "ClearDamage empties the damage list"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := NewScreen(5, 3)
			s.Write([]byte("Hello"))
			s.ClearDamage()
			d := s.Damage()
			if len(d) != 0 {
				t.Fatalf("expected empty damage after ClearDamage, got %+v", d)
			}
		})
	}
}

func TestDamageReturnsReference(t *testing.T) {
	tests := []struct {
		name string
	}{
		{name: "Damage() returns a reference to the internal slice"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Damage() should return a reference to internal slice (no copy).
			s := NewScreen(10, 3)
			s.Write([]byte("X"))
			d1 := s.Damage()
			// Internal slice should be the same (or at least reference the same backing).
			if len(d1) == 0 {
				t.Fatal("expected damage")
			}
			// Write more to trigger record.
			s.Write([]byte("Y"))
			d2 := s.Damage()
			_ = d2
			// Both d1 and d2 reference the internal slice; we just don't crash.
		})
	}
}

func TestDamageCaptureAcknowledgementPreservesConcurrentWrite(t *testing.T) {
	s := NewScreen(8, 2)
	s.ClearDamage()
	s.Write([]byte("a"))

	captured := s.CaptureDamage()
	require.NotEmpty(t, captured.Damage)
	s.Write([]byte("b"))

	require.False(t, s.AcknowledgeDamage(captured.Generation))
	require.Equal(t, []renderer.Damage{renderer.FullRedraw()}, s.Damage(), "stale acknowledgement must retain later writes conservatively")
}

func TestDamageCaptureAcknowledgementClearsMatchingGeneration(t *testing.T) {
	s := NewScreen(8, 2)
	s.ClearDamage()
	s.Write([]byte("a"))

	captured := s.CaptureDamage()
	require.True(t, s.AcknowledgeDamage(captured.Generation))
	require.Empty(t, s.Damage())
}

func TestDamageCaptureOwnsDamageSlice(t *testing.T) {
	s := NewScreen(8, 2)
	s.ClearDamage()
	s.Write([]byte("a"))

	captured := s.CaptureDamage()
	s.ClearDamage()
	require.NotEmpty(t, captured.Damage, "captured damage must not alias mutable screen storage")
}
