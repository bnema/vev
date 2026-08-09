package vt

import renderer "github.com/bnema/vev/pkg/vtcore"

// maxPendingDamage bounds metadata retained while no render transaction can
// acknowledge a screen. Saturation falls back to one exact full redraw.
const maxPendingDamage = 1024

// DamageCapture is an immutable copy of pending damage at one screen generation.
type DamageCapture struct {
	Damage     []renderer.Damage
	Generation uint64
}

// Damage returns the current damage list. The caller must not modify the
// returned slice; ClearDamage must be called after the damage is consumed.
func (s *Screen) Damage() []renderer.Damage { return s.damage }
func (s *Screen) ClearDamage() {
	s.damage = s.damage[:0]
	s.damageSaturated = false
	s.damageFullRedrawSticky = false
}

// CaptureDamage snapshots pending damage without consuming it.
func (s *Screen) CaptureDamage() DamageCapture {
	return DamageCapture{Damage: append([]renderer.Damage(nil), s.damage...), Generation: s.damageGeneration}
}

// AcknowledgeDamage consumes a capture only if no screen mutation occurred
// since it was taken. A stale acknowledgement conservatively requests a full
// redraw, ensuring intervening writes remain visible to the next capture.
func (s *Screen) AcknowledgeDamage(generation uint64) bool {
	if generation != s.damageGeneration {
		s.fullRedraw()
		return false
	}
	s.ClearDamage()
	return true
}

func (s *Screen) record(d renderer.Damage) {
	s.damageGeneration++
	if s.damageSaturated || s.damageFullRedrawSticky {
		return
	}
	if len(s.damage) >= maxPendingDamage {
		s.damage = []renderer.Damage{renderer.FullRedraw()}
		s.damageSaturated = true
		return
	}
	// Replace FullRedraw with the first concrete damage item.
	if len(s.damage) == 1 && s.damage[0].Kind == renderer.DamageFullRedraw {
		s.damage[0] = d
		return
	}
	// Coalesce adjacent single-cell text damage on the same line.
	if d.Kind == renderer.DamageText && d.Width == 1 && d.Height == 1 && len(s.damage) > 0 {
		last := &s.damage[len(s.damage)-1]
		if last.Kind == renderer.DamageText && last.Y == d.Y && last.X+last.Width == d.X && last.Height == 1 {
			last.Width++
			return
		}
	}
	s.damage = append(s.damage, d)
}

func (s *Screen) fullRedraw() {
	s.damageGeneration++
	s.damage = []renderer.Damage{renderer.FullRedraw()}
	// A structural frame replacement (such as DEC 1049 exit) cannot be
	// represented by the following text damage alone. Retain the redraw until
	// the render owner acknowledges this screen generation.
	s.damageFullRedrawSticky = true
}
