package renderer

import (
	"cmp"
	"slices"
)

const maxPlannedDamageSpans = 4096

// buildDamagePlan creates the bounded, canonical view of text and clear damage
// used for terminal emission. The source damage remains untouched because the
// scroll and shadow paths need its original structural information.
func buildDamagePlan(frame Frame, damage []Damage, skip *Damage) ([]Span, bool) {
	if len(damage) == 1 {
		d := damage[0]
		if skip != nil && sameDamage(d, *skip) {
			return nil, false
		}
		if d.Kind != DamageText && d.Kind != DamageClear {
			return nil, false
		}
		return buildSingleDamageSpans(frame, d)
	}

	spans := make([]Span, 0)
	for _, d := range damage {
		if skip != nil && sameDamage(d, *skip) {
			continue
		}
		if d.Kind != DamageText && d.Kind != DamageClear {
			continue
		}
		x, y, width, height, ok := clampRect(frame, d.X, d.Y, d.Width, d.Height)
		if !ok {
			continue
		}
		for row := y; row < y+height; row++ {
			if len(spans) == maxPlannedDamageSpans {
				return nil, true
			}
			spans = append(spans, Span{Y: row, X: x, Width: width})
		}
	}

	slices.SortFunc(spans, func(a, b Span) int {
		if c := cmp.Compare(a.Y, b.Y); c != 0 {
			return c
		}
		return cmp.Compare(a.X, b.X)
	})

	return mergeDamageSpans(spans), false
}

func buildSingleDamageSpans(frame Frame, d Damage) ([]Span, bool) {
	x, y, width, height, ok := clampRect(frame, d.X, d.Y, d.Width, d.Height)
	if !ok {
		return nil, false
	}
	if height > maxPlannedDamageSpans {
		return nil, true
	}
	spans := make([]Span, height)
	for row := range height {
		spans[row] = Span{Y: y + row, X: x, Width: width}
	}
	return spans, false
}

func mergeDamageSpans(spans []Span) []Span {
	if len(spans) < 2 {
		return spans
	}

	out := spans[:0]
	for _, span := range spans {
		if len(out) == 0 {
			out = append(out, span)
			continue
		}
		last := &out[len(out)-1]
		// Both endpoints are bounded by frame.Width after clampRect, so these
		// additions cannot overflow. The comparison includes adjacency.
		lastEnd := last.X + last.Width
		spanEnd := span.X + span.Width
		if span.Y != last.Y || span.X > lastEnd {
			out = append(out, span)
			continue
		}
		if spanEnd > lastEnd {
			last.Width = spanEnd - last.X
		}
	}
	return out
}
