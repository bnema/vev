package renderer

import (
	"cmp"
	"slices"
)

const maxPlannedDamageSpans = 4096

type damageSpan struct {
	y, x, width int
}

// buildDamagePlan creates the bounded, canonical view of text and clear damage
// used for terminal emission. The source damage remains untouched because the
// scroll and shadow paths need its original structural information.
func buildDamagePlan(frame Frame, damage []Damage, skip *Damage) ([]damageSpan, bool) {
	spans := make([]damageSpan, 0)
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
			spans = append(spans, damageSpan{y: row, x: x, width: width})
		}
	}

	slices.SortFunc(spans, func(a, b damageSpan) int {
		if c := cmp.Compare(a.y, b.y); c != 0 {
			return c
		}
		return cmp.Compare(a.x, b.x)
	})

	return mergeDamageSpans(spans), false
}

func mergeDamageSpans(spans []damageSpan) []damageSpan {
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
		lastEnd := last.x + last.width
		spanEnd := span.x + span.width
		if span.y != last.y || span.x > lastEnd {
			out = append(out, span)
			continue
		}
		if spanEnd > lastEnd {
			last.width = spanEnd - last.x
		}
	}
	return out
}
