package renderer

// Span identifies a changed horizontal range in a logical frame row.
type Span struct {
	Y     int
	X     int
	Width int
}

// Scroll identifies a safe full-width upward scroll.
type Scroll struct {
	Y      int
	Height int
	Count  int
}

// DeltaPlan describes how to advance a committed frame to the candidate frame.
// Snapshot requests replacement of the committed frame; Spans, when present,
// are the canonical ranges to encode before that replacement.
type DeltaPlan struct {
	Snapshot bool
	Scroll   Scroll
	Spans    []Span
}

// DeltaCandidate holds a plan and owns a source frame snapshot until Commit.
type DeltaCandidate struct {
	Plan  DeltaPlan
	frame Frame
}

// PlanDelta prepares a non-mutating update from committed to frame.
func PlanDelta(frame Frame, damage []Damage, committed Frame, reset bool) (DeltaCandidate, error) {
	if err := frame.Validate(); err != nil {
		return DeltaCandidate{}, err
	}

	candidate := DeltaCandidate{frame: frame.Clone()}
	if reset || frame.Width != committed.Width || frame.Height != committed.Height {
		candidate.Plan.Snapshot = true
		return candidate, nil
	}
	if err := committed.Validate(); err != nil {
		return DeltaCandidate{}, err
	}

	if len(damage) == 1 && (damage[0].Kind == DamageText || damage[0].Kind == DamageClear) {
		candidate.Plan = planSingleDamage(frame, damage[0])
		return candidate, nil
	}

	if needsFull(damage) {
		dirty, full := buildDirtyLinePlan(frame, committed)
		if len(dirty) > 0 || full {
			candidate.Plan.Snapshot = true
			candidate.Plan.Spans = dirty
		}
		return candidate, nil
	}

	var skip *Damage
	if scroll, ok := findSafeScroll(frame, damage); ok && canApplyScrollAgainst(frame, scroll, damage, committed) {
		candidate.Plan.Scroll = Scroll{Y: scroll.Y, Height: scroll.Height, Count: scroll.Count}
		skip = &scroll
	} else if hasScrollDamage(damage) {
		candidate.Plan.Snapshot = true
		return candidate, nil
	}

	var spans []Span
	var full bool
	if len(damage) == 0 {
		spans, full = buildDirtyLinePlan(frame, committed)
	} else {
		spans, full = buildDamagePlan(frame, damage, skip)
	}
	if full {
		candidate.Plan = DeltaPlan{Snapshot: true}
		return candidate, nil
	}
	if deltaCostsSnapshot(frame, spans, candidate.Plan.Scroll.Height != 0) {
		if candidate.Plan.Scroll.Height != 0 {
			candidate.Plan = DeltaPlan{Snapshot: true}
		} else {
			candidate.Plan.Snapshot = true
			candidate.Plan.Spans = spans
		}
		return candidate, nil
	}
	candidate.Plan.Spans = spans
	return candidate, nil
}

func planSingleDamage(frame Frame, d Damage) DeltaPlan {
	spans, full := buildSingleDamageSpans(frame, d)
	if full {
		return DeltaPlan{Snapshot: true}
	}
	plan := DeltaPlan{Spans: spans}
	if deltaCostsSnapshot(frame, spans, false) {
		plan.Snapshot = true
	}
	return plan
}

func buildDirtyLinePlan(frame, committed Frame) ([]Span, bool) {
	var spans []Span
	for y := range frame.Height {
		frameRow := frame.Row(y)
		committedRow := committed.Row(y)
		dirty := false
		for x := range frame.Width {
			if !frameRow[x].Equal(committedRow[x]) {
				dirty = true
				break
			}
		}
		if dirty {
			if len(spans) == maxPlannedDamageSpans {
				return nil, true
			}
			spans = append(spans, Span{Y: y, Width: frame.Width})
		}
	}
	return spans, false
}

func deltaCostsSnapshot(frame Frame, spans []Span, hasScroll bool) bool {
	if !hasScroll && frame.Height > 1 && len(spans) == 1 {
		return false
	}

	var deltaCost int64
	if hasScroll {
		deltaCost = 6
	}
	for _, span := range spans {
		deltaCost += 12 + int64(span.Width)
	}

	minimumSnapshotCost := int64(frame.Height) * (12 + int64(frame.Width))
	if deltaCost < minimumSnapshotCost {
		return false
	}
	maximumSnapshotCost := int64(frame.Height) * (8 + 5*int64(frame.Width))
	if deltaCost >= maximumSnapshotCost {
		return true
	}
	if !hasScroll && fullWidthRows(frame, spans) {
		return true
	}

	for _, span := range spans {
		deltaCost += 4 * int64(styleRunCount(frame.Row(span.Y)[span.X:span.X+span.Width])-1)
	}
	var snapshotCost int64
	for y := range frame.Height {
		snapshotCost += spanCost(frame, y, 0, frame.Width)
	}
	return deltaCost >= snapshotCost
}

func fullWidthRows(frame Frame, spans []Span) bool {
	if len(spans) != frame.Height {
		return false
	}
	for y, span := range spans {
		if span.Y != y || span.X != 0 || span.Width != frame.Width {
			return false
		}
	}
	return true
}

func spanCost(frame Frame, y, x, width int) int64 {
	return 8 + int64(width) + 4*int64(styleRunCount(frame.Row(y)[x:x+width]))
}

func styleRunCount(cells []Cell) int {
	if len(cells) == 0 {
		return 0
	}
	runs := 1
	for i := 1; i < len(cells); i++ {
		if cells[i-1].Style != cells[i].Style && !cells[i-1].Style.Equal(cells[i].Style) {
			runs++
		}
	}
	return runs
}

// Commit advances dst to the frame snapshot owned by the candidate.
func (c DeltaCandidate) Commit(dst *Frame) {
	if c.Plan.Snapshot || dst.Width != c.frame.Width || dst.Height != c.frame.Height {
		replaceFrame(dst, c.frame)
		return
	}
	if c.Plan.Scroll.Height != 0 {
		scroll := c.Plan.Scroll
		dst.ScrollUp(scroll.Y, scroll.Y+scroll.Height-1, scroll.Count)
	}
	for _, span := range c.Plan.Spans {
		dstStart := dst.lineOffset[span.Y] + span.X
		srcStart := c.frame.lineOffset[span.Y] + span.X
		copy(dst.Cells[dstStart:dstStart+span.Width], c.frame.Cells[srcStart:srcStart+span.Width])
	}
}

func replaceFrame(dst *Frame, src Frame) {
	sameDimensions := dst.Width == src.Width && dst.Height == src.Height
	cellCount := src.Width * src.Height
	if sameDimensions && cap(dst.Cells) >= cellCount {
		dst.Cells = dst.Cells[:cellCount]
	} else {
		dst.Cells = make([]Cell, cellCount)
	}
	if sameDimensions && cap(dst.lineOffset) >= src.Height {
		dst.lineOffset = dst.lineOffset[:src.Height]
	} else {
		dst.lineOffset = make([]int, src.Height)
	}
	dst.Width = src.Width
	dst.Height = src.Height
	for y := range src.Height {
		start := y * src.Width
		copy(dst.Cells[start:start+src.Width], src.Row(y))
		dst.lineOffset[y] = start
	}
}
