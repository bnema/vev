package renderer

import (
	"reflect"
	"testing"
)

func testFrame(rows ...string) Frame {
	frame := NewFrame(len([]rune(rows[0])), len(rows))
	fillFrame(frame, rows)
	return frame
}

func TestPlanDelta(t *testing.T) {
	scrollCommitted := testFrame("0000", "1111", "2222", "3333", "4444", "5555", "6666", "7777")
	scrolled := testFrame("0000", "2222", "3333", "4444", "5555", "6666", "new!", "7777")
	largeFrame := NewFrame(1, maxPlannedDamageSpans+1)
	styledCommitted := NewFrame(10, 1)
	styledCommitted.Set(3, 0, Cell{Rune: ' ', Style: Style{Bold: true, Foreground: -1, Background: -1}})
	styledFrame := styledCommitted.Clone()
	styledFrame.Set(0, 0, Cell{Rune: 'a', Style: DefaultStyle()})
	styledFrame.Set(9, 0, Cell{Rune: 'z', Style: DefaultStyle()})

	tests := []struct {
		name      string
		frame     Frame
		damage    []Damage
		committed Frame
		reset     bool
		want      DeltaPlan
	}{
		{
			name:      "reset snapshots",
			frame:     NewFrame(4, 2),
			committed: NewFrame(4, 2),
			reset:     true,
			want:      DeltaPlan{Snapshot: true},
		},
		{
			name:      "dimension mismatch snapshots",
			frame:     NewFrame(4, 2),
			committed: NewFrame(3, 2),
			want:      DeltaPlan{Snapshot: true},
		},
		{
			name:      "unchanged same size has no spans",
			frame:     NewFrame(4, 2),
			committed: NewFrame(4, 2),
			want:      DeltaPlan{},
		},
		{
			name:      "unchanged full redraw has no spans",
			frame:     NewFrame(4, 2),
			damage:    []Damage{FullRedraw()},
			committed: NewFrame(4, 2),
			want:      DeltaPlan{},
		},
		{
			name:      "changed row without damage becomes a full width span",
			frame:     testFrame("    ", "edit", "    "),
			committed: NewFrame(4, 3),
			want:      DeltaPlan{Spans: []Span{{Y: 1, X: 0, Width: 4}}},
		},
		{
			name:  "adjacent and overlapping damage is ordered and merged",
			frame: testFrame("          ", "aaaaaaaaa ", "          "),
			damage: []Damage{
				{Kind: DamageText, X: 4, Y: 1, Width: 5, Height: 1},
				{Kind: DamageClear, X: 0, Y: 1, Width: 5, Height: 1},
				{Kind: DamageText, X: 2, Y: 1, Width: 2, Height: 1},
			},
			committed: NewFrame(10, 3),
			want:      DeltaPlan{Spans: []Span{{Y: 1, X: 0, Width: 9}}},
		},
		{
			name:  "safe full width scroll retains exposed span",
			frame: scrolled,
			damage: []Damage{
				{Kind: DamageScrollUp, X: 0, Y: 1, Width: 4, Height: 6, Count: 1},
				{Kind: DamageText, X: 0, Y: 6, Width: 4, Height: 1},
			},
			committed: scrollCommitted,
			want: DeltaPlan{
				Scroll: Scroll{Y: 1, Height: 6, Count: 1},
				Spans:  []Span{{Y: 6, X: 0, Width: 4}},
			},
		},
		{
			name:  "rectangular scroll snapshots",
			frame: scrolled,
			damage: []Damage{
				{Kind: DamageScrollUp, X: 1, Y: 1, Width: 3, Height: 6, Count: 1},
			},
			committed: scrollCommitted,
			want:      DeltaPlan{Snapshot: true},
		},
		{
			name:  "unsafe scroll content snapshots",
			frame: testFrame("0000", "2222", "xxxx", "4444", "5555", "6666", "new!", "7777"),
			damage: []Damage{
				{Kind: DamageScrollUp, X: 0, Y: 1, Width: 4, Height: 6, Count: 1},
				{Kind: DamageText, X: 0, Y: 6, Width: 4, Height: 1},
			},
			committed: scrollCommitted,
			want:      DeltaPlan{Snapshot: true},
		},
		{
			name:      "more than 4096 canonical spans snapshots",
			frame:     largeFrame,
			damage:    []Damage{{Kind: DamageText, X: 0, Y: 0, Width: 1, Height: maxPlannedDamageSpans + 1}},
			committed: NewFrame(1, maxPlannedDamageSpans+1),
			want:      DeltaPlan{Snapshot: true},
		},
		{
			name:      "delta cost equal to snapshot cost snapshots",
			frame:     testFrame("changed!"),
			damage:    []Damage{{Kind: DamageText, X: 0, Y: 0, Width: 8, Height: 1}},
			committed: NewFrame(8, 1),
			want:      DeltaPlan{Snapshot: true},
		},
		{
			name:      "single broad damage snapshots completely",
			frame:     testFrame("aaaa", "bbbb"),
			damage:    []Damage{{Kind: DamageText, X: 0, Y: 0, Width: 4, Height: 2}},
			committed: NewFrame(4, 2),
			want:      DeltaPlan{Snapshot: true},
		},
		{
			name:  "snapshot cost includes style runs",
			frame: styledFrame,
			damage: []Damage{
				{Kind: DamageText, X: 0, Y: 0, Width: 1, Height: 1},
				{Kind: DamageText, X: 9, Y: 0, Width: 1, Height: 1},
			},
			committed: styledCommitted,
			want:      DeltaPlan{Spans: []Span{{Y: 0, X: 0, Width: 1}, {Y: 0, X: 9, Width: 1}}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			candidate, err := PlanDelta(tt.frame, tt.damage, tt.committed, tt.reset)
			if err != nil {
				t.Fatalf("PlanDelta() error = %v", err)
			}
			if !reflect.DeepEqual(candidate.Plan, tt.want) {
				t.Fatalf("PlanDelta() = %#v, want %#v", candidate.Plan, tt.want)
			}
		})
	}
}

func TestPlanDeltaDoesNotMutateCommitted(t *testing.T) {
	committed := testFrame("0000", "1111", "2222", "3333", "4444")
	committed.ScrollUp(1, 3, 1)
	beforeCells := append([]Cell(nil), committed.Cells...)
	beforeOffsets := append([]int(nil), committed.lineOffset...)
	frame := testFrame("0000", "2222", "3333", "new!", "4444")
	damage := []Damage{
		{Kind: DamageScrollUp, X: 0, Y: 1, Width: 4, Height: 3, Count: 1},
		{Kind: DamageText, X: 0, Y: 3, Width: 4, Height: 1},
	}

	if _, err := PlanDelta(frame, damage, committed, false); err != nil {
		t.Fatalf("PlanDelta() error = %v", err)
	}

	if !reflect.DeepEqual(committed.Cells, beforeCells) {
		t.Fatal("PlanDelta mutated committed cells")
	}
	if !reflect.DeepEqual(committed.lineOffset, beforeOffsets) {
		t.Fatal("PlanDelta mutated committed row offsets")
	}
}

func TestDeltaCandidateCommitCopiesLogicalRowsAndReusesCapacity(t *testing.T) {
	frame := testFrame("aaaa", "bbbb", "cccc", "dddd")
	frame.ScrollUp(0, 3, 1)
	fillFrameRow(frame, 3, "new!")
	committed := NewFrame(4, 4)
	cellsBase := &committed.Cells[0]

	candidate, err := PlanDelta(frame, nil, committed, true)
	if err != nil {
		t.Fatalf("PlanDelta() error = %v", err)
	}
	candidate.Commit(&committed)

	if &committed.Cells[0] != cellsBase {
		t.Fatal("Commit replaced same-sized cell storage")
	}
	for y := range frame.Height {
		if !reflect.DeepEqual(committed.Row(y), frame.Row(y)) {
			t.Fatalf("committed row %d = %#v, want %#v", y, committed.Row(y), frame.Row(y))
		}
	}
	if err := committed.CheckInvariants(); err != nil {
		t.Fatalf("committed frame invariants: %v", err)
	}
}

func TestPlanDeltaRejectsInvalidFrame(t *testing.T) {
	frame := NewFrame(2, 1)
	frame.Cells = nil
	if _, err := PlanDelta(frame, nil, Frame{}, false); err == nil {
		t.Fatal("PlanDelta accepted an invalid frame")
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

func TestSnapshotCommitCopiesCellsOutsideDamage(t *testing.T) {
	frame := NewFrame(2, maxPlannedDamageSpans+1)
	frame.Set(1, 0, Cell{Rune: 'x', Style: DefaultStyle()})
	committed := NewFrame(2, maxPlannedDamageSpans+1)
	damage := []Damage{{Kind: DamageText, X: 0, Y: 0, Width: 1, Height: maxPlannedDamageSpans + 1}}

	candidate, err := PlanDelta(frame, damage, committed, false)
	if err != nil {
		t.Fatalf("PlanDelta() error = %v", err)
	}
	if !candidate.Plan.Snapshot || len(candidate.Plan.Spans) != 0 {
		t.Fatalf("snapshot plan = %#v, want complete snapshot without partial spans", candidate.Plan)
	}
	candidate.Commit(&committed)
	if got := committed.At(1, 0); !got.Equal(frame.At(1, 0)) {
		t.Fatalf("committed cell = %#v, want %#v", got, frame.At(1, 0))
	}
}

func TestDeltaCandidateCommitAppliesScrollToLogicalRows(t *testing.T) {
	committed := testFrame("0000", "1111", "2222", "3333", "4444", "5555", "6666", "7777")
	frame := committed.Clone()
	frame.ScrollUp(1, 6, 1)
	fillFrameRow(frame, 6, "new!")
	damage := []Damage{
		{Kind: DamageScrollUp, X: 0, Y: 1, Width: 4, Height: 6, Count: 1},
		{Kind: DamageText, X: 0, Y: 6, Width: 4, Height: 1},
	}

	candidate, err := PlanDelta(frame, damage, committed, false)
	if err != nil {
		t.Fatalf("PlanDelta() error = %v", err)
	}
	if candidate.Plan.Scroll.Height == 0 {
		t.Fatal("PlanDelta dropped safe scroll")
	}
	candidate.Commit(&committed)

	for y := range frame.Height {
		if !reflect.DeepEqual(committed.Row(y), frame.Row(y)) {
			t.Fatalf("committed row %d = %#v, want %#v", y, committed.Row(y), frame.Row(y))
		}
	}
	if err := committed.CheckInvariants(); err != nil {
		t.Fatalf("committed frame invariants: %v", err)
	}
}

func fillFrameRow(frame Frame, y int, row string) {
	for x, r := range row {
		frame.Set(x, y, Cell{Rune: r, Style: DefaultStyle()})
	}
}
