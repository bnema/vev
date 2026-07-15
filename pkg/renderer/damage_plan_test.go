package renderer

import (
	"math"
	"reflect"
	"testing"
)

func TestBuildDamagePlan(t *testing.T) {
	tests := []struct {
		name   string
		frame  Frame
		damage []Damage
		want   []damageSpan
		full   bool
	}{
		{
			name:  "merges reverse overlaps",
			frame: NewFrame(10, 2),
			damage: []Damage{
				{Kind: DamageText, X: 4, Y: 0, Width: 5, Height: 1},
				{Kind: DamageText, X: 0, Y: 0, Width: 5, Height: 1},
			},
			want: []damageSpan{{y: 0, x: 0, width: 9}},
		},
		{
			name:  "merges text and clear",
			frame: NewFrame(10, 1),
			damage: []Damage{
				{Kind: DamageClear, X: 5, Y: 0, Width: 4, Height: 1},
				{Kind: DamageText, X: 0, Y: 0, Width: 5, Height: 1},
			},
			want: []damageSpan{{y: 0, x: 0, width: 9}},
		},
		{
			name:  "orders multi-row spans",
			frame: NewFrame(10, 3),
			damage: []Damage{
				{Kind: DamageText, X: 5, Y: 2, Width: 2, Height: 1},
				{Kind: DamageText, X: 4, Y: 0, Width: 2, Height: 2},
				{Kind: DamageText, X: 1, Y: 0, Width: 2, Height: 2},
				{Kind: DamageScrollUp, X: 0, Y: 0, Width: 10, Height: 3, Count: 1},
			},
			want: []damageSpan{
				{y: 0, x: 1, width: 2}, {y: 0, x: 4, width: 2},
				{y: 1, x: 1, width: 2}, {y: 1, x: 4, width: 2},
				{y: 2, x: 5, width: 2},
			},
		},
		{
			name:  "clamps rectangles",
			frame: NewFrame(10, 3),
			damage: []Damage{
				{Kind: DamageText, X: -2, Y: -1, Width: 5, Height: 3},
				{Kind: DamageClear, X: 8, Y: 1, Width: 5, Height: 3},
			},
			want: []damageSpan{{y: 0, x: 0, width: 3}, {y: 1, x: 0, width: 3}, {y: 1, x: 8, width: 2}, {y: 2, x: 8, width: 2}},
		},
		{
			name:  "does not mutate input",
			frame: NewFrame(10, 2),
			damage: []Damage{
				{Kind: DamageText, X: 5, Y: 1, Width: 2, Height: 1},
				{Kind: DamageText, X: 1, Y: 0, Width: 2, Height: 1},
			},
			want: []damageSpan{{y: 0, x: 1, width: 2}, {y: 1, x: 5, width: 2}},
		},
		{
			name:   "exceeding budget requests full redraw",
			frame:  NewFrame(1, maxPlannedDamageSpans+1),
			damage: []Damage{{Kind: DamageText, X: 0, Y: 0, Width: 1, Height: maxPlannedDamageSpans + 1}},
			want:   nil,
			full:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			original := append([]Damage(nil), tt.damage...)
			got, full := buildDamagePlan(tt.frame, tt.damage, nil)
			if full != tt.full || !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("buildDamagePlan() = %#v, full = %v; want %#v, %v", got, full, tt.want, tt.full)
			}
			if !reflect.DeepEqual(tt.damage, original) {
				t.Fatalf("buildDamagePlan mutated damage: got %#v, want %#v", tt.damage, original)
			}
		})
	}
}

func TestClampRectRejectsOverflowingBounds(t *testing.T) {
	frame := NewFrame(10, 10)
	if _, _, _, _, ok := clampRect(frame, math.MaxInt-1, 0, 4, 1); ok {
		t.Fatal("clampRect accepted overflowing x bound")
	}
	if _, _, _, _, ok := clampRect(frame, 0, math.MaxInt-1, 1, 4); ok {
		t.Fatal("clampRect accepted overflowing y bound")
	}
}

func TestSyncRectRejectsOverflowingBounds(t *testing.T) {
	r := New(Capabilities{})
	frame := NewFrame(2, 1)
	r.replaceShadow(frame)
	before := append([]Cell(nil), r.shadow...)
	r.syncRect(frame, math.MaxInt-1, 0, 4, 1)
	if !reflect.DeepEqual(r.shadow, before) {
		t.Fatal("syncRect changed shadow for overflowing bounds")
	}
}
