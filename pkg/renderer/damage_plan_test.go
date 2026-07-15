package renderer

import (
	"math"
	"reflect"
	"testing"
)

func TestBuildDamagePlanMergesReverseOverlaps(t *testing.T) {
	frame := NewFrame(10, 2)
	got, full := buildDamagePlan(frame, []Damage{
		{Kind: DamageText, X: 4, Y: 0, Width: 5, Height: 1},
		{Kind: DamageText, X: 0, Y: 0, Width: 5, Height: 1},
	}, nil)
	want := []damageSpan{{y: 0, x: 0, width: 9}}
	if full || !reflect.DeepEqual(got, want) {
		t.Fatalf("buildDamagePlan() = %#v, full = %v; want %#v, false", got, full, want)
	}
}

func TestBuildDamagePlanMergesTextAndClear(t *testing.T) {
	frame := NewFrame(10, 1)
	got, full := buildDamagePlan(frame, []Damage{
		{Kind: DamageClear, X: 5, Y: 0, Width: 4, Height: 1},
		{Kind: DamageText, X: 0, Y: 0, Width: 5, Height: 1},
	}, nil)
	want := []damageSpan{{y: 0, x: 0, width: 9}}
	if full || !reflect.DeepEqual(got, want) {
		t.Fatalf("buildDamagePlan() = %#v, full = %v; want %#v, false", got, full, want)
	}
}

func TestBuildDamagePlanOrdersMultiRowSpans(t *testing.T) {
	frame := NewFrame(10, 3)
	got, full := buildDamagePlan(frame, []Damage{
		{Kind: DamageText, X: 5, Y: 2, Width: 2, Height: 1},
		{Kind: DamageText, X: 4, Y: 0, Width: 2, Height: 2},
		{Kind: DamageText, X: 1, Y: 0, Width: 2, Height: 2},
		{Kind: DamageScrollUp, X: 0, Y: 0, Width: 10, Height: 3, Count: 1},
	}, nil)
	want := []damageSpan{
		{y: 0, x: 1, width: 2}, {y: 0, x: 4, width: 2},
		{y: 1, x: 1, width: 2}, {y: 1, x: 4, width: 2},
		{y: 2, x: 5, width: 2},
	}
	if full || !reflect.DeepEqual(got, want) {
		t.Fatalf("buildDamagePlan() = %#v, full = %v; want %#v, false", got, full, want)
	}
}

func TestBuildDamagePlanClampsRectangles(t *testing.T) {
	frame := NewFrame(10, 3)
	got, full := buildDamagePlan(frame, []Damage{
		{Kind: DamageText, X: -2, Y: -1, Width: 5, Height: 3},
		{Kind: DamageClear, X: 8, Y: 1, Width: 5, Height: 3},
	}, nil)
	want := []damageSpan{{y: 0, x: 0, width: 3}, {y: 1, x: 0, width: 3}, {y: 1, x: 8, width: 2}, {y: 2, x: 8, width: 2}}
	if full || !reflect.DeepEqual(got, want) {
		t.Fatalf("buildDamagePlan() = %#v, full = %v; want %#v, false", got, full, want)
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

func TestBuildDamagePlanDoesNotMutateInput(t *testing.T) {
	frame := NewFrame(10, 2)
	damage := []Damage{
		{Kind: DamageText, X: 5, Y: 1, Width: 2, Height: 1},
		{Kind: DamageText, X: 1, Y: 0, Width: 2, Height: 1},
	}
	original := append([]Damage(nil), damage...)
	_, _ = buildDamagePlan(frame, damage, nil)
	if !reflect.DeepEqual(damage, original) {
		t.Fatalf("buildDamagePlan mutated damage: got %#v, want %#v", damage, original)
	}
}

func TestBuildDamagePlanExceedingBudgetRequestsFullRedraw(t *testing.T) {
	frame := NewFrame(1, maxPlannedDamageSpans+1)
	got, full := buildDamagePlan(frame, []Damage{{Kind: DamageText, X: 0, Y: 0, Width: 1, Height: frame.Height}}, nil)
	if !full || got != nil {
		t.Fatalf("buildDamagePlan() = %#v, full = %v; want nil, true", got, full)
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
