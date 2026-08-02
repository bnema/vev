package daemon

import (
	"reflect"
	"testing"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/pkg/renderer"
)

func TestProxyScreenStateApplyIsAtomicAndReplaysScrolls(t *testing.T) {
	s := newProxyScreenState(domain.Size{Cols: 4, Rows: 3})
	if err := s.Apply(screenSnapshot(4, 3, "abcd", "efgh", "ijkl")); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(s.frame, s.scratch) {
		t.Fatal("live and scratch frames diverged after snapshot")
	}
	applyProxyDelta(t, s, ports.ScreenUpdate{
		Size:   domain.Size{Cols: 4, Rows: 3},
		Scroll: &ports.ScreenScroll{Top: 0, Height: 3, Count: 1},
		Spans:  []ports.ScreenSpan{{Y: 2, Cells: cells("WXYZ")}},
	})
	for range 7 {
		applyProxyDelta(t, s, ports.ScreenUpdate{
			Size:   domain.Size{Cols: 4, Rows: 3},
			Scroll: &ports.ScreenScroll{Top: 0, Height: 3, Count: 1},
			Spans:  []ports.ScreenSpan{{Y: 2, Cells: cells("WXYZ")}},
		})
	}
	if !reflect.DeepEqual(s.frame, s.scratch) {
		t.Fatal("live and scratch frames diverged after repeated scrolls")
	}
	if got := proxyFrameText(s.frame); got != "WXYZWXYZWXYZ" {
		t.Fatalf("replayed scroll state = %q", got)
	}
}

func TestProxyScreenStateInvalidUpdateIsAtomic(t *testing.T) {
	s := newProxyScreenState(domain.Size{Cols: 4, Rows: 2})
	if err := s.Apply(screenSnapshot(4, 2, "abcd", "efgh")); err != nil {
		t.Fatal(err)
	}
	beforeFrame, beforeScratch := s.frame.Clone(), s.scratch.Clone()
	beforeCursor, beforeGeneration, beforeState := s.cursorOut, s.generation, s.stateNum
	invalid := ports.ScreenUpdate{
		Kind:         ports.ScreenUpdateDelta,
		BaseStateNum: 1,
		NewStateNum:  2,
		Size:         domain.Size{Cols: 4, Rows: 2},
		Scroll:       &ports.ScreenScroll{Top: 1, Height: 2, Count: 1},
		Spans:        []ports.ScreenSpan{{Y: 2, Cells: cells("x")}},
		Cursor:       ports.ScreenCursor{Row: 1, Col: 1, Visible: true},
	}
	if err := s.Apply(invalid); err == nil {
		t.Fatal("invalid geometry was accepted")
	}
	if !reflect.DeepEqual(s.frame, beforeFrame) || !reflect.DeepEqual(s.scratch, beforeScratch) {
		t.Fatal("invalid update mutated a frame")
	}
	if s.cursorOut != beforeCursor || s.generation != beforeGeneration || s.stateNum != beforeState {
		t.Fatal("invalid update mutated metadata")
	}

	invalid = ports.ScreenUpdate{
		Kind:         ports.ScreenUpdateDelta,
		BaseStateNum: 1,
		NewStateNum:  2,
		Size:         domain.Size{Cols: 4, Rows: 2},
		Scroll:       &ports.ScreenScroll{Top: 0, Height: 2, Count: 2},
		Cursor:       ports.ScreenCursor{Row: 1, Col: 1, Visible: true},
	}
	if err := s.Apply(invalid); err == nil {
		t.Fatal("full-height scroll was accepted")
	}
	if !reflect.DeepEqual(s.frame, beforeFrame) || !reflect.DeepEqual(s.scratch, beforeScratch) || s.cursorOut != beforeCursor || s.generation != beforeGeneration || s.stateNum != beforeState {
		t.Fatal("invalid full-height scroll mutated state")
	}
}

func TestProxyScreenStateScreenAreaLimitIsAtomic(t *testing.T) {
	const boundaryCols = 512
	boundaryRows := maxProxyScreenCells / boundaryCols
	boundary := newProxyScreenState(domain.Size{Cols: boundaryCols, Rows: boundaryRows})
	if boundary.frame.Width != boundaryCols || boundary.frame.Height != boundaryRows || boundary.scratch.Width != boundaryCols || boundary.scratch.Height != boundaryRows {
		t.Fatalf("boundary state dimensions = %dx%d and %dx%d", boundary.frame.Width, boundary.frame.Height, boundary.scratch.Width, boundary.scratch.Height)
	}
	if boundary.ResizePlaceholder(domain.Size{Cols: boundaryCols, Rows: boundaryRows + 1}) {
		t.Fatal("over-cap resize was accepted")
	}
	if boundary.frame.Width != boundaryCols || boundary.frame.Height != boundaryRows || boundary.scratch.Width != boundaryCols || boundary.scratch.Height != boundaryRows {
		t.Fatal("over-cap resize changed boundary state")
	}
	if !boundary.damageFullRedrawSticky {
		t.Fatal("initial full redraw must remain sticky until acknowledged")
	}

	s := newProxyScreenState(domain.Size{Cols: 2, Rows: 2})
	if err := s.Apply(screenSnapshot(2, 2, "ab", "cd")); err != nil {
		t.Fatal(err)
	}
	beforeFrame, beforeScratch := s.frame.Clone(), s.scratch.Clone()
	beforeCursor, beforeGeneration, beforeState, beforeDamage := s.cursorOut, s.generation, s.stateNum, s.CaptureDamage()
	if err := s.Apply(ports.ScreenUpdate{
		NewStateNum: 2,
		Kind:        ports.ScreenUpdateSnapshot,
		Size:        domain.Size{Cols: 513, Rows: 512},
		Cursor:      ports.ScreenCursor{Visible: true},
	}); err == nil {
		t.Fatal("over-cap snapshot was accepted")
	}
	if !reflect.DeepEqual(s.frame, beforeFrame) || !reflect.DeepEqual(s.scratch, beforeScratch) || s.cursorOut != beforeCursor || s.generation != beforeGeneration || s.stateNum != beforeState || !reflect.DeepEqual(s.CaptureDamage(), beforeDamage) {
		t.Fatal("over-cap snapshot mutated state")
	}
	if got := newProxyScreenState(domain.Size{Cols: 513, Rows: 512}); got.frame.Validate() == nil || got.scratch.Validate() == nil {
		t.Fatal("over-cap constructor allocated a valid frame")
	}
}

func TestProxyScreenStateRejectsInvalidStateNumbersAtomically(t *testing.T) {
	s := newProxyScreenState(domain.Size{Cols: 2, Rows: 2})
	if err := s.Apply(screenSnapshot(2, 2, "ab", "cd")); err != nil {
		t.Fatal(err)
	}
	if got := proxyFrameText(s.frame); got != "abcd" || !reflect.DeepEqual(s.frame, s.scratch) {
		t.Fatalf("2x2 snapshot = %q, frames equal=%v", got, reflect.DeepEqual(s.frame, s.scratch))
	}
	updates := []ports.ScreenUpdate{
		func() ports.ScreenUpdate {
			m := screenSnapshot(2, 2, "ef", "gh")
			m.NewStateNum = 0
			return m
		}(),
		func() ports.ScreenUpdate {
			m := screenSnapshot(2, 2, "ef", "gh")
			m.BaseStateNum, m.NewStateNum = 1, 2
			return m
		}(),
		{Kind: ports.ScreenUpdateDelta, BaseStateNum: 0, NewStateNum: 0, Size: domain.Size{Cols: 2, Rows: 2}},
		{Kind: ports.ScreenUpdateDelta, BaseStateNum: 1, NewStateNum: 3, Size: domain.Size{Cols: 2, Rows: 2}},
		{Kind: ports.ScreenUpdateDelta, BaseStateNum: 2, NewStateNum: 3, Size: domain.Size{Cols: 2, Rows: 2}},
	}
	for i, update := range updates {
		beforeFrame, beforeScratch := s.frame.Clone(), s.scratch.Clone()
		beforeCursor, beforeGeneration, beforeState := s.cursorOut, s.generation, s.stateNum
		if err := s.Apply(update); err == nil {
			t.Fatalf("invalid state update %d was accepted", i)
		}
		if !reflect.DeepEqual(s.frame, beforeFrame) || !reflect.DeepEqual(s.scratch, beforeScratch) || s.cursorOut != beforeCursor || s.generation != beforeGeneration || s.stateNum != beforeState {
			t.Fatalf("invalid state update %d mutated live state", i)
		}
	}
}

func TestProxyScreenStateRejectsWireInvalidSemanticUpdatesAtomically(t *testing.T) {
	s := newProxyScreenState(domain.Size{Cols: 2, Rows: 2})
	if err := s.Apply(screenSnapshot(2, 2, "ab", "cd")); err != nil {
		t.Fatal(err)
	}
	assertUnchanged := func(beforeFrame, beforeScratch renderer.Frame, beforeCursor ports.ScreenCursor, beforeGeneration, beforeState uint64, beforeDamage proxyScreenDamageCapture) {
		t.Helper()
		if !reflect.DeepEqual(s.frame, beforeFrame) || !reflect.DeepEqual(s.scratch, beforeScratch) || s.cursorOut != beforeCursor || s.generation != beforeGeneration || s.stateNum != beforeState || !reflect.DeepEqual(s.CaptureDamage(), beforeDamage) {
			t.Fatal("invalid semantic update mutated live state")
		}
	}

	beforeFrame, beforeScratch := s.frame.Clone(), s.scratch.Clone()
	beforeCursor, beforeGeneration, beforeState := s.cursorOut, s.generation, s.stateNum
	beforeDamage := s.CaptureDamage()
	invalidCursor := nextProxyDelta(s, ports.ScreenUpdate{
		Size:   domain.Size{Cols: 2, Rows: 2},
		Cursor: ports.ScreenCursor{Style: 7, StyleSet: true},
	})
	if err := s.Apply(invalidCursor); err == nil {
		t.Fatal("cursor style 7 was accepted")
	}
	assertUnchanged(beforeFrame, beforeScratch, beforeCursor, beforeGeneration, beforeState, beforeDamage)

	beforeFrame, beforeScratch = s.frame.Clone(), s.scratch.Clone()
	beforeCursor, beforeGeneration, beforeState = s.cursorOut, s.generation, s.stateNum
	beforeDamage = s.CaptureDamage()
	invalidSpans := nextProxyDelta(s, ports.ScreenUpdate{
		Size:  domain.Size{Cols: 2, Rows: 2},
		Spans: make([]ports.ScreenSpan, maxProxyScreenSpans+1),
	})
	if err := s.Apply(invalidSpans); err == nil {
		t.Fatal("too many spans were accepted")
	}
	assertUnchanged(beforeFrame, beforeScratch, beforeCursor, beforeGeneration, beforeState, beforeDamage)
}

func TestProxyScreenStateAcceptsConsecutiveHorizontalSpans(t *testing.T) {
	s := newProxyScreenState(domain.Size{Cols: 4, Rows: 1})
	if err := s.Apply(screenSnapshot(4, 1, "abcd")); err != nil {
		t.Fatal(err)
	}
	applyProxyDelta(t, s, ports.ScreenUpdate{
		Size: domain.Size{Cols: 4, Rows: 1},
		Spans: []ports.ScreenSpan{
			{Y: 0, X: 0, Cells: cells("xy")},
			{Y: 0, X: 2, Cells: cells("Zq")},
		},
	})
	if got := proxyFrameText(s.frame); got != "xyZq" || !reflect.DeepEqual(s.frame, s.scratch) {
		t.Fatalf("adjacent spans replay = %q, frames equal=%v", got, reflect.DeepEqual(s.frame, s.scratch))
	}
}

func TestProxyScreenStateDamageGenerationAndCapture(t *testing.T) {
	s := newProxyScreenState(domain.Size{Cols: 3, Rows: 2})
	if err := s.Apply(screenSnapshot(3, 2, "   ", "   ")); err != nil {
		t.Fatal(err)
	}
	first := s.CaptureDamage()
	if len(first.Damage) != 1 || first.Damage[0].Kind != renderer.DamageFullRedraw {
		t.Fatalf("initial damage = %+v", first.Damage)
	}
	if !s.AcknowledgeDamage(first.Generation) {
		t.Fatal("exact initial acknowledgement failed")
	}
	if len(s.CaptureDamage().Damage) != 0 {
		t.Fatal("exact acknowledgement left damage")
	}
	applyProxyDelta(t, s, ports.ScreenUpdate{
		Size:  domain.Size{Cols: 3, Rows: 2},
		Spans: []ports.ScreenSpan{{Y: 1, Cells: cells("xyz")}},
	})
	captured := s.CaptureDamage()
	if len(captured.Damage) != 1 || captured.Damage[0].Width != 3 {
		t.Fatalf("span damage = %+v", captured.Damage)
	}
	if s.AcknowledgeDamage(captured.Generation - 1) {
		t.Fatal("stale acknowledgement succeeded")
	}
	forced := s.CaptureDamage()
	if len(forced.Damage) != 1 || forced.Damage[0].Kind != renderer.DamageFullRedraw {
		t.Fatalf("stale acknowledgement damage = %+v", forced.Damage)
	}
	if !s.AcknowledgeDamage(forced.Generation) || len(s.CaptureDamage().Damage) != 0 {
		t.Fatal("full redraw did not clear on exact acknowledgement")
	}
}

func TestProxyScreenStateCaptureAndResize(t *testing.T) {
	s := newProxyScreenState(domain.Size{Cols: 4, Rows: 2})
	if err := s.Apply(screenSnapshot(4, 2, "abcd", "efgh")); err != nil {
		t.Fatal(err)
	}
	_ = s.AcknowledgeDamage(s.CaptureDamage().Generation)
	var dst renderer.Frame
	s.CaptureInto(&dst)
	if got := proxyFrameText(dst); got != "abcdefgh" {
		t.Fatalf("captured frame = %q", got)
	}
	applyProxyDelta(t, s, ports.ScreenUpdate{
		Size:  domain.Size{Cols: 4, Rows: 2},
		Spans: []ports.ScreenSpan{{Y: 0, X: 1, Cells: cells("Z")}},
	})
	s.CaptureInto(&dst)
	if got := proxyFrameText(dst); got != "aZcdefgh" {
		t.Fatalf("incrementally captured frame = %q", got)
	}
	if !s.ResizePlaceholder(domain.Size{Cols: 3, Rows: 3}) {
		t.Fatal("resize placeholder rejected valid size")
	}
	if got := proxyFrameText(s.frame); got != "aZcefg   " {
		t.Fatalf("overlap-preserving resize = %q", got)
	}
	capture := s.CaptureDamage()
	if len(capture.Damage) != 1 || capture.Damage[0].Kind != renderer.DamageFullRedraw {
		t.Fatalf("resize damage = %+v", capture.Damage)
	}
}

func TestProxyScreenStateCaptureFallsBackAfterSpanThenScroll(t *testing.T) {
	s := newProxyScreenState(domain.Size{Cols: 4, Rows: 2})
	if err := s.Apply(screenSnapshot(4, 2, "abcd", "efgh")); err != nil {
		t.Fatal(err)
	}
	_ = s.AcknowledgeDamage(s.CaptureDamage().Generation)
	dst := s.frame.Clone()
	applyProxyDelta(t, s, ports.ScreenUpdate{
		Size:  domain.Size{Cols: 4, Rows: 2},
		Spans: []ports.ScreenSpan{{Y: 0, Cells: cells("Z")}},
	})
	applyProxyDelta(t, s, ports.ScreenUpdate{
		Size:   domain.Size{Cols: 4, Rows: 2},
		Scroll: &ports.ScreenScroll{Top: 0, Height: 2, Count: 1},
		Spans:  []ports.ScreenSpan{{Y: 1, Cells: cells("IJKL")}},
	})
	capture := s.CaptureDamage()
	if len(capture.Damage) != 1 || capture.Damage[0].Kind != renderer.DamageFullRedraw {
		t.Fatalf("span-then-scroll damage = %+v", capture.Damage)
	}
	s.CaptureInto(&dst)
	if got, want := proxyFrameText(dst), proxyFrameText(s.frame); got != want {
		t.Fatalf("span-then-scroll capture = %q, want %q", got, want)
	}
}

func TestProxyScreenStateCaptureFallsBackAfterRepeatedScroll(t *testing.T) {
	s := newProxyScreenState(domain.Size{Cols: 4, Rows: 3})
	if err := s.Apply(screenSnapshot(4, 3, "abcd", "efgh", "ijkl")); err != nil {
		t.Fatal(err)
	}
	_ = s.AcknowledgeDamage(s.CaptureDamage().Generation)
	dst := s.frame.Clone()
	scroll := func(line string) {
		t.Helper()
		applyProxyDelta(t, s, ports.ScreenUpdate{
			Size:   domain.Size{Cols: 4, Rows: 3},
			Scroll: &ports.ScreenScroll{Top: 0, Height: 3, Count: 1},
			Spans:  []ports.ScreenSpan{{Y: 2, Cells: cells(line)}},
		})
	}
	scroll("WXYZ")
	if len(s.CaptureDamage().Damage) != 2 {
		t.Fatalf("first scroll damage = %+v", s.CaptureDamage().Damage)
	}
	scroll("QRST")
	capture := s.CaptureDamage()
	if len(capture.Damage) != 1 || capture.Damage[0].Kind != renderer.DamageFullRedraw {
		t.Fatalf("repeated-scroll damage = %+v", capture.Damage)
	}
	s.CaptureInto(&dst)
	if got, want := proxyFrameText(dst), proxyFrameText(s.frame); got != want {
		t.Fatalf("repeated-scroll capture = %q, want %q", got, want)
	}
}

func TestProxyScreenStateCursorOnlyPreservesCapturedDamageGeneration(t *testing.T) {
	s := newProxyScreenState(domain.Size{Cols: 2, Rows: 2})
	if err := s.Apply(screenSnapshot(2, 2, "  ", "  ")); err != nil {
		t.Fatal(err)
	}
	snapshotDamage := s.CaptureDamage()
	applyProxyDelta(t, s, ports.ScreenUpdate{
		Size:   domain.Size{Cols: 2, Rows: 2},
		Cursor: ports.ScreenCursor{Row: 1, Col: 1, Visible: true},
	})
	if s.generation != snapshotDamage.Generation {
		t.Fatalf("cursor-only update changed damage generation from %d to %d", snapshotDamage.Generation, s.generation)
	}
	if !s.AcknowledgeDamage(snapshotDamage.Generation) || len(s.damage) != 0 || s.damageFullRedrawSticky {
		t.Fatalf("cursor-only interleaving invalidated captured damage: generation=%d damage=%+v sticky=%v", s.generation, s.damage, s.damageFullRedrawSticky)
	}

	applyProxyDelta(t, s, ports.ScreenUpdate{
		Size:  domain.Size{Cols: 2, Rows: 2},
		Spans: []ports.ScreenSpan{{Y: 0, Cells: cells("x")}},
	})
	actualDamage := s.CaptureDamage()
	if actualDamage.Generation <= snapshotDamage.Generation {
		t.Fatalf("actual damage did not advance generation: snapshot=%d actual=%d", snapshotDamage.Generation, actualDamage.Generation)
	}
	applyProxyDelta(t, s, ports.ScreenUpdate{
		Size:   domain.Size{Cols: 2, Rows: 2},
		Cursor: ports.ScreenCursor{Row: 0, Col: 1, Visible: true},
	})
	if s.generation != actualDamage.Generation || !s.AcknowledgeDamage(actualDamage.Generation) || len(s.damage) != 0 || s.damageFullRedrawSticky {
		t.Fatalf("cursor-only update invalidated actual damage: generation=%d want=%d damage=%+v sticky=%v", s.generation, actualDamage.Generation, s.damage, s.damageFullRedrawSticky)
	}
}

func TestProxyScreenStateCursorOnlyAndDamageBound(t *testing.T) {
	s := newProxyScreenState(domain.Size{Cols: 2, Rows: 2})
	if err := s.Apply(screenSnapshot(2, 2, "  ", "  ")); err != nil {
		t.Fatal(err)
	}
	_ = s.AcknowledgeDamage(s.generation)
	applyProxyDelta(t, s, ports.ScreenUpdate{
		Size:   domain.Size{Cols: 2, Rows: 2},
		Cursor: ports.ScreenCursor{Row: 1, Col: 1, Visible: true},
	})
	if s.cursorOut.Row != 1 || s.cursorOut.Col != 1 || len(s.damage) != 0 {
		t.Fatalf("cursor-only update = cursor=%+v damage=%+v", s.cursorOut, s.damage)
	}
	for range maxProxyScreenDamage {
		applyProxyDelta(t, s, ports.ScreenUpdate{
			Size:  domain.Size{Cols: 2, Rows: 2},
			Spans: []ports.ScreenSpan{{Y: 0, Cells: cells("x")}},
		})
	}
	if len(s.damage) != maxProxyScreenDamage {
		t.Fatalf("damage length = %d, want %d", len(s.damage), maxProxyScreenDamage)
	}
	applyProxyDelta(t, s, ports.ScreenUpdate{
		Size:  domain.Size{Cols: 2, Rows: 2},
		Spans: []ports.ScreenSpan{{Y: 0, Cells: cells("y")}},
	})
	if len(s.damage) != 1 || s.damage[0].Kind != renderer.DamageFullRedraw {
		t.Fatalf("saturated damage = %+v", s.damage)
	}
}

func TestProxyScreenStateStyleValidationMatchesWireSemantics(t *testing.T) {
	styles := []struct {
		name  string
		style renderer.Style
	}{
		{name: "indexed foreground", style: renderer.Style{Foreground: 255, Background: -1}},
		{name: "indexed foreground out of range", style: renderer.Style{Foreground: 256, Background: -1}},
		{name: "foreground RGB ignores index", style: renderer.Style{Foreground: 1 << 20, Background: -1, HasForegroundRGB: true, ForegroundRGB: renderer.RGB{R: 1}}},
		{name: "background RGB ignores index", style: renderer.Style{Foreground: -1, Background: 1 << 20, HasBackgroundRGB: true, BackgroundRGB: renderer.RGB{G: 2}}},
		{name: "indexed underline", style: renderer.Style{Foreground: -1, Background: -1, HasUnderlineColor: true, UnderlineColor: 255}},
		{name: "indexed underline with RGB", style: renderer.Style{Foreground: -1, Background: -1, HasUnderlineColor: true, UnderlineColor: 1, UnderlineColorRGB: renderer.RGB{B: 3}}},
		{name: "underline RGB ignores index", style: renderer.Style{Foreground: -1, Background: -1, HasUnderlineColorRGB: true, UnderlineColor: 1 << 20, UnderlineColorRGB: renderer.RGB{R: 4}}},
		{name: "both underline modes", style: renderer.Style{Foreground: -1, Background: -1, HasUnderlineColor: true, HasUnderlineColorRGB: true}},
		{name: "unset underline ignores index", style: renderer.Style{Foreground: -1, Background: -1, UnderlineColor: 1 << 20}},
	}
	for _, tt := range styles {
		t.Run(tt.name, func(t *testing.T) {
			update := ports.ScreenUpdate{
				NewStateNum: 1,
				Kind:        ports.ScreenUpdateSnapshot,
				Size:        domain.Size{Cols: 1, Rows: 1},
				Cursor:      ports.ScreenCursor{Visible: true},
				Spans:       []ports.ScreenSpan{{Cells: []renderer.Cell{{Rune: 'x', Style: tt.style}}}},
			}
			_, wireErr := ports.MarshalScreenUpdate(update)
			stateErr := newProxyScreenState(update.Size).Apply(update)
			if (wireErr == nil) != (stateErr == nil) {
				t.Fatalf("daemon/wire style acceptance differs: wire=%v daemon=%v", wireErr, stateErr)
			}
		})
	}
}

// BenchmarkProxyScreenStateApply is a component diagnostic for applying an
// already-decoded update; it is not a pipeline comparison with ANSI apply.
func BenchmarkProxyScreenStateApply(b *testing.B) {
	size := domain.Size{Cols: 120, Rows: 40}
	cases := []struct {
		name         string
		update       ports.ScreenUpdate
		cells, spans int
	}{
		{name: "one-cell", update: ports.ScreenUpdate{Kind: ports.ScreenUpdateDelta, Size: size, Spans: []ports.ScreenSpan{{Y: 1, X: 0, Cells: cells("x")}}}, cells: 1, spans: 1},
		{name: "full-line", update: ports.ScreenUpdate{Kind: ports.ScreenUpdateDelta, Size: size, Spans: []ports.ScreenSpan{{Y: 2, Cells: cellsRepeat('x', 120)}}}, cells: 120, spans: 1},
		{name: "styled-fragments", update: ports.ScreenUpdate{Kind: ports.ScreenUpdateDelta, Size: size, Spans: []ports.ScreenSpan{{Y: 3, Cells: styledCells(120)}}}, cells: 120, spans: 1},
		{name: "full-screen-scroll", update: ports.ScreenUpdate{Kind: ports.ScreenUpdateDelta, Size: size, Scroll: &ports.ScreenScroll{Top: 0, Height: 40, Count: 1}, Spans: []ports.ScreenSpan{{Y: 39, Cells: cellsRepeat('x', 120)}}}, cells: 120 * 40, spans: 2},
	}
	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			s := newProxyScreenState(size)
			if err := s.Apply(blankSnapshot(size)); err != nil {
				b.Fatal(err)
			}
			_ = s.AcknowledgeDamage(s.generation)
			// Warm the bounded damage buffer before timing the replay loop.
			if err := s.Apply(nextProxyDelta(s, tc.update)); err != nil {
				b.Fatal(err)
			}
			_ = s.AcknowledgeDamage(s.generation)
			b.ResetTimer()
			b.ReportMetric(float64(tc.cells), "cells/op")
			b.ReportMetric(float64(tc.spans), "spans/op")
			for b.Loop() {
				if err := s.Apply(nextProxyDelta(s, tc.update)); err != nil {
					b.Fatal(err)
				}
				_ = s.AcknowledgeDamage(s.generation)
			}
		})
	}
}

func applyProxyDelta(t *testing.T, s *proxyScreenState, update ports.ScreenUpdate) {
	t.Helper()
	if err := s.Apply(nextProxyDelta(s, update)); err != nil {
		t.Fatal(err)
	}
}

func nextProxyDelta(s *proxyScreenState, update ports.ScreenUpdate) ports.ScreenUpdate {
	update.Kind = ports.ScreenUpdateDelta
	update.BaseStateNum = s.stateNum
	update.NewStateNum = s.stateNum + 1
	return update
}

func screenSnapshot(cols, rows int, lines ...string) ports.ScreenUpdate {
	spans := make([]ports.ScreenSpan, len(lines))
	for y, line := range lines {
		spans[y] = ports.ScreenSpan{Y: uint16(y), Cells: cells(line)}
	}
	return ports.ScreenUpdate{NewStateNum: 1, Kind: ports.ScreenUpdateSnapshot, Size: domain.Size{Cols: cols, Rows: rows}, Spans: spans}
}

func blankSnapshot(size domain.Size) ports.ScreenUpdate {
	spans := make([]ports.ScreenSpan, size.Rows)
	for y := range spans {
		spans[y] = ports.ScreenSpan{Y: uint16(y), Cells: cellsRepeat(' ', size.Cols)}
	}
	return ports.ScreenUpdate{NewStateNum: 1, Kind: ports.ScreenUpdateSnapshot, Size: size, Spans: spans}
}

func cells(text string) []renderer.Cell {
	out := make([]renderer.Cell, 0, len(text))
	for _, r := range text {
		out = append(out, renderer.Cell{Rune: r, Style: renderer.DefaultStyle()})
	}
	return out
}

func cellsRepeat(r rune, n int) []renderer.Cell {
	out := make([]renderer.Cell, n)
	for i := range out {
		out[i] = renderer.Cell{Rune: r, Style: renderer.DefaultStyle()}
	}
	return out
}

func styledCells(n int) []renderer.Cell {
	out := cellsRepeat('s', n)
	for i := range out {
		out[i].Style.Bold = i%2 == 0
	}
	return out
}

func proxyFrameText(frame renderer.Frame) string {
	out := make([]rune, 0, frame.Width*frame.Height)
	for y := 0; y < frame.Height; y++ {
		for _, cell := range frame.Row(y) {
			out = append(out, cell.Rune)
		}
	}
	return string(out)
}
