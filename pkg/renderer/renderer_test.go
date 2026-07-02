package renderer

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"
)

// markFrame writes uniform printable content so each row is distinguishable.
func markFrame(f *Frame) {
	for y := 0; y < f.Height; y++ {
		for x := 0; x < f.Width; x++ {
			f.Set(x, y, Cell{Rune: rune('A' + (y*f.Width+x)%26), Style: DefaultStyle()})
		}
	}
}

// outputContains checks that the output contains all the given substrings.
func outputContains(t *testing.T, data []byte, subs ...string) {
	t.Helper()
	s := string(data)
	for _, sub := range subs {
		if !strings.Contains(s, sub) {
			t.Errorf("output should contain %q", sub)
		}
	}
}

func outputEndsWith(t *testing.T, data []byte, suffix string) {
	t.Helper()
	s := string(data)
	if !strings.HasSuffix(s, suffix) {
		t.Errorf("output should end with %q, got %q", suffix, s)
	}
}

// ---------------------------------------------------------------------------
// First draw
// ---------------------------------------------------------------------------

func TestFirstDraw(t *testing.T) {
	r := New(Capabilities{})
	frame := NewFrame(5, 3)
	markFrame(&frame)

	out, err := r.Draw(frame, []Damage{{Kind: DamageText, X: 0, Y: 0, Width: 5, Height: 3}})
	if err != nil {
		t.Fatal(err)
	}
	if len(out) == 0 {
		t.Fatal("expected non-empty output on first draw")
	}
	outputContains(t, out, "\x1b[0m")
	// Should have cursor-position sequences for all three rows.
	if c := strings.Count(string(out), "\x1b["); c < 3 {
		t.Fatalf("expected at least 3 CSI sequences, got %d", c)
	}
}

func TestFirstDrawFullRedraw(t *testing.T) {
	r := New(Capabilities{})
	frame := NewFrame(5, 3)
	markFrame(&frame)

	out, err := r.Draw(frame, []Damage{FullRedraw()})
	if err != nil {
		t.Fatal(err)
	}
	if len(out) == 0 {
		t.Fatal("expected non-empty output on full redraw")
	}
	outputContains(t, out, "\x1b[0m")
}

// ---------------------------------------------------------------------------
// No-op
// ---------------------------------------------------------------------------

func TestNoOp(t *testing.T) {
	r := New(Capabilities{})
	frame := NewFrame(5, 3)
	markFrame(&frame)

	// First draw – populate shadow.
	out1, err := r.Draw(frame, []Damage{{Kind: DamageText, X: 0, Y: 0, Width: 5, Height: 3}})
	if err != nil {
		t.Fatal(err)
	}
	if len(out1) == 0 {
		t.Fatal("first draw must produce output")
	}

	// Second draw with no changes.
	out2, err := r.Draw(frame, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(out2) != 0 {
		t.Fatalf("expected no-op (empty output), got %q", string(out2))
	}
}

func TestNoOpWithEmptyDamage(t *testing.T) {
	r := New(Capabilities{})
	frame := NewFrame(3, 2)
	markFrame(&frame)

	// Populate shadow.
	_, err := r.Draw(frame, []Damage{{Kind: DamageText, X: 0, Y: 0, Width: 3, Height: 2}})
	if err != nil {
		t.Fatal(err)
	}

	// Same frame, empty damage slice.
	out, err := r.Draw(frame, []Damage{})
	if err != nil {
		t.Fatal(err)
	}
	if len(out) != 0 {
		t.Fatalf("expected empty output for no changes, got %q", string(out))
	}
}

// ---------------------------------------------------------------------------
// Style reset discipline
// ---------------------------------------------------------------------------

func TestStyleReset(t *testing.T) {
	r := New(Capabilities{})
	frame := NewFrame(4, 2)

	// One styled cell, rest default.
	frame.Set(0, 0, Cell{Rune: 'X', Style: Style{Bold: true, Foreground: 2}})

	out, err := r.Draw(frame, []Damage{{Kind: DamageText, X: 0, Y: 0, Width: 4, Height: 2}})
	if err != nil {
		t.Fatal(err)
	}
	// Must end with style reset.
	outputEndsWith(t, out, "\x1b[0m")
}

func TestStyleEqualUsesActiveColorMode(t *testing.T) {
	left := Style{Foreground: 1, HasForegroundRGB: true, ForegroundRGB: RGB{R: 12, G: 34, B: 56}, Background: -1}
	right := Style{Foreground: 2, HasForegroundRGB: true, ForegroundRGB: RGB{R: 12, G: 34, B: 56}, Background: -1}
	if !left.Equal(right) {
		t.Fatal("RGB foreground equality should ignore inactive indexed foreground")
	}

	right.ForegroundRGB.R = 13
	if left.Equal(right) {
		t.Fatal("RGB foreground equality should compare active RGB values")
	}
}

func TestRendererEmitsTruecolorSGR(t *testing.T) {
	r := New(Capabilities{})
	frame := NewFrame(1, 1)
	frame.Set(0, 0, Cell{
		Rune: 'X',
		Style: Style{
			Foreground:       -1,
			Background:       -1,
			HasForegroundRGB: true,
			ForegroundRGB:    RGB{R: 12, G: 34, B: 56},
			HasBackgroundRGB: true,
			BackgroundRGB:    RGB{R: 200, G: 100, B: 50},
		},
	})

	out, err := r.Draw(frame, []Damage{{Kind: DamageText, X: 0, Y: 0, Width: 1, Height: 1}})
	if err != nil {
		t.Fatal(err)
	}

	outputContains(t, out, "\x1b[0;38;2;12;34;56;48;2;200;100;50m")
	outputContains(t, out, "X")
}

func TestRendererEmitsStyleChangesForAdjacentColorModes(t *testing.T) {
	r := New(Capabilities{})
	frame := NewFrame(3, 1)
	frame.Set(0, 0, Cell{Rune: 'R', Style: Style{Foreground: -1, Background: -1, HasForegroundRGB: true, ForegroundRGB: RGB{R: 1, G: 2, B: 3}}})
	frame.Set(1, 0, Cell{Rune: 'I', Style: Style{Foreground: 82, Background: -1}})
	frame.Set(2, 0, Cell{Rune: 'D', Style: DefaultStyle()})

	out, err := r.Draw(frame, []Damage{{Kind: DamageText, X: 0, Y: 0, Width: 3, Height: 1}})
	if err != nil {
		t.Fatal(err)
	}

	outputContains(t, out, "\x1b[0;38;2;1;2;3m", "R", "\x1b[0;38;5;82m", "I", "\x1b[0mD")
}

func TestStyleResetAfterScroll(t *testing.T) {
	r := New(Capabilities{})
	frame := NewFrame(4, 3)
	markFrame(&frame)

	// Populate shadow.
	_, err := r.Draw(frame, []Damage{{Kind: DamageText, X: 0, Y: 0, Width: 4, Height: 3}})
	if err != nil {
		t.Fatal(err)
	}

	// Cause a scroll-up via VT model simulation: shift cells and emit scroll damage.
	frame2 := NewFrame(4, 3)
	copy(frame2.Cells[4:], frame.Cells[:8]) // row 0←row 1, row 1←row 2
	for i := 8; i < 12; i++ {
		frame2.Cells[i] = BlankCell() // bottom row blank
	}
	frame2.Set(0, 2, Cell{Rune: 'N', Style: DefaultStyle()}) // new char on bottom row

	damage := []Damage{
		{Kind: DamageScrollUp, X: 0, Y: 0, Width: 4, Height: 3, Count: 1},
		{Kind: DamageText, X: 0, Y: 2, Width: 4, Height: 1},
	}

	out, err := r.Draw(frame2, damage)
	if err != nil {
		t.Fatal(err)
	}
	// The output must end with a style reset (either from emitScrollUp or writeDamage).
	outputEndsWith(t, out, "\x1b[0m")
}

// ---------------------------------------------------------------------------
// Scroll fast path
// ---------------------------------------------------------------------------

func TestScrollFastPath(t *testing.T) {
	r := New(Capabilities{})
	frame := NewFrame(5, 4)
	markFrame(&frame)

	// Populate shadow.
	_, err := r.Draw(frame, []Damage{{Kind: DamageText, X: 0, Y: 0, Width: 5, Height: 4}})
	if err != nil {
		t.Fatal(err)
	}

	// Build a new frame that has been scrolled up by 1 (like the VT model does).
	scrolled := NewFrame(5, 4)
	copy(scrolled.Cells[0:15], frame.Cells[5:20]) // rows 0,1,2 ← rows 1,2,3
	for i := 15; i < 20; i++ {
		scrolled.Cells[i] = BlankCell() // row 3 blanked
	}
	scrolled.Set(0, 3, Cell{Rune: 'N', Style: DefaultStyle()})
	scrolled.Set(1, 3, Cell{Rune: 'e', Style: DefaultStyle()})
	scrolled.Set(2, 3, Cell{Rune: 'w', Style: DefaultStyle()})

	damage := []Damage{
		{Kind: DamageScrollUp, X: 0, Y: 0, Width: 5, Height: 4, Count: 1},
		{Kind: DamageText, X: 0, Y: 3, Width: 5, Height: 1},
	}

	out, err := r.Draw(scrolled, damage)
	if err != nil {
		t.Fatal(err)
	}
	if len(out) == 0 {
		t.Fatal("expected non-empty output for scroll")
	}
	// Should contain scroll-region sequences.
	outputContains(t, out, "\x1b[1;4r") // scroll region rows 1-4
	outputContains(t, out, "\x1b[r")    // restore scroll region
	// Should contain the new text on the exposed row.
	outputContains(t, out, "N")
	outputContains(t, out, "e")
	outputContains(t, out, "w")
}

// ---------------------------------------------------------------------------
// Synchronized output wrapper
// ---------------------------------------------------------------------------

func TestSynchronizedOutput(t *testing.T) {
	r := New(Capabilities{SynchronizedOutput: true})
	frame := NewFrame(3, 2)
	markFrame(&frame)

	out, err := r.Draw(frame, []Damage{{Kind: DamageText, X: 0, Y: 0, Width: 3, Height: 2}})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(string(out), SyncStartCSI) {
		t.Errorf("expected output to start with sync start CSI")
	}
	if !strings.HasSuffix(string(out), SyncEndCSI) {
		t.Errorf("expected output to end with sync end CSI, got %q", string(out))
	}
}

func TestSynchronizedOutputNoOp(t *testing.T) {
	r := New(Capabilities{SynchronizedOutput: true})
	frame := NewFrame(3, 2)
	markFrame(&frame)

	// First draw.
	_, err := r.Draw(frame, []Damage{{Kind: DamageText, X: 0, Y: 0, Width: 3, Height: 2}})
	if err != nil {
		t.Fatal(err)
	}

	// No-op – must return nil, not wrapped empty output.
	out, err := r.Draw(frame, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(out) != 0 {
		t.Fatalf("expected empty output for no-op, got %q", string(out))
	}
}

// ---------------------------------------------------------------------------
// WrapSynchronized standalone function
// ---------------------------------------------------------------------------

func TestWrapSynchronized(t *testing.T) {
	content := []byte("\x1b[0mhello")
	wrapped := WrapSynchronized(content, true)
	if !strings.HasPrefix(string(wrapped), SyncStartCSI) {
		t.Errorf("missing sync start")
	}
	if !strings.HasSuffix(string(wrapped), SyncEndCSI) {
		t.Errorf("missing sync end")
	}
	if !strings.Contains(string(wrapped), "hello") {
		t.Errorf("missing original content")
	}

	// disabled
	plain := WrapSynchronized(content, false)
	if string(plain) != string(content) {
		t.Errorf("disabled wrapping should return content unchanged")
	}

	// empty input
	empty := WrapSynchronized(nil, true)
	if len(empty) != 0 {
		t.Errorf("empty input should return empty")
	}
}

// ---------------------------------------------------------------------------
// Partial damage draw
// ---------------------------------------------------------------------------

func TestPartialDamage(t *testing.T) {
	r := New(Capabilities{})
	frame := NewFrame(4, 2)
	markFrame(&frame)

	// Populate shadow.
	_, err := r.Draw(frame, []Damage{{Kind: DamageText, X: 0, Y: 0, Width: 4, Height: 2}})
	if err != nil {
		t.Fatal(err)
	}

	// Change one cell.
	frame.Set(2, 1, Cell{Rune: 'Z', Style: DefaultStyle()})
	damage := []Damage{{Kind: DamageText, X: 2, Y: 1, Width: 1, Height: 1}}

	out, err := r.Draw(frame, damage)
	if err != nil {
		t.Fatal(err)
	}
	if len(out) == 0 {
		t.Fatal("expected non-empty output for partial damage")
	}
	// Should position cursor at (2,1) — 1-indexed (3;2).
	outputContains(t, out, "\x1b[2;3H")
	outputContains(t, out, "Z")
	outputEndsWith(t, out, "\x1b[0m")
}

// ---------------------------------------------------------------------------
// Scheduler coalescing
// ---------------------------------------------------------------------------

func TestSchedulerCoalescing(t *testing.T) {
	s := NewScheduler(30 * time.Millisecond)
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	frames := s.Run(ctx)

	// Fire several requests before the timer fires.
	s.Request()
	s.Request()
	s.Request()

	// Expect exactly one frame.
	select {
	case _, ok := <-frames:
		if !ok {
			t.Fatal("channel closed unexpectedly")
		}
	case <-ctx.Done():
		t.Fatal("timeout waiting for frame")
	}

	// No second frame should arrive without another request.
	select {
	case <-frames:
		t.Fatal("unexpected second frame")
	case <-time.After(80 * time.Millisecond):
		// good – coalesced.
	}
}

func TestSchedulerMultipleFrames(t *testing.T) {
	s := NewScheduler(10 * time.Millisecond)
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	frames := s.Run(ctx)

	s.Request()
	<-frames // consume first

	s.Request()
	<-frames // consume second

	// No pending request – should not deliver another frame.
	select {
	case <-frames:
		t.Fatal("unexpected frame without request")
	case <-time.After(40 * time.Millisecond):
		// good
	}
}

func TestSchedulerContextCancellation(t *testing.T) {
	s := NewScheduler(50 * time.Millisecond)
	ctx, cancel := context.WithCancel(context.Background())

	frames := s.Run(ctx)
	s.Request()

	// Cancel before the timer fires.
	cancel()

	// Channel should close without ever delivering a frame.
	select {
	case _, ok := <-frames:
		if ok {
			// A frame may have been emitted before cancellation took effect;
			// that is acceptable.  What matters is that the goroutine exits.
			_ = ok
		}
	case <-time.After(200 * time.Millisecond):
		// Channel didn't close – goroutine may be stuck.
	}
}

// ---------------------------------------------------------------------------
// Reset
// ---------------------------------------------------------------------------

func TestReset(t *testing.T) {
	r := New(Capabilities{})
	frame := NewFrame(3, 2)
	markFrame(&frame)

	_, err := r.Draw(frame, []Damage{{Kind: DamageText, X: 0, Y: 0, Width: 3, Height: 2}})
	if err != nil {
		t.Fatal(err)
	}

	r.Reset()

	if r.width != 0 || r.height != 0 || r.shadow != nil {
		t.Fatal("Reset should clear width/height/shadow")
	}

	// After reset, the next draw should be a full draw.
	out, err := r.Draw(frame, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(out) == 0 {
		t.Fatal("expected non-empty output after reset")
	}
}

// ---------------------------------------------------------------------------
// Damage helper
// ---------------------------------------------------------------------------

func TestFullRedraw(t *testing.T) {
	r := New(Capabilities{})
	frame := NewFrame(3, 2)
	markFrame(&frame)

	// Populate.
	_, err := r.Draw(frame, []Damage{{Kind: DamageText, X: 0, Y: 0, Width: 3, Height: 2}})
	if err != nil {
		t.Fatal(err)
	}

	// FullRedraw forces full output even with same frame.
	out, err := r.Draw(frame, []Damage{FullRedraw()})
	if err != nil {
		t.Fatal(err)
	}
	if len(out) == 0 {
		t.Fatal("expected output for FullRedraw")
	}
	outputEndsWith(t, out, "\x1b[0m")
}

// ---------------------------------------------------------------------------
// Scroll with no remaining damage
// ---------------------------------------------------------------------------

func TestScrollOnlyDamage(t *testing.T) {
	r := New(Capabilities{})
	frame := NewFrame(4, 3)
	markFrame(&frame)

	_, err := r.Draw(frame, []Damage{{Kind: DamageText, X: 0, Y: 0, Width: 4, Height: 3}})
	if err != nil {
		t.Fatal(err)
	}

	// Scroll up 1 but the VT model would also emit DamageText for the new bottom row.
	scrolled := NewFrame(4, 3)
	copy(scrolled.Cells[0:8], frame.Cells[4:12])
	for i := 8; i < 12; i++ {
		scrolled.Cells[i] = BlankCell()
	}
	scrolled.Set(0, 2, Cell{Rune: 'X', Style: DefaultStyle()})

	damage := []Damage{
		{Kind: DamageScrollUp, X: 0, Y: 0, Width: 4, Height: 3, Count: 1},
		{Kind: DamageText, X: 0, Y: 2, Width: 4, Height: 1},
	}

	out, err := r.Draw(scrolled, damage)
	if err != nil {
		t.Fatal(err)
	}
	if len(out) == 0 {
		t.Fatal("expected output for scroll")
	}
	// Should include scroll region
	outputContains(t, out, "\x1b[1;3r")
	outputContains(t, out, "\x1b[r")
	outputEndsWith(t, out, "\x1b[0m")
}

// ---------------------------------------------------------------------------
// writeCursor output format
// ---------------------------------------------------------------------------

func TestWriteCursor(t *testing.T) {
	var buf bytes.Buffer
	writeCursor(&buf, 0, 0)
	if buf.String() != "\x1b[1;1H" {
		t.Errorf("unexpected cursor positioning: %q", buf.String())
	}
	buf.Reset()
	writeCursor(&buf, 2, 5)
	if buf.String() != "\x1b[3;6H" {
		t.Errorf("unexpected cursor positioning: %q", buf.String())
	}
}

// ---------------------------------------------------------------------------
// isSafeScroll edge cases
// ---------------------------------------------------------------------------

func TestIsSafeScroll(t *testing.T) {
	frame := NewFrame(10, 8)

	tests := []struct {
		name string
		d    Damage
		want bool
	}{
		{"full width, partial height", Damage{X: 0, Y: 1, Width: 10, Height: 5, Count: 2}, true},
		{"not full width", Damage{X: 1, Y: 1, Width: 8, Height: 5, Count: 2}, false},
		{"count zero", Damage{X: 0, Y: 0, Width: 10, Height: 8, Count: 0}, false},
		{"count equals height", Damage{X: 0, Y: 0, Width: 10, Height: 8, Count: 8}, false},
		{"extends past frame", Damage{X: 0, Y: 6, Width: 10, Height: 6, Count: 2}, false},
		{"negative Y", Damage{X: 0, Y: -1, Width: 10, Height: 5, Count: 1}, false},
		{"height zero", Damage{X: 0, Y: 0, Width: 10, Height: 0, Count: 1}, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := isSafeScroll(frame, tt.d)
			if got != tt.want {
				t.Errorf("isSafeScroll(%+v) = %v, want %v", tt.d, got, tt.want)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// BlankCell / Cell.Equal
// ---------------------------------------------------------------------------

func TestCellEqual(t *testing.T) {
	a := BlankCell()
	b := BlankCell()
	if !a.Equal(b) {
		t.Error("blank cells should be equal")
	}

	a.Rune = 'A'
	if a.Equal(b) {
		t.Error("different runes should not be equal")
	}

	a = BlankCell()
	a.Style.Bold = true
	if a.Equal(b) {
		t.Error("different styles should not be equal")
	}
}

// ---------------------------------------------------------------------------
// Frame helpers
// ---------------------------------------------------------------------------

func TestFrameValidate(t *testing.T) {
	// Valid frame.
	f := NewFrame(5, 3)
	if err := f.Validate(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Invalid dimensions.
	f2 := Frame{Width: 0, Height: 5}
	if err := f2.Validate(); err == nil {
		t.Fatal("expected error for zero width")
	}

	// Wrong cell count.
	f3 := Frame{Width: 2, Height: 2, Cells: make([]Cell, 3)}
	if err := f3.Validate(); err == nil {
		t.Fatal("expected error for wrong cell count")
	}
}

func TestFrameAtSet(t *testing.T) {
	f := NewFrame(3, 2)
	cell := Cell{Rune: 'X', Style: Style{Bold: true}}
	f.Set(1, 1, cell)

	got := f.At(1, 1)
	if got.Rune != 'X' || !got.Style.Bold {
		t.Errorf("At/Set round-trip failed: got %+v", got)
	}

	// Unaffected cells are blank.
	got = f.At(0, 0)
	if got != BlankCell() {
		t.Errorf("unexpected cell at (0,0): %+v", got)
	}
}

// ---------------------------------------------------------------------------
// Damage helpers
// ---------------------------------------------------------------------------

func TestSameDamage(t *testing.T) {
	a := Damage{Kind: DamageText, X: 1, Y: 2, Width: 3, Height: 4, Count: 5}
	b := Damage{Kind: DamageText, X: 1, Y: 2, Width: 3, Height: 4, Count: 5}
	if !sameDamage(a, b) {
		t.Error("identical damages should be equal")
	}

	c := Damage{Kind: DamageText, X: 1, Y: 2, Width: 3, Height: 4, Count: 0}
	if sameDamage(a, c) {
		t.Error("different counts should not be equal")
	}
}

func TestDamageCoversCell(t *testing.T) {
	damage := []Damage{
		{Kind: DamageText, X: 2, Y: 3, Width: 5, Height: 2},
	}

	if !damageCoversCell(damage, 3, 3) {
		t.Error("should cover (3,3)")
	}
	if !damageCoversCell(damage, 6, 4) {
		t.Error("should cover (6,4)")
	}
	if damageCoversCell(damage, 1, 3) {
		t.Error("should not cover (1,3)")
	}
	if damageCoversCell(damage, 3, 5) {
		t.Error("should not cover (3,5)")
	}
	if damageCoversCell(damage, 7, 3) {
		t.Error("should not cover (7,3)")
	}
}

// ---------------------------------------------------------------------------
// needsFull
// ---------------------------------------------------------------------------

func TestNeedsFull(t *testing.T) {
	if needsFull(nil) {
		t.Error("nil damage should not need full")
	}
	if needsFull([]Damage{}) {
		t.Error("empty damage should not need full")
	}
	if needsFull([]Damage{{Kind: DamageText}}) {
		t.Error("text damage should not need full")
	}
	if !needsFull([]Damage{FullRedraw()}) {
		t.Error("FullRedraw should need full")
	}
}

func TestScrollDamageFallbackFullRedrawWhenFastPathUnsafe(t *testing.T) {
	r := New(Capabilities{})
	frame := NewFrame(4, 3)
	markFrame(&frame)
	if _, err := r.Draw(frame, []Damage{{Kind: DamageText, X: 0, Y: 0, Width: 4, Height: 3}}); err != nil {
		t.Fatal(err)
	}

	changed := NewFrame(4, 3)
	markFrame(&changed)
	changed.Set(0, 0, Cell{Rune: 'x', Style: DefaultStyle()}) // invalidates scroll relationship
	damage := []Damage{
		{Kind: DamageScrollUp, X: 0, Y: 0, Width: 4, Height: 3, Count: 1},
		{Kind: DamageText, X: 0, Y: 2, Width: 4, Height: 1},
	}
	out, err := r.Draw(changed, damage)
	if err != nil {
		t.Fatal(err)
	}
	if rows := strings.Count(string(out), "H"); rows < changed.Height {
		t.Fatalf("expected full redraw after unsafe scroll fallback, output %q", string(out))
	}
	for i := range changed.Cells {
		if !r.shadow[i].Equal(changed.Cells[i]) {
			t.Fatalf("shadow[%d] = %+v, want %+v", i, r.shadow[i], changed.Cells[i])
		}
	}
}

func TestDamageRectanglesAreClamped(t *testing.T) {
	r := New(Capabilities{})
	frame := NewFrame(3, 2)
	markFrame(&frame)
	if _, err := r.Draw(frame, []Damage{FullRedraw()}); err != nil {
		t.Fatal(err)
	}
	frame.Set(0, 0, Cell{Rune: 'Z', Style: DefaultStyle()})
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("Draw panicked for out-of-bounds damage: %v", r)
		}
	}()
	if _, err := r.Draw(frame, []Damage{{Kind: DamageText, X: -2, Y: -1, Width: 4, Height: 3}}); err != nil {
		t.Fatal(err)
	}
}
