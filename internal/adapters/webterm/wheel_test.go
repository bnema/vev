package webterm

import (
	"context"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/bnema/vev-vt/html/browser"
	"github.com/bnema/vev/internal/domain"
	"github.com/stretchr/testify/require"
)

func TestWheelNotchesConvertModes(t *testing.T) {
	tests := []struct {
		name      string
		deltaY    float64
		deltaMode int
		want      float64
	}{
		{"pixel notch", 100, 0, 1},
		{"pixel fraction", 25, 0, 0.25},
		{"pixel negative", -100, 0, -1},
		{"line notch", 3, 1, 1},
		{"line fraction", 1, 1, 1.0 / 3.0},
		{"page notch", 1, 2, 10},
		{"page negative", -1, 2, -10},
		{"unknown mode", 100, 7, 0},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := wheelNotches(test.deltaY, test.deltaMode); got != test.want {
				t.Fatalf("wheelNotches(%v, %d) = %v, want %v", test.deltaY, test.deltaMode, got, test.want)
			}
		})
	}
}

func TestConsumeWheelQuantizesNotches(t *testing.T) {
	tests := []struct {
		name          string
		deltas        []float64
		mode          int
		shift         bool
		wantReports   []int
		wantUp        []bool
		wantRemainder float64
	}{
		{
			name:          "one pixel notch down",
			deltas:        []float64{100},
			wantReports:   []int{1},
			wantUp:        []bool{false},
			wantRemainder: 0,
		},
		{
			name:          "one pixel notch up",
			deltas:        []float64{-100},
			wantReports:   []int{1},
			wantUp:        []bool{true},
			wantRemainder: 0,
		},
		{
			name:          "micro burst forms one notch",
			deltas:        []float64{5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5},
			wantReports:   []int{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1},
			wantUp:        make([]bool, 20),
			wantRemainder: 0,
		},
		{
			name:          "same distance at once or fragmented",
			deltas:        []float64{200},
			wantReports:   []int{2},
			wantUp:        []bool{false},
			wantRemainder: 0,
		},
		{
			name:          "line mode notch",
			deltas:        []float64{3},
			mode:          1,
			wantReports:   []int{1},
			wantUp:        []bool{false},
			wantRemainder: 0,
		},
		{
			name:          "remainder carries over",
			deltas:        []float64{150, 150},
			wantReports:   []int{1, 2},
			wantUp:        []bool{false, false},
			wantRemainder: 0,
		},
		{
			name:          "shift multiplies notches",
			deltas:        []float64{100},
			shift:         true,
			wantReports:   []int{10},
			wantUp:        []bool{false},
			wantRemainder: 0,
		},
		{
			name:          "emission is capped",
			deltas:        []float64{5000},
			wantReports:   []int{10},
			wantUp:        []bool{false},
			wantRemainder: 0,
		},
		{
			name:          "accumulator is capped",
			deltas:        []float64{50000, 0},
			wantReports:   []int{10, 0},
			wantUp:        []bool{false, false},
			wantRemainder: 0,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			terminal := &Terminal{}
			if len(test.wantReports) != len(test.deltas) {
				t.Fatalf("test declares %d deltas but %d expectations", len(test.deltas), len(test.wantReports))
			}
			for i, delta := range test.deltas {
				reports, button := terminal.consumeWheel(delta, test.mode, test.shift)
				if reports != test.wantReports[i] {
					t.Fatalf("delta %d: reports = %d, want %d", i, reports, test.wantReports[i])
				}
				if reports == 0 {
					continue
				}
				wantButton := 65
				if test.wantUp[i] {
					wantButton = 64
				}
				if button != wantButton {
					t.Fatalf("delta %d: button = %d, want %d", i, button, wantButton)
				}
			}
			if terminal.wheelAcc != test.wantRemainder {
				t.Fatalf("remainder = %v, want %v", terminal.wheelAcc, test.wantRemainder)
			}
		})
	}
}

func TestConsumeWheelKeepsDirectionAcrossFractions(t *testing.T) {
	terminal := &Terminal{}
	if reports, _ := terminal.consumeWheel(-90, 0, false); reports != 0 {
		t.Fatalf("reports = %d, want 0 (banked)", reports)
	}
	reports, button := terminal.consumeWheel(-30, 0, false)
	if reports != 1 || button != 64 {
		t.Fatalf("reports = %d button = %d, want 1/64", reports, button)
	}
}

func TestWheelIgnoredInputResetsRemainder(t *testing.T) {
	terminal := &Terminal{}
	ctx := context.Background()
	down := browser.Event{
		Kind: browser.EventWheel,
		Wheel: &browser.WheelEvent{
			DeltaY: 90, DeltaMode: 0, Row: 9999, Column: 9999,
		},
	}
	// Out-of-bounds cells are ignored and must drop the banked fraction.
	if err := terminal.Handle(ctx, down); err != nil {
		t.Fatal(err)
	}
	if terminal.wheelAcc != 0 {
		t.Fatalf("remainder = %v, want 0", terminal.wheelAcc)
	}
}

func TestHandleWheelShiftOmitsShiftModifier(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	terminal, err := New(ctx, domain.Geometry{Size: domain.Size{Cols: 10, Rows: 4}})
	require.NoError(t, err)
	defer terminal.Close()
	_, err = terminal.Write([]byte("\x1b[?1000h\x1b[?1006h"))
	require.NoError(t, err)
	require.NoError(t, terminal.Flush())

	// Shift is consumed as the x10 multiplier: reports stay plain wheel
	// buttons (65) instead of Shift-flagged buttons (69).
	event := browser.Event{Kind: browser.EventWheel, Wheel: &browser.WheelEvent{
		DeltaY: 100, DeltaMode: 0, Row: 0, Column: 0, Modifiers: browser.Modifiers{Shift: true},
	}}
	require.NoError(t, terminal.Handle(ctx, event))
	want := strings.Repeat("\x1b[<65;1;1M", 10)
	data := make([]byte, len(want))
	_, err = io.ReadFull(terminal.In(), data)
	require.NoError(t, err)
	require.Equal(t, want, string(data))
}

func TestHandleWheelEmitsOneReportPerNotch(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	terminal, err := New(ctx, domain.Geometry{Size: domain.Size{Cols: 10, Rows: 4}})
	require.NoError(t, err)
	defer terminal.Close()
	_, err = terminal.Write([]byte("\x1b[?1000h\x1b[?1006h"))
	require.NoError(t, err)
	require.NoError(t, terminal.Flush())

	wheel := func(deltaY float64) browser.Event {
		return browser.Event{Kind: browser.EventWheel, Wheel: &browser.WheelEvent{
			DeltaY: deltaY, DeltaMode: 0, Row: 0, Column: 0,
		}}
	}
	// A 5px micro-event is banked: nothing is sent yet.
	require.NoError(t, terminal.Handle(ctx, wheel(5)))
	// Nineteen more micro-events complete the notch: exactly one report.
	for i := 0; i < 19; i++ {
		require.NoError(t, terminal.Handle(ctx, wheel(5)))
	}
	want := "\x1b[<65;1;1M"
	data := make([]byte, len(want))
	_, err = io.ReadFull(terminal.In(), data)
	require.NoError(t, err)
	require.Equal(t, want, string(data))
}
