package daemon

import (
	"testing"

	"github.com/bnema/vev/internal/domain"
)

func TestFloatingSlotTransitions(t *testing.T) {
	first := &pane{}
	second := &pane{}
	launch := domain.FloatingConfig{Command: "top", Width: 80, Height: 80}

	tests := []struct {
		name string
		run  func(t *testing.T, tb *tab)
	}{
		{
			name: "warming double toggle retains hidden completion",
			run: func(t *testing.T, tb *tab) {
				tb.mu.Lock()
				generation := tb.beginFloatingWarmLocked(launch, false)
				start, gotGeneration := tb.toggleFloatingLocked(launch)
				if start || gotGeneration != generation || !tb.floating.desiredVisible {
					t.Fatalf("first warming toggle = start %v, generation %d, desired %v", start, gotGeneration, tb.floating.desiredVisible)
				}
				start, gotGeneration = tb.toggleFloatingLocked(launch)
				if start || gotGeneration != generation || tb.floating.desiredVisible {
					t.Fatalf("second warming toggle = start %v, generation %d, desired %v", start, gotGeneration, tb.floating.desiredVisible)
				}
				if !tb.installFloatingLocked(first, generation) || tb.floating.state != floatingHidden || tb.floating.pane != first {
					t.Fatal("warming completion did not retain a hidden pane")
				}
				tb.mu.Unlock()
			},
		},
		{
			name: "open and hide retain same pane",
			run: func(t *testing.T, tb *tab) {
				tb.mu.Lock()
				generation := tb.beginFloatingWarmLocked(launch, false)
				if !tb.installFloatingLocked(first, generation) {
					t.Fatal("install failed")
				}
				start, _ := tb.toggleFloatingLocked(launch)
				if start || tb.floating.state != floatingVisible || tb.floating.pane != first {
					t.Fatal("hidden open did not retain pane")
				}
				tb.toggleFloatingLocked(launch)
				if tb.floating.state != floatingHidden || tb.floating.pane != first {
					t.Fatal("visible hide did not retain pane")
				}
				tb.mu.Unlock()
			},
		},
		{
			name: "current launch failure clears desired visibility",
			run: func(t *testing.T, tb *tab) {
				tb.mu.Lock()
				generation := tb.beginFloatingWarmLocked(launch, true)
				if !tb.failFloatingLocked(generation) || tb.floating.state != floatingUninitialized || tb.floating.desiredVisible || tb.floating.pane != nil {
					t.Fatal("current launch failure did not clear slot")
				}
				tb.mu.Unlock()
			},
		},
		{
			name: "current exit clears and stale completion is ignored",
			run: func(t *testing.T, tb *tab) {
				tb.mu.Lock()
				generation := tb.beginFloatingWarmLocked(launch, true)
				if !tb.installFloatingLocked(first, generation) {
					t.Fatal("install failed")
				}
				if !tb.clearFloatingLocked(first, generation) || tb.floating.state != floatingUninitialized || tb.floating.pane != nil {
					t.Fatal("current exit did not clear slot")
				}
				newGeneration := tb.beginFloatingWarmLocked(launch, true)
				if tb.installFloatingLocked(second, generation) {
					t.Fatal("stale completion installed")
				}
				if tb.floating.state != floatingWarming || tb.floating.generation != newGeneration || tb.floating.pane != nil {
					t.Fatal("stale completion changed newer warming state")
				}
				tb.mu.Unlock()
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) { tt.run(t, &tab{}) })
	}
}

func TestFloatingInnerSize(t *testing.T) {
	tests := []struct {
		name string
		tab  domain.Size
		cfg  domain.FloatingConfig
		want domain.Size
	}{
		{name: "one percent", tab: domain.Size{Cols: 100, Rows: 100}, cfg: domain.FloatingConfig{Width: 1, Height: 1}, want: domain.Size{Cols: 1, Rows: 1}},
		{name: "full size reserves borders", tab: domain.Size{Cols: 100, Rows: 80}, cfg: domain.FloatingConfig{Width: 100, Height: 100}, want: domain.Size{Cols: 98, Rows: 78}},
		{name: "tiny tab omits borders", tab: domain.Size{Cols: 2, Rows: 1}, cfg: domain.FloatingConfig{Width: 100, Height: 100}, want: domain.Size{Cols: 2, Rows: 1}},
		{name: "three cells leaves one inner cell", tab: domain.Size{Cols: 3, Rows: 3}, cfg: domain.FloatingConfig{Width: 100, Height: 100}, want: domain.Size{Cols: 1, Rows: 1}},
		{name: "invalid tab has no pty size", tab: domain.Size{}, cfg: domain.FloatingConfig{Width: 80, Height: 80}, want: domain.Size{}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := floatingInnerSize(tt.tab, tt.cfg); got != tt.want {
				t.Fatalf("floatingInnerSize(%+v, %+v) = %+v, want %+v", tt.tab, tt.cfg, got, tt.want)
			}
		})
	}
}
