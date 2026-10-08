package client

import (
	"testing"

	"github.com/bnema/vev/internal/protocol"
)

func TestNavigationState(t *testing.T) {
	tests := []struct {
		name string
		run  func(t *testing.T)
	}{
		{"takeInPlace stale seq keeps pending", func(t *testing.T) {
			var n navigationState
			n.startInPlace(2, pickerAttachmentTarget{})
			if got := n.takeInPlace(1); got != nil {
				t.Fatalf("stale takeInPlace = %v, want nil", got)
			}
			if n.inPlaceIdle() {
				t.Fatal("stale takeInPlace cleared pending")
			}
		}},
		{"takeInPlace matching seq clears", func(t *testing.T) {
			var n navigationState
			n.startInPlace(2, pickerAttachmentTarget{})
			if got := n.takeInPlace(2); got == nil || got.seq != 2 {
				t.Fatalf("takeInPlace = %v, want seq 2", got)
			}
			if !n.inPlaceIdle() {
				t.Fatal("matching takeInPlace kept pending")
			}
		}},
		{"resolveRoute zero value", func(t *testing.T) {
			var n navigationState
			if _, ok := n.resolveRoute(protocol.RouteRef{Key: 1, Generation: 1}); ok {
				t.Fatal("resolveRoute on zero value = true, want false")
			}
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, tt.run)
	}
}
