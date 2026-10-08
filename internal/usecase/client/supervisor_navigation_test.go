package client

import (
	"testing"

	"github.com/bnema/vev/internal/ports"
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
		{"buildRoutes creates ledger lazily", func(t *testing.T) {
			var n navigationState
			if n.routes != nil {
				t.Fatal("routes non-nil at zero value")
			}
			n.buildRoutes(ports.BrokerSnapshot{}, routeActive{})
			if n.routes == nil {
				t.Fatal("routes nil after buildRoutes")
			}
		}},
		{"takeSwap returns and clears", func(t *testing.T) {
			var n navigationState
			want := &pickerAttachmentTarget{}
			n.pendingSwap = want
			if got := n.takeSwap(); got != want {
				t.Fatalf("takeSwap = %p, want %p", got, want)
			}
			if n.pendingSwap != nil {
				t.Fatal("takeSwap kept swap")
			}
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, tt.run)
	}
}
