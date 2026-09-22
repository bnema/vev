package ports_test

import (
	"testing"

	"github.com/bnema/vev/internal/ports"
)

// TestUIPresentationStatusClosedUnion pins the closed union of exactly three
// presentations: Picker, Connecting, and Attached. The former transition states
// are removed without alias, so no legacy wire value is ever accepted as a
// presentation, and the zero value is not a presentation either.
func TestUIPresentationStatusClosedUnion(t *testing.T) {
	valid := map[ports.UIPresentationStatus]bool{
		ports.UIStatusPicker:     true,
		ports.UIStatusConnecting: true,
		ports.UIStatusAttached:   true,
	}
	for status, want := range valid {
		if got := status.Valid(); got != want {
			t.Errorf("%q.Valid() = %v, want %v", status, got, want)
		}
	}
	for _, legacy := range []ports.UIPresentationStatus{"", "detached", "reconnecting", "transitioning", "unknown"} {
		if legacy.Valid() {
			t.Errorf("removed or unknown status %q is accepted as a presentation", legacy)
		}
	}
	if ports.UIStatusPicker != "picker" || ports.UIStatusConnecting != "connecting" || ports.UIStatusAttached != "attached" {
		t.Fatalf("presentation wire values changed: %q %q %q", ports.UIStatusPicker, ports.UIStatusConnecting, ports.UIStatusAttached)
	}
}
