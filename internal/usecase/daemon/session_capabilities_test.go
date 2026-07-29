package daemon

import (
	"errors"
	"testing"

	"github.com/bnema/vev/internal/domain"
)

func TestCapabilitiesZeroValueIsFullyCapable(t *testing.T) {
	s := &session{id: domain.SessionID("s1"), name: "alpha"}
	caps := s.capabilities()
	if caps.cannotAcceptMoves || caps.cannotYieldMoves || !caps.yieldsMoves() {
		t.Fatalf("zero-value capabilities = %+v, want fully capable", caps)
	}
}

func TestSnapshotViewCarriesCannotAcceptMoves(t *testing.T) {
	tests := []struct {
		name string
		caps sessionCapabilities
		want bool
	}{
		{name: "default local", caps: sessionCapabilities{}, want: false},
		{name: "restricted proxy", caps: sessionCapabilities{cannotAcceptMoves: true}, want: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &session{id: domain.SessionID("s1"), name: "alpha", caps: tt.caps}
			if got := s.snapshotView(viewOptions{}).cannotAcceptMoves; got != tt.want {
				t.Fatalf("snapshotView().cannotAcceptMoves = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestEnterPickerForIntentRejectsNonYieldingSource(t *testing.T) {
	d, sess, ac, _ := newManualSessionWithPTYs(t)
	sess.caps = sessionCapabilities{cannotYieldMoves: true}
	err := d.enterPickerForIntent(sess, ac, pickerMoveTab, moveSourceLocator{})
	if !errors.Is(err, errSessionCannotYieldMoves) {
		t.Fatalf("enterPickerForIntent error = %v, want errSessionCannotYieldMoves", err)
	}
	// Navigation intent must stay unaffected by the yield capability.
	if err := d.enterPickerForIntent(sess, ac, pickerNavigate, moveSourceLocator{}); err != nil {
		t.Fatalf("navigate intent returned %v, want nil", err)
	}
}
