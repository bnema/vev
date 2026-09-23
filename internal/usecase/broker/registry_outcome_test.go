package broker

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
)

func TestObservationOutcomeKeepsTypedDialFailure(t *testing.T) {
	tests := []struct {
		name string
		err  error
		kind domain.RemoteFailureKind
		want domain.RemoteAvailability
	}{
		{name: "plain error is transport", err: errors.New("offline"), kind: domain.RemoteFailureTransport, want: domain.RemoteAvailabilityUnreachable},
		{name: "deadline is timeout", err: context.DeadlineExceeded, kind: domain.RemoteFailureTimeout, want: domain.RemoteAvailabilityUnreachable},
		{name: "wrapped authentication", err: fmt.Errorf("dial: %w", domain.RemoteFailure{Kind: domain.RemoteFailureAuthentication, Err: errors.New("exit 255")}), kind: domain.RemoteFailureAuthentication, want: domain.RemoteAvailabilityAuthFailed},
		{name: "wrapped trust", err: fmt.Errorf("dial: %w", domain.RemoteFailure{Kind: domain.RemoteFailureTrust}), kind: domain.RemoteFailureTrust, want: domain.RemoteAvailabilityUnreachable},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			kind, availability, failed := observationOutcome(probeResult{err: tt.err})
			require.True(t, failed)
			require.Equal(t, tt.kind, kind)
			require.Equal(t, tt.want, availability)
		})
	}
}
