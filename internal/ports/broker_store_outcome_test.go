package ports

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestBrokerStoreOutcomeUnknownErrorClassification pins the indeterminate-write
// contract at the ports seam: the error reports a store failure that happened
// after the durable commit point, stays classifiable after wrapping, and keeps
// the underlying store cause inspectable so a caller can log or classify it
// without importing the adapter.
func TestBrokerStoreOutcomeUnknownErrorClassification(t *testing.T) {
	cause := errors.New("power loss")
	unknown := BrokerStoreOutcomeUnknownError{Err: cause}

	require.ErrorContains(t, unknown, "outcome unknown")
	require.ErrorContains(t, unknown, "power loss")
	require.ErrorIs(t, unknown, cause, "the underlying store cause stays inspectable")
	var classified BrokerStoreOutcomeUnknownError
	require.ErrorAs(t, unknown, &classified)
	require.Equal(t, cause, classified.Err)

	// Wrapping by a caller or a composing use case must not erase the
	// classification: that is the whole point of a typed error here.
	wrapped := fmt.Errorf("broker: replace hosts: %w", unknown)
	require.ErrorAs(t, wrapped, &classified)
	require.ErrorIs(t, wrapped, cause)

	// A definite store failure is never an unknown outcome, so a caller cannot
	// mistake a retryable conflict for a reopen-required indeterminate write.
	for name, definite := range map[string]error{
		"conflict":      ErrBrokerHostConflict,
		"locked":        ErrBrokerStoreLocked,
		"invalid state": ErrBrokerStoreInvalidState,
		"immutable":     ErrBrokerMembershipImmutable,
	} {
		t.Run(name, func(t *testing.T) {
			require.False(t, errors.As(definite, &classified))
			require.False(t, errors.As(fmt.Errorf("broker: replace hosts: %w", definite), &classified))
		})
	}
}
