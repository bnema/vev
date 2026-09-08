package client

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/protocol"
)

// TestNavigationSelectionCaptureIsLedgerDetached pins the fallback-capture
// contract the Run loop relies on: prior and selected routes returned for a
// creation or recent-route handoff are copies detached from ledger state, so
// later request rebasing (committed identity, cursor, tab memory) cannot
// corrupt the recorded return route. P3 migrates the bookkeeping to a shared
// transition but must preserve these detachment facts.
func TestNavigationSelectionCaptureIsLedgerDetached(t *testing.T) {
	setup := func(t *testing.T) (*routeLedger, routeIdentity, routeGeneration) {
		t.Helper()
		ledger := newRouteLedger()
		first, err := ledger.commit(routeTestCandidate(1, protocol.RouteOriginLocal))
		require.NoError(t, err)
		second, err := ledger.commit(routeTestCandidate(2, protocol.RouteOriginLocal))
		require.NoError(t, err)
		require.NotEqual(t, first, second)
		generation := routeGeneration(ledger.snapshot().Generation)
		return ledger, first, generation
	}

	tests := []struct {
		name    string
		capture func(t *testing.T, ledger *routeLedger, selected routeIdentity, generation routeGeneration) (prior, current routeRecord)
	}{
		{name: "recent", capture: func(t *testing.T, ledger *routeLedger, selected routeIdentity, generation routeGeneration) (routeRecord, routeRecord) {
			t.Helper()
			selection, ok := ledger.navigationSelection(selected.wireNavigationAction(generation))
			require.True(t, ok)
			require.False(t, selection.noOp)
			return selection.prior, selection.selected
		}},
		{name: "creation", capture: func(t *testing.T, ledger *routeLedger, selected routeIdentity, generation routeGeneration) (routeRecord, routeRecord) {
			t.Helper()
			selection, ok := ledger.creationSelection(protocol.RouteCreateSessionAction{
				RequestID: 9, SnapshotGeneration: uint64(generation),
				Key: uint64(selected.key), Generation: uint64(selected.generation), SessionName: "example",
			})
			require.True(t, ok)
			return selection.prior, selection.selected
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ledger, selected, generation := setup(t)
			before := ledger.snapshot()
			prior, current := tt.capture(t, ledger, selected, generation)

			prior.request.SessionName = "mutated-prior"
			prior.request.Environment = append(prior.request.Environment, "MUTATED=1")
			current.request.SessionName = "mutated-selected"
			require.Equal(t, before, ledger.snapshot(), "captured fallback routes must not alias ledger state")
		})
	}
}

// TestNavigationSelectionRejectsStaleCapture pins the prevalidation contract:
// a selection against a superseded snapshot generation is rejected before
// any fallback is captured, so a stale palette action keeps the current
// attachment intact.
func TestNavigationSelectionRejectsStaleCapture(t *testing.T) {
	ledger := newRouteLedger()
	selected, err := ledger.commit(routeTestCandidate(1, protocol.RouteOriginLocal))
	require.NoError(t, err)
	staleGeneration := routeGeneration(ledger.snapshot().Generation)
	_, err = ledger.commit(routeTestCandidate(2, protocol.RouteOriginLocal))
	require.NoError(t, err)

	_, ok := ledger.navigationSelection(selected.wireNavigationAction(staleGeneration))
	require.False(t, ok)
	_, ok = ledger.creationSelection(protocol.RouteCreateSessionAction{
		RequestID: 9, SnapshotGeneration: uint64(staleGeneration),
		Key: uint64(selected.key), Generation: uint64(selected.generation), SessionName: "example",
	})
	require.False(t, ok)
}
