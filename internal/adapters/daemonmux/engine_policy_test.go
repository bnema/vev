package daemonmux

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/ports"
)

// TestStreamEngineRestrictAdmissions proves the accepted physical policy seam:
// it is configured once before any admission, refuses a mismatched fresh Open
// before that Open records anything, leaves a replayed identity classified as a
// duplicate rather than a policy refusal, and admits a matching Open.
func TestStreamEngineRestrictAdmissions(t *testing.T) {
	engine := NewStreamEngine()
	require.NoError(t, engine.RestrictAdmissions(testPolicy()))
	require.ErrorIs(t, engine.RestrictAdmissions(testPolicy()), ErrStreamState, "the policy is fixed once")

	mismatched := openFor(1)
	mismatched.Policy = alternatePolicy(testPolicy())
	require.ErrorIs(t, engine.Open(mismatched), ErrOpenPolicyRefused)
	_, tracked := engine.Status(1)
	require.False(t, tracked, "a policy refusal records no stream")
	require.Zero(t, engine.Live())

	matching := openFor(1)
	matching.Policy = testPolicy()
	require.NoError(t, engine.Open(matching))
	require.Equal(t, 1, engine.Live())

	// A duplicate identity is still fenced by reference/identity first, so a
	// mismatched replay never disturbs the live stream it names.
	replay := openFor(1)
	replay.Policy = alternatePolicy(testPolicy())
	require.ErrorIs(t, engine.Open(replay), ErrStreamIDReused)
	require.Equal(t, 1, engine.Live())
}

// TestStreamEngineRestrictAdmissionsGuards proves the seam refuses an invalid
// policy, a terminal engine, and a nil receiver, and that a policy gate is
// optional: an unrestricted engine admits any validly encoded Open.
func TestStreamEngineRestrictAdmissionsGuards(t *testing.T) {
	var nilEngine *StreamEngine
	require.ErrorIs(t, nilEngine.RestrictAdmissions(testPolicy()), ErrPhysicalClosed)

	engine := NewStreamEngine()
	require.Error(t, engine.RestrictAdmissions(ports.BrokerPolicy{}), "an invalid policy is refused")

	unrestricted := NewStreamEngine()
	require.NoError(t, unrestricted.Open(openFor(1)))

	terminated := NewStreamEngine()
	terminated.Terminate()
	require.ErrorIs(t, terminated.RestrictAdmissions(testPolicy()), ErrPhysicalClosed)
}
