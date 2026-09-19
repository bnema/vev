package daemonmux

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestOpenFailureReturnsNilInterface proves a failed open hands back an
// interface that compares equal to nil. The logical connector returns a concrete
// *LogicalConnection, so returning that value directly on the error path wraps a
// nil pointer in a non-nil interface: a caller that checks the interface against
// nil then calls a method on a nil receiver instead of reporting the failure.
func TestOpenFailureReturnsNilInterface(t *testing.T) {
	// A connector without a pump fails every open with ErrLogicalConfig, which
	// is the failure shape that used to return a typed nil.
	physical := &PhysicalConnection{logical: &LogicalConnector{}, policy: logicalTestPolicy()}

	connection, err := physical.OpenStream(context.Background(), controlRequest(1))
	require.Error(t, err)
	require.True(t, connection == nil, "an open failure must not return a typed-nil connection")
}

// TestNilLogicalConnectionCloseIsSafe proves the receiver guard: a caller that
// received a typed-nil connection from anywhere can still release it, exactly as
// a physical connection can.
func TestNilLogicalConnectionCloseIsSafe(t *testing.T) {
	var connection *LogicalConnection
	require.NoError(t, connection.Close())
}
