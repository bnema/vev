package daemon

import (
	"errors"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSnapshotFailureSignatureIsStableAndNilSafe(t *testing.T) {
	t.Parallel()

	first := &os.PathError{Op: "write", Path: "/private/first", Err: os.ErrPermission}
	second := &os.PathError{Op: "write", Path: "/volatile/second", Err: os.ErrPermission}

	require.Equal(t, snapshotFailureSignature("publish", first), snapshotFailureSignature("publish", second))
	require.NotContains(t, snapshotFailureSignature("publish", first), "/private/first")
	require.NotContains(t, snapshotFailureSignature("publish", nil), "%!w(<nil>)")
	require.NotEqual(t, snapshotFailureSignature("publish", first), snapshotFailureSignature("encode", first))
	require.NotEqual(t, snapshotFailureSignature("publish", first), snapshotFailureSignature("publish", errors.New("other")))
}
