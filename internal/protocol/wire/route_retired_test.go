package wire

import (
	"encoding/hex"
	"testing"

	"github.com/bnema/vev/internal/protocol"
	"github.com/stretchr/testify/require"
)

func TestRouteRetiredCodec(t *testing.T) {
	want := protocol.RouteRetired{Ref: protocol.RouteRef{Key: 7, Generation: 3}, Target: testExactTarget()}
	encoded, err := MarshalRouteRetired(want)
	require.NoError(t, err)
	require.Equal(t, "00000000000000070000000000000003010203000000000000000000000000000004776f726b", hex.EncodeToString(encoded))
	got, err := UnmarshalRouteRetired(encoded)
	require.NoError(t, err)
	require.Equal(t, want, got)
	assertAllPrefixesFail(t, encoded, UnmarshalRouteRetired)
	_, err = UnmarshalRouteRetired(append(append([]byte(nil), encoded...), 0))
	require.Error(t, err)
	for _, message := range []protocol.RouteRetired{{}, {Ref: want.Ref}, {Target: want.Target}, {Ref: protocol.RouteRef{Key: 1}, Target: want.Target}} {
		_, err := MarshalRouteRetired(message)
		require.Error(t, err)
	}
}
