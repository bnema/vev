package sessionwire

import (
	"testing"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/protocol/wire"
	"github.com/stretchr/testify/require"
)

func TestRouteRetiredServerMessage(t *testing.T) {
	message := protocol.RouteRetired{Ref: protocol.RouteRef{Key: 1, Generation: 2}, Target: protocol.ExactSessionTarget{LifecycleID: domain.SessionLifecycleID{3}, SessionName: "work"}}
	for _, value := range []protocol.ServerMessage{message, &message} {
		raw, err := EncodeServerMessage(value)
		require.NoError(t, err)
		decoded, err := DecodeServerEnvelope(raw)
		require.NoError(t, err)
		require.Equal(t, message, decoded)
		_, err = DecodeClientEnvelope(raw)
		require.Error(t, err)
	}
}

// TestRouteRetiredRequiresNonzeroReference restores validation parity: a
// retirement must carry the nonzero reference fence on both the encode and
// decode sides of the wire conversion.
func TestRouteRetiredRequiresNonzeroReference(t *testing.T) {
	exact := protocol.ExactSessionTarget{LifecycleID: domain.SessionLifecycleID{3}, SessionName: "work"}
	tests := []struct {
		name  string
		value protocol.RouteRetired
	}{
		{name: "zero reference", value: protocol.RouteRetired{Target: exact}},
		{name: "missing generation", value: protocol.RouteRetired{Ref: protocol.RouteRef{Key: 1}, Target: exact}},
		{name: "missing key", value: protocol.RouteRetired{Ref: protocol.RouteRef{Generation: 1}, Target: exact}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := routeRetiredToWire(tt.value)
			require.ErrorIs(t, err, protocol.ErrInvalidRouteWire)

			_, err = routeRetiredFromWire(&wire.RouteRetired{
				Ref:    &wire.RouteRef{Key: tt.value.Ref.Key, Generation: tt.value.Ref.Generation},
				Target: exactTargetToWire(&exact),
			})
			require.ErrorIs(t, err, protocol.ErrInvalidRouteWire)
		})
	}
}
