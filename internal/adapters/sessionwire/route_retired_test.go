package sessionwire

import (
	"testing"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/protocol"
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
