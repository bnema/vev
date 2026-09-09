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
		frame, err := encodeServer(value)
		require.NoError(t, err)
		require.Equal(t, wire.MsgRouteRetired, frame.Type)
		decoded, err := decodeServer(frame)
		require.NoError(t, err)
		require.Equal(t, message, decoded)
		_, err = decodeClient(frame)
		require.ErrorIs(t, err, ErrWrongDirection)
	}
}
