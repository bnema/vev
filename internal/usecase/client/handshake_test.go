package client

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/ports"
	portsmocks "github.com/bnema/vev/internal/ports/mocks"
	"github.com/bnema/vev/internal/protocol"
)

// boundedDial is retained only for this old timeout-contract test; the
// autonomous supervisor opens through the broker instead.
func boundedDial(ctx context.Context, dialer ports.ClientDialer) (ports.ClientConnection, error) {
	bounded, _, finish := newBoundedContext(ctx, systemClock{}, protocol.HandshakeTimeout)
	defer finish()
	return dialer.Dial(bounded)
}

func TestBoundedDialCancelsPrivateContextAfterFailure(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var dialCtx context.Context
		dialer := portsmocks.NewMockClientDialer(t)
		dialer.EXPECT().Dial(mock.Anything).RunAndReturn(func(ctx context.Context) (ports.ClientConnection, error) {
			dialCtx = ctx
			return nil, errors.New("dial failed")
		})
		_, err := boundedDial(context.Background(), dialer)
		require.ErrorContains(t, err, "dial failed")
		require.NotNil(t, dialCtx)
		require.ErrorIs(t, dialCtx.Err(), context.Canceled)
	})
}
