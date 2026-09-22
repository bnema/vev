package client

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol"
)

type failedContextDialer struct {
	ctx context.Context
}

func (d *failedContextDialer) Dial(ctx context.Context) (ports.ClientConnection, error) {
	d.ctx = ctx
	return nil, errors.New("dial failed")
}

// boundedDial is retained only for this old timeout-contract test; the
// autonomous supervisor opens through the broker instead.
func boundedDial(ctx context.Context, dialer ports.ClientDialer) (ports.ClientConnection, error) {
	bounded, _, finish := newBoundedContext(ctx, systemClock{}, protocol.HandshakeTimeout)
	defer finish()
	return dialer.Dial(bounded)
}

func TestBoundedDialCancelsPrivateContextAfterFailure(t *testing.T) {
	dialer := &failedContextDialer{}
	_, err := boundedDial(context.Background(), dialer)
	require.ErrorContains(t, err, "dial failed")
	require.NotNil(t, dialer.ctx)
	require.ErrorIs(t, dialer.ctx.Err(), context.Canceled)
}
