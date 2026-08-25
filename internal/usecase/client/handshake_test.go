package client

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/ports"
)

type failedContextDialer struct {
	ctx context.Context
}

func (d *failedContextDialer) Dial(ctx context.Context) (ports.Transport, error) {
	d.ctx = ctx
	return nil, errors.New("dial failed")
}

func TestBoundedDialCancelsPrivateContextAfterFailure(t *testing.T) {
	dialer := &failedContextDialer{}
	_, err := boundedDial(context.Background(), dialer)
	require.ErrorContains(t, err, "dial failed")
	require.NotNil(t, dialer.ctx)
	require.ErrorIs(t, dialer.ctx.Err(), context.Canceled)
}
