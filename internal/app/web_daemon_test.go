package app

import (
	"context"
	"os"
	"testing"

	"github.com/bnema/vev/internal/adapters/webterm"
	"github.com/bnema/vev/internal/platform"
	"github.com/stretchr/testify/require"
)

func TestWebControlTokenLifetime(t *testing.T) {
	t.Setenv("XDG_STATE_HOME", t.TempDir())
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	token, err := webterm.NewToken()
	require.NoError(t, err)
	settings, err := webterm.ParseSettings("", "https://terminal.example.internal")
	require.NoError(t, err)
	server, err := webterm.NewServer(ctx, settings, token, func(context.Context, *webterm.Terminal) error { return nil })
	require.NoError(t, err)
	listener, err := startWebControl(ctx, server, settings)
	require.NoError(t, err)
	defer listener.Close()
	got, err := webControlRequest(ctx, false)
	require.NoError(t, err)
	require.Equal(t, token, got.Token)
	require.Equal(t, settings, got.Settings)
	renewed, err := webControlRequest(ctx, true)
	require.NoError(t, err)
	require.NotEqual(t, token, renewed.Token)
	require.Equal(t, settings, renewed.Settings)
	got, err = webControlRequest(ctx, false)
	require.NoError(t, err)
	require.Equal(t, renewed, got)
	_, err = os.Stat(platform.StateDir())
	require.True(t, os.IsNotExist(err), "control must not persist a credential")
}
