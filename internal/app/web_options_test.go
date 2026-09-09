package app

import (
	"context"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/bnema/vev/internal/adapters/webterm"
	"github.com/bnema/vev/pkg/safedir"
	"github.com/stretchr/testify/require"
)

func TestWebFlags(t *testing.T) {
	for _, tt := range []struct {
		args []string
		want webOptions
		fail bool
	}{
		{[]string{"--web-daemon"}, webOptions{}, false},
		{[]string{"--web-daemon", "--web-listen", "127.0.0.1:9000", "--web-origin=https://terminal.example.internal"}, webOptions{"127.0.0.1:9000", "https://terminal.example.internal"}, false},
		{[]string{"--web-daemon", "--web-listen"}, webOptions{}, true},
		{[]string{"--web-daemon", "--web-origin="}, webOptions{}, true},
		{[]string{"--web-daemon", "--web-listen", "--web-origin=x"}, webOptions{}, true},
		{[]string{"--web-daemon", "extra"}, webOptions{}, true},
		{[]string{"--web-renew-token", "--web-origin=x"}, webOptions{}, true},
	} {
		t.Run(strings.Join(tt.args, " "), func(t *testing.T) {
			got, err := parseArgs(tt.args)
			if tt.fail {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, kindWebDaemon, got.kind)
			require.Equal(t, tt.want, got.web)
		})
	}
}

func TestWebConfigPrecedence(t *testing.T) {
	root := t.TempDir()
	t.Setenv("XDG_CONFIG_HOME", root)
	dir := filepath.Join(root, "vev")
	require.NoError(t, safedir.EnsurePrivate(dir))
	path := filepath.Join(dir, "config")
	got, err := loadWebSettings(webOptions{})
	require.NoError(t, err)
	require.Equal(t, webterm.Settings{Listen: webterm.Address, Origin: webterm.Origin}, got)
	for _, tt := range []struct {
		name, config   string
		flags          webOptions
		listen, origin string
		fail           bool
	}{
		{"persisted", "web.listen = 127.0.0.1:9000\nweb.origin = https://terminal.example.internal\n", webOptions{}, "127.0.0.1:9000", "https://terminal.example.internal", false},
		{"flags", "web.listen = invalid\nweb.origin = invalid\n", webOptions{"127.0.0.1:9001", "http://localhost:9001"}, "127.0.0.1:9001", "http://localhost:9001", false},
		{"invalid config", "web.listen = invalid\n", webOptions{}, "", "", true},
		{"explicit exposure", "web.listen = 0.0.0.0:9000\n", webOptions{}, "", "", true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			require.NoError(t, os.WriteFile(path, []byte(tt.config), 0600))
			got, err := loadWebSettings(tt.flags)
			if tt.fail {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, webterm.Settings{Listen: tt.listen, Origin: tt.origin}, got)
		})
	}
}

func TestWebReadinessUsesLocalListener(t *testing.T) {
	token, err := webterm.NewToken()
	require.NoError(t, err)
	settings, err := webterm.ParseSettings("", "https://unresolvable.example.invalid")
	require.NoError(t, err)
	handler, err := webterm.NewServer(t.Context(), settings, token, func(context.Context, *webterm.Terminal) error { return nil })
	require.NoError(t, err)
	server := httptest.NewServer(handler)
	defer server.Close()
	settings.Listen = server.Listener.Addr().String()
	access := webAccess{Token: token, Settings: settings}
	t.Setenv("HTTP_PROXY", "http://unresolvable.example.invalid")
	require.True(t, webReachable(t.Context(), access, settings))
	other := settings
	other.Origin = "https://other.example.invalid"
	require.False(t, webReachable(t.Context(), access, other), "a healthy different gateway must not satisfy readiness")
	other = settings
	other.Listen = "127.0.0.1:1"
	require.False(t, webReachable(t.Context(), access, other), "the requested listener must match too")
	access.Token = "wrong"
	require.False(t, webReachable(t.Context(), access, settings))
}
