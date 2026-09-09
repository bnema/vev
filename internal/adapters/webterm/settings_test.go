package webterm

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestWebSettings(t *testing.T) {
	for _, tt := range []struct {
		name, listen, origin, wantListen, wantOrigin string
	}{
		{"defaults", "", "", Address, Origin},
		{"loopback port", "127.0.0.1:9000", "", "127.0.0.1:9000", "http://127.0.0.1:9000"},
		{"IPv6", "[::1]:9000", "", "[::1]:9000", "http://[::1]:9000"},
		{"proxy", "", "https://Terminal.example.internal:443/", Address, "https://terminal.example.internal"},
		{"private HTTP", "100.64.0.10:8778", "http://100.64.0.10:8778", "100.64.0.10:8778", "http://100.64.0.10:8778"},
		{"wildcard explicit", "0.0.0.0:9000", "https://terminal.example.internal", "0.0.0.0:9000", "https://terminal.example.internal"},
		{"IPv6 proxy", "[::]:9000", "https://[fd00::1]:9443", "[::]:9000", "https://[fd00::1]:9443"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseSettings(tt.listen, tt.origin)
			require.NoError(t, err)
			require.Equal(t, Settings{Listen: tt.wantListen, Origin: tt.wantOrigin}, got)
		})
	}
	for _, listen := range []string{"localhost:8778", ":8778", "127.0.0.1:0", "127.0.0.1:65536", "127.0.0.1:http", "[fe80::1%eth0]:8778", "224.0.0.1:8778"} {
		t.Run(listen, func(t *testing.T) {
			_, err := ParseSettings(listen, Origin)
			require.Error(t, err)
		})
	}
	for _, origin := range []string{"//example.internal", "ftp://example.internal", "https://user@example.internal", "https://example.internal/path", "https://example.internal?", "https://example.internal#", "https://example.internal:0", "https://example.internal:65536", "https://example.internal:", "https://*.example.internal", "https://0.0.0.0", "https://[::]", "https://bad_host", "https://example.internal/%2f", "https://[fe80::1%25eth0]"} {
		t.Run(origin, func(t *testing.T) {
			_, err := ParseSettings("", origin)
			require.Error(t, err)
		})
	}
	for _, listen := range []string{"0.0.0.0:8778", "[::]:8778", "100.64.0.10:8778"} {
		_, err := ParseSettings(listen, "")
		require.Error(t, err)
	}
}

func TestWebProbeAddressZeroValue(t *testing.T) {
	var settings Settings
	require.Equal(t, "", settings.ProbeAddress(), "zero-value settings must not panic")
	settings.Listen = "not-an-address"
	require.Equal(t, "", settings.ProbeAddress())
}

func TestWebProbeAddress(t *testing.T) {
	for _, tt := range []struct{ listen, want string }{
		{"0.0.0.0:9000", "127.0.0.1:9000"},
		{"[::]:9000", "[::1]:9000"},
		{"127.0.0.2:9000", "127.0.0.2:9000"},
	} {
		t.Run(tt.listen, func(t *testing.T) {
			settings, err := ParseSettings(tt.listen, "https://terminal.example.internal")
			require.NoError(t, err)
			require.Equal(t, tt.want, settings.ProbeAddress())
		})
	}
}
