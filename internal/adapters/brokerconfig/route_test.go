package brokerconfig

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestParseRouteVariants pins the explicit bounded route union: a bare string
// is the original Unix path form, an object carries an explicit kind, and every
// kind/field mismatch or bound violation is refused.
func TestParseRouteVariants(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		raw     string
		wantErr string
		check   func(t *testing.T, route Route)
	}{
		{
			name: "bare string is a Unix route",
			raw:  `"/tmp/mux.sock"`,
			check: func(t *testing.T, route Route) {
				require.Equal(t, RouteUnix, route.Kind())
				require.Equal(t, "/tmp/mux.sock", route.Path())
				require.True(t, route.IsLocal())
			},
		},
		{
			name: "explicit unix route",
			raw:  `{"kind":"unix","path":"/tmp/mux.sock"}`,
			check: func(t *testing.T, route Route) {
				require.Equal(t, RouteUnix, route.Kind())
				require.Equal(t, "/tmp/mux.sock", route.Path())
			},
		},
		{
			name: "ssh stdio route with trust inputs",
			raw: `{"kind":"ssh-stdio","target":"user@host:2222","argv":["vev","_broker-mux-stdio","--offline-root","/srv/vev"],` +
				`"trust":{"knownHostsFile":"/etc/ssh/known_hosts","connectTimeout":"30s"}}`,
			check: func(t *testing.T, route Route) {
				require.Equal(t, RouteSSHStdio, route.Kind())
				require.Equal(t, "user@host:2222", route.Target())
				require.Equal(t, []string{"vev", "_broker-mux-stdio", "--offline-root", "/srv/vev"}, route.Argv())
				require.Equal(t, "/etc/ssh/known_hosts", route.KnownHostsFile())
				require.Equal(t, 30*time.Second, route.ConnectTimeout())
				require.False(t, route.IsLocal())
			},
		},
		{
			name: "ssh quic route without trust",
			raw:  `{"kind":"ssh-quic","target":"host","argv":["vev","_broker-mux-quic-bootstrap","--offline-root","/srv/vev"]}`,
			check: func(t *testing.T, route Route) {
				require.Equal(t, RouteSSHQUIC, route.Kind())
				require.Equal(t, "host", route.Target())
				require.Empty(t, route.KnownHostsFile())
				require.Zero(t, route.ConnectTimeout())
			},
		},
		{name: "empty route", raw: ``, wantErr: "empty"},
		{name: "null route", raw: `null`, wantErr: "path string or an object"},
		{name: "number route", raw: `42`, wantErr: "path string or an object"},
		{name: "unknown kind", raw: `{"kind":"carrier-pigeon","target":"h","argv":["x"]}`, wantErr: "invalid"},
		{name: "missing kind", raw: `{"path":"/tmp/mux.sock"}`, wantErr: "requires a kind"},
		{name: "unknown field", raw: `{"kind":"unix","path":"/tmp/mux.sock","extra":1}`, wantErr: "unknown field"},
		{name: "trailing json", raw: `{"kind":"unix","path":"/tmp/mux.sock"} {}`, wantErr: "trailing"},
		{name: "unix carries ssh fields", raw: `{"kind":"unix","path":"/tmp/mux.sock","target":"h"}`, wantErr: "must not carry ssh"},
		{name: "ssh without argv", raw: `{"kind":"ssh-stdio","target":"h"}`, wantErr: "argv"},
		{name: "ssh empty argv word", raw: `{"kind":"ssh-stdio","target":"h","argv":["vev",""]}`, wantErr: "empty"},
		{name: "ssh word control char", raw: `{"kind":"ssh-stdio","target":"h","argv":["vev","a\nb"]}`, wantErr: "control"},
		{name: "ssh target whitespace", raw: `{"kind":"ssh-stdio","target":"a b","argv":["vev"]}`, wantErr: "whitespace"},
		{name: "ssh target leading dash", raw: `{"kind":"ssh-stdio","target":"-oProxyCommand=x","argv":["vev"]}`, wantErr: "dash"},
		{name: "ssh target too long", raw: `{"kind":"ssh-stdio","target":"` + strings.Repeat("a", MaxRouteTargetBytes+1) + `","argv":["vev"]}`, wantErr: "exceeds"},
		{name: "too many argv words", raw: `{"kind":"ssh-stdio","target":"h","argv":[` + strings.Repeat(`"w",`, MaxRouteArgvWords) + `"w"]}`, wantErr: "words"},
		{name: "relative trust path", raw: `{"kind":"ssh-stdio","target":"h","argv":["vev"],"trust":{"knownHostsFile":"known_hosts"}}`, wantErr: "not absolute"},
		{name: "unclean trust path", raw: `{"kind":"ssh-stdio","target":"h","argv":["vev"],"trust":{"knownHostsFile":"/etc/ssh/../known_hosts"}}`, wantErr: "not clean"},
		{name: "space in trust path", raw: `{"kind":"ssh-stdio","target":"h","argv":["vev"],"trust":{"knownHostsFile":"/etc/ssh/known hosts"}}`, wantErr: "whitespace"},
		{name: "bad timeout duration", raw: `{"kind":"ssh-stdio","target":"h","argv":["vev"],"trust":{"connectTimeout":"soon"}}`, wantErr: "not a duration"},
		{name: "negative timeout", raw: `{"kind":"ssh-stdio","target":"h","argv":["vev"],"trust":{"connectTimeout":"-1s"}}`, wantErr: "positive"},
		{name: "excessive timeout", raw: `{"kind":"ssh-stdio","target":"h","argv":["vev"],"trust":{"connectTimeout":"1h"}}`, wantErr: "between"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			route, err := parseRoute(json.RawMessage(tt.raw))
			if tt.wantErr != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tt.wantErr)
				return
			}
			require.NoError(t, err)
			require.NoError(t, route.Validate())
			if tt.check != nil {
				tt.check(t, route)
			}
		})
	}
}

// TestRouteAddressIsOpaqueAndDeterministic proves the pool address leaks no
// route detail, is bounded, is stable across parses, and distinguishes distinct
// routes.
func TestRouteAddressIsOpaqueAndDeterministic(t *testing.T) {
	t.Parallel()

	unix, err := parseRoute(json.RawMessage(`{"kind":"unix","path":"/tmp/secret-dir/mux.sock"}`))
	require.NoError(t, err)
	stdio, err := parseRoute(json.RawMessage(`{"kind":"ssh-stdio","target":"user@secret-host","argv":["vev","_broker-mux-stdio","--offline-root","/srv/secret"]}`))
	require.NoError(t, err)
	quic, err := parseRoute(json.RawMessage(`{"kind":"ssh-quic","target":"user@secret-host","argv":["vev","_broker-mux-quic-bootstrap","--offline-root","/srv/secret"]}`))
	require.NoError(t, err)

	require.NotEmpty(t, unix.Address())
	require.LessOrEqual(t, len(unix.Address()), MaxRouteAddressBytes)
	require.Contains(t, unix.Address(), "offline-route-")

	for _, secret := range []string{"secret-host", "secret-dir", "/srv/secret", "_broker-mux", "mux.sock"} {
		for _, route := range []Route{unix, stdio, quic} {
			require.NotContains(t, route.Address(), secret, "address must not leak route detail")
		}
	}
	require.NotEqual(t, unix.Address(), stdio.Address())
	require.NotEqual(t, stdio.Address(), quic.Address())

	again, err := parseRoute(json.RawMessage(`{"kind":"ssh-stdio","target":"user@secret-host","argv":["vev","_broker-mux-stdio","--offline-root","/srv/secret"]}`))
	require.NoError(t, err)
	require.Equal(t, stdio.Address(), again.Address(), "address must be stable across parses")

	// A different trust input is a different route and therefore a different
	// address.
	trusted, err := parseRoute(json.RawMessage(`{"kind":"ssh-stdio","target":"user@secret-host","argv":["vev","_broker-mux-stdio","--offline-root","/srv/secret"],"trust":{"connectTimeout":"5s"}}`))
	require.NoError(t, err)
	require.NotEqual(t, stdio.Address(), trusted.Address())
}

// TestConfigLocalMuxRouteRules proves a helper only ever bridges to exactly one
// provisioned Unix route.
func TestConfigLocalMuxRouteRules(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	writeConfig(t, root, emptyRouteConfig(), 0o600)
	layout, err := ResolveLayout(root, nil)
	require.NoError(t, err)
	config, err := Load(layout)
	require.NoError(t, err)
	_, err = config.LocalMuxRoute()
	require.Error(t, err)
	require.Contains(t, err.Error(), "no Unix daemon-mux route")

	root = t.TempDir()
	writeConfig(t, root, routeConfig(
		map[string]any{"kind": "unix", "path": "/tmp/one.sock"},
		map[string]any{"kind": "unix", "path": "/tmp/two.sock"},
	), 0o600)
	layout, err = ResolveLayout(root, nil)
	require.NoError(t, err)
	config, err = Load(layout)
	require.NoError(t, err)
	_, err = config.LocalMuxRoute()
	require.Error(t, err)
	require.Contains(t, err.Error(), "more than one")

	root = t.TempDir()
	writeConfig(t, root, routeConfig(map[string]any{"kind": "unix", "path": "/tmp/one.sock"}), 0o600)
	layout, err = ResolveLayout(root, nil)
	require.NoError(t, err)
	config, err = Load(layout)
	require.NoError(t, err)
	route, err := config.LocalMuxRoute()
	require.NoError(t, err)
	require.Equal(t, "/tmp/one.sock", route.Path())
}

// emptyRouteConfig renders a marker-valid document with no registrations.
func emptyRouteConfig() map[string]any {
	return map[string]any{"marker": Marker, "registrations": []any{}}
}

// routeConfig renders one marker-valid document whose registrations carry the
// supplied explicit route objects. Each registration gets a distinct endpoint
// and authenticated identity, so a document with several routes exercises the
// route rules rather than the one-route-per-pooled-identity rule.
func routeConfig(routes ...map[string]any) map[string]any {
	registrations := make([]any, 0, len(routes))
	for i, route := range routes {
		registrations = append(registrations, map[string]any{
			"endpoint":    testEndpoint + "-" + string(rune('a'+i)),
			"incarnation": "0102030405060708090a0b0c0d0e0f10",
			"generation":  1,
			"identity":    "offline-daemon-" + string(rune('a'+i)),
			"route":       route,
			"policy": map[string]any{
				"protocolVersion":      1,
				"catalogSchemaVersion": 3,
				"environmentPolicy":    "client-owned",
				"transport":            "offline",
				"trust":                "offline-trust",
				"launch":               "offline-launch",
				"isolation":            "offline-isolation",
			},
		})
	}
	return map[string]any{"marker": Marker, "registrations": registrations}
}
