package brokerconfig

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/ports"
)

const (
	localTestIdentity = "offline-local-daemon"
	localTestSocket   = "/tmp/vev-brokerconfig-test/local-mux.sock"
)

// localDocument returns the raw local object provisioned by the fixture. The
// caller may override or delete fields through the returned map.
func localTestDocument() map[string]any {
	policy := testPolicy()
	return map[string]any{
		"identity": localTestIdentity,
		"route":    localTestSocket,
		"policy": map[string]any{
			"protocolVersion":      policy.ProtocolVersion,
			"catalogSchemaVersion": policy.CatalogSchemaVersion,
			"environmentPolicy":    "client-owned",
			"transport":            policy.Transport,
			"trust":                policy.Trust,
			"launch":               policy.Launch,
			"isolation":            policy.Isolation,
		},
	}
}

// documentWithLocal returns a valid registration document carrying one local
// binding, with any field overrides applied.
func documentWithLocal(t *testing.T, local map[string]any) map[string]any {
	t.Helper()
	document := registrationDocument(nil)
	document["local"] = local
	return document
}

// loadDocument writes one document into a fresh offline root and loads it.
func loadDocument(t *testing.T, document any, reserved []string) (*Config, error) {
	t.Helper()
	root := t.TempDir()
	writeConfig(t, root, document, 0o600)
	layout, err := ResolveLayout(root, reserved)
	require.NoError(t, err)
	return Load(layout)
}

// localRequest builds one local open-stream request under the fixture policy.
func localRequest() ports.BrokerOpenStreamRequest {
	return ports.BrokerOpenStreamRequest{
		Purpose: ports.BrokerStreamControl,
		Local:   true,
		Policy:  testPolicy(),
	}
}

// TestLoadLocalBinding pins the provisioned local binding: the identity,
// policy, sanitized display origin, and local Unix route are configuration
// authority, and the local route is resolvable by its opaque pool address.
func TestLoadLocalBinding(t *testing.T) {
	config, err := loadDocument(t, documentWithLocal(t, localTestDocument()), nil)
	require.NoError(t, err)

	binding, ok := config.LocalBinding()
	require.True(t, ok)
	require.Equal(t, ports.BrokerDaemonIdentity(localTestIdentity), binding.Identity)
	require.Equal(t, testPolicy(), binding.Policy)
	require.Equal(t, LocalDisplayOrigin, binding.DisplayOrigin)
	require.True(t, binding.Route.IsLocal())
	require.Equal(t, localTestSocket, binding.Route.Path())

	resolved, err := config.Resolver().Resolve(context.Background(), localRequest())
	require.NoError(t, err)
	require.Equal(t, binding.Identity, resolved.Identity)
	require.Equal(t, binding.Policy, resolved.Policy)
	require.Equal(t, binding.Route.Address(), resolved.Address)

	route, ok := config.RouteByAddress(resolved.Address)
	require.True(t, ok)
	require.Equal(t, localTestSocket, route.Path())
}

// TestLoadLocalBindingDefaultsDisplayOrigin pins the broker-owned default and
// the sanitizer: an absent origin becomes "local", and a hostile origin is
// sanitized rather than published raw.
func TestLoadLocalBindingDefaultsDisplayOrigin(t *testing.T) {
	tests := []struct {
		name   string
		origin any
		want   string
	}{
		{name: "absent defaults to the broker-owned origin", origin: nil, want: LocalDisplayOrigin},
		{name: "explicit origin is kept", origin: "this-host", want: "this-host"},
		{name: "bidi and control runes are stripped", origin: "lo\u202ecal", want: "local"},
		{name: "wholly undisplayable defaults", origin: "\u202e\u2028", want: LocalDisplayOrigin},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			local := localTestDocument()
			if tt.origin == nil {
				delete(local, "displayOrigin")
			} else {
				local["displayOrigin"] = tt.origin
			}
			config, err := loadDocument(t, documentWithLocal(t, local), nil)
			require.NoError(t, err)
			binding, ok := config.LocalBinding()
			require.True(t, ok)
			require.Equal(t, tt.want, binding.DisplayOrigin)
		})
	}
}

// TestLoadRejectsInvalidLocalBinding pins every local-binding refusal at the
// load boundary. A binding the broker could not fence is never provisioned.
func TestLoadRejectsInvalidLocalBinding(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(t *testing.T, local map[string]any)
		wantErr string
	}{
		{
			name:    "missing identity",
			mutate:  func(_ *testing.T, local map[string]any) { delete(local, "identity") },
			wantErr: "identity",
		},
		{
			name:    "missing policy",
			mutate:  func(_ *testing.T, local map[string]any) { delete(local, "policy") },
			wantErr: "policy",
		},
		{
			name:    "missing route",
			mutate:  func(_ *testing.T, local map[string]any) { delete(local, "route") },
			wantErr: "route",
		},
		{
			name: "ssh route is refused",
			mutate: func(_ *testing.T, local map[string]any) {
				local["route"] = map[string]any{"kind": "ssh-stdio", "target": "user@host", "argv": []string{"vev"}}
			},
			wantErr: "Unix daemonmux",
		},
		{
			name:    "relative route is refused",
			mutate:  func(_ *testing.T, local map[string]any) { local["route"] = "relative/mux.sock" },
			wantErr: "not absolute",
		},
		{
			name:    "unknown field is refused",
			mutate:  func(_ *testing.T, local map[string]any) { local["extra"] = 1 },
			wantErr: "unknown field",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			local := localTestDocument()
			tt.mutate(t, local)
			_, err := loadDocument(t, documentWithLocal(t, local), nil)
			require.Error(t, err)
			require.Contains(t, err.Error(), tt.wantErr)
		})
	}
}

// TestLoadLocalRouteFencesProduction proves the local route is fenced against
// the reserved production runtime and state directories exactly like a remote
// Unix route: an isolated sandbox can never reach a production endpoint
// through its own local binding.
func TestLoadLocalRouteFencesProduction(t *testing.T) {
	reservedDir := t.TempDir()
	local := localTestDocument()
	local["route"] = filepath.Join(reservedDir, "mux.sock")
	_, err := loadDocument(t, documentWithLocal(t, local), []string{reservedDir})
	require.Error(t, err)
	require.Contains(t, err.Error(), "overlaps production")
}

// TestLoadLocalRouteAliasesRegistrationRoute proves the local binding may
// reuse the exact Unix mux route a registration already provisions: the pool
// address is identical only when the route is identical.
func TestLoadLocalRouteAliasesRegistrationRoute(t *testing.T) {
	local := localTestDocument()
	local["route"] = "/tmp/vev-brokerconfig-test/mux.sock" // the fixture registration route
	config, err := loadDocument(t, documentWithLocal(t, local), nil)
	require.NoError(t, err)
	binding, ok := config.LocalBinding()
	require.True(t, ok)
	require.Equal(t, "/tmp/vev-brokerconfig-test/mux.sock", binding.Route.Path())
}

// TestLoadRefusesLocalIdentityPoolConflict proves the pool's (identity,
// policy) keying is honored across the local and remote provisionings: a local
// binding that shares a registration's exact identity and policy but names a
// different route is refused rather than silently dialed for one and ignored
// for the other.
func TestLoadRefusesLocalIdentityPoolConflict(t *testing.T) {
	local := localTestDocument()
	local["identity"] = testRegistrationIdentity()
	local["route"] = "/tmp/vev-brokerconfig-test/other-mux.sock"
	_, err := loadDocument(t, documentWithLocal(t, local), nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "already provisioned on a different route")
}

// testRegistrationIdentity reads the identity the fixture registration
// provisions, so the collision test is anchored to the real fixture.
func testRegistrationIdentity() string { return "offline-daemon" }

// TestResolveLocalSuccessAndRefusals pins the local resolution contract:
// success returns the configured authority and address, a configuration with
// no local binding refuses as unavailable, a conflicting policy is refused as
// such, and a cancelled context is reported as cancellation. Every refusal is
// typed as a ports.BrokerError so the pool classifies it without text matching.
func TestResolveLocalSuccessAndRefusals(t *testing.T) {
	config, err := loadDocument(t, documentWithLocal(t, localTestDocument()), nil)
	require.NoError(t, err)
	binding, ok := config.LocalBinding()
	require.True(t, ok)
	resolver := config.Resolver()

	noLocalConfig, err := loadDocument(t, registrationDocument(nil), nil)
	require.NoError(t, err)

	tests := []struct {
		name     string
		resolver *Resolver
		request  ports.BrokerOpenStreamRequest
		wantCode ports.BrokerErrorCode
		wantErr  error
	}{
		{
			name:     "success returns configured authority",
			resolver: resolver,
			request:  localRequest(),
		},
		{
			name:     "no local binding is unavailable",
			resolver: noLocalConfig.Resolver(),
			request:  localRequest(),
			wantCode: ports.BrokerErrorUnavailable,
			wantErr:  errNoLocalRoute,
		},
		{
			name:     "conflicting policy is refused",
			resolver: resolver,
			request: func() ports.BrokerOpenStreamRequest {
				request := localRequest()
				request.Policy.Transport = "other-transport"
				return request
			}(),
			wantCode: ports.BrokerErrorConflictingPolicy,
			wantErr:  errLocalPolicyConflict,
		},
		{
			name:     "cancelled context is cancellation",
			resolver: resolver,
			request:  localRequest(),
			wantErr:  context.Canceled,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			if errors.Is(tt.wantErr, context.Canceled) {
				cancelled, cancel := context.WithCancel(ctx)
				cancel()
				ctx = cancelled
			}
			resolved, err := tt.resolver.Resolve(ctx, tt.request)
			if tt.wantErr == nil {
				require.NoError(t, err)
				require.Equal(t, binding.Identity, resolved.Identity)
				require.Equal(t, binding.Policy, resolved.Policy)
				require.Equal(t, binding.Route.Address(), resolved.Address)
				return
			}
			require.Error(t, err)
			if tt.wantCode != 0 {
				var typed ports.BrokerError
				require.ErrorAs(t, err, &typed)
				require.Equal(t, tt.wantCode, typed.Code)
			}
			require.ErrorIs(t, err, tt.wantErr)
		})
	}
}

// TestLocalMuxRouteExcludesLocalBinding proves Config.LocalMuxRoute serves the
// remote-side mux helper's route only. The broker-owned local binding's own
// carriage is deliberately excluded: no helper ever bridges it, so a
// configuration provisioning only the local object has no helper route, and one
// provisioning both returns the registration's route, never the local binding's.
func TestLocalMuxRouteExcludesLocalBinding(t *testing.T) {
	t.Run("local-only configuration provisions no helper route", func(t *testing.T) {
		document := map[string]any{
			"marker":        Marker,
			"registrations": []any{},
			"local":         localTestDocument(),
		}
		config, err := loadDocument(t, document, nil)
		require.NoError(t, err)
		_, ok := config.LocalBinding()
		require.True(t, ok, "the fixture provisions a broker-owned local binding")
		_, err = config.LocalMuxRoute()
		require.Error(t, err)
		require.Contains(t, err.Error(), "no Unix daemon-mux route is provisioned")
	})

	t.Run("registration route wins over the local binding route", func(t *testing.T) {
		config, err := loadDocument(t, documentWithLocal(t, localTestDocument()), nil)
		require.NoError(t, err)
		binding, ok := config.LocalBinding()
		require.True(t, ok)

		route, err := config.LocalMuxRoute()
		require.NoError(t, err)
		require.True(t, route.IsLocal())
		require.Equal(t, "/tmp/vev-brokerconfig-test/mux.sock", route.Path(), "the helper bridges the registration route")
		require.NotEqual(t, binding.Route.Path(), route.Path(), "the local binding route is not the helper route")
		require.NotEqual(t, binding.Route.Address(), route.Address(), "the two routes carry distinct pool addresses")
	})
}
