package brokerconfig

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol"
)

const (
	testIncarnation = "0102030405060708090a0b0c0d0e0f10"
	testEndpoint    = "offline@daemon:2222"
)

// testPolicy is one fully valid, exact policy for the offline sandbox.
func testPolicy() ports.BrokerPolicy {
	return ports.BrokerPolicy{
		ProtocolVersion:      protocol.Version,
		CatalogSchemaVersion: 3,
		EnvironmentPolicy:    protocol.EnvironmentPolicyClientOwned,
		Transport:            "unix-mux",
		Trust:                "offline-trust",
		Launch:               "offline-launch",
		Isolation:            "offline-isolation",
	}
}

// registrationDocument renders one registration with the supplied overrides.
func registrationDocument(overrides map[string]any) map[string]any {
	entry := map[string]any{
		"endpoint":    testEndpoint,
		"incarnation": testIncarnation,
		"generation":  1,
		"identity":    "offline-daemon",
		"route":       "/tmp/vev-brokerconfig-test/mux.sock",
		"policy": map[string]any{
			"protocolVersion":      testPolicy().ProtocolVersion,
			"catalogSchemaVersion": testPolicy().CatalogSchemaVersion,
			"environmentPolicy":    "client-owned",
			"transport":            testPolicy().Transport,
			"trust":                testPolicy().Trust,
			"launch":               testPolicy().Launch,
			"isolation":            testPolicy().Isolation,
		},
	}
	for key, value := range overrides {
		entry[key] = value
	}
	return map[string]any{
		"marker":        Marker,
		"registrations": []any{entry},
	}
}

// writeConfig marshals one document into root/config.json with owner-only
// permissions.
func writeConfig(t *testing.T, root string, document any, perm os.FileMode) {
	t.Helper()
	raw, err := json.Marshal(document)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(root, ConfigFileName), raw, perm))
}

func TestResolveLayout(t *testing.T) {
	base := t.TempDir()
	linkParent := filepath.Join(t.TempDir(), "link")
	require.NoError(t, os.Symlink(base, linkParent))

	tests := []struct {
		name     string
		root     string
		reserved []string
		wantErr  string
	}{
		{name: "valid absolute clean root", root: filepath.Join(base, "sandbox")},
		{name: "empty root", root: "", wantErr: "empty"},
		{name: "relative root", root: "sandbox", wantErr: "not absolute"},
		{name: "unclean root", root: base + "/sandbox/../sandbox", wantErr: "not clean"},
		{name: "filesystem root", root: string(filepath.Separator), wantErr: "filesystem root"},
		{name: "symlinked component", root: filepath.Join(linkParent, "sandbox"), wantErr: "symlink"},
		{name: "reserved equals root", root: filepath.Join(base, "sandbox"), reserved: []string{filepath.Join(base, "sandbox")}, wantErr: "overlaps"},
		{name: "reserved is parent", root: filepath.Join(base, "sandbox"), reserved: []string{base}, wantErr: "overlaps"},
		{name: "reserved is child", root: filepath.Join(base, "sandbox"), reserved: []string{filepath.Join(base, "sandbox", "state")}, wantErr: "overlaps"},
		{name: "reserved relative", root: filepath.Join(base, "sandbox"), reserved: []string{"relative"}, wantErr: "not absolute"},
		{name: "unrelated reserved is allowed", root: filepath.Join(base, "sandbox"), reserved: []string{filepath.Join(base, "other")}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			layout, err := ResolveLayout(tt.root, tt.reserved)
			if tt.wantErr != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tt.wantErr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, layout.Root, tt.root)
			require.Equal(t, filepath.Join(tt.root, RuntimeDirName), layout.Runtime)
			require.Equal(t, filepath.Join(tt.root, StateDirName), layout.State)
			require.Equal(t, filepath.Join(tt.root, LogDirName), layout.Log)
		})
	}
}

func TestLoadValidConfig(t *testing.T) {
	root := t.TempDir()
	writeConfig(t, root, registrationDocument(nil), 0o600)
	layout, err := ResolveLayout(root, nil)
	require.NoError(t, err)

	config, err := Load(layout)
	require.NoError(t, err)
	require.Equal(t, []string{testEndpoint}, config.Endpoints())

	resolved, err := config.Resolver().Resolve(context.Background(), ports.BrokerOpenStreamRequest{
		Purpose:      ports.BrokerStreamControl,
		Endpoint:     testEndpoint,
		Registration: testRegistration(),
		Policy:       testPolicy(),
	})
	require.NoError(t, err)
	require.Equal(t, ports.BrokerDaemonIdentity("offline-daemon"), resolved.Identity)
	require.Equal(t, testPolicy(), resolved.Policy)

	local, err := config.LocalMuxRoute()
	require.NoError(t, err)
	require.Equal(t, RouteUnix, local.Kind())
	require.Equal(t, "/tmp/vev-brokerconfig-test/mux.sock", local.Path())
	require.Equal(t, local.Address(), resolved.Address)
	require.LessOrEqual(t, len(resolved.Address), MaxRouteAddressBytes)
	route, ok := config.RouteByAddress(resolved.Address)
	require.True(t, ok)
	require.Equal(t, local.Path(), route.Path())
	_, ok = config.RouteByAddress("offline-route-unknown")
	require.False(t, ok)
}

func TestLoadAcceptsEmptyRegistrations(t *testing.T) {
	root := t.TempDir()
	writeConfig(t, root, map[string]any{"marker": Marker, "registrations": []any{}}, 0o600)
	layout, err := ResolveLayout(root, nil)
	require.NoError(t, err)
	config, err := Load(layout)
	require.NoError(t, err)
	require.Empty(t, config.Endpoints())
}

func TestLoadRejects(t *testing.T) {
	tests := []struct {
		name    string
		build   func(t *testing.T, root string)
		wantErr string
	}{
		{
			name:    "missing file",
			build:   func(*testing.T, string) {},
			wantErr: "open",
		},
		{
			name: "wrong marker",
			build: func(t *testing.T, root string) {
				writeConfig(t, root, map[string]any{"marker": "other", "registrations": []any{}}, 0o600)
			},
			wantErr: "marker",
		},
		{
			name: "unknown field",
			build: func(t *testing.T, root string) {
				document := registrationDocument(nil)
				document["extra"] = true
				writeConfig(t, root, document, 0o600)
			},
			wantErr: "unknown field",
		},
		{
			name: "duplicate key",
			build: func(t *testing.T, root string) {
				writeRawConfig(t, root, `{"marker":"`+Marker+`","marker":"`+Marker+`"}`)
			},
			wantErr: "duplicate JSON key",
		},
		{
			name: "trailing json",
			build: func(t *testing.T, root string) {
				writeRawConfig(t, root, `{"marker":"`+Marker+`","registrations":[]} {}`)
			},
			wantErr: "trailing JSON",
		},
		{
			name:    "invalid utf8",
			build:   func(t *testing.T, root string) { writeRawConfig(t, root, "{\"marker\":\"\xff\"}") },
			wantErr: "encoding",
		},
		{
			name:    "group readable",
			build:   func(t *testing.T, root string) { writeConfig(t, root, registrationDocument(nil), 0o640) },
			wantErr: "owner-only",
		},
		{
			name: "symlinked config",
			build: func(t *testing.T, root string) {
				target := filepath.Join(t.TempDir(), "real.json")
				raw, err := json.Marshal(registrationDocument(nil))
				require.NoError(t, err)
				require.NoError(t, os.WriteFile(target, raw, 0o600))
				require.NoError(t, os.Symlink(target, filepath.Join(root, ConfigFileName)))
			},
			wantErr: "symlink",
		},
		{
			name: "nonregular config",
			build: func(t *testing.T, root string) {
				require.NoError(t, os.Mkdir(filepath.Join(root, ConfigFileName), 0o700))
			},
			wantErr: "not a regular file",
		},
		{
			name: "oversized config",
			build: func(t *testing.T, root string) {
				require.NoError(t, os.WriteFile(filepath.Join(root, ConfigFileName), make([]byte, MaxConfigBytes+1), 0o600))
			},
			wantErr: "exceeds",
		},
		{
			name: "symlinked route",
			build: func(t *testing.T, root string) {
				link := filepath.Join(t.TempDir(), "route-link")
				require.NoError(t, os.Symlink(t.TempDir(), link))
				writeConfig(t, root, registrationDocument(map[string]any{"route": filepath.Join(link, "mux.sock")}), 0o600)
			},
			wantErr: "symlink",
		},
		{
			name: "bad endpoint",
			build: func(t *testing.T, root string) {
				writeConfig(t, root, registrationDocument(map[string]any{"endpoint": "bad host"}), 0o600)
			},
			wantErr: "whitespace",
		},
		{
			name: "incarnation length",
			build: func(t *testing.T, root string) {
				writeConfig(t, root, registrationDocument(map[string]any{"incarnation": "abcd"}), 0o600)
			},
			wantErr: "32 hexadecimal",
		},
		{
			name: "incarnation zero",
			build: func(t *testing.T, root string) {
				writeConfig(t, root, registrationDocument(map[string]any{"incarnation": strings.Repeat("0", 32)}), 0o600)
			},
			wantErr: "no incarnation",
		},
		{
			name: "generation zero",
			build: func(t *testing.T, root string) {
				writeConfig(t, root, registrationDocument(map[string]any{"generation": 0}), 0o600)
			},
			wantErr: "no generation",
		},
		{
			name: "empty identity",
			build: func(t *testing.T, root string) {
				writeConfig(t, root, registrationDocument(map[string]any{"identity": ""}), 0o600)
			},
			wantErr: "identity",
		},
		{
			name: "bad environment policy",
			build: func(t *testing.T, root string) {
				writeConfig(t, root, documentWithPolicy(t, "environmentPolicy", "shared"), 0o600)
			},
			wantErr: "environmentPolicy",
		},
		{
			name: "zero protocol version",
			build: func(t *testing.T, root string) {
				writeConfig(t, root, documentWithPolicy(t, "protocolVersion", 0), 0o600)
			},
			wantErr: "no protocol version",
		},
		{
			name: "relative route",
			build: func(t *testing.T, root string) {
				writeConfig(t, root, registrationDocument(map[string]any{"route": "mux.sock"}), 0o600)
			},
			wantErr: "not absolute",
		},
		{
			name: "duplicate endpoint",
			build: func(t *testing.T, root string) {
				document := registrationDocument(nil)
				registrations := document["registrations"].([]any)
				document["registrations"] = append(registrations, registrations[0])
				writeConfig(t, root, document, 0o600)
			},
			wantErr: "duplicate endpoint",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			root := t.TempDir()
			tt.build(t, root)
			layout, err := ResolveLayout(root, nil)
			require.NoError(t, err)
			_, err = Load(layout)
			require.Error(t, err)
			require.Contains(t, err.Error(), tt.wantErr)
		})
	}
}

func TestLoadRejectsRouteOverlappingProduction(t *testing.T) {
	prodRuntime := t.TempDir()
	prodState := t.TempDir()

	tests := []struct {
		name    string
		route   string
		wantErr bool
	}{
		{name: "route under production runtime", route: filepath.Join(prodRuntime, "broker.sock"), wantErr: true},
		{name: "route equals production state", route: prodState, wantErr: true},
		{name: "route contains production runtime", route: filepath.Dir(prodRuntime), wantErr: true},
		{name: "unrelated route", route: filepath.Join(t.TempDir(), "mux.sock")},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			root := t.TempDir()
			writeConfig(t, root, registrationDocument(map[string]any{"route": tt.route}), 0o600)
			layout, err := ResolveLayout(root, []string{prodRuntime, prodState})
			require.NoError(t, err)
			_, err = Load(layout)
			if !tt.wantErr {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
			require.Contains(t, err.Error(), "overlaps production path")
		})
	}
}

func TestLayoutVerifyCreatedDetectsSymlinkedComponent(t *testing.T) {
	root := filepath.Join(t.TempDir(), "sandbox")
	layout, err := ResolveLayout(root, nil)
	require.NoError(t, err)

	// Nothing is created yet, so the walk ends at the first missing component.
	require.NoError(t, layout.VerifyCreated())

	require.NoError(t, os.MkdirAll(root, 0o700))
	require.NoError(t, os.Symlink(t.TempDir(), layout.State))
	require.ErrorContains(t, layout.VerifyCreated(), "symlink")

	// Once the swap is removed, every created component is symlink-free again.
	require.NoError(t, os.Remove(layout.State))
	require.NoError(t, layout.VerifyCreated())
}

// registrationsDocument renders one document carrying the supplied registration
// entries in order.
func registrationsDocument(entries ...map[string]any) map[string]any {
	list := make([]any, 0, len(entries))
	for _, entry := range entries {
		list = append(list, entry)
	}
	return map[string]any{"marker": Marker, "registrations": list}
}

// registrationEntry renders one registration entry with the supplied overrides.
func registrationEntry(overrides map[string]any) map[string]any {
	return registrationDocument(overrides)["registrations"].([]any)[0].(map[string]any)
}

// TestLoadEnforcesOneRoutePerPooledIdentity pins the pooling invariant: the
// broker pool keys one physical transport by the exact authenticated identity
// plus policy pair and deliberately ignores the opaque route address, so two
// registrations that share the pair must resolve to the same route. An alias on
// an identical route is accepted; a second route for the same pair is refused,
// because the pool would otherwise dial one of the two addresses and silently
// strand the other.
func TestLoadEnforcesOneRoutePerPooledIdentity(t *testing.T) {
	const (
		aliasOne = "alias@daemon:2222"
		aliasTwo = "alias@daemon:2223"
		otherID  = "other@daemon:2224"
		routeOne = "/tmp/vev-brokerconfig-test/one.sock"
		routeTwo = "/tmp/vev-brokerconfig-test/two.sock"
	)
	withPolicy := func(entry map[string]any, key string, value any) map[string]any {
		entry["policy"].(map[string]any)[key] = value
		return entry
	}
	tests := []struct {
		name       string
		entries    []map[string]any
		wantErr    string
		wantRoutes []string
	}{
		{
			name: "same identity and policy on a different route is refused",
			entries: []map[string]any{
				registrationEntry(map[string]any{"endpoint": aliasOne, "route": routeOne}),
				registrationEntry(map[string]any{"endpoint": aliasTwo, "route": routeTwo}),
			},
			wantErr: "different route",
		},
		{
			name: "same identity and policy on the identical route is an alias",
			entries: []map[string]any{
				registrationEntry(map[string]any{"endpoint": aliasOne, "route": routeOne}),
				registrationEntry(map[string]any{"endpoint": aliasTwo, "route": routeOne}),
			},
			wantRoutes: []string{aliasOne, aliasTwo},
		},
		{
			name: "distinct identities on different routes are independent",
			entries: []map[string]any{
				registrationEntry(map[string]any{"endpoint": aliasOne, "route": routeOne}),
				registrationEntry(map[string]any{"endpoint": otherID, "route": routeTwo, "identity": "other-daemon"}),
			},
			wantRoutes: []string{aliasOne, otherID},
		},
		{
			name: "same identity with a distinct policy on a different route is independent",
			entries: []map[string]any{
				registrationEntry(map[string]any{"endpoint": aliasOne, "route": routeOne}),
				withPolicy(registrationEntry(map[string]any{"endpoint": aliasTwo, "route": routeTwo}), "transport", "other-transport"),
			},
			wantRoutes: []string{aliasOne, aliasTwo},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			root := t.TempDir()
			writeConfig(t, root, registrationsDocument(tt.entries...), 0o600)
			layout, err := ResolveLayout(root, nil)
			require.NoError(t, err)
			config, err := Load(layout)
			if tt.wantErr != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tt.wantErr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.wantRoutes, config.Endpoints())
		})
	}
}

// TestLoadAliasesShareOneRoute proves an accepted alias resolves both endpoints
// to the identical opaque address and that the shared local route is still
// reported exactly once, so a remote-side helper is never handed a duplicate of
// the same carriage.
func TestLoadAliasesShareOneRoute(t *testing.T) {
	const (
		first  = "alias@daemon:2222"
		second = "alias@daemon:2223"
		route  = "/tmp/vev-brokerconfig-test/shared.sock"
	)
	root := t.TempDir()
	writeConfig(t, root, registrationsDocument(
		registrationEntry(map[string]any{"endpoint": first, "route": route}),
		registrationEntry(map[string]any{"endpoint": second, "route": route}),
	), 0o600)
	layout, err := ResolveLayout(root, nil)
	require.NoError(t, err)
	config, err := Load(layout)
	require.NoError(t, err)

	local, err := config.LocalMuxRoute()
	require.NoError(t, err)
	require.Equal(t, route, local.Path())

	for _, endpoint := range []string{first, second} {
		registration := testRegistration()
		registration.Endpoint = endpoint
		resolved, err := config.Resolver().Resolve(context.Background(), ports.BrokerOpenStreamRequest{
			Purpose:      ports.BrokerStreamControl,
			Endpoint:     endpoint,
			Registration: registration,
			Policy:       testPolicy(),
		})
		require.NoError(t, err)
		require.Equal(t, local.Address(), resolved.Address)
	}
}

// documentWithPolicy renders a valid document with one policy field overridden.
func documentWithPolicy(t *testing.T, key string, value any) map[string]any {
	t.Helper()
	document := registrationDocument(nil)
	entry := document["registrations"].([]any)[0].(map[string]any)
	entry["policy"].(map[string]any)[key] = value
	return document
}

// writeRawConfig writes one raw config file with owner-only permissions.
func writeRawConfig(t *testing.T, root, content string) {
	t.Helper()
	require.NoError(t, os.WriteFile(filepath.Join(root, ConfigFileName), []byte(content), 0o600))
}

// testRegistration is the exact registration the fixture document provisions.
func testRegistration() domain.RemoteRegistration {
	var registration domain.RemoteRegistration
	registration.Endpoint = testEndpoint
	for i := range registration.Incarnation {
		registration.Incarnation[i] = byte(i + 1)
	}
	registration.Generation = 1
	return registration
}

func TestResolverRefusals(t *testing.T) {
	root := t.TempDir()
	writeConfig(t, root, registrationDocument(nil), 0o600)
	layout, err := ResolveLayout(root, nil)
	require.NoError(t, err)
	config, err := Load(layout)
	require.NoError(t, err)
	resolver := config.Resolver()

	base := ports.BrokerOpenStreamRequest{
		Purpose:      ports.BrokerStreamControl,
		Endpoint:     testEndpoint,
		Registration: testRegistration(),
		Policy:       testPolicy(),
	}

	t.Run("unknown endpoint", func(t *testing.T) {
		request := base
		request.Endpoint = "other@daemon"
		_, err := resolver.Resolve(context.Background(), request)
		var typed ports.BrokerError
		require.ErrorAs(t, err, &typed)
		require.Equal(t, ports.BrokerErrorUnavailable, typed.Code)
		require.ErrorIs(t, err, errUnknownEndpoint)
	})
	t.Run("local request", func(t *testing.T) {
		request := base
		request.Local = true
		_, err := resolver.Resolve(context.Background(), request)
		var typed ports.BrokerError
		require.ErrorAs(t, err, &typed)
		require.Equal(t, ports.BrokerErrorUnavailable, typed.Code)
		require.ErrorIs(t, err, errNoLocalRoute)
	})
	t.Run("stale registration", func(t *testing.T) {
		request := base
		request.Registration.Generation = 2
		_, err := resolver.Resolve(context.Background(), request)
		var typed ports.BrokerError
		require.ErrorAs(t, err, &typed)
		require.Equal(t, ports.BrokerErrorUnavailable, typed.Code)
		require.ErrorIs(t, err, errStaleRegistration)
	})
	t.Run("conflicting policy", func(t *testing.T) {
		request := base
		request.Policy.Transport = "other-transport"
		_, err := resolver.Resolve(context.Background(), request)
		var typed ports.BrokerError
		require.ErrorAs(t, err, &typed)
		require.Equal(t, ports.BrokerErrorConflictingPolicy, typed.Code)
	})
	t.Run("cancelled context", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		_, err := resolver.Resolve(ctx, base)
		require.ErrorIs(t, err, context.Canceled)
	})
}

func TestDecodeStrictRejectsNesting(t *testing.T) {
	root := t.TempDir()
	nested := strings.Repeat("[", 130) + strings.Repeat("]", 130)
	writeRawConfig(t, root, `{"marker":"`+Marker+`","registrations":`+nested+`}`)
	layout, err := ResolveLayout(root, nil)
	require.NoError(t, err)
	_, err = Load(layout)
	require.Error(t, err)
	require.Contains(t, err.Error(), "nesting")
}

func TestResolveLayoutDoesNotCreatePaths(t *testing.T) {
	root := filepath.Join(t.TempDir(), "does-not-exist")
	layout, err := ResolveLayout(root, nil)
	require.NoError(t, err)
	require.Equal(t, filepath.Join(root, StateDirName), layout.State)
	require.Equal(t, filepath.Join(root, RuntimeDirName, SpawnDirName), layout.Spawn)
	_, statErr := os.Lstat(root)
	require.ErrorIs(t, statErr, os.ErrNotExist)
}

func TestLoadIdleGrace(t *testing.T) {
	tests := []struct {
		name     string
		document map[string]any
		want     time.Duration
		wantSet  bool
		wantErr  string
	}{
		{
			name:     "provisioned grace is parsed",
			document: map[string]any{"marker": Marker, "registrations": []any{}, "idleGrace": "90s"},
			want:     90 * time.Second,
			wantSet:  true,
		},
		{name: "absent grace provisions nothing", document: map[string]any{"marker": Marker, "registrations": []any{}}},
		{
			name:     "invalid duration is refused",
			document: map[string]any{"marker": Marker, "registrations": []any{}, "idleGrace": "soon"},
			wantErr:  "not a duration",
		},
		{
			name:     "non-positive grace is refused",
			document: map[string]any{"marker": Marker, "registrations": []any{}, "idleGrace": "0s"},
			wantErr:  "must be positive",
		},
		{
			name:     "oversized grace is refused",
			document: map[string]any{"marker": Marker, "registrations": []any{}, "idleGrace": (MaxIdleGrace + time.Second).String()},
			wantErr:  "must be positive and at most",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			root := t.TempDir()
			writeConfig(t, root, tt.document, 0o600)
			layout, err := ResolveLayout(root, nil)
			require.NoError(t, err)
			config, err := Load(layout)
			if tt.wantErr != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tt.wantErr)
				return
			}
			require.NoError(t, err)
			grace, set := config.IdleGrace()
			require.Equal(t, tt.wantSet, set)
			require.Equal(t, tt.want, grace)
		})
	}
}
