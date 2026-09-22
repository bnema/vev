package app

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestConnectivityMatrixCoversEveryCommandKind pins P1.2: every cmdKind
// constant declared in run.go has exactly one matrix row. A new CLI entry
// point without a row fails here before any transport work can ignore it.
func TestConnectivityMatrixCoversEveryCommandKind(t *testing.T) {
	t.Parallel()

	kinds := commandKindsFromRunGo(t)
	require.NotEmpty(t, kinds, "run.go must declare cmdKind constants")

	seen := make(map[string]int, len(connectivityMatrix))
	for _, entry := range connectivityMatrix {
		require.NotEmpty(t, entry.Kind, "matrix row must name a cmdKind")
		require.NotEmpty(t, entry.Summary, "matrix row %q must summarize the operation", entry.Kind)
		require.NotEmpty(t, entry.Notes, "matrix row %q must record cutover direction", entry.Kind)
		seen[entry.Kind]++
	}
	for kind, count := range seen {
		require.Equal(t, 1, count, "matrix row %q must appear exactly once", kind)
	}
	var missing, extra []string
	for _, kind := range kinds {
		if _, ok := seen[kind]; !ok {
			missing = append(missing, kind)
		}
	}
	kindSet := make(map[string]struct{}, len(kinds))
	for _, kind := range kinds {
		kindSet[kind] = struct{}{}
	}
	for kind := range seen {
		if _, ok := kindSet[kind]; !ok {
			extra = append(extra, kind)
		}
	}
	sort.Strings(missing)
	sort.Strings(extra)
	require.Empty(t, missing, "matrix is missing cmdKind rows")
	require.Empty(t, extra, "matrix references unknown cmdKind rows")

	owner, ok := connectivityOwnerFor("kindAttach")
	require.True(t, ok, "matrix lookup must resolve known kinds")
	require.Equal(t, connectivityBrokerOnly, owner)
	_, ok = connectivityOwnerFor("kindDoesNotExist")
	require.False(t, ok, "matrix lookup must reject unknown kinds")
}

// TestConnectivityMatrixOwnershipRules pins the P1.2 ownership invariants:
// broker-only rows must declare migration debt, infra rows must never gain
// store or persistence ownership, and local-only rows carry no debt.
func TestConnectivityMatrixOwnershipRules(t *testing.T) {
	t.Parallel()

	tests := make([]struct {
		name  string
		entry connectivityEntry
	}, 0, len(connectivityMatrix))
	for _, entry := range connectivityMatrix {
		tests = append(tests, struct {
			name  string
			entry connectivityEntry
		}{name: entry.Kind, entry: entry})
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			entry := tt.entry
			switch entry.Owner {
			case connectivityBrokerOnly:
				hasDebt := len(entry.DirectDialDebt) > 0 || entry.HostStoreWrite || entry.PersistMutation
				if entry.Migrated {
					require.False(t, hasDebt, "migrated broker-only %q must declare no remaining migration debt", entry.Kind)
					break
				}
				require.True(t, hasDebt, "broker-only %q must declare migration debt (direct dial, host-store write, or persist mutation)", entry.Kind)
			case connectivityTransportInfra:
				require.False(t, entry.HostStoreWrite, "infra %q must never own the host store", entry.Kind)
				require.False(t, entry.PersistMutation, "infra %q must never own session persistence", entry.Kind)
				if len(entry.DirectDialDebt) > 0 {
					require.NotEmpty(t, entry.Notes, "infra %q with local dial debt must justify why it stays infra", entry.Kind)
				}
			case connectivityLocalOnly:
				require.Empty(t, entry.DirectDialDebt, "local-only %q must carry no dial debt", entry.Kind)
				require.False(t, entry.HostStoreWrite, "local-only %q must never own the host store", entry.Kind)
				require.False(t, entry.PersistMutation, "local-only %q must never own session persistence", entry.Kind)
			default:
				t.Fatalf("row %q has unknown owner %v", entry.Kind, entry.Owner)
			}
			for _, debt := range entry.DirectDialDebt {
				require.NotEmpty(t, debt, "row %q has an empty debt entry", entry.Kind)
			}
		})
	}
}

// TestConnectivityMatrixDebtFilesExist keeps the matrix honest: every file
// named in a debt entry must exist under internal/app.
func TestConnectivityMatrixDebtFilesExist(t *testing.T) {
	t.Parallel()

	fileRef := regexp.MustCompile(`([a-z_]+\.go)`)
	for _, entry := range connectivityMatrix {
		for _, debt := range entry.DirectDialDebt {
			for _, match := range fileRef.FindAllStringSubmatch(debt, -1) {
				path := filepath.Join("internal", "app", match[1])
				// Tests run with the package dir as CWD, so resolve from there.
				if _, err := os.Stat(match[1]); err == nil {
					continue
				}
				if _, err := os.Stat(path); err == nil {
					continue
				}
				t.Errorf("row %q references missing file %q (debt %q)", entry.Kind, match[1], debt)
			}
		}
	}
}

// TestConnectivityMatrixCurrentDebtPresent pins today's direct-connectivity
// debt so the P7 cutover can find every site it must remove in the same
// change that activates the broker owner. If any of these symbols disappear,
// update the matrix in the same change.
func TestConnectivityMatrixCurrentDebtPresent(t *testing.T) {
	t.Parallel()

	sources := loadAppSources(t, false)

	assertContains := func(symbol, file string) {
		t.Helper()
		body, ok := sources[file]
		require.True(t, ok, "expected app source file %q", file)
		require.Contains(t, body, symbol, "expected debt symbol %q in %q", symbol, file)
	}

	assertContains("func ensureDaemonWithLifecycle", "spawn.go")
	assertContains("func realDial", "spawn.go")
	assertContains("func realSpawn", "spawn.go")
}

// TestConnectivityMatrixListKillStopAreBrokerOnly pins the P7.4f cutover for
// the list/kill/stop rows: each reaches the per-user broker through the private
// connectBroker seam and keeps no removed bypass, so runList and runKill/stop
// can never read durable state or signal a process directly.
func TestConnectivityMatrixListKillStopAreBrokerOnly(t *testing.T) {
	t.Parallel()

	sources := loadAppSources(t, false)
	runBody, ok := sources["run.go"]
	require.True(t, ok, "expected app source file run.go")
	require.Contains(t, runBody, "var connectBroker = connectProductionBroker",
		"run.go must keep the private connectBroker composition seam")
	require.Contains(t, runBody, "func runList", "run.go must keep runList")
	require.Contains(t, runBody, "func runKill", "run.go must keep runKill")
	require.Contains(t, runBody, "func requestDaemonStop", "run.go must keep requestDaemonStop")

	forbiddenFiles := map[string][]string{
		"run.go": {
			"runOfflineNamedKill", "persist.LoadReadOnly", "forceStopDaemonFallback",
			"forceStopDaemonFn", "waitForDaemonOrLifecycle", "waitForLifecycleAvailability",
		},
	}
	for file, forbidden := range forbiddenFiles {
		body, ok := sources[file]
		require.True(t, ok, "expected app source file %q", file)
		for _, symbol := range forbidden {
			require.NotContains(t, body, symbol,
				"%s must not reintroduce the removed bypass %q", file, symbol)
		}
	}

	// The whole force-stop subsystem is removed, so no production file may
	// resurrect an interactive process-signal fallback.
	for file, body := range sources {
		require.NotContains(t, body, "forceStop",
			"the force-stop subsystem must stay removed (%s)", file)
	}
}

// TestConnectivityMatrixSingleHostStoreWriterSet fails when a new
// RemoteHostStore constructor call site appears outside the declared set.
// After cutover the broker is the sole writer; until then the matrix must
// name every writer so none is missed.
func TestConnectivityMatrixSingleHostStoreWriterSet(t *testing.T) {
	t.Parallel()

	sources := loadAppSources(t, false)
	var writers []string
	for file, body := range sources {
		if strings.Contains(body, "NewFileHostStore") {
			writers = append(writers, file)
		}
	}
	sort.Strings(writers)
	require.Empty(t, writers,
		"the broker is the sole host-store owner; app must not construct another writer")
}

// TestConnectivityMatrixReadyProbeIsLocalDialOnly pins the kindBrokerReady row
// and its production source: the hidden readiness probe is local-only, reaches
// no daemon or store, and its production body dials, subscribes, and reads only.
// It must never ensure or spawn a broker, open a logical stream, request a
// reconcile, or mutate membership; each of those would turn a machine probe
// into a client façade that changes the state it reports.
func TestConnectivityMatrixReadyProbeIsLocalDialOnly(t *testing.T) {
	t.Parallel()

	owner, ok := connectivityOwnerFor("kindBrokerReady")
	require.True(t, ok, "the readiness probe must have a matrix row")
	require.Equal(t, connectivityLocalOnly, owner, "the readiness probe is dial-only local infrastructure")
	for _, entry := range connectivityMatrix {
		if entry.Kind != "kindBrokerReady" {
			continue
		}
		require.Empty(t, entry.DirectDialDebt, "a dial-only probe owes no connectivity migration")
		require.False(t, entry.HostStoreWrite, "the readiness probe never owns the host store")
		require.False(t, entry.PersistMutation, "the readiness probe never mutates session persistence")
	}

	sources := loadAppSources(t, false)
	body, ok := sources["broker_ready.go"]
	require.True(t, ok, "expected app source file broker_ready.go")

	forbidden := []struct {
		symbol string
		reason string
	}{
		{"ensureBrokerReady", "a readiness probe must never ensure a broker"},
		{"ensureDaemonWithLifecycle", "a readiness probe must never start a daemon"},
		{"spawnBrokerLauncher", "a readiness probe must never spawn a broker"},
		{"spawnProductionBrokerLauncher", "a readiness probe must never spawn a broker"},
		{"realDial", "a readiness probe dials the broker endpoint only"},
		{"OpenStream", "a readiness probe opens no logical stream"},
		{"RequestReconcile", "a readiness probe requests no reconcile"},
		{"AddHost", "a readiness probe mutates no membership"},
		{"RemoveHost", "a readiness probe mutates no membership"},
		{"UpdateHostPolicy", "a readiness probe mutates no membership"},
		{"NewFileHostStore", "a readiness probe owns no host store"},
	}
	for _, forbidden := range forbidden {
		require.NotContains(t, body, forbidden.symbol, "%s (%s)", forbidden.symbol, forbidden.reason)
	}

	// Positive pins: the probe is one subscription over one snapshot loop, and
	// it classifies terminal failures without retrying them.
	for _, required := range []string{"func runBrokerReady", "func observeBrokerReady", "service.Subscribe()", "service.Snapshot()", "terminalReadyReason", "brokerReadyEndpointSecurity"} {
		require.Contains(t, body, required, "broker_ready.go must keep %s", required)
	}
}

// commandKindsFromRunGo extracts the kind* const names from run.go so the
// matrix test tracks the real dispatch table instead of a hardcoded copy.
func commandKindsFromRunGo(t *testing.T) []string {
	t.Helper()

	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "run.go", nil, 0)
	require.NoError(t, err, "parse run.go for cmdKind constants")

	var kinds []string
	for _, decl := range file.Decls {
		gen, ok := decl.(*ast.GenDecl)
		if !ok || gen.Tok != token.CONST {
			continue
		}
		for _, spec := range gen.Specs {
			value, ok := spec.(*ast.ValueSpec)
			if !ok {
				continue
			}
			for _, name := range value.Names {
				if strings.HasPrefix(name.Name, "kind") {
					kinds = append(kinds, name.Name)
				}
			}
		}
	}
	sort.Strings(kinds)
	return kinds
}

// loadAppSources reads production (non-test) .go files in this package.
// When includeTests is false, _test.go files are skipped.
func loadAppSources(t *testing.T, includeTests bool) map[string]string {
	t.Helper()

	entries, err := os.ReadDir(".")
	require.NoError(t, err, "read app package dir")
	sources := make(map[string]string)
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") {
			continue
		}
		if !includeTests && strings.HasSuffix(name, "_test.go") {
			continue
		}
		raw, err := os.ReadFile(name)
		require.NoError(t, err, "read app source %q", name)
		sources[name] = string(raw)
	}
	require.NotEmpty(t, sources, "app sources must not be empty")
	return sources
}
