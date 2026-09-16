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

	assertContains("type localDaemonDialer struct", "run.go")
	assertContains("type dialOnlyLocalDialer struct", "run.go")
	assertContains("func (d localDaemonDialer) Dial", "run.go")
	assertContains("func (d dialOnlyLocalDialer) Dial", "run.go")
	assertContains("func ensureDaemonWithLifecycle", "spawn.go")
	assertContains("func waitForDaemonOrLifecycle", "spawn.go")
	assertContains("func realDial", "spawn.go")
	assertContains("func realSpawn", "spawn.go")
	assertContains("func (d remoteHostDeps) hostStore", "remote_hosts.go")
	assertContains("func hostAdd", "remote_hosts.go")
	assertContains("func hostRm", "remote_hosts.go")
	assertContains("func listSessionsWithDialer", "run.go")
	assertContains("func sessionExists", "attach_preflight.go")
	assertContains("func runOfflineNamedKill", "run.go")
	assertContains("persist.LoadReadOnly", "run.go")
	assertContains("persist.OpenOrCreate", "run.go")
	assertContains("func newClientHostRegistry", "client_hosts.go")
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
	require.Equal(t, []string{"remote_hosts.go", "run.go"}, writers,
		"host-store constructor sites changed; update the matrix debt set in the same change")
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
