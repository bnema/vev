package main_test

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
)

const modulePath = "github.com/bnema/vev"

type packageLayer string

const (
	layerRoot        packageLayer = "root"
	layerCommand     packageLayer = "command"
	layerScript      packageLayer = "script"
	layerApp         packageLayer = "app"
	layerAdapter     packageLayer = "adapter"
	layerUsecase     packageLayer = "usecase"
	layerPorts       packageLayer = "ports"
	layerProtocol    packageLayer = "protocol"
	layerCatalogue   packageLayer = "catalogue"
	layerWire        packageLayer = "wire"
	layerDomain      packageLayer = "domain"
	layerPersist     packageLayer = "persist"
	layerPlatform    packageLayer = "platform"
	layerLogging     packageLayer = "logging"
	layerPkg         packageLayer = "pkg"
	layerTestSupport packageLayer = "test-support"
)

var productionDependencies = map[packageLayer]map[packageLayer]bool{
	layerRoot:      {layerApp: true},
	layerCommand:   {layerPkg: true},
	layerScript:    {layerAdapter: true, layerDomain: true, layerPorts: true, layerProtocol: true, layerCatalogue: true, layerWire: true, layerPkg: true},
	layerApp:       {layerAdapter: true, layerDomain: true, layerPorts: true, layerProtocol: true, layerCatalogue: true, layerWire: true, layerUsecase: true, layerPersist: true, layerPlatform: true, layerLogging: true, layerPkg: true},
	layerAdapter:   {layerAdapter: true, layerDomain: true, layerPorts: true, layerProtocol: true, layerCatalogue: true, layerWire: true, layerPlatform: true, layerPkg: true},
	layerUsecase:   {layerUsecase: true, layerDomain: true, layerPorts: true, layerProtocol: true, layerCatalogue: true},
	layerPorts:     {layerDomain: true, layerProtocol: true, layerCatalogue: true},
	layerProtocol:  {layerDomain: true},
	layerCatalogue: {layerDomain: true, layerProtocol: true},
	layerWire:      {layerDomain: true, layerProtocol: true},
	layerDomain:    {layerDomain: true},
	layerPersist:   {layerDomain: true, layerPorts: true, layerProtocol: true, layerPkg: true},
	layerPlatform:  {},
	layerLogging:   {layerPkg: true},
	layerPkg:       {layerPkg: true},
}

func TestImportBoundaries(t *testing.T) {
	violations, packages := inspectRepositoryDependencies(t)
	if len(violations) != 0 {
		t.Fatalf("invalid package dependencies:\n%s", strings.Join(violations, "\n"))
	}
	if len(packages) == 0 {
		t.Fatal("package inventory is empty")
	}
}

// externalDependencyOwners confines technology imports to their owning
// layers. Use cases never touch protobuf runtime APIs, generated wire
// envelopes, QUIC, stream framing, or concrete adapters; the wire package
// never touches QUIC either (carriage lives in adapters).
var externalDependencyOwners = map[string][]packageLayer{
	"google.golang.org/protobuf/": {layerAdapter, layerApp, layerWire, layerTestSupport, layerScript},
	"github.com/quic-go/quic-go":  {layerAdapter, layerApp},
}

func TestExternalDependencyBoundaries(t *testing.T) {
	violations := inspectExternalDependencies(t)
	if len(violations) != 0 {
		t.Fatalf("invalid external dependencies:\n%s", strings.Join(violations, "\n"))
	}
}

func TestImportBoundaryNegativeFixtures(t *testing.T) {
	tests := []struct {
		name, source, target string
		testFile             bool
		want                 bool
	}{
		{"usecase accepts semantic protocol", modulePath + "/internal/usecase/client", modulePath + "/internal/protocol", false, true},
		{"usecase rejects wire", modulePath + "/internal/usecase/daemon", modulePath + "/internal/protocol/wire", false, false},
		{"usecase rejects adapter", modulePath + "/internal/usecase/client", modulePath + "/internal/adapters/ipc", false, false},
		{"domain rejects ports", modulePath + "/internal/domain", modulePath + "/internal/ports", false, false},
		{"pkg rejects internal", modulePath + "/pkg/rawterm", modulePath + "/internal/domain", false, false},
		{"snapshot adapter exception", modulePath + "/internal/adapters/snapshot", modulePath + "/internal/usecase/snapshot", false, true},
		{"other adapter rejects usecase", modulePath + "/internal/adapters/ipc", modulePath + "/internal/usecase/client", false, false},
		{"usecase test may use wire fixture", modulePath + "/internal/usecase/client", modulePath + "/internal/protocol/wire", true, true},
		{"pkg test still rejects internal", modulePath + "/pkg/rawterm", modulePath + "/internal/domain", true, false},
		// P1.1 broker ownership: broker consumes ports/protocol/domain
		// (catalogue counts as protocol family) and no sibling use case.
		{"broker accepts ports", modulePath + "/internal/usecase/broker", modulePath + "/internal/ports", false, true},
		{"broker accepts protocol", modulePath + "/internal/usecase/broker", modulePath + "/internal/protocol", false, true},
		{"broker accepts domain", modulePath + "/internal/usecase/broker", modulePath + "/internal/domain", false, true},
		{"broker accepts catalogue", modulePath + "/internal/usecase/broker", modulePath + "/internal/protocol/catalogue", false, true},
		{"broker accepts own subpackage", modulePath + "/internal/usecase/broker", modulePath + "/internal/usecase/broker/streams", false, true},
		{"broker subpackage accepts broker root", modulePath + "/internal/usecase/broker/streams", modulePath + "/internal/usecase/broker", false, true},
		{"broker rejects client", modulePath + "/internal/usecase/broker", modulePath + "/internal/usecase/client", false, false},
		{"broker rejects daemon", modulePath + "/internal/usecase/broker", modulePath + "/internal/usecase/daemon", false, false},
		{"broker rejects sibling usecase", modulePath + "/internal/usecase/broker", modulePath + "/internal/usecase/picker", false, false},
		{"broker rejects adapter", modulePath + "/internal/usecase/broker", modulePath + "/internal/adapters/ipc", false, false},
		{"broker rejects wire", modulePath + "/internal/usecase/broker", modulePath + "/internal/protocol/wire", false, false},
		{"broker rejects app", modulePath + "/internal/usecase/broker", modulePath + "/internal/app", false, false},
		{"broker rejects persist", modulePath + "/internal/usecase/broker", modulePath + "/internal/persist", false, false},
		// P1.1 client/daemon must not own broker, each other, or remote discovery.
		{"client rejects daemon", modulePath + "/internal/usecase/client", modulePath + "/internal/usecase/daemon", false, false},
		{"client rejects broker", modulePath + "/internal/usecase/client", modulePath + "/internal/usecase/broker", false, false},
		{"daemon rejects client", modulePath + "/internal/usecase/daemon", modulePath + "/internal/usecase/client", false, false},
		{"daemon rejects broker", modulePath + "/internal/usecase/daemon", modulePath + "/internal/usecase/broker", false, false},
		{"daemon rejects app", modulePath + "/internal/usecase/daemon", modulePath + "/internal/app", false, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := dependencyAllowed(tt.source, tt.target, tt.testFile)
			if err != nil {
				t.Fatalf("dependencyAllowed: %v", err)
			}
			if got != tt.want {
				t.Fatalf("dependencyAllowed(%q, %q, test=%t) = %t, want %t", tt.source, tt.target, tt.testFile, got, tt.want)
			}
		})
	}
}

func inspectRepositoryDependencies(t *testing.T) ([]string, map[string]struct{}) {
	t.Helper()
	var violations []string
	packages := make(map[string]struct{})
	fset := token.NewFileSet()
	err := filepath.WalkDir(".", func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			name := entry.Name()
			if name == ".git" || name == ".worktrees" || name == "vendor" {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") {
			return nil
		}
		source := importPathForFile(path)
		packages[source] = struct{}{}
		if _, err := classifyPackage(source); err != nil {
			violations = append(violations, path+": "+err.Error())
			return nil
		}
		file, err := parser.ParseFile(fset, path, nil, parser.ImportsOnly)
		if err != nil {
			return err
		}
		testFile := strings.HasSuffix(path, "_test.go")
		for _, spec := range file.Imports {
			target, err := strconv.Unquote(spec.Path.Value)
			if err != nil || (target != modulePath && !strings.HasPrefix(target, modulePath+"/")) {
				continue
			}
			allowed, err := dependencyAllowed(source, target, testFile)
			if err != nil {
				violations = append(violations, fmt.Sprintf("%s: %v", path, err))
				continue
			}
			if !allowed {
				violations = append(violations, fmt.Sprintf("%s: %s may not import %s", path, source, target))
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walking repository: %v", err)
	}
	sort.Strings(violations)
	return violations, packages
}

func dependencyAllowed(source, target string, testFile bool) (bool, error) {
	sourceLayer, err := classifyPackage(source)
	if err != nil {
		return false, err
	}
	targetLayer, err := classifyPackage(target)
	if err != nil {
		return false, err
	}
	if testFile {
		return sourceLayer != layerPkg, nil
	}
	if sourceLayer == layerTestSupport {
		return testSupportDependencyAllowed(source, targetLayer), nil
	}
	if source == modulePath+"/internal/adapters/snapshot" && target == modulePath+"/internal/usecase/snapshot" {
		return true, nil
	}
	if packageImportDenied(source, target) {
		return false, nil
	}
	return productionDependencies[sourceLayer][targetLayer], nil
}

// packageImportDenied encodes ADR 001 ownership at package granularity for
// production files. Layer rules alone permit any usecase-to-usecase import;
// other, and keep the broker free of every sibling use case. Test files stay
// exempt so composition tests can wire adapters and orchestrators together.
func packageImportDenied(source, target string) bool {
	brokerPkg := modulePath + "/internal/usecase/broker"
	clientPkg := modulePath + "/internal/usecase/client"
	daemonPkg := modulePath + "/internal/usecase/daemon"
	usecasePrefix := modulePath + "/internal/usecase/"
	under := func(path, root string) bool {
		return path == root || strings.HasPrefix(path, root+"/")
	}
	switch {
	case under(source, brokerPkg):
		return !under(target, brokerPkg) && strings.HasPrefix(target, usecasePrefix)
	case under(source, clientPkg):
		return under(target, daemonPkg) || under(target, brokerPkg)
	case under(source, daemonPkg):
		return under(target, clientPkg) || under(target, daemonPkg) || under(target, brokerPkg)
	default:
		return false
	}
}

func testSupportDependencyAllowed(source string, target packageLayer) bool {
	switch {
	case source == modulePath+"/internal/ports/mocks":
		return target == layerPorts || target == layerDomain || target == layerProtocol || target == layerCatalogue
	case source == modulePath+"/internal/protocol/wire/mocks":
		return target == layerWire
	case strings.HasPrefix(source, modulePath+"/internal/testutil"):
		return target == layerDomain || target == layerProtocol || target == layerWire
	default:
		return false
	}
}

func classifyPackage(path string) (packageLayer, error) {
	switch {
	case path == modulePath:
		return layerRoot, nil
	case strings.HasPrefix(path, modulePath+"/cmd/"):
		return layerCommand, nil
	case strings.HasPrefix(path, modulePath+"/scripts/"):
		return layerScript, nil
	case path == modulePath+"/internal/app":
		return layerApp, nil
	case strings.HasPrefix(path, modulePath+"/internal/adapters/"):
		return layerAdapter, nil
	case path == modulePath+"/internal/ports/mocks", path == modulePath+"/internal/protocol/wire/mocks", strings.HasPrefix(path, modulePath+"/internal/testutil"):
		return layerTestSupport, nil
	case path == modulePath+"/internal/ports":
		return layerPorts, nil
	case path == modulePath+"/internal/protocol/wire":
		return layerWire, nil
	case path == modulePath+"/internal/protocol/catalogue":
		return layerCatalogue, nil
	case path == modulePath+"/internal/protocol":
		return layerProtocol, nil
	case path == modulePath+"/internal/domain" || strings.HasPrefix(path, modulePath+"/internal/domain/"):
		return layerDomain, nil
	case path == modulePath+"/internal/usecase" || strings.HasPrefix(path, modulePath+"/internal/usecase/"):
		return layerUsecase, nil
	case path == modulePath+"/internal/persist":
		return layerPersist, nil
	case path == modulePath+"/internal/platform":
		return layerPlatform, nil
	case path == modulePath+"/internal/logging":
		return layerLogging, nil
	case path == modulePath+"/pkg" || strings.HasPrefix(path, modulePath+"/pkg/"):
		return layerPkg, nil
	default:
		return "", fmt.Errorf("unclassified package %q", path)
	}
}

func importPathForFile(path string) string {
	dir := filepath.ToSlash(filepath.Dir(path))
	if dir == "." {
		return modulePath
	}
	return modulePath + "/" + strings.TrimPrefix(dir, "./")
}

func inspectExternalDependencies(t *testing.T) []string {
	t.Helper()
	var violations []string
	fset := token.NewFileSet()
	err := filepath.WalkDir(".", func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			name := entry.Name()
			if name == ".git" || name == ".worktrees" || name == "vendor" {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") {
			return nil
		}
		source := importPathForFile(path)
		sourceLayer, err := classifyPackage(source)
		if err != nil {
			violations = append(violations, path+": "+err.Error())
			return nil
		}
		// Test files may use any external dependency except inside pkg,
		// mirroring the internal test-import policy.
		if strings.HasSuffix(path, "_test.go") && sourceLayer != layerPkg {
			return nil
		}
		file, err := parser.ParseFile(fset, path, nil, parser.ImportsOnly)
		if err != nil {
			return err
		}
		for _, spec := range file.Imports {
			target, err := strconv.Unquote(spec.Path.Value)
			if err != nil {
				continue
			}
			for prefix, owners := range externalDependencyOwners {
				if !strings.HasPrefix(target, prefix) {
					continue
				}
				allowed := false
				for _, owner := range owners {
					if sourceLayer == owner {
						allowed = true
						break
					}
				}
				if !allowed {
					violations = append(violations, fmt.Sprintf("%s: %s may not import %s", path, source, target))
				}
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walking repository: %v", err)
	}
	sort.Strings(violations)
	return violations
}

// TestProductionDaemonBindingComposition pins the daemon authority composition
// at the source level: the daemon's process binding may only be built by the one
// production site that provisions the daemon's closed policy set, and the
// single-policy shorthand must not reappear outside test fixtures.
//
// The multi-policy constructor exists because a daemon serves one exact policy
// per accepted carriage shape (its own local Unix daemonmux policy, plus each
// provisioned remote transport policy). A production composition that used the
// single-policy shorthand would publish an authority that refuses every remote
// carriage, so the composition is pinned to the multi-policy call, and the
// shorthand stays a fixtures-only convenience.
func TestProductionDaemonBindingComposition(t *testing.T) {
	const runFile = "internal/app/run.go"

	var (
		bindingSites []string
		runBindings  int
	)
	fset := token.NewFileSet()
	err := filepath.WalkDir(".", func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		name := entry.Name()
		if entry.IsDir() {
			if name == ".git" || name == ".worktrees" || name == "vendor" {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		file, err := parser.ParseFile(fset, path, nil, 0)
		if err != nil {
			return err
		}
		slashed := filepath.ToSlash(path)
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			ast.Inspect(fn.Body, func(node ast.Node) bool {
				call, ok := node.(*ast.CallExpr)
				if !ok {
					return true
				}
				selector, ok := call.Fun.(*ast.SelectorExpr)
				if !ok {
					return true
				}
				// Only the adapter's own constructors count: a method spelled
				// the same way elsewhere is unrelated.
				ident, ok := selector.X.(*ast.Ident)
				if !ok || ident.Name != "daemonmux" {
					return true
				}
				switch selector.Sel.Name {
				case "NewServerBinding":
					bindingSites = append(bindingSites, fmt.Sprintf("%s:%d: %s must build the closed policy set with NewServerBindings", slashed, fset.Position(call.Pos()).Line, selector.Sel.Name))
				case "NewServerBindings":
					if slashed == runFile {
						runBindings++
					}
				}
				return true
			})
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walking repository: %v", err)
	}
	if len(bindingSites) != 0 {
		t.Fatalf("production code uses the single-policy daemon binding shorthand:\n%s", strings.Join(bindingSites, "\n"))
	}
	if runBindings == 0 {
		t.Fatalf("%s must compose the daemon authority with daemonmux.NewServerBindings", runFile)
	}
}
