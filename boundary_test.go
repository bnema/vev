package main_test

import (
	"fmt"
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
	return productionDependencies[sourceLayer][targetLayer], nil
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
