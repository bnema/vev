package main_test

import (
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"strings"
	"testing"
)

// boundaryRule expresses a single import-boundary constraint: no .go file
// under root may import a package whose path has disallowedPrefix.
type boundaryRule struct {
	name             string
	root             string
	disallowedPrefix string
}

func TestImportBoundaries(t *testing.T) {
	rules := []boundaryRule{
		{
			name:             "pkg must not import internal",
			root:             "pkg",
			disallowedPrefix: "github.com/bnema/vev/internal",
		},
		{
			name:             "usecase must not import adapters",
			root:             "internal/usecase",
			disallowedPrefix: "github.com/bnema/vev/internal/adapters",
		},
		{
			name:             "usecase must not import platform",
			root:             "internal/usecase",
			disallowedPrefix: "github.com/bnema/vev/internal/platform",
		},
	}

	for _, rule := range rules {
		t.Run(rule.name, func(t *testing.T) {
			violations := findImportViolations(t, rule.root, rule.disallowedPrefix)
			if len(violations) > 0 {
				t.Errorf("found disallowed imports of %q under %q:\n%s", rule.disallowedPrefix, rule.root, strings.Join(violations, "\n"))
			}
		})
	}
}

// findImportViolations walks root, parses each .go file's imports, and
// returns a description for every import whose path starts with
// disallowedPrefix.
func findImportViolations(t *testing.T, root, disallowedPrefix string) []string {
	t.Helper()

	var violations []string
	fset := token.NewFileSet()

	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() || !strings.HasSuffix(path, ".go") {
			return nil
		}

		f, err := parser.ParseFile(fset, path, nil, parser.ImportsOnly)
		if err != nil {
			return err
		}

		for _, imp := range f.Imports {
			importPath := strings.Trim(imp.Path.Value, `"`)
			if strings.HasPrefix(importPath, disallowedPrefix) {
				violations = append(violations, path+": imports "+importPath)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walking %s: %v", root, err)
	}

	return violations
}
