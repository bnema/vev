package main_test

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"go/ast"
	"go/importer"
	"go/parser"
	"go/token"
	"go/types"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"
)

// This file enforces the "Test doubles" and "Time and concurrency" sections
// of AGENTS.md on every *_test.go file in the module. It uses only the
// standard library: go/ast for the syntactic rules and go/types (fed by the
// export data reported by `go list -export`) for the double detection.
//
// Limits of rule 4 (hand-written doubles):
//   - generic interfaces and generic types are skipped;
//   - only mocked interfaces of the package under test and of its direct
//     imports are considered;
//   - packages are type-checked with the host GOOS and build tags, so files
//     for other platforms or tags are only syntax-checked (rules 1-3), never
//     type-checked for doubles.
//
// Rules 1-3 are syntactic: time functions are matched on the resolved import
// name of "time" (calls and plain value uses), dot-imports of "time" and
// "testing/synctest" are rejected, and rule 1 ignores selectors on packages
// other than testify's require and assert.

const (
	policyHint = "see AGENTS.md sections \"Test doubles\" and \"Time and concurrency\""

	ruleNoPolling  = "no-polling-assertions"
	ruleNoSleep    = "no-sleep-outside-bubble"
	ruleNoRealtime = "no-realtime-wait-outside-bubble"
	ruleNoDoubles  = "no-hand-written-doubles"
)

var (
	generatedHeader = regexp.MustCompile(`^// Code generated .* DO NOT EDIT\.$`)

	pollingSelectors = map[string]bool{
		"Eventually": true, "Eventuallyf": true, "EventuallyWithT": true, "EventuallyWithTf": true,
		"Never": true, "Neverf": true,
	}
	realtimeFuncs = map[string]bool{
		"After": true, "NewTimer": true, "NewTicker": true, "Tick": true, "AfterFunc": true,
	}
	excludedMockDirs = []string{"internal/ports/mocks/", "internal/protocol/wire/mocks/"}
)

type policyViolation struct {
	rule string
	pos  token.Position
	msg  string
}

func (v policyViolation) String() string {
	return fmt.Sprintf("%s:%d:%d: %s", v.pos.Filename, v.pos.Line, v.pos.Column, v.msg)
}

func reportPolicy(t *testing.T, rule string, violations []policyViolation) {
	t.Helper()
	var lines []string
	for _, v := range violations {
		if v.rule == rule {
			lines = append(lines, v.String())
		}
	}
	if len(lines) == 0 {
		return
	}
	sort.Strings(lines)
	t.Errorf("%d violation(s) of %s (%s):\n%s", len(lines), rule, policyHint, strings.Join(lines, "\n"))
}

// policyExcludedPath reports whether a slash-separated, module-relative path
// is outside the policy by location or file name.
func policyExcludedPath(rel string) bool {
	rel = filepath.ToSlash(rel)
	for _, seg := range strings.Split(rel, "/") {
		if seg == ".worktrees" || seg == "vendor" || seg == "testdata" {
			return true
		}
	}
	for _, dir := range excludedMockDirs {
		if strings.HasPrefix(rel, dir) {
			return true
		}
	}
	return false
}

func isGeneratedFile(file *ast.File) bool {
	for _, group := range file.Comments {
		if group.Pos() >= file.Package {
			break
		}
		for _, c := range group.List {
			if generatedHeader.MatchString(c.Text) {
				return true
			}
		}
	}
	return false
}

// directiveReason returns the reason of a `//testpolicy:<name> <reason>`
// comment line, and whether the directive is present with a non-empty reason.
func directiveReason(text, name string) bool {
	prefix := "//testpolicy:" + name
	if !strings.HasPrefix(text, prefix) {
		return false
	}
	rest := text[len(prefix):]
	if rest == "" || (rest[0] != ' ' && rest[0] != '\t') {
		return false
	}
	return strings.TrimSpace(rest) != ""
}

func docHasDirective(doc *ast.CommentGroup, name string) bool {
	if doc == nil {
		return false
	}
	for _, c := range doc.List {
		if directiveReason(c.Text, name) {
			return true
		}
	}
	return false
}

func docHasRealIO(doc *ast.CommentGroup) bool {
	if doc == nil {
		return false
	}
	for _, c := range doc.List {
		if rest, ok := strings.CutPrefix(c.Text, "// real I/O:"); ok && strings.TrimSpace(rest) != "" {
			return true
		}
	}
	return false
}

func importName(file *ast.File, path, def string) string {
	for _, spec := range file.Imports {
		p, err := strconv.Unquote(spec.Path.Value)
		if err != nil || p != path {
			continue
		}
		if spec.Name != nil {
			return spec.Name.Name
		}
		return def
	}
	return ""
}

// importedPackages maps each named import of file to its path, using the last
// path element as the default name. Blank and dot imports are omitted.
func importedPackages(file *ast.File) map[string]string {
	out := make(map[string]string)
	for _, spec := range file.Imports {
		p, err := strconv.Unquote(spec.Path.Value)
		if err != nil {
			continue
		}
		name := p[strings.LastIndex(p, "/")+1:]
		if spec.Name != nil {
			name = spec.Name.Name
		}
		if name == "_" || name == "." {
			continue
		}
		out[name] = p
	}
	return out
}

func isTestifyAssertPath(p string) bool {
	return p == "github.com/stretchr/testify/require" || p == "github.com/stretchr/testify/assert"
}

// checkPolicySyntax applies rules 1-3 to one parsed file.
func checkPolicySyntax(fset *token.FileSet, file *ast.File) []policyViolation {
	timeName := importName(file, "time", "time")
	synctestName := importName(file, "testing/synctest", "synctest")

	sleepDirectiveLines := make(map[int]bool)
	for _, group := range file.Comments {
		for _, c := range group.List {
			if directiveReason(c.Text, "sleep") {
				sleepDirectiveLines[fset.Position(c.Pos()).Line] = true
			}
		}
	}

	imported := importedPackages(file)

	isPkgSel := func(sel *ast.SelectorExpr, pkg string) (string, bool) {
		if pkg == "" || pkg == "_" || pkg == "." {
			return "", false
		}
		id, ok := sel.X.(*ast.Ident)
		if !ok || id.Name != pkg {
			return "", false
		}
		return sel.Sel.Name, true
	}

	isPkgCall := func(call *ast.CallExpr, pkg string) (string, bool) {
		sel, ok := call.Fun.(*ast.SelectorExpr)
		if !ok {
			return "", false
		}
		return isPkgSel(sel, pkg)
	}

	var out []policyViolation
	add := func(rule string, pos token.Pos, format string, args ...any) {
		out = append(out, policyViolation{rule: rule, pos: fset.Position(pos), msg: fmt.Sprintf(format, args...)})
	}

	for _, spec := range file.Imports {
		p, err := strconv.Unquote(spec.Path.Value)
		if err != nil || spec.Name == nil || spec.Name.Name != "." {
			continue
		}
		if p == "time" || p == "testing/synctest" {
			add(ruleNoSleep, spec.Pos(), "dot-import of %q hides time and bubble use from the policy; use a named import", p)
		}
	}

	type scope struct {
		bubble   bool
		realtime bool // enclosing top-level func allows real-time waits
	}
	var walk func(node ast.Node, sc scope)
	walk = func(node ast.Node, sc scope) {
		ast.Inspect(node, func(n ast.Node) bool {
			if sel, ok := n.(*ast.SelectorExpr); ok {
				if pollingSelectors[sel.Sel.Name] {
					other := false
					if id, ok := sel.X.(*ast.Ident); ok {
						if path, isPkg := imported[id.Name]; isPkg && !isTestifyAssertPath(path) {
							other = true
						}
					}
					if !other {
						add(ruleNoPolling, sel.Pos(), "%s is a polling assertion; use synctest.Test with synctest.Wait", sel.Sel.Name)
					}
				}
				if name, ok := isPkgSel(sel, timeName); ok {
					switch {
					case name == "Sleep":
						line := fset.Position(sel.Pos()).Line
						if !sc.bubble && !sleepDirectiveLines[line] && !sleepDirectiveLines[line-1] {
							add(ruleNoSleep, sel.Pos(), "time.Sleep outside a synctest bubble (add a `//testpolicy:sleep <reason>` directive only for fake-time helpers)")
						}
					case realtimeFuncs[name]:
						if !sc.bubble && !sc.realtime {
							add(ruleNoRealtime, sel.Pos(), "time.%s outside a synctest bubble (mark the func `// real I/O: <reason>` or `//testpolicy:realtime <reason>`)", name)
						}
					}
				}
				return true
			}
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			if name, ok := isPkgCall(call, synctestName); ok && name == "Test" {
				walk(call.Fun, sc)
				for _, arg := range call.Args {
					if lit, ok := arg.(*ast.FuncLit); ok {
						walk(lit.Body, scope{bubble: true, realtime: sc.realtime})
					} else {
						walk(arg, sc)
					}
				}
				return false
			}
			return true
		})
	}

	for _, decl := range file.Decls {
		sc := scope{}
		if fn, ok := decl.(*ast.FuncDecl); ok {
			n := fn.Name.Name
			sc.realtime = docHasRealIO(fn.Doc) || docHasDirective(fn.Doc, "realtime") ||
				strings.HasPrefix(n, "Benchmark") || strings.HasPrefix(n, "Fuzz") || strings.HasPrefix(n, "Example")
		}
		walk(decl, sc)
	}
	return out
}

func parsePolicyFile(fset *token.FileSet, path string, src any) (*ast.File, error) {
	return parser.ParseFile(fset, path, src, parser.ParseComments|parser.SkipObjectResolution)
}

// policyTestFiles lists the module's non-excluded, non-generated test files.
func policyTestFiles(t *testing.T) (*token.FileSet, map[string]*ast.File) {
	t.Helper()
	fset := token.NewFileSet()
	files := make(map[string]*ast.File)
	err := filepath.WalkDir(".", func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			name := entry.Name()
			if name == ".git" || name == ".worktrees" || name == "vendor" || name == "testdata" {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, "_test.go") || policyExcludedPath(path) {
			return nil
		}
		file, err := parsePolicyFile(fset, path, nil)
		if err != nil {
			return err
		}
		if isGeneratedFile(file) {
			return nil
		}
		files[path] = file
		return nil
	})
	if err != nil {
		t.Fatalf("walking repository: %v", err)
	}
	return fset, files
}

func syntacticViolations(t *testing.T) []policyViolation {
	t.Helper()
	fset, files := policyTestFiles(t)
	var all []policyViolation
	for _, file := range files {
		all = append(all, checkPolicySyntax(fset, file)...)
	}
	return all
}

func TestTestPolicyNoPollingAssertions(t *testing.T) {
	reportPolicy(t, ruleNoPolling, syntacticViolations(t))
}

func TestTestPolicyNoSleepOutsideBubble(t *testing.T) {
	reportPolicy(t, ruleNoSleep, syntacticViolations(t))
}

func TestTestPolicyNoRealtimeWaits(t *testing.T) {
	reportPolicy(t, ruleNoRealtime, syntacticViolations(t))
}

func TestTestPolicyNegativeFixtures(t *testing.T) {
	const header = "package p\n\nimport (\n\t\"testing\"\n\t\"testing/synctest\"\n\t\"time\"\n\n\t\"github.com/stretchr/testify/require\"\n)\n\nvar _ = require.True\nvar _ = synctest.Wait\nvar _ = time.Second\n\n"
	tests := []struct {
		name string
		src  string
		want []string // rule names, one per expected violation
	}{
		{"eventually", "func TestX(t *testing.T) { require.Eventually(t, nil, 0, 0) }", []string{ruleNoPolling}},
		{"never-f via assert alias", "func TestX(t *testing.T) { a := require.New(t); a.Neverf(nil, 0, 0, \"x\") }", []string{ruleNoPolling}},
		{"sleep violation", "func TestX(t *testing.T) { time.Sleep(time.Second) }", []string{ruleNoSleep}},
		{"sleep in bubble", "func TestX(t *testing.T) { synctest.Test(t, func(t *testing.T) { t.Run(\"a\", func(t *testing.T) { time.Sleep(1) }) }) }", nil},
		{"sleep after bubble", "func TestX(t *testing.T) { synctest.Test(t, func(t *testing.T) {}); time.Sleep(1) }", []string{ruleNoSleep}},
		{"sleep directive line above", "func h() {\n\t//testpolicy:sleep fake-time helper called from bubbles\n\ttime.Sleep(1)\n}", nil},
		{"sleep directive same line", "func h() {\n\ttime.Sleep(1) //testpolicy:sleep subprocess body\n}", nil},
		{"sleep directive without reason", "func h() {\n\t//testpolicy:sleep\n\ttime.Sleep(1)\n}", []string{ruleNoSleep}},
		{"sleep directive blank reason", "func h() {\n\t//testpolicy:sleep   \n\ttime.Sleep(1)\n}", []string{ruleNoSleep}},
		{"sleep directive two lines away", "func h() {\n\t//testpolicy:sleep reason\n\n\ttime.Sleep(1)\n}", []string{ruleNoSleep}},
		{"timer violation", "func h() { <-time.After(1); time.NewTimer(1); time.NewTicker(1); time.Tick(1); time.AfterFunc(1, nil) }", []string{ruleNoRealtime, ruleNoRealtime, ruleNoRealtime, ruleNoRealtime, ruleNoRealtime}},
		{"timer in bubble", "func TestX(t *testing.T) { synctest.Test(t, func(t *testing.T) { <-time.After(1) }) }", nil},
		{"timer in real I/O func", "// TestX does things.\n//\n// real I/O: unix socket\nfunc TestX(t *testing.T) { func() { <-time.After(1) }() }", nil},
		{"real I/O without reason", "// real I/O:\nfunc TestX(t *testing.T) { <-time.After(1) }", []string{ruleNoRealtime}},
		{"timer in realtime directive func", "// pollUntil polls.\n//testpolicy:realtime shared helper for real-I/O tests\nfunc pollUntil() { time.NewTicker(1) }", nil},
		{"realtime directive without reason", "//testpolicy:realtime\nfunc pollUntil() { time.NewTicker(1) }", []string{ruleNoRealtime}},
		{"timer in benchmark", "func BenchmarkX(b *testing.B) { <-time.After(1) }", nil},
		{"timer in fuzz and example", "func FuzzX(f *testing.F) { time.Tick(1) }\nfunc ExampleX() { time.Tick(1) }", nil},
		{"time.After as value", "func h() { f := time.After; _ = f }", []string{ruleNoRealtime}},
		{"time.Sleep as value", "func h() { run(time.Sleep) }\nfunc run(func(time.Duration)) {}", []string{ruleNoSleep}},
		{"time.After value in bubble", "func TestX(t *testing.T) { synctest.Test(t, func(t *testing.T) { f := time.After; _ = f }) }", nil},
		{"polling on other package", "func h() { _ = testing.Eventually }", nil},
		{"polling on non-package receiver", "func h(r interface{ Eventually() }) { r.Eventually() }", []string{ruleNoPolling}},
		{"polling via assert package", "func h() { _ = require.Never }", []string{ruleNoPolling}},
		{"sleep not allowed by real I/O", "// real I/O: sockets\nfunc TestX(t *testing.T) { time.Sleep(1) }", []string{ruleNoSleep}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fset := token.NewFileSet()
			file, err := parsePolicyFile(fset, "fixture_test.go", header+tt.src)
			if err != nil {
				t.Fatalf("parse: %v", err)
			}
			var got []string
			for _, v := range checkPolicySyntax(fset, file) {
				got = append(got, v.rule)
			}
			sort.Strings(got)
			want := append([]string(nil), tt.want...)
			sort.Strings(want)
			if strings.Join(got, ",") != strings.Join(want, ",") {
				t.Fatalf("violations = %v, want %v", got, want)
			}
		})
	}

	t.Run("renamed imports", func(t *testing.T) {
		src := "package p\nimport (\n\tclk \"time\"\n\tsyn \"testing/synctest\"\n\t\"testing\"\n)\nfunc TestX(t *testing.T) {\n\tclk.Sleep(1)\n\tsyn.Test(t, func(t *testing.T) { clk.Sleep(1) })\n}\n"
		fset := token.NewFileSet()
		file, err := parsePolicyFile(fset, "fixture_test.go", src)
		if err != nil {
			t.Fatal(err)
		}
		vs := checkPolicySyntax(fset, file)
		if len(vs) != 1 || vs[0].rule != ruleNoSleep || vs[0].pos.Line != 8 {
			t.Fatalf("violations = %v, want one sleep violation on line 8", vs)
		}
	})

	t.Run("dot imports", func(t *testing.T) {
		src := "package p\nimport (\n\t. \"time\"\n\t. \"testing/synctest\"\n)\n"
		fset := token.NewFileSet()
		file, err := parsePolicyFile(fset, "fixture_test.go", src)
		if err != nil {
			t.Fatal(err)
		}
		vs := checkPolicySyntax(fset, file)
		if len(vs) != 2 || vs[0].rule != ruleNoSleep || vs[1].rule != ruleNoSleep {
			t.Fatalf("violations = %v, want two %s violations", vs, ruleNoSleep)
		}
	})

	t.Run("hand-written mock-named test file is scanned", func(t *testing.T) {
		src := "package p\nimport \"time\"\nfunc h() { time.Sleep(1) }\n"
		if policyExcludedPath("internal/app/foo_mock_test.go") {
			t.Fatal("foo_mock_test.go must not be excluded by name")
		}
		fset := token.NewFileSet()
		file, err := parsePolicyFile(fset, "foo_mock_test.go", src)
		if err != nil {
			t.Fatal(err)
		}
		if isGeneratedFile(file) {
			t.Fatal("file without generated header reported as generated")
		}
		vs := checkPolicySyntax(fset, file)
		if len(vs) != 1 || vs[0].rule != ruleNoSleep {
			t.Fatalf("violations = %v, want one sleep violation", vs)
		}
	})

	t.Run("excluded paths", func(t *testing.T) {
		for path, want := range map[string]bool{
			"internal/app/x_test.go":                 false,
			"internal/app/lifecycle_mock_test.go":    false,
			"internal/ports/mocks/x_test.go":         true,
			"internal/protocol/wire/mocks/x_test.go": true,
			".worktrees/feat/internal/app/x_test.go": true,
			"internal/foo/testdata/x_test.go":        true,
			"vendor/x/x_test.go":                     true,
			"internal/ports/mocks_test.go":           false,
		} {
			if got := policyExcludedPath(path); got != want {
				t.Errorf("policyExcludedPath(%q) = %t, want %t", path, got, want)
			}
		}
		fset := token.NewFileSet()
		file, err := parsePolicyFile(fset, "gen_test.go", "// Code generated by mockery. DO NOT EDIT.\n\npackage p\n")
		if err != nil {
			t.Fatal(err)
		}
		if !isGeneratedFile(file) {
			t.Error("generated header not recognised")
		}
	})
}

// ---- Rule 4: hand-written doubles of mocked interfaces -------------------

type mockeryPackage struct {
	all        bool
	interfaces map[string]bool
}

// parseMockeryConfig extracts, for each package under `packages:`, whether
// `config.all` is true and the interface names listed under `interfaces:`.
// It is a deliberately minimal indentation-based reader of .mockery.yml.
func parseMockeryConfig(data []byte) map[string]*mockeryPackage {
	result := make(map[string]*mockeryPackage)
	var cur *mockeryPackage
	inPackages := false
	section := ""
	scanner := bufio.NewScanner(bytes.NewReader(data))
	for scanner.Scan() {
		raw := scanner.Text()
		if i := strings.Index(raw, " #"); i >= 0 {
			raw = raw[:i]
		}
		trimmed := strings.TrimSpace(raw)
		if trimmed == "" || strings.HasPrefix(trimmed, "#") {
			continue
		}
		indent := len(raw) - len(strings.TrimLeft(raw, " "))
		key, value, _ := strings.Cut(trimmed, ":")
		key = strings.Trim(key, `"'`)
		value = strings.TrimSpace(value)
		switch {
		case indent == 0:
			inPackages = key == "packages"
			cur, section = nil, ""
		case !inPackages:
		case indent == 2:
			cur = &mockeryPackage{interfaces: make(map[string]bool)}
			result[key] = cur
			section = ""
		case cur == nil:
		case indent == 4:
			section = key
		case indent == 6 && section == "config" && key == "all":
			cur.all = value == "true"
		case indent == 6 && section == "interfaces":
			cur.interfaces[key] = true
		}
	}
	return result
}

type listedPackage struct {
	ImportPath string
	Dir        string
	Name       string
	Export     string
	ForTest    string
	DepOnly    bool
	Standard   bool
	GoFiles    []string
	CgoFiles   []string
	ImportMap  map[string]string
	Deps       []string
	Module     *struct{ Path string }
}

func stripVariant(importPath string) string {
	before, _, _ := strings.Cut(importPath, " [")
	return before
}

func goListExport(t *testing.T) []listedPackage {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, "go", "list", "-export", "-deps", "-test", "-json", "./...")
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	stdout, err := cmd.Output()
	if err != nil {
		t.Fatalf("go list -export: %v\n%s", err, stderr.String())
	}
	var pkgs []listedPackage
	dec := json.NewDecoder(bytes.NewReader(stdout))
	for {
		var p listedPackage
		if err := dec.Decode(&p); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			t.Fatalf("decoding go list output: %v", err)
		}
		pkgs = append(pkgs, p)
	}
	return pkgs
}

// mockedInterfaces returns the named interfaces of pkg that mockery mocks
// according to cfg: all non-generic ones, or only the listed ones.
func mockedInterfaces(pkg *types.Package, cfg *mockeryPackage) map[string]*types.Named {
	out := make(map[string]*types.Named)
	scope := pkg.Scope()
	for _, name := range scope.Names() {
		if !cfg.all && !cfg.interfaces[name] {
			continue
		}
		tn, ok := scope.Lookup(name).(*types.TypeName)
		if !ok || tn.IsAlias() {
			continue
		}
		named, ok := tn.Type().(*types.Named)
		if !ok || named.TypeParams().Len() != 0 {
			continue
		}
		if iface, ok := named.Underlying().(*types.Interface); ok && iface.NumMethods() > 0 {
			out[name] = named
		}
	}
	return out
}

// directlyDeclaredMethods returns the methods of iface that are declared
// directly on named (value or pointer receiver), not merely promoted from an
// embedded field.
func directlyDeclaredMethods(named *types.Named, iface *types.Interface) []string {
	want := make(map[string]bool, iface.NumMethods())
	for i := 0; i < iface.NumMethods(); i++ {
		want[iface.Method(i).Name()] = true
	}
	var got []string
	for i := 0; i < named.NumMethods(); i++ {
		if name := named.Method(i).Name(); want[name] {
			got = append(got, name)
		}
	}
	sort.Strings(got)
	return got
}

func implementsEither(named *types.Named, iface *types.Interface) bool {
	return types.Implements(named, iface) || types.Implements(types.NewPointer(named), iface)
}

// doubleExemptions collects the type names whose declaration doc carries a
// `//testpolicy:double <reason>` directive, keyed by the identifier position.
func doubleExemptions(files []*ast.File) map[token.Pos]bool {
	exempt := make(map[token.Pos]bool)
	for _, file := range files {
		for _, decl := range file.Decls {
			gen, ok := decl.(*ast.GenDecl)
			if !ok || gen.Tok != token.TYPE {
				continue
			}
			for _, spec := range gen.Specs {
				ts := spec.(*ast.TypeSpec)
				if docHasDirective(gen.Doc, "double") || docHasDirective(ts.Doc, "double") {
					exempt[ts.Name.Pos()] = true
				}
			}
		}
	}
	return exempt
}

// checkPackageDoubles type-checks one package variant and reports hand-written
// doubles of mocked interfaces declared in its non-excluded test files.
func checkPackageDoubles(fset *token.FileSet, pkgPath string, files []*ast.File, imp types.Importer, cfg map[string]*mockeryPackage, root string) ([]policyViolation, []error) {
	var typeErrs []error
	info := &types.Info{Defs: make(map[*ast.Ident]types.Object)}
	conf := types.Config{
		Importer: imp,
		Error:    func(err error) { typeErrs = append(typeErrs, err) },
	}
	pkg, _ := conf.Check(pkgPath, fset, files, info)
	if pkg == nil {
		return nil, typeErrs
	}

	// Mocked interfaces visible to this package: its own, plus those of
	// directly imported packages.
	type mocked struct {
		pkgName, name string
		named         *types.Named
		iface         *types.Interface
	}
	var visible []mocked
	consider := func(p *types.Package) {
		c, ok := cfg[stripVariant(p.Path())]
		if !ok {
			return
		}
		ifaces := mockedInterfaces(p, c)
		names := make([]string, 0, len(ifaces))
		for n := range ifaces {
			names = append(names, n)
		}
		sort.Strings(names)
		for _, n := range names {
			visible = append(visible, mocked{p.Name(), n, ifaces[n], ifaces[n].Underlying().(*types.Interface)})
		}
	}
	consider(pkg)
	for _, p := range pkg.Imports() {
		consider(p)
	}
	if len(visible) == 0 {
		return nil, typeErrs
	}

	exempt := doubleExemptions(files)
	var out []policyViolation
	for ident, obj := range info.Defs {
		tn, ok := obj.(*types.TypeName)
		if !ok || tn.IsAlias() || exempt[ident.Pos()] {
			continue
		}
		pos := fset.Position(tn.Pos())
		rel, err := filepath.Rel(root, pos.Filename)
		if err != nil || !strings.HasSuffix(rel, "_test.go") || policyExcludedPath(rel) {
			continue
		}
		named, ok := tn.Type().(*types.Named)
		if !ok || named.TypeParams().Len() != 0 {
			continue
		}
		if _, isIface := named.Underlying().(*types.Interface); isIface {
			continue
		}
		pos.Filename = filepath.ToSlash(rel)
		for _, m := range visible {
			if named == m.named || !implementsEither(named, m.iface) {
				continue
			}
			direct := directlyDeclaredMethods(named, m.iface)
			if len(direct) == 0 {
				continue
			}
			out = append(out, policyViolation{
				rule: ruleNoDoubles,
				pos:  pos,
				msg: fmt.Sprintf("type %s is a hand-written double of %s.%s (declares %s); use the generated mock or add `//testpolicy:double <reason>`",
					tn.Name(), m.pkgName, m.name, strings.Join(direct, ", ")),
			})
		}
	}
	return out, typeErrs
}

func TestTestPolicyNoHandWrittenDoubles(t *testing.T) {
	root, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(".mockery.yml")
	if err != nil {
		t.Fatalf("reading .mockery.yml: %v", err)
	}
	cfg := parseMockeryConfig(data)
	if len(cfg) == 0 {
		t.Fatal(".mockery.yml lists no packages")
	}

	listed := goListExport(t)
	byImportPath := make(map[string]string, len(listed))
	for _, q := range listed {
		byImportPath[q.ImportPath] = q.Export
	}
	var violations []policyViolation
	var loadErrs []string
	for _, p := range listed {
		if p.ForTest == "" || p.DepOnly || p.Standard || !strings.Contains(p.ImportPath, " [") || p.Module == nil || !strings.HasPrefix(p.Dir, root) {
			continue
		}
		hasTest := false
		for _, f := range p.GoFiles {
			if strings.HasSuffix(f, "_test.go") {
				hasTest = true
			}
		}
		if !hasTest {
			continue
		}

		// Resolve imports to the export data of the exact variant this
		// package was built against.
		exports := make(map[string]string)
		for _, d := range p.Deps {
			if exp := byImportPath[d]; exp != "" {
				exports[stripVariant(d)] = exp
			}
		}
		lookup := func(path string) (io.ReadCloser, error) {
			exp, ok := exports[path]
			if !ok {
				return nil, fmt.Errorf("no export data for %q", path)
			}
			return os.Open(exp)
		}
		fset := token.NewFileSet()
		imp := importer.ForCompiler(fset, "gc", lookup)

		var files []*ast.File
		for _, name := range p.GoFiles {
			file, err := parsePolicyFile(fset, filepath.Join(p.Dir, name), nil)
			if err != nil {
				t.Fatalf("parse %s: %v", name, err)
			}
			if strings.HasSuffix(name, "_test.go") && isGeneratedFile(file) {
				// Still type-checked, but its types are not reported.
				markGenerated(fset, file)
			}
			files = append(files, file)
		}
		// Rewrite ImportMap-ed paths by wrapping the importer.
		wrapped := importerFunc(func(path string) (*types.Package, error) {
			if mapped, ok := p.ImportMap[path]; ok {
				path = stripVariant(mapped)
			}
			return imp.Import(path)
		})
		vs, errs := checkPackageDoubles(fset, stripVariant(p.ImportPath), files, wrapped, cfg, root)
		violations = append(violations, vs...)
		for _, e := range errs {
			loadErrs = append(loadErrs, fmt.Sprintf("%s: %v", p.ImportPath, e))
		}
	}
	if len(loadErrs) > 0 {
		sort.Strings(loadErrs)
		if len(loadErrs) > 20 {
			loadErrs = append(loadErrs[:20], fmt.Sprintf("... and %d more", len(loadErrs)-20))
		}
		t.Errorf("type-checking test packages failed:\n%s", strings.Join(loadErrs, "\n"))
	}
	reportPolicy(t, ruleNoDoubles, dropGenerated(violations))
}

type importerFunc func(path string) (*types.Package, error)

func (f importerFunc) Import(path string) (*types.Package, error) { return f(path) }

// generatedFiles records absolute paths of generated test files so that their
// declared types are not reported.
var generatedFiles = map[string]bool{}

func markGenerated(fset *token.FileSet, file *ast.File) {
	generatedFiles[fset.Position(file.Package).Filename] = true
}

func dropGenerated(vs []policyViolation) []policyViolation {
	var out []policyViolation
	for _, v := range vs {
		abs, _ := filepath.Abs(v.pos.Filename)
		if !generatedFiles[abs] {
			out = append(out, v)
		}
	}
	return out
}

func TestTestPolicyDoubleFixtures(t *testing.T) {
	const src = `package p

type I interface{ A(); B() }

type base struct{}

func (base) A() {}
func (base) B() {}

// Embedder only inherits the methods by promotion.
type Embedder struct{ base }

// Direct declares one method itself and promotes the other.
type Direct struct{ base }

func (*Direct) A() {}

type Partial struct{}

func (Partial) A() {}
`
	fset := token.NewFileSet()
	file, err := parsePolicyFile(fset, "p.go", src)
	if err != nil {
		t.Fatal(err)
	}
	pkg, err := (&types.Config{}).Check("p", fset, []*ast.File{file}, nil)
	if err != nil {
		t.Fatal(err)
	}
	iface := pkg.Scope().Lookup("I").Type().Underlying().(*types.Interface)
	ifaces := mockedInterfaces(pkg, &mockeryPackage{all: true})
	if _, ok := ifaces["I"]; !ok || len(ifaces) != 1 {
		t.Fatalf("mockedInterfaces = %v, want only I", ifaces)
	}
	tests := []struct {
		name       string
		implements bool
		direct     string
	}{
		{"Embedder", true, ""},
		{"Direct", true, "A"},
		{"Partial", false, "A"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			named := pkg.Scope().Lookup(tt.name).Type().(*types.Named)
			if got := implementsEither(named, iface); got != tt.implements {
				t.Errorf("implements = %t, want %t", got, tt.implements)
			}
			if got := strings.Join(directlyDeclaredMethods(named, iface), ","); got != tt.direct {
				t.Errorf("directlyDeclaredMethods = %q, want %q", got, tt.direct)
			}
		})
	}

	cfg := parseMockeryConfig([]byte("all: false\npackages:\n  example.com/a:\n    config:\n      all: true\n  example.com/b:\n    config:\n      dir: x\n    interfaces:\n      Foo:\n      Bar:\n        config:\n          dir: y\n"))
	if !cfg["example.com/a"].all || cfg["example.com/b"].all || !cfg["example.com/b"].interfaces["Foo"] || !cfg["example.com/b"].interfaces["Bar"] || len(cfg["example.com/b"].interfaces) != 2 {
		t.Errorf("parseMockeryConfig = %+v", cfg)
	}
}
