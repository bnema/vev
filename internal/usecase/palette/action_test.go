package palette

import (
	"testing"
	"time"

	"github.com/bnema/vev/internal/usecase/command"
)

func TestParseActionExactEffectiveToken(t *testing.T) {
	results := CommandResults([]command.Command{
		{Code: "BCK", Arguments: command.ArgumentsNone},
		{Code: "JRS", Arguments: command.ArgumentsRequired, ContextHint: command.ContextHintRecentSessions},
	})
	tests := []struct {
		name  string
		input string
		ok    bool
		code  string
		args  []string
	}{
		{name: "static token ignores case and whitespace", input: "  bCk  ", ok: true, code: "BCK"},
		{name: "required arguments accept repeated whitespace", input: " JRS   2 ", ok: true, code: "JRS", args: []string{"2"}},
		{name: "partial token rejected", input: "JR", ok: false},
		{name: "concatenated token rejected", input: "JRS2", ok: false},
		{name: "static arguments rejected", input: "BCK 2", ok: false},
		{name: "missing required argument rejected", input: "JRS", ok: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			action, ok := ParseAction(results, tt.input)
			if ok != tt.ok {
				t.Fatalf("ParseAction(%q) ok = %v, want %v", tt.input, ok, tt.ok)
			}
			if !ok {
				return
			}
			if action.Command.Code != tt.code || !sameStrings(action.Args, tt.args) {
				t.Fatalf("ParseAction(%q) = %#v, want %s %#v", tt.input, action, tt.code, tt.args)
			}
		})
	}
}

func TestArgumentCommandKeepsExactRowVisible(t *testing.T) {
	cmd := command.Command{Code: "JRS", Arguments: command.ArgumentsRequired}
	if got, ok := ArgumentCommand(CommandResults([]command.Command{cmd}), "JRS 1"); !ok || got.Code != "JRS" {
		t.Fatalf("ArgumentCommand() = %#v, %v; want JRS, true", got, ok)
	}
}

func TestArgumentResultReturnsTheMatchingRowAndCommand(t *testing.T) {
	jrs := command.Command{Code: "JRS", Arguments: command.ArgumentsRequired}
	results := []Result{NewStoppedSessionResult("JRS", time.Time{}), NewCommandResult(jrs)}

	result, got, ok := argumentResult(results, "jrs 1")

	if !ok || result.Kind() != ResultKindCommand || got.Code != jrs.Code {
		t.Fatalf("argumentResult() = %#v, %#v, %v; want matching JRS row and command", result, got, ok)
	}
}

func sameStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
