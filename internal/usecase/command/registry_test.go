package command

import (
	"fmt"
	"regexp"
	"testing"
)

func TestRegistryCodesAreUniqueUppercaseThreeLettersInOrder(t *testing.T) {
	commands := Registry()
	wantCodes := []string{"CNT", "CLT", "NXT", "PVT", "SSP", "VIS", "RNS", "DET"}

	if len(commands) != len(wantCodes) {
		t.Fatalf("Registry() returned %d commands, want %d", len(commands), len(wantCodes))
	}

	codePattern := regexp.MustCompile(`^[A-Z]{3}$`)
	seen := make(map[string]bool, len(commands))
	for i, cmd := range commands {
		if cmd.Code != wantCodes[i] {
			t.Fatalf("Registry()[%d].Code = %q, want %q", i, cmd.Code, wantCodes[i])
		}
		if !codePattern.MatchString(cmd.Code) {
			t.Errorf("Registry()[%d].Code = %q, want three uppercase letters", i, cmd.Code)
		}
		if seen[cmd.Code] {
			t.Errorf("Registry()[%d].Code = %q, duplicate code", i, cmd.Code)
		}
		seen[cmd.Code] = true
	}
}

func TestByCodeIsCaseInsensitive(t *testing.T) {
	for _, code := range []string{"CNT", "cnt", "CnT"} {
		cmd, ok := ByCode(code)
		if !ok {
			t.Fatalf("ByCode(%q) ok = false, want true", code)
		}
		if cmd.Code != "CNT" {
			t.Fatalf("ByCode(%q).Code = %q, want CNT", code, cmd.Code)
		}
	}

	if _, ok := ByCode("missing"); ok {
		t.Fatal("ByCode(unknown) ok = true, want false")
	}
}

func TestCommandRunCallsMatchingContextMethod(t *testing.T) {
	tests := []struct {
		code string
		want string
	}{
		{code: "CNT", want: "CreateTab"},
		{code: "CLT", want: "CloseTab"},
		{code: "NXT", want: "NextTab"},
		{code: "PVT", want: "PrevTab"},
		{code: "SSP", want: "OpenSessionPicker"},
		{code: "VIS", want: "EnterVisualMode"},
		{code: "RNS", want: "RenameSession"},
		{code: "DET", want: "Detach"},
	}

	for _, tt := range tests {
		t.Run(tt.code, func(t *testing.T) {
			cmd, ok := ByCode(tt.code)
			if !ok {
				t.Fatalf("ByCode(%q) ok = false, want true", tt.code)
			}

			ctx := &spyContext{}
			if err := cmd.Run(ctx); err != nil {
				t.Fatalf("Run() error = %v, want nil", err)
			}
			if got := ctx.onlyCall(); got != tt.want {
				t.Fatalf("Run() called %q, want %q", got, tt.want)
			}
		})
	}
}

type spyContext struct {
	calls []string
}

func (s *spyContext) record(name string) error {
	s.calls = append(s.calls, name)
	return nil
}

func (s *spyContext) onlyCall() string {
	if len(s.calls) != 1 {
		return fmt.Sprintf("%v", s.calls)
	}
	return s.calls[0]
}

func (s *spyContext) CreateTab() error         { return s.record("CreateTab") }
func (s *spyContext) CloseTab() error          { return s.record("CloseTab") }
func (s *spyContext) NextTab() error           { return s.record("NextTab") }
func (s *spyContext) PrevTab() error           { return s.record("PrevTab") }
func (s *spyContext) Detach() error            { return s.record("Detach") }
func (s *spyContext) EnterVisualMode() error   { return s.record("EnterVisualMode") }
func (s *spyContext) RenameSession() error     { return s.record("RenameSession") }
func (s *spyContext) OpenSessionPicker() error { return s.record("OpenSessionPicker") }
