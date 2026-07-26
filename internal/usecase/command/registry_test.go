package command

import (
	"errors"
	"regexp"
	"strconv"
	"testing"
)

func TestRegistryCodesAndSlugsAreUniqueInOrder(t *testing.T) {
	commands := PaletteRegistry()
	wantCodes := []string{"CNT", "CNS", "CLT", "SPR", "SPL", "SPU", "SPD", "STP", "TST", "FLT", "CLP", "FPL", "FPR", "FPU", "FPD", "RSZ", "GPW", "SPW", "GPH", "SPH", "EQP", "NXT", "PVT", "BSK", "JRS", "SSP", "NTC", "YLN", "VIS", "RNS", "RNT", "DET"}

	if len(commands) != len(wantCodes) {
		t.Fatalf("Registry() returned %d commands, want %d", len(commands), len(wantCodes))
	}

	codePattern := regexp.MustCompile(`^[A-Z]{3}$`)
	seenCodes := make(map[string]bool, len(commands))
	seenSlugs := make(map[string]bool, len(commands))
	for i, cmd := range commands {
		if cmd.Code != wantCodes[i] {
			t.Fatalf("Registry()[%d].Code = %q, want %q", i, cmd.Code, wantCodes[i])
		}
		if !codePattern.MatchString(cmd.Code) {
			t.Errorf("Registry()[%d].Code = %q, want three uppercase letters", i, cmd.Code)
		}
		if seenCodes[cmd.Code] {
			t.Errorf("Registry()[%d].Code = %q, duplicate code", i, cmd.Code)
		}
		seenCodes[cmd.Code] = true
		if cmd.Slug == "" {
			t.Errorf("Registry()[%d].Slug is empty", i)
		}
		if seenSlugs[cmd.Slug] {
			t.Errorf("Registry()[%d].Slug = %q, duplicate slug", i, cmd.Slug)
		}
		seenSlugs[cmd.Slug] = true
	}
}

func TestBySlugExactMatch(t *testing.T) {
	tests := []struct {
		slug string
		ok   bool
	}{
		{slug: "split-right", ok: true},
		{slug: "SPLIT-RIGHT", ok: false},
		{slug: "toast", ok: true},
		{slug: "list-panes", ok: true},
		{slug: "does-not-exist", ok: false},
	}
	for _, tt := range tests {
		t.Run(tt.slug, func(t *testing.T) {
			if _, ok := BySlug(tt.slug); ok != tt.ok {
				t.Fatalf("BySlug(%q) ok = %v, want %v", tt.slug, ok, tt.ok)
			}
		})
	}
}

func TestRegistryControlMetadata(t *testing.T) {
	scriptable := map[string]TargetKind{
		"split-right": TargetPane, "split-left": TargetPane, "split-up": TargetPane,
		"split-down": TargetPane, "stack-pane": TargetPane, "toggle-stack": TargetPane,
		"close-pane": TargetPane, "focus-pane-left": TargetPane, "focus-pane-right": TargetPane,
		"focus-pane-up": TargetPane, "focus-pane-down": TargetPane,
		"new-tab": TargetSession, "close-tab": TargetTab, "next-tab": TargetSession,
		"previous-tab": TargetSession, "rename-session": TargetSession, "rename-tab": TargetTab,
		"new-session": TargetSession, "toast": TargetSession,
		"grow-pane-width": TargetPane, "shrink-pane-width": TargetPane,
		"grow-pane-height": TargetPane, "shrink-pane-height": TargetPane, "equalize-panes": TargetTab,
		"list-sessions": TargetNone, "list-tabs": TargetSession, "list-panes": TargetTab,
	}
	notScriptable := []string{
		"session-picker", "notifications", "visual-mode", "yank-last-notification",
		"detach", "back-session", "jump-recent-session", "toggle-floating-pane", "resize-pane",
	}
	for slug, target := range scriptable {
		cmd, ok := BySlug(slug)
		if !ok || !cmd.Scriptable {
			t.Errorf("%s: want scriptable entry, ok=%v scriptable=%v", slug, ok, cmd.Scriptable)
			continue
		}
		if cmd.Target != target || cmd.Control == nil || cmd.Usage == "" || cmd.Desc == "" {
			t.Errorf("%s: incomplete control metadata: %#v", slug, cmd)
		}
	}
	for _, slug := range notScriptable {
		cmd, ok := BySlug(slug)
		if !ok || cmd.Scriptable || cmd.Control != nil {
			t.Errorf("%s: want present and non-scriptable, got %#v, ok=%v", slug, cmd, ok)
		}
	}
}

func TestPaletteRegistryHidesAPIOnly(t *testing.T) {
	for _, cmd := range PaletteRegistry() {
		if !cmd.PaletteVisible || cmd.Run == nil {
			t.Errorf("palette command is not executable and visible: %#v", cmd)
		}
		if cmd.Slug == "toast" || cmd.Slug == "list-sessions" || cmd.Slug == "list-tabs" || cmd.Slug == "list-panes" {
			t.Errorf("API-only command %q is visible", cmd.Slug)
		}
	}
}

func TestSessionRecoveryCommand(t *testing.T) {
	cmd, ok := BySlug("session-recovery")
	if !ok || !cmd.Scriptable || cmd.Target != TargetNone {
		t.Fatalf("session-recovery command = %#v, ok=%v", cmd, ok)
	}
	tests := []struct {
		args     []string
		wantCall string
		wantErr  error
	}{
		{args: []string{"retry"}, wantCall: "session-recovery:retry:"},
		{args: []string{"restore", "7"}, wantCall: "session-recovery:restore:7"},
		{args: []string{"restore", "18446744073709551615"}, wantCall: "session-recovery:restore:18446744073709551615"},
		{args: []string{"export", "/tmp/export"}, wantCall: "session-recovery:export:/tmp/export"},
		{args: []string{"discard"}, wantCall: "session-recovery:discard:"},
		{args: nil, wantErr: ErrInvalidArguments},
		{args: []string{"retry", "extra"}, wantErr: ErrInvalidArguments},
		{args: []string{"restore"}, wantErr: ErrInvalidArguments},
		{args: []string{"restore", "0"}, wantErr: ErrInvalidArguments},
		{args: []string{"restore", "01"}, wantErr: ErrInvalidArguments},
		{args: []string{"restore", "18446744073709551616"}, wantErr: ErrInvalidArguments},
		{args: []string{"export", "relative/path"}, wantErr: ErrInvalidArguments},
		{args: []string{"export"}, wantErr: ErrInvalidArguments},
		{args: []string{"discard", "extra"}, wantErr: ErrInvalidArguments},
		{args: []string{"unknown"}, wantErr: ErrInvalidArguments},
	}
	for _, test := range tests {
		ctx := &controlSpy{recoveryOutput: "recovery output"}
		got, err := cmd.Control(ctx, test.args, ControlOptions{})
		wantOutput := ""
		if test.wantCall != "" {
			wantOutput = ctx.recoveryOutput
		}
		if !errors.Is(err, test.wantErr) || ctx.call != test.wantCall || got.Output != wantOutput {
			t.Errorf("args %v: call/output/error = %q/%q/%v, want %q/%q/%v", test.args, ctx.call, got.Output, err, test.wantCall, wantOutput, test.wantErr)
		}
	}
}

func TestControlHandlersValidateAndDelegate(t *testing.T) {
	tests := []struct {
		name       string
		slug       string
		args       []string
		opts       ControlOptions
		wantCall   string
		wantOutput string
		wantErr    error
	}{
		{name: "zero argument mutation delegates", slug: "split-right", wantCall: "split-right"},
		{name: "zero argument mutation rejects args", slug: "split-right", args: []string{"extra"}, wantErr: ErrInvalidArguments},
		{name: "grow width delegates", slug: "grow-pane-width", wantCall: "grow-pane-width"},
		{name: "shrink width delegates", slug: "shrink-pane-width", wantCall: "shrink-pane-width"},
		{name: "grow height delegates", slug: "grow-pane-height", wantCall: "grow-pane-height"},
		{name: "shrink height delegates", slug: "shrink-pane-height", wantCall: "shrink-pane-height"},
		{name: "equalize delegates", slug: "equalize-panes", wantCall: "equalize-panes"},
		{name: "resize mutation rejects arguments", slug: "grow-pane-width", args: []string{"extra"}, wantErr: ErrInvalidArguments},
		{name: "equalize rejects arguments", slug: "equalize-panes", args: []string{"extra"}, wantErr: ErrInvalidArguments},
		{name: "rename delegates name", slug: "rename-tab", args: []string{"editor"}, wantCall: "rename-tab:editor"},
		{name: "rename rejects empty name", slug: "rename-tab", args: []string{""}, wantErr: ErrInvalidArguments},
		{name: "toast delegates severity", slug: "toast", args: []string{"-l", "warn", "hello"}, wantCall: "toast:warn:hello"},
		{name: "toast rejects malformed flags", slug: "toast", args: []string{"-l", "warn"}, wantErr: ErrInvalidArguments},
		{name: "query delegates JSON option", slug: "list-panes", opts: ControlOptions{JSON: true}, wantCall: "list-panes:true", wantOutput: "panes"},
		{name: "query rejects args", slug: "list-panes", args: []string{"extra"}, wantErr: ErrInvalidArguments},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cmd, ok := BySlug(tt.slug)
			if !ok {
				t.Fatalf("BySlug(%q) missing", tt.slug)
			}
			ctx := &controlSpy{}
			got, err := cmd.Control(ctx, tt.args, tt.opts)
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("Control() error = %v, want %v", err, tt.wantErr)
			}
			if ctx.call != tt.wantCall || got.Output != tt.wantOutput {
				t.Fatalf("Control() call/output = %q/%q, want %q/%q", ctx.call, got.Output, tt.wantCall, tt.wantOutput)
			}
		})
	}
}

type controlSpy struct {
	call           string
	recoveryOutput string
}

func (s *controlSpy) record(call string) error          { s.call = call; return nil }
func (s *controlSpy) CreateTab() error                  { return s.record("new-tab") }
func (s *controlSpy) CreateSessionNamed(v string) error { return s.record("new-session:" + v) }
func (s *controlSpy) CloseTab() error                   { return s.record("close-tab") }
func (s *controlSpy) ClosePane() error                  { return s.record("close-pane") }
func (s *controlSpy) SplitRight() error                 { return s.record("split-right") }
func (s *controlSpy) SplitLeft() error                  { return s.record("split-left") }
func (s *controlSpy) SplitUp() error                    { return s.record("split-up") }
func (s *controlSpy) SplitDown() error                  { return s.record("split-down") }
func (s *controlSpy) StackPane() error                  { return s.record("stack-pane") }
func (s *controlSpy) ToggleStack() error                { return s.record("toggle-stack") }
func (s *controlSpy) GrowPaneWidth() error              { return s.record("grow-pane-width") }
func (s *controlSpy) ShrinkPaneWidth() error            { return s.record("shrink-pane-width") }
func (s *controlSpy) GrowPaneHeight() error             { return s.record("grow-pane-height") }
func (s *controlSpy) ShrinkPaneHeight() error           { return s.record("shrink-pane-height") }
func (s *controlSpy) EqualizePanes() error              { return s.record("equalize-panes") }
func (s *controlSpy) FocusPaneLeft() error              { return s.record("focus-pane-left") }
func (s *controlSpy) FocusPaneRight() error             { return s.record("focus-pane-right") }
func (s *controlSpy) FocusPaneUp() error                { return s.record("focus-pane-up") }
func (s *controlSpy) FocusPaneDown() error              { return s.record("focus-pane-down") }
func (s *controlSpy) NextTab() error                    { return s.record("next-tab") }
func (s *controlSpy) PrevTab() error                    { return s.record("previous-tab") }
func (s *controlSpy) RenameSessionTo(v string) error    { return s.record("rename-session:" + v) }
func (s *controlSpy) RenameTabTo(v string) error        { return s.record("rename-tab:" + v) }
func (s *controlSpy) Toast(level, message string) error {
	return s.record("toast:" + level + ":" + message)
}
func (s *controlSpy) SessionRecovery(action, argument string) (string, error) {
	return s.recoveryOutput, s.record("session-recovery:" + action + ":" + argument)
}
func (s *controlSpy) ListSessions(json bool) (string, error) {
	_ = s.record("list-sessions:" + strconv.FormatBool(json))
	return "sessions", nil
}
func (s *controlSpy) ListTabs(json bool) (string, error) {
	_ = s.record("list-tabs:" + strconv.FormatBool(json))
	return "tabs", nil
}
func (s *controlSpy) ListPanes(json bool) (string, error) {
	_ = s.record("list-panes:" + strconv.FormatBool(json))
	return "panes", nil
}

func TestRegistryIncludesFloatingPaneToggle(t *testing.T) {
	cmd, ok := ByCode("FLT")
	if !ok {
		t.Fatal("ByCode(\"FLT\") ok = false, want true")
	}
	if cmd.Slug != "toggle-floating-pane" || cmd.Code != "FLT" || cmd.Name != "Toggle floating pane" {
		t.Fatalf("floating command = %#v, want toggle-floating-pane/FLT/Toggle floating pane", cmd)
	}
}

func TestRegistryIncludesNotificationCommands(t *testing.T) {
	for _, tt := range []struct {
		code string
		slug string
		name string
	}{
		{code: "NTC", slug: "notifications", name: "Notifications"},
		{code: "YLN", slug: "yank-last-notification", name: "Yank last notification"},
	} {
		t.Run(tt.code, func(t *testing.T) {
			cmd, ok := ByCode(tt.code)
			if !ok {
				t.Fatalf("ByCode(%q) ok = false, want true", tt.code)
			}
			if cmd.Slug != tt.slug || cmd.Code != tt.code || cmd.Name != tt.name {
				t.Fatalf("command = %#v, want %s/%s/%s", cmd, tt.slug, tt.code, tt.name)
			}
		})
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
		code   string
		expect func(*MockContext)
	}{
		{code: "CNT", expect: func(ctx *MockContext) { ctx.EXPECT().CreateTab().Return(nil).Once() }},
		{code: "CNS", expect: func(ctx *MockContext) { ctx.EXPECT().CreateSession().Return(nil).Once() }},
		{code: "CLT", expect: func(ctx *MockContext) { ctx.EXPECT().CloseTab().Return(nil).Once() }},
		{code: "SPR", expect: func(ctx *MockContext) { ctx.EXPECT().SplitRight().Return(nil).Once() }},
		{code: "SPL", expect: func(ctx *MockContext) { ctx.EXPECT().SplitLeft().Return(nil).Once() }},
		{code: "SPU", expect: func(ctx *MockContext) { ctx.EXPECT().SplitUp().Return(nil).Once() }},
		{code: "SPD", expect: func(ctx *MockContext) { ctx.EXPECT().SplitDown().Return(nil).Once() }},
		{code: "STP", expect: func(ctx *MockContext) { ctx.EXPECT().StackPane().Return(nil).Once() }},
		{code: "TST", expect: func(ctx *MockContext) { ctx.EXPECT().ToggleStack().Return(nil).Once() }},
		{code: "FLT", expect: func(ctx *MockContext) { ctx.EXPECT().ToggleFloatingPane().Return(nil).Once() }},
		{code: "CLP", expect: func(ctx *MockContext) { ctx.EXPECT().ClosePane().Return(nil).Once() }},
		{code: "FPL", expect: func(ctx *MockContext) { ctx.EXPECT().FocusPaneLeft().Return(nil).Once() }},
		{code: "FPR", expect: func(ctx *MockContext) { ctx.EXPECT().FocusPaneRight().Return(nil).Once() }},
		{code: "FPU", expect: func(ctx *MockContext) { ctx.EXPECT().FocusPaneUp().Return(nil).Once() }},
		{code: "FPD", expect: func(ctx *MockContext) { ctx.EXPECT().FocusPaneDown().Return(nil).Once() }},
		{code: "RSZ", expect: func(ctx *MockContext) { ctx.EXPECT().EnterResizeMode().Return(nil).Once() }},
		{code: "GPW", expect: func(ctx *MockContext) { ctx.EXPECT().GrowPaneWidth().Return(nil).Once() }},
		{code: "SPW", expect: func(ctx *MockContext) { ctx.EXPECT().ShrinkPaneWidth().Return(nil).Once() }},
		{code: "GPH", expect: func(ctx *MockContext) { ctx.EXPECT().GrowPaneHeight().Return(nil).Once() }},
		{code: "SPH", expect: func(ctx *MockContext) { ctx.EXPECT().ShrinkPaneHeight().Return(nil).Once() }},
		{code: "EQP", expect: func(ctx *MockContext) { ctx.EXPECT().EqualizePanes().Return(nil).Once() }},
		{code: "NXT", expect: func(ctx *MockContext) { ctx.EXPECT().NextTab().Return(nil).Once() }},
		{code: "PVT", expect: func(ctx *MockContext) { ctx.EXPECT().PrevTab().Return(nil).Once() }},
		{code: "BSK", expect: func(ctx *MockContext) { ctx.EXPECT().BackSession().Return(nil).Once() }},
		{code: "JRS", expect: func(ctx *MockContext) { ctx.EXPECT().JumpRecentSession(1).Return(nil).Once() }},
		{code: "SSP", expect: func(ctx *MockContext) { ctx.EXPECT().OpenSessionPicker().Return(nil).Once() }},
		{code: "NTC", expect: func(ctx *MockContext) { ctx.EXPECT().OpenNotifications().Return(nil).Once() }},
		{code: "YLN", expect: func(ctx *MockContext) { ctx.EXPECT().YankLastNotification().Return(nil).Once() }},
		{code: "VIS", expect: func(ctx *MockContext) { ctx.EXPECT().EnterVisualMode().Return(nil).Once() }},
		{code: "RNS", expect: func(ctx *MockContext) { ctx.EXPECT().RenameSession().Return(nil).Once() }},
		{code: "RNT", expect: func(ctx *MockContext) { ctx.EXPECT().RenameTab().Return(nil).Once() }},
		{code: "DET", expect: func(ctx *MockContext) { ctx.EXPECT().Detach().Return(nil).Once() }},
	}

	for _, tt := range tests {
		t.Run(tt.code, func(t *testing.T) {
			cmd, ok := ByCode(tt.code)
			if !ok {
				t.Fatalf("ByCode(%q) ok = false, want true", tt.code)
			}

			ctx := NewMockContext(t)
			tt.expect(ctx)
			args := []string(nil)
			if tt.code == "JRS" {
				args = []string{"1"}
			}
			if err := cmd.Run(ctx, args); err != nil {
				t.Fatalf("Run() error = %v, want nil", err)
			}
		})
	}
}
