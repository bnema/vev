package command

import (
	"regexp"
	"testing"
)

func TestRegistryCodesAndSlugsAreUniqueInOrder(t *testing.T) {
	commands := Registry()
	wantCodes := []string{"CNT", "CNS", "CLT", "SPR", "SPL", "SPU", "SPD", "STP", "TST", "FLT", "CLP", "FPL", "FPR", "FPU", "FPD", "NXT", "PVT", "BSK", "JRS", "SSP", "NTC", "YLN", "VIS", "RNS", "RNT", "DET"}

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

func TestRegistryIncludesFloatingPaneToggle(t *testing.T) {
	cmd, ok := ByCode("FLT")
	if !ok {
		t.Fatal("ByCode(\"FLT\") ok = false, want true")
	}
	if cmd.Slug != "toggle-floating-pane" || cmd.Code != "FLT" || cmd.Name != "Toggle floating pane" {
		t.Fatalf("floating command = %#v, want toggle-floating-pane/FLT/Toggle floating pane", cmd)
	}
}

func TestRegistryIncludesNotifications(t *testing.T) {
	cmd, ok := ByCode("NTC")
	if !ok {
		t.Fatal("ByCode(\"NTC\") ok = false, want true")
	}
	if cmd.Slug != "notifications" || cmd.Code != "NTC" || cmd.Name != "Notifications" {
		t.Fatalf("notifications command = %#v, want notifications/NTC/Notifications", cmd)
	}
}

func TestRegistryIncludesYankLastNotification(t *testing.T) {
	cmd, ok := ByCode("YLN")
	if !ok {
		t.Fatal("ByCode(\"YLN\") ok = false, want true")
	}
	if cmd.Slug != "yank-last-notification" || cmd.Code != "YLN" || cmd.Name != "Yank last notification" {
		t.Fatalf("yank command = %#v, want yank-last-notification/YLN/Yank last notification", cmd)
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
