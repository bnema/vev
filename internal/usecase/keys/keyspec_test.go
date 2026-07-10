package keys

import (
	"testing"

	"github.com/bnema/vev/internal/domain"
	"github.com/stretchr/testify/require"
)

func TestParseKeySpecAcceptsValidSpecs(t *testing.T) {
	cases := []struct {
		name      string
		spec      string
		wantRunes []rune
		wantArrow byte
	}{
		{name: "letter", spec: "alt+x", wantRunes: []rune{'x'}},
		{name: "unicode single char", spec: "alt+ñ", wantRunes: []rune{'ñ'}},
		{name: "space", spec: "alt+space", wantRunes: []rune{' '}},
		{name: "left arrow", spec: "alt+left", wantArrow: 'D'},
		{name: "right arrow", spec: "alt+right", wantArrow: 'C'},
		{name: "up arrow", spec: "alt+up", wantArrow: 'A'},
		{name: "down arrow", spec: "alt+down", wantArrow: 'B'},
		{name: "digit includes layout aliases", spec: "alt+2", wantRunes: []rune{'2', 'é'}},
		{name: "digit eight includes known aliases", spec: "alt+8", wantRunes: []rune{'8', '_', '!'}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ParseKeySpec(tc.spec)
			require.NoError(t, err)
			require.Equal(t, tc.wantRunes, got.altRunes)
			require.Equal(t, tc.wantArrow, got.altArrow)
		})
	}
}

func TestParseKeySpecRejectsMalformedSpecs(t *testing.T) {
	cases := []string{
		"",
		"x",
		"ctrl+x",
		"alt+",
		"alt+xy",
		"alt+0",
		"alt+10",
		"alt+tab",
		"alt+Space",
		" alt+x",
		"alt+x ",
	}

	for _, spec := range cases {
		t.Run(spec, func(t *testing.T) {
			_, err := ParseKeySpec(spec)
			require.Error(t, err)
		})
	}
}

func TestActionNamesAreCanonical(t *testing.T) {
	cases := []struct {
		a    Action
		name string
	}{
		{ActionOpenPalette, "open-palette"},
		{ActionToggleFloatingPane, "toggle-floating-pane"},
		{ActionJumpAttention, "jump-attention"},
		{ActionFocusPaneLeft, "focus-pane-left"},
		{ActionFocusPaneRight, "focus-pane-right"},
		{ActionFocusPaneUp, "focus-pane-up"},
		{ActionFocusPaneDown, "focus-pane-down"},
		{ActionSwitchTab1, "switch-tab-1"},
		{ActionSwitchTab2, "switch-tab-2"},
		{ActionSwitchTab3, "switch-tab-3"},
		{ActionSwitchTab4, "switch-tab-4"},
		{ActionSwitchTab5, "switch-tab-5"},
		{ActionSwitchTab6, "switch-tab-6"},
		{ActionSwitchTab7, "switch-tab-7"},
		{ActionSwitchTab8, "switch-tab-8"},
		{ActionSwitchTab9, "switch-tab-9"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.name, tc.a.Name())
			got, ok := ActionByName(tc.name)
			require.True(t, ok)
			require.Equal(t, tc.a, got)
		})
	}

	require.Equal(t, "unknown", Action(99).Name())
	_, ok := ActionByName("unknown-action")
	require.False(t, ok)
}

func TestBuildBindingsAppliesOverrides(t *testing.T) {
	bindings, warnings := BuildBindings(map[string]string{
		"open-palette":         "alt+o",
		"toggle-floating-pane": "alt+g",
		"focus-pane-left":      "alt+left",
		"switch-tab-2":         "alt+2",
	})
	require.Empty(t, warnings)

	assertAltRuneAction(t, bindings, 'o', ActionOpenPalette)
	assertAltRuneUnbound(t, bindings, ' ')
	assertAltRuneAction(t, bindings, 'g', ActionToggleFloatingPane)
	assertAltRuneUnbound(t, bindings, 'f')
	assertAltArrowAction(t, bindings, 'D', ActionFocusPaneLeft)
	assertAltRuneUnbound(t, bindings, 'h')
	assertAltRuneAction(t, bindings, '2', ActionSwitchTab2)
	assertAltRuneAction(t, bindings, 'é', ActionSwitchTab2)
}

func TestBuildBindingsWarnsAndSkipsInvalidEntries(t *testing.T) {
	bindings, warnings := BuildBindings(map[string]string{
		"missing":        "alt+m",
		"open-palette":   "ctrl+o",
		"jump-attention": "alt+j",
	})

	require.Len(t, warnings, 3)
	warningText := warnings[0].Msg + "\n" + warnings[1].Msg + "\n" + warnings[2].Msg
	require.Contains(t, warningText, "unknown action")
	require.Contains(t, warningText, "invalid key")
	require.Contains(t, warningText, "duplicate key")
	assertAltRuneAction(t, bindings, ' ', ActionOpenPalette)
	assertAltRuneAction(t, bindings, 'a', ActionJumpAttention)
	assertAltRuneAction(t, bindings, 'j', ActionFocusPaneDown)
}

func TestBuildBindingsAllowsSameActionToKeepDefaultKey(t *testing.T) {
	bindings, warnings := BuildBindings(map[string]string{
		"focus-pane-left": "alt+h",
	})

	require.Empty(t, warnings)
	assertAltRuneAction(t, bindings, 'h', ActionFocusPaneLeft)
}

func TestBuildBindingEntriesDuplicateKeyConflictsUseFileOrder(t *testing.T) {
	tests := []struct {
		name              string
		overrides         []domain.ConfigEntry
		wantAltP          Action
		wantOpenDefault   bool
		wantFocusDefault  bool
		wantWarningAction string
	}{
		{
			name: "open palette first wins",
			overrides: []domain.ConfigEntry{
				{Key: "open-palette", Value: "alt+p"},
				{Key: "focus-pane-left", Value: "alt+p"},
			},
			wantAltP:          ActionOpenPalette,
			wantFocusDefault:  true,
			wantWarningAction: "focus-pane-left",
		},
		{
			name: "focus pane left first wins",
			overrides: []domain.ConfigEntry{
				{Key: "focus-pane-left", Value: "alt+p"},
				{Key: "open-palette", Value: "alt+p"},
			},
			wantAltP:          ActionFocusPaneLeft,
			wantOpenDefault:   true,
			wantWarningAction: "open-palette",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bindings, warnings := BuildBindingEntries(tt.overrides)

			require.Len(t, warnings, 1)
			require.Contains(t, warnings[0].Msg, tt.wantWarningAction)
			assertAltRuneAction(t, bindings, 'p', tt.wantAltP)
			if tt.wantOpenDefault {
				assertAltRuneAction(t, bindings, ' ', ActionOpenPalette)
			} else {
				assertAltRuneUnbound(t, bindings, ' ')
			}
			if tt.wantFocusDefault {
				assertAltRuneAction(t, bindings, 'h', ActionFocusPaneLeft)
			} else {
				assertAltRuneUnbound(t, bindings, 'h')
			}
		})
	}
}

func TestBuildBindingsFreesDefaultsForOverriddenActionsBeforeConflictChecks(t *testing.T) {
	bindings, warnings := BuildBindings(map[string]string{
		"open-palette":    "alt+o",
		"focus-pane-down": "alt+space",
	})

	require.Empty(t, warnings)
	assertAltRuneAction(t, bindings, 'o', ActionOpenPalette)
	assertAltRuneAction(t, bindings, ' ', ActionFocusPaneDown)
	assertAltRuneUnbound(t, bindings, 'j')
}

func TestBuildBindingEntriesFailedOverrideRestoreDoesNotOverwriteEarlierOverride(t *testing.T) {
	bindings, warnings := BuildBindingEntries([]domain.ConfigEntry{
		{Key: "open-palette", Value: "alt+a"},
		{Key: "jump-attention", Value: "alt+a"},
	})

	require.Len(t, warnings, 1)
	assertAltRuneAction(t, bindings, 'a', ActionOpenPalette)
	assertAltRuneUnbound(t, bindings, ' ')
}

func assertAltRuneAction(t *testing.T, bindings *Bindings, key rune, want Action) {
	t.Helper()
	got, _, ok := bindings.actionForAltBytes([]byte(string(key)))
	require.True(t, ok)
	require.Equal(t, want, got)
}

func assertAltRuneUnbound(t *testing.T, bindings *Bindings, key rune) {
	t.Helper()
	_, _, ok := bindings.actionForAltBytes([]byte(string(key)))
	require.False(t, ok)
}

func assertAltArrowAction(t *testing.T, bindings *Bindings, final byte, want Action) {
	t.Helper()
	got, ok := bindings.actionForAltArrow(final)
	require.True(t, ok)
	require.Equal(t, want, got)
}
