package keys

import (
	"fmt"
	"sort"
	"strings"
	"unicode/utf8"

	"github.com/bnema/vev/internal/domain"
)

const altPrefix = "alt+"

// KeySpec is one supported user-facing key binding specification.
type KeySpec struct {
	altRunes []rune
	altArrow byte
}

// ParseKeySpec parses vev's config key grammar.
func ParseKeySpec(spec string) (KeySpec, error) {
	if !strings.HasPrefix(spec, altPrefix) {
		return KeySpec{}, fmt.Errorf("key spec must start with %q", altPrefix)
	}
	key := strings.TrimPrefix(spec, altPrefix)
	switch key {
	case "space":
		return KeySpec{altRunes: []rune{' '}}, nil
	case "left":
		return KeySpec{altArrow: 'D'}, nil
	case "right":
		return KeySpec{altArrow: 'C'}, nil
	case "up":
		return KeySpec{altArrow: 'A'}, nil
	case "down":
		return KeySpec{altArrow: 'B'}, nil
	}
	if utf8.RuneCountInString(key) != 1 {
		return KeySpec{}, fmt.Errorf("key spec must be alt+<char>, alt+space, or alt+left/right/up/down")
	}
	r, _ := utf8.DecodeRuneInString(key)
	if idx, ok := topRowDigitIndex(r); ok {
		return KeySpec{altRunes: append([]rune(nil), topRowDigitAliases[idx]...)}, nil
	}
	if r >= '0' && r <= '9' {
		return KeySpec{}, fmt.Errorf("only alt+1 through alt+9 are supported digit bindings")
	}
	return KeySpec{altRunes: []rune{r}}, nil
}

var actionNames = map[Action]string{
	ActionOpenPalette:             "open-palette",
	ActionToggleFloatingPane:      "toggle-floating-pane",
	ActionJumpAttention:           "jump-attention",
	ActionFocusPaneLeft:           "focus-pane-left",
	ActionFocusPaneRight:          "focus-pane-right",
	ActionFocusPaneUp:             "focus-pane-up",
	ActionFocusPaneDown:           "focus-pane-down",
	ActionSwitchTab1:              "switch-tab-1",
	ActionSwitchTab2:              "switch-tab-2",
	ActionSwitchTab3:              "switch-tab-3",
	ActionSwitchTab4:              "switch-tab-4",
	ActionSwitchTab5:              "switch-tab-5",
	ActionSwitchTab6:              "switch-tab-6",
	ActionSwitchTab7:              "switch-tab-7",
	ActionSwitchTab8:              "switch-tab-8",
	ActionSwitchTab9:              "switch-tab-9",
	ActionGrowPaneWidth:           "grow-pane-width",
	ActionShrinkPaneWidth:         "shrink-pane-width",
	ActionGrowPaneHeight:          "grow-pane-height",
	ActionShrinkPaneHeight:        "shrink-pane-height",
	ActionEqualizePanes:           "equalize-panes",
	ActionConsumeOrExpelPaneLeft:  "consume-or-expel-pane-left",
	ActionConsumeOrExpelPaneRight: "consume-or-expel-pane-right",
}

var actionsByName = func() map[string]Action {
	byName := make(map[string]Action, len(actionNames))
	for action, name := range actionNames {
		if _, dup := byName[name]; dup {
			panic(fmt.Sprintf("keys: duplicate action name %q", name))
		}
		byName[name] = action
	}
	return byName
}()

// Name returns the canonical config name for an action.
func (a Action) Name() string {
	name, ok := actionNames[a]
	if !ok {
		return "unknown"
	}
	return name
}

// ActionByName returns the action for a canonical config name.
func ActionByName(name string) (Action, bool) {
	action, ok := actionsByName[name]
	return action, ok
}

// BuildBindings starts from vev's defaults and applies config overrides. Map
// inputs have no file order, so names are applied lexicographically for stable
// behavior. Parsed configs should call BuildBindingEntries to preserve file
// order for duplicate-key first-wins semantics.
func BuildBindings(overrides map[string]string) (*Bindings, []domain.Warning) {
	entries := make([]domain.ConfigEntry, 0, len(overrides))
	for _, name := range sortedOverrideNames(overrides) {
		entries = append(entries, domain.ConfigEntry{Key: name, Value: overrides[name]})
	}
	return BuildBindingEntries(entries)
}

// BuildBindingEntries starts from vev's defaults and applies config overrides
// in the order they appeared in the config file. When two actions request the
// same key, the first valid override wins and later actions keep their default.
func BuildBindingEntries(overrides []domain.ConfigEntry) (*Bindings, []domain.Warning) {
	bindings := DefaultBindings().clone()
	var warnings []domain.Warning
	var desired []bindingOverride
	for _, entry := range overrides {
		action, ok := ActionByName(entry.Key)
		if !ok {
			warnings = append(warnings, domain.Warning{Msg: fmt.Sprintf("unknown action %q", entry.Key)})
			continue
		}
		spec, err := ParseKeySpec(entry.Value)
		if err != nil {
			warnings = append(warnings, domain.Warning{Msg: fmt.Sprintf("invalid key for %q: %v", entry.Key, err)})
			continue
		}
		desired = append(desired, bindingOverride{name: entry.Key, action: action, spec: spec})
	}
	for _, override := range desired {
		bindings.removeAction(override.action)
	}
	for _, override := range desired {
		if conflict, ok := bindings.conflictingAction(override.spec, override.action); ok {
			warnings = append(warnings, domain.Warning{Msg: fmt.Sprintf("duplicate key for %q conflicts with %q", override.name, conflict.Name())})
			bindings.restoreDefaultAction(override.action)
			continue
		}
		bindings.bind(override.action, override.spec)
	}
	return bindings, warnings
}

type bindingOverride struct {
	name   string
	action Action
	spec   KeySpec
}

func sortedOverrideNames(overrides map[string]string) []string {
	names := make([]string, 0, len(overrides))
	for name := range overrides {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}
