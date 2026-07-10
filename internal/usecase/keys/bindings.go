package keys

import "unicode/utf8"

// Bindings maps intercepted Alt keys to actions. A Bindings value is mutable
// while being built, then treated as frozen once published through an atomic
// pointer; callers must clone before using mutators.
type Bindings struct {
	altRunes  map[rune]Action
	altArrows map[byte]Action
}

var defaultBindings = DefaultBindings()

// DefaultBindings returns vev's built-in Alt-key bindings.
func DefaultBindings() *Bindings {
	altRunes := map[rune]Action{
		' ': ActionOpenPalette,
		'a': ActionJumpAttention,
		'f': ActionToggleFloatingPane,
		'h': ActionFocusPaneLeft,
		'j': ActionFocusPaneDown,
		'k': ActionFocusPaneUp,
		'l': ActionFocusPaneRight,
	}
	for idx, aliases := range topRowDigitAliases {
		for _, alias := range aliases {
			altRunes[alias] = ActionSwitchTab1 + Action(idx)
		}
	}
	return &Bindings{
		altRunes: altRunes,
		altArrows: map[byte]Action{
			'A': ActionFocusPaneUp,
			'B': ActionFocusPaneDown,
			'C': ActionFocusPaneRight,
			'D': ActionFocusPaneLeft,
		},
	}
}

func (b *Bindings) clone() *Bindings {
	if b == nil {
		return DefaultBindings()
	}
	clone := &Bindings{
		altRunes:  make(map[rune]Action, len(b.altRunes)),
		altArrows: make(map[byte]Action, len(b.altArrows)),
	}
	for key, action := range b.altRunes {
		clone.altRunes[key] = action
	}
	for key, action := range b.altArrows {
		clone.altArrows[key] = action
	}
	return clone
}

func (b *Bindings) removeAction(action Action) {
	for key, bound := range b.altRunes {
		if bound == action {
			delete(b.altRunes, key)
		}
	}
	for key, bound := range b.altArrows {
		if bound == action {
			delete(b.altArrows, key)
		}
	}
}

func (b *Bindings) bind(action Action, spec KeySpec) {
	for _, key := range spec.altRunes {
		b.altRunes[key] = action
	}
	if spec.altArrow != 0 {
		b.altArrows[spec.altArrow] = action
	}
}

func (b *Bindings) restoreDefaultAction(action Action) {
	defaults := DefaultBindings()
	for key, bound := range defaults.altRunes {
		if bound == action {
			if _, occupied := b.altRunes[key]; !occupied {
				b.altRunes[key] = action
			}
		}
	}
	for key, bound := range defaults.altArrows {
		if bound == action {
			if _, occupied := b.altArrows[key]; !occupied {
				b.altArrows[key] = action
			}
		}
	}
}

func (b *Bindings) conflictingAction(spec KeySpec, action Action) (Action, bool) {
	for _, key := range spec.altRunes {
		if bound, ok := b.altRunes[key]; ok && bound != action {
			return bound, true
		}
	}
	if spec.altArrow != 0 {
		if bound, ok := b.altArrows[spec.altArrow]; ok && bound != action {
			return bound, true
		}
	}
	return 0, false
}

func (b *Bindings) actionForAltBytes(data []byte) (Action, int, bool) {
	if b == nil {
		b = defaultBindings
	}
	if len(data) == 0 {
		return 0, 0, false
	}
	key, size := utf8.DecodeRune(data)
	if key == utf8.RuneError && size == 1 {
		return 0, 0, false
	}
	action, ok := b.altRunes[key]
	return action, size, ok
}

func (b *Bindings) actionForAltArrow(final byte) (Action, bool) {
	if b == nil {
		b = defaultBindings
	}
	action, ok := b.altArrows[final]
	return action, ok
}
