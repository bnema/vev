package keys

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestConsumeOrExpelPaneActionsBindWithoutDefaults(t *testing.T) {
	tests := []struct {
		name   string
		action Action
		key    rune
	}{
		{name: "consume-or-expel-pane-left", action: ActionConsumeOrExpelPaneLeft, key: 'H'},
		{name: "consume-or-expel-pane-right", action: ActionConsumeOrExpelPaneRight, key: 'L'},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bindings, warnings := BuildBindings(map[string]string{
				tt.name: "alt+" + string(tt.key),
			})

			require.Empty(t, warnings)
			assertAltRuneAction(t, bindings, tt.key, tt.action)
		})
	}
}

func TestConsumeOrExpelPaneActionsAreAbsentFromAllDefaultBindings(t *testing.T) {
	tests := []struct {
		name   string
		action Action
	}{
		{name: "left", action: ActionConsumeOrExpelPaneLeft},
		{name: "right", action: ActionConsumeOrExpelPaneRight},
	}
	defaults := DefaultBindings()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for _, bound := range defaults.altRunes {
				require.NotEqual(t, tt.action, bound)
			}
			for _, bound := range defaults.altArrows {
				require.NotEqual(t, tt.action, bound)
			}
		})
	}
}
