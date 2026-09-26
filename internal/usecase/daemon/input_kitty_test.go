package daemon

import (
	"testing"

	vt "github.com/bnema/vev-vt"
	"github.com/stretchr/testify/require"
)

func TestPaneKeyInputFollowsPaneKeyboardFlags(t *testing.T) {
	tests := []struct {
		name   string
		screen string
		in     string
		want   string
	}{
		{name: "legacy pane gets control byte", in: "\x1b[97;5u", want: "\x01"},
		{name: "legacy pane text untouched", in: "hello", want: "hello"},
		{name: "kitty pane keeps sequence", screen: "\x1b[>1u", in: "\x1b[97;5u", want: "\x1b[97;5u"},
		{name: "kitty pane without alternates strips them", screen: "\x1b[>1u", in: "\x1b[38::49;5u", want: "\x1b[38;5u"},
		{name: "popped pane is legacy again", screen: "\x1b[>1u\x1b[<u", in: "\x1b[27u", want: "\x1b"},
		{name: "nil pane passthrough", in: "\x1b[97;5u", want: "\x1b[97;5u"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var p *pane
			if tt.name != "nil pane passthrough" {
				p = &pane{screen: vt.NewScreen(10, 3)}
				p.screen.Write([]byte(tt.screen))
			}
			require.Equal(t, []byte(tt.want), paneKeyInput(p, []byte(tt.in)))
		})
	}
}
