package daemon

import (
	"strings"
	"testing"
	"testing/synctest"

	"bnema.dev/vev/internal/usecase/command"
	"bnema.dev/vev/internal/usecase/keys"
	"github.com/stretchr/testify/require"
)

func TestMarkNotePromptEntryPoints(t *testing.T) {
	for _, palette := range []bool{false, true} {
		for _, tt := range []struct {
			name, input, note string
			marked            bool
		}{
			{"text", "remember\r", "remember", true}, {"empty", "\r", "", true}, {"escape", "\x1b", "", false},
		} {
			t.Run(tt.name+map[bool]string{false: "/key", true: "/palette"}[palette], func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					d, sess, ac, _, releases := newManualTabSession(t, 1)
					defer releases[0]()
					stopOverlayTimersAtCleanup(t, ac)
					if palette {
						cmd, ok := command.ByCode("MRK")
						require.True(t, ok)
						require.NoError(t, cmd.Run(paletteExec{d: d, sess: sess, ac: ac}, nil))
					} else {
						(daemonKeyHandler{d: d, ac: ac}).Action(keys.ActionToggleMark, nil)
					}
					require.False(t, sess.tabs[0].marked)
					require.NotNil(t, ac.overlays.prompt)
					d.handlePromptInput(ac, []byte(tt.input))
					require.Equal(t, tt.marked, sess.tabs[0].marked)
					require.Equal(t, tt.note, sess.tabs[0].markNote)
					require.Nil(t, ac.overlays.prompt)
					if tt.marked {
						require.NoError(t, (paletteExec{d: d, sess: sess, ac: ac}).ToggleMark())
						require.False(t, sess.tabs[0].marked)
						require.Nil(t, ac.overlays.prompt)
					}
				})
			})
		}
	}
}

func TestEditMarkNote(t *testing.T) {
	for _, tt := range []struct {
		name, input, want string
		marked            bool
	}{
		{"save", "new\r", "oldnew", true}, {"clear", "\x7f\x7f\x7f\r", "", true}, {"escape", "new\x1b", "old", true}, {"unmarked", "", "old", false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				d, sess, ac, _, releases := newManualTabSession(t, 1)
				defer releases[0]()
				stopOverlayTimersAtCleanup(t, ac)
				sess.tabs[0].marked = tt.marked
				sess.tabs[0].markNote = "old"
				require.NoError(t, (paletteExec{d: d, sess: sess, ac: ac}).EditMarkNote())
				if !tt.marked {
					require.Nil(t, ac.overlays.prompt)
					toasts, _ := visibleToasts(ac)
					require.NotEmpty(t, toasts)
					require.Equal(t, "Tab is not marked", toasts[0].Message)
					require.Empty(t, d.notices.history())
					return
				}
				require.Equal(t, "old", ac.overlays.prompt.Value())
				d.handlePromptInput(ac, []byte(tt.input))
				require.True(t, sess.tabs[0].marked)
				require.Equal(t, tt.want, sess.tabs[0].markNote)
			})
		})
	}
}

func TestSanitizeMarkNote(t *testing.T) {
	for _, tt := range []struct{ name, in, want string }{
		{"trim", "  hello  ", "hello"}, {"controls", "a\x00\n\tb", "ab"}, {"ansi", "\x1b[31mred\x1b[0m", "red"}, {"osc", "a\x1b]0;title\x07b", "ab"}, {"rune limit", strings.Repeat("界", 121), strings.Repeat("界", 120)},
	} {
		t.Run(tt.name, func(t *testing.T) { require.Equal(t, tt.want, sanitizeMarkNote(tt.in)) })
	}
}
