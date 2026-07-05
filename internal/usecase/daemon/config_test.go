package daemon

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/usecase/keys"
)

type captureKeyHandler struct {
	actions []keys.Action
	forward [][]byte
}

func (h *captureKeyHandler) Forward(data []byte) {
	h.forward = append(h.forward, append([]byte(nil), data...))
}
func (h *captureKeyHandler) Action(action keys.Action) { h.actions = append(h.actions, action) }

func TestApplyConfigHotReloadSwapsBindingsAndCodes(t *testing.T) {
	d := newTestDaemon(t, nil, stubClock{})
	h := &captureKeyHandler{}
	r := keys.NewRouter(stubClock{}, h, &d.bindings)

	r.Route([]byte("\x1bh"))
	require.Equal(t, []keys.Action{keys.ActionFocusPaneLeft}, h.actions)

	d.ApplyConfig(domain.Config{
		Theme: domain.ThemeAuto,
		Bindings: map[string]string{
			"focus-pane-left": "alt+q",
		},
		Codes: map[string]string{
			"new-tab": "nt",
		},
	})

	r.Route([]byte("\x1bh"))
	require.Equal(t, []keys.Action{keys.ActionFocusPaneLeft}, h.actions, "old key should no longer be intercepted")
	require.Equal(t, [][]byte{{'\x1b', 'h'}}, h.forward)
	r.Route([]byte("\x1bq"))
	require.Equal(t, []keys.Action{keys.ActionFocusPaneLeft, keys.ActionFocusPaneLeft}, h.actions)

	cmd, ok := d.commandByCode("NT")
	require.True(t, ok)
	require.Equal(t, "new-tab", cmd.Slug)
	require.Equal(t, "NT", cmd.Code)
}

func TestPaletteCommandsApplyOverridesAndResolveByCode(t *testing.T) {
	d := newTestDaemon(t, nil, stubClock{})
	d.ApplyConfig(domain.Config{
		Theme: domain.ThemeAuto,
		Codes: map[string]string{
			"new-tab": "NN",
		},
	})

	commands := d.paletteCommands()
	require.NotEmpty(t, commands)
	require.Equal(t, "new-tab", commands[0].Slug)
	require.Equal(t, "NN", commands[0].Code)

	cmd, ok := d.commandByCode("nn")
	require.True(t, ok)
	require.Equal(t, "new-tab", cmd.Slug)
	require.Equal(t, "NN", cmd.Code)
}

func TestPaletteCommandsUseOverrideSnapshotForRecentAndListing(t *testing.T) {
	d := newTestDaemon(t, nil, stubClock{})
	d.ApplyConfig(domain.Config{
		Theme: domain.ThemeAuto,
		Codes: map[string]string{
			"split-right": "SRX",
		},
	})
	d.recordPaletteUse("SRX")

	commands := d.paletteCommands()
	require.NotEmpty(t, commands)
	require.Equal(t, "split-right", commands[0].Slug)
	require.Equal(t, "SRX", commands[0].Code)

	seen := 0
	for _, cmd := range commands {
		if cmd.Slug == "split-right" {
			seen++
			require.Equal(t, "SRX", cmd.Code)
		}
	}
	require.Equal(t, 1, seen)
}

func TestCodeOverrideConflictRestoresDefaultWithoutDuplicates(t *testing.T) {
	d := newTestDaemon(t, nil, stubClock{})
	d.ApplyConfig(domain.Config{
		Theme: domain.ThemeAuto,
		Codes: map[string]string{
			"new-session": "CNT",
			"new-tab":     "DET",
		},
	})

	commands := d.paletteCommands()
	byCode := make(map[string]string, len(commands))
	for _, cmd := range commands {
		if existing, ok := byCode[cmd.Code]; ok {
			t.Fatalf("duplicate effective code %q for %q and %q", cmd.Code, existing, cmd.Slug)
		}
		byCode[cmd.Code] = cmd.Slug
	}
	require.Equal(t, "new-tab", byCode["CNT"])
	require.Equal(t, "new-session", byCode["CNS"])
	require.Equal(t, "detach", byCode["DET"])
}

func TestApplyConfigInvalidDefaultsWithoutPanic(t *testing.T) {
	d := newTestDaemon(t, nil, stubClock{})
	require.NotPanics(t, func() {
		d.ApplyConfig(domain.Config{
			Theme: domain.ThemeAuto,
			Bindings: map[string]string{
				"unknown-action":  "alt+x",
				"focus-pane-left": "ctrl+x",
			},
			Codes: map[string]string{
				"new-tab":         "toolong",
				"split-right":     "det",
				"missing-command": "OK",
			},
		})
	})

	cmd, ok := d.commandByCode("CNT")
	require.True(t, ok)
	require.Equal(t, "new-tab", cmd.Slug)
	_, ok = d.commandByCode("TOOLONG")
	require.False(t, ok)
}
