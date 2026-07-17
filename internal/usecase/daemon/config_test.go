package daemon

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/usecase/keys"
	themeui "github.com/bnema/vev/internal/usecase/theme"
	"github.com/bnema/vev/pkg/renderer"
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
		BindingEntries: []domain.ConfigEntry{
			{Key: "focus-pane-left", Value: "alt+q"},
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

	cmd, ok := d.commandByEffectiveCode("NT")
	require.True(t, ok)
	require.Equal(t, "new-tab", cmd.Slug)
	require.Equal(t, "NT", cmd.Code)
}

func TestApplyThemeCopiesTerminalPaletteAndHotReloadGatesIt(t *testing.T) {
	p, release := newBlockingPTY(t)
	defer release()
	d, sess, ac, _ := newManualSessionWithPTYs(t, p)

	clientPalette := [16]renderer.RGB{}
	clientPalette[1] = renderer.RGB{R: 1, G: 2, B: 3}
	clientPalette[12] = renderer.RGB{R: 4, G: 5, B: 6}
	d.applyTheme(sess, ac, ports.Theme{
		HasForeground: true,
		Foreground:    renderer.RGB{R: 7, G: 8, B: 9},
		HasBackground: true,
		Background:    renderer.RGB{R: 10, G: 11, B: 12},
		Palette:       clientPalette,
		PaletteKnown:  1<<1 | 1<<12,
	})

	require.Equal(t, clientPalette, ac.getClientTheme().Palette)
	require.Equal(t, uint16(1<<1|1<<12), ac.getClientTheme().PaletteKnown)
	require.True(t, ac.getTheme().UsePalette)

	cfg := domain.Defaults()
	cfg.ThemePalette = false
	d.ApplyConfig(cfg)

	// ApplyConfig reuses the live client report and repaints without requiring
	// a reconnect; only palette inheritance changes.
	reloaded := ac.getTheme()
	require.Equal(t, clientPalette, reloaded.Palette)
	require.Equal(t, uint16(1<<1|1<<12), reloaded.PaletteKnown)
	require.False(t, reloaded.UsePalette)
}

func TestEffectiveThemePaletteGate(t *testing.T) {
	clientPalette := [16]renderer.RGB{}
	clientPalette[3] = renderer.RGB{R: 1, G: 2, B: 3}
	clientTheme := themeui.Theme{
		Palette:      clientPalette,
		PaletteKnown: 1 << 3,
	}

	tests := []struct {
		name       string
		config     domain.Config
		want       themeui.Theme
		usePalette bool
	}{
		{
			name:       "auto inherits terminal palette by default",
			config:     domain.Defaults(),
			want:       clientTheme,
			usePalette: true,
		},
		{
			name: "auto disables terminal palette",
			config: func() domain.Config {
				cfg := domain.Defaults()
				cfg.ThemePalette = false
				return cfg
			}(),
			want: clientTheme,
		},
		{
			name: "forced dark uses palette free builtin",
			config: domain.Config{
				Theme:        domain.ThemeDark,
				ThemePalette: true,
			},
			want: themeui.BuiltinDark,
		},
		{
			name: "forced light uses palette free builtin",
			config: domain.Config{
				Theme:        domain.ThemeLight,
				ThemePalette: true,
			},
			want: themeui.BuiltinLight,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := newTestDaemon(t, nil, stubClock{})
			d.ApplyConfig(tt.config)

			got := d.effectiveTheme(clientTheme)
			want := tt.want
			want.UsePalette = tt.usePalette
			require.Equal(t, want, got)
		})
	}
}

func TestApplyConfigPublishesCopyConfig(t *testing.T) {
	tests := []struct {
		name  string
		apply func(*Daemon)
		want  domain.CopyConfig
	}{
		{
			name: "default",
			want: domain.CopyConfig{WordSeparators: domain.DefaultWordSeparators},
		},
		{
			name: "custom",
			apply: func(d *Daemon) {
				cfg := domain.Defaults()
				cfg.Copy = domain.CopyConfig{WordSeparators: "/:"}
				d.ApplyConfig(cfg)
			},
			want: domain.CopyConfig{WordSeparators: "/:"},
		},
		{
			name: "reload empty",
			apply: func(d *Daemon) {
				configured := domain.Defaults()
				configured.Copy = domain.CopyConfig{WordSeparators: "/:"}
				d.ApplyConfig(configured)

				reloaded := domain.Defaults()
				reloaded.Copy = domain.CopyConfig{}
				d.ApplyConfig(reloaded)
			},
			want: domain.CopyConfig{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := newTestDaemon(t, nil, stubClock{})
			if tt.apply != nil {
				tt.apply(d)
			}
			require.Equal(t, tt.want, d.currentCopyConfig())
		})
	}
}

func TestApplyConfigPublishesImmutablePaletteSnapshot(t *testing.T) {
	d := newTestDaemon(t, nil, stubClock{})
	require.Equal(t, domain.Defaults().Palette, d.currentPaletteConfig())

	firstConfig := domain.Defaults()
	firstConfig.Palette = domain.PaletteConfig{Anchor: domain.AnchorTopLeft, AnchorSet: true}
	d.ApplyConfig(firstConfig)
	first := d.currentPaletteConfig()

	secondConfig := domain.Defaults()
	secondConfig.Palette = domain.PaletteConfig{Anchor: domain.AnchorBottomRight, AnchorSet: true}
	d.ApplyConfig(secondConfig)
	second := d.currentPaletteConfig()

	require.Equal(t, domain.PaletteConfig{Anchor: domain.AnchorTopLeft, AnchorSet: true}, first)
	require.Equal(t, domain.PaletteConfig{Anchor: domain.AnchorBottomRight, AnchorSet: true}, second)
}

func TestApplyConfigRepaintsActivePaletteWithoutReplacingModel(t *testing.T) {
	p, release := newBlockingPTY(t)
	defer release()
	d, sess, ac, sends := newManualSessionWithPTYs(t, p)
	d.enterPalette(sess, ac)
	awaitFrame(t, sends, ports.MsgOutput)
	d.handlePaletteInput(ac, []byte("new"))
	awaitFrame(t, sends, ports.MsgOutput)
	d.handlePaletteInput(ac, []byte{0x0e})
	awaitFrame(t, sends, ports.MsgOutput)

	ac.overlays.paletteMu.Lock()
	model := ac.overlays.palette
	query := model.Query()
	selected, ok := model.Selected()
	ac.overlays.paletteMu.Unlock()
	require.True(t, ok)

	cfg := domain.Defaults()
	cfg.Palette = domain.PaletteConfig{Anchor: domain.AnchorTopLeft, AnchorSet: true}
	d.ApplyConfig(cfg)
	awaitFrame(t, sends, ports.MsgOutput)

	ac.overlays.paletteMu.Lock()
	defer ac.overlays.paletteMu.Unlock()
	require.Same(t, model, ac.overlays.palette)
	require.Equal(t, query, ac.overlays.palette.Query())
	selectedAfter, ok := ac.overlays.palette.Selected()
	require.True(t, ok)
	selectedCommand, ok := selected.Command()
	require.True(t, ok)
	selectedAfterCommand, ok := selectedAfter.Command()
	require.True(t, ok)
	require.Equal(t, selectedCommand.Slug, selectedAfterCommand.Slug)
	require.Equal(t, selectedCommand.Code, selectedAfterCommand.Code)
}

func TestApplyConfigPublishesImmutableFloatingSnapshot(t *testing.T) {
	d := newTestDaemon(t, nil, stubClock{})
	require.Equal(t, domain.Defaults().Floating, d.currentFloatingConfig())

	firstConfig := domain.Defaults()
	firstConfig.Floating = domain.FloatingConfig{Command: "btop", Width: 70, Height: 60}
	d.ApplyConfig(firstConfig)
	first := d.currentFloatingConfig()

	secondConfig := domain.Defaults()
	secondConfig.Floating = domain.FloatingConfig{Command: "lazygit", Width: 90, Height: 85}
	d.ApplyConfig(secondConfig)
	second := d.currentFloatingConfig()

	require.Equal(t, domain.FloatingConfig{Command: "btop", Width: 70, Height: 60}, first)
	require.Equal(t, domain.FloatingConfig{Command: "lazygit", Width: 90, Height: 85}, second)
}

func TestApplyConfigPublishesImmutableTabsSnapshot(t *testing.T) {
	d := newTestDaemon(t, nil, stubClock{})
	require.Equal(t, domain.Defaults().Tabs, d.currentTabsConfig())

	firstConfig := domain.Defaults()
	firstConfig.Tabs = domain.TabsConfig{TerminalTitle: false}
	d.ApplyConfig(firstConfig)
	first := d.currentTabsConfig()

	secondConfig := domain.Defaults()
	secondConfig.Tabs = domain.TabsConfig{TerminalTitle: true}
	d.ApplyConfig(secondConfig)
	second := d.currentTabsConfig()

	require.Equal(t, domain.TabsConfig{TerminalTitle: false}, first)
	require.Equal(t, domain.TabsConfig{TerminalTitle: true}, second)
}

func TestApplyConfigSnapshotRestoreProcesses(t *testing.T) {
	tests := []struct {
		name  string
		cfg   domain.Config
		check func(t *testing.T, allow map[string]struct{})
	}{
		{
			name: "default allowlist",
			check: func(t *testing.T, allow map[string]struct{}) {
				t.Helper()
				require.Contains(t, allow, "claude")
			},
		},
		{
			name: "explicit empty disables restore",
			cfg:  domain.Config{Snapshot: domain.SnapshotConfig{RestoreProcessesSet: true}},
			check: func(t *testing.T, allow map[string]struct{}) {
				t.Helper()
				require.Empty(t, allow)
			},
		},
		{
			name: "explicit values are trimmed",
			cfg: domain.Config{Snapshot: domain.SnapshotConfig{
				RestoreProcessesSet: true,
				RestoreProcesses:    []string{"less", "", " pi "},
			}},
			check: func(t *testing.T, allow map[string]struct{}) {
				t.Helper()
				require.Contains(t, allow, "less")
				require.Contains(t, allow, "pi")
				require.NotContains(t, allow, "")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := newTestDaemon(t, nil, stubClock{})
			if tt.cfg.Snapshot.RestoreProcessesSet || len(tt.cfg.Snapshot.RestoreProcesses) > 0 {
				d.ApplyConfig(tt.cfg)
			}
			tt.check(t, d.restoreProcessAllowlistSnapshot())
		})
	}
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

	cmd, ok := d.commandByEffectiveCode("nn")
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

func TestCodeOverrideVacatedDefaultIsReusableRegardlessOfSlugOrder(t *testing.T) {
	d := newTestDaemon(t, nil, stubClock{})
	d.ApplyConfig(domain.Config{
		Theme: domain.ThemeAuto,
		Codes: map[string]string{
			"new-tab":     "NT",
			"new-session": "CNT",
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
	require.Equal(t, "new-tab", byCode["NT"])
	require.Equal(t, "new-session", byCode["CNT"])
}

func TestApplyConfigInvalidDefaultsWithoutPanic(t *testing.T) {
	d := newTestDaemon(t, nil, stubClock{})
	require.NotPanics(t, func() {
		d.ApplyConfig(domain.Config{
			Theme: domain.ThemeAuto,
			BindingEntries: []domain.ConfigEntry{
				{Key: "unknown-action", Value: "alt+x"},
				{Key: "focus-pane-left", Value: "ctrl+x"},
			},
			Codes: map[string]string{
				"new-tab":         "toolong",
				"split-right":     "det",
				"missing-command": "OK",
			},
		})
	})

	cmd, ok := d.commandByEffectiveCode("CNT")
	require.True(t, ok)
	require.Equal(t, "new-tab", cmd.Slug)
	_, ok = d.commandByEffectiveCode("TOOLONG")
	require.False(t, ok)
}
