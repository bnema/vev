# Configuration

vev reads `~/.config/vev/config` (`$XDG_CONFIG_HOME` respected). No file means defaults. The daemon picks up changes within a couple of seconds; no restart needed.

```text
# Theme: auto follows the client; dark/light use neutral built-in defaults.
theme = auto
# In auto mode with palette inheritance enabled, infer a terminal accent.
theme.palette = on
# Accent policy: auto or one exact ANSI slot number from 0 through 15.
theme.accent = auto

# Palette placement: auto, center, top-left, top, top-right, left, right,
# bottom-left, bottom, or bottom-right.
palette.anchor = center

# One prewarmed floating terminal per tab. An empty command uses the normal shell.
floating.command =
floating.width = 80%
floating.height = 80%

# Let directional keyboard focus continue past a pane edge. Both default off.
nav.overflow-tabs = off
nav.overflow-sessions = off

# Show the focused pane's terminal title in tab labels; off keeps the process name only.
tabs.terminal-title = on

# Rebindable actions. Leave a line out to keep its built-in binding.
open-palette = alt+space
toggle-floating-pane = alt+f
jump-attention = alt+a
focus-pane-left = alt+h
focus-pane-right = alt+l
focus-pane-up = alt+k
focus-pane-down = alt+j
switch-tab-1 = alt+1
# ... through switch-tab-9 = alt+9

# Processes relaunched when a named session is restored. Empty disables relaunch.
snapshot.restore_processes = vi,vim,nvim,emacs,man,less,more,tail,top,htop,btop,claude,codex,pi,opencode

# Optional right bar anchors: commands run on the daemon host. Empty disables an anchor.
bar.top-right =
bar.bottom-right =
bar.interval = 5s

# Command palette codes: 2-3 letters or digits.
code.new-tab = CNT
code.new-session = CNS
code.close-tab = CLT
code.split-right = SPR
code.split-left = SPL
code.split-up = SPU
code.split-down = SPD
code.stack-pane = STP
code.toggle-stack = TST
code.close-pane = CLP
code.focus-pane-left = FPL
code.focus-pane-right = FPR
code.focus-pane-up = FPU
code.focus-pane-down = FPD
code.next-tab = NXT
code.previous-tab = PVT
code.back-session = BSK
code.jump-recent-session = JRS
code.session-picker = SSP
code.visual-mode = VIS
code.toggle-floating-pane = FLT
code.rename-session = RNS
code.rename-tab = RNT
code.detach = DET
```

Invalid values log a warning and resolve that setting to its default on both initial load and reload.

## Logs and durable state

Set `VEV_LOG=debug`, `VEV_LOG=warn`, or `VEV_LOG=error` to change verbosity; the default is `info`. JSON-line logs such as `vev-daemon.log` live in `$XDG_STATE_HOME/vev`, or `~/.local/state/vev` when unset. The same state directory contains the strict session catalogue, migration journal, notices, and `snapshots/`. The lifecycle lock and socket live in `$XDG_RUNTIME_DIR/vev` (with platform runtime fallbacks).

Recovery events include `lifecycle_owner_wait`, `lifecycle_owner_acquired`, `lifecycle_owner_released`, `catalogue_validated`, `catalogue_compaction_recovery_complete`, `session_restore_complete`, `fallback_checkpoint_promoted`, `snapshot_head_repair_complete`, `session_degraded`, `snapshot_maintenance_progress`, `interrupted_transaction_recovery_complete`, and `daemon_startup_complete`.

Catalogue failure is fail-closed: vev does not publish an empty replacement daemon. Preserve the state directory, inspect `catalogue_validation_failed`, correct storage or ownership problems, and retry without editing catalogue files. See [Durable session recovery](durable-session-recovery.md) for explicit recovery commands, migration, diagnostics, and the committed checkpoint plus up to two direct fallbacks retention policy.

## Theme

With `theme = auto`, vev follows the terminal's reported foreground, background, light/dark scheme, and ANSI palette. `theme.palette = on` is the default. `theme.accent = auto` derives one accent from the terminal's chromatic ANSI colors; repeated terminal colors are preferred, otherwise vev uses an eligible blue slot when available. `theme.accent = 0` through `theme.accent = 15` selects exactly that ANSI slot. Arbitrary RGB values and `off` are not accepted for `theme.accent`.

When the terminal provides truecolor defaults and a usable RGB palette slot, vev uses the resolved accent for active chrome and derives softer bar, inactive, recent-session, and border colors from it. It preserves readability by reducing a surface intensity when needed. Terminal application/pane colors are not recolored. Without truecolor default colors or a usable RGB resolved accent, chrome backgrounds remain neutral; an available selected ANSI slot can still decorate foregrounds or borders. An explicit unavailable slot is never replaced with another slot.

Slots `0`, `7`, `8`, and `15` are valid explicit selections, but log a warning because conventional neutral slots may provide little or no accent separation. An invalid accent value logs a warning and falls back to `auto`, including on hot reload.

`theme.palette = off` is authoritative: it keeps exact neutral foreground/background rendering and ignores `theme.accent`. Forced `theme = dark` or `theme = light` is also neutral and ignores both palette and accent policy. Configuration reload immediately reapplies the current terminal snapshot; vev updates a live palette when the terminal reports a light/dark scheme change, but cannot detect palette changes a terminal does not report.

## Bindings

Key specs: `alt+<char>`, `alt+space`, `alt+left/right/up/down`, `alt+1` through `alt+9`. Configuring an action replaces all of its built-in aliases (set `focus-pane-left` and the Alt+Arrow alias is gone). Tab switching also accepts the top-row symbols of non-QWERTY layouts, so AZERTY works without extra config.

## Navigation overflow

Both settings are independent and default to `off`. With `nav.overflow-tabs = on`, left/right keyboard focus (`Alt+H`/`Alt+L` by default) continues from a pane edge to the adjacent tab. With `nav.overflow-sessions = on`, up/down keyboard focus (`Alt+K`/`Alt+J` by default) continues to the adjacent live session in alphabetical order. Neither setting wraps at the first or last destination.

Overflow applies only to keyboard focus actions; mouse navigation does not overflow, and floating panes never overflow. Hot reload applies either setting to subsequent navigation without restarting the daemon.

## Copy mode

Scroll up with the mouse to enter copy mode. Use `h`, `j`, `k`, and `l` to move, `w`, `b`, and `e` for word motions, `v` or Space to start line selection, and `y` or Enter to copy.

Mouse drag selects a text range. Double-click selects the word under the pointer; dragging after a double-click extends by complete words.

```ini
# Unicode whitespace always separates words.
# The default is " -_@".
copy.word-separators = " -_@"
```

Set `copy.word-separators = ""` to use only Unicode whitespace as a separator.

## Palette anchor

The command palette is centered by default. Set `palette.anchor` to another anchor to reposition it, or use `auto` for a bottom shelf on narrow terminals and a bottom-right rail from 96 columns up. Reload moves an open palette without losing your query.

## Floating terminal

The command runs through your shell. A changed command applies on the next launch; changed dimensions on the next show or resize. Floating state is not restored across daemon restarts.

## Bar anchors

Anchor commands run on the daemon host every `bar.interval` (minimum 1s). vev reads the first line of stdout, strips ANSI codes, and keeps the last good value on failure. Scripts get `VEV_ANCHOR`, `VEV_SESSION`, `VEV_TAB`, `VEV_PANE`, `VEV_PANE_CWD`, and `VEV_COLS`.

Commands resolve against the environment of the client currently attached to the session, the same environment new panes inherit. A command that works in your shell will work as an anchor without restarting the daemon. When an anchor fails, the daemon logs the exit code and the command's stderr; exit 127 means the command was not found on that `PATH`.

Both anchors are disabled by default, so `go install github.com/bnema/vev@latest` needs no companion scripts. Set either command to opt in; explicit commands are run unchanged. Checkout and release installs include `vev-bar-top-right` and `vev-bar-bottom-right` example scripts, which can be configured by name when they are on the attaching client's `PATH`. The bottom-right example runs `git status --porcelain` on each refresh; raise the interval or replace it if that is too heavy for your repository.
