# Client preferences contract (P4.1, design-only)

Status: proposed. No production edits accompany this document. Any scope
change below requires explicit approval before implementation.

## Ground rules

- Shared pane VT defaults stay daemon-arbitrated. `applyHostThemeLocked`
  (`internal/usecase/daemon/theme_resolve.go`) fans the resolved theme out to
  every pane VT in every tab of the session; per-attachment values never reach
  pane screens.
- Picker sort scope is unchanged: `togglePickerSort` flips the daemon-wide
  `pickerSort` atomic (`internal/usecase/daemon/picker.go:40`); it is session
  chrome, not an attachment preference.
- Bindings stay daemon-admitted. `keys.BuildBindingEntries` +
  `keys.Router.RouteWithHandler` (`internal/usecase/keys`) remain the single
  admission path; no duplicate client-side key router is proposed.
- Config source of truth stays the daemon-loaded file parsed by
  `internal/adapters/config` into `domain.Config`, published atomically via
  `Daemon.ApplyConfig` snapshots (`internal/usecase/daemon/config.go`).

## Classes

- **Client-local interaction**: affects only the local terminal device, never
  pane VT state, session state, or another attachment. The only class eligible
  for client-side ownership without a P5 lease.
- **Attachment-rendering**: affects presentation of one attachment, executed by
  the serving daemon from per-attachment state. Shared session/pane state
  untouched.
- **Shared application-visible**: affects panes, sessions, or all attachments.
  Daemon-owned; not candidates for client ownership in this plan.

## Field classification

All keys are the existing `internal/adapters/config` keys. "Two-attachment
result" describes two simultaneous attachments A and B on the same session
with different local wishes.

| Key | Class | Source / default / reload / validation | Two-attachment result |
|---|---|---|---|
| `theme`, `theme.palette`, `theme.accent` | Shared | Daemon config file; defaults `auto`/`on`/`auto` (`domain.Defaults`); atomic reload via `storeThemeConfig`, reapplied with `reapplyThemeAllSessions`; invalid values warn and keep prior (`config_test.go`) | Single effective theme per session (`effectiveThemeForConfig`, `resolveAppliedTheme`); per-attachment client theme is only an input to arbitration, never applied directly to panes. **Preserved as-is.** |
| `scrollback.*` | Shared | Daemon config; defaults 50 MiB / 10k lines; `Scrollback.Valid()` falls back to defaults with a warning | Per-pane retention policy; identical for every viewer. **Preserved as-is.** |
| `snapshot.*` | Shared | Daemon config; allowlist snapshot via `restoreProcessAllowlistFromConfig` | Session restore; identical for every viewer. **Preserved as-is.** |
| `floating.command`, `floating.width`, `floating.height` | Shared | Daemon config; defaults 80x80; stored via `floatingConfig` | Geometry arbitration is daemon-owned (`calculateContentFloatingGeometry`). **Preserved as-is.** |
| `bar.top-right`, `bar.bottom-right`, `bar.interval` | Shared | Daemon config; defaults empty/empty/5s, clamped to `MinBarInterval` (1s) | Daemon-host execution, per-session outputs. Governed by the status contract; **no client execution proposed here.** |
| `code.*`, `binding.*` | Shared | Daemon config; `buildCodeOverrides` + `BuildBindingEntries`, warn-and-keep on invalid | Daemon-admitted palette commands and key bindings. **Single router preserved.** |
| `copy.word-separators`, `copy.reduce-motion` | Shared (daemon-executed attached interaction) | Daemon config; defaults ` -_@` / false; `currentCopyConfig` snapshot | Copy mode stays daemon-owned per plan non-goals. Selection documents (`scopy.NewDocument`) and scroll animation execute in the daemon. **Not candidates; moving them requires the P5 lease.** |
| `nav.overflow-tabs`, `nav.overflow-sessions` | Shared (attached-action behavior) | Daemon config; defaults false; `currentNavConfig` | Directional navigation is daemon-admitted (`pane_actions.go:465`). **Not candidates.** |
| `tabs.terminal-title` | Attachment-rendering | Daemon config; default true; `currentTabsConfig` snapshot read at status composition (`status.go:303,334`, `picker.go:96`) | Label-only; per-attachment divergence is safe in principle, but no per-attachment source exists today. **Proposed future field (example): read from attachment claim, defaulting to daemon config. Requires approval; not implemented here.** |
| `palette.anchor` | Attachment-rendering | Daemon config; default center; `currentPaletteConfig` read at render (`render.go:443`) | Placement-only; same future-field shape as above. **Requires approval; not implemented here.** |

## Proposed fields and examples

No new configuration keys are introduced by this contract. The only proposed
shape, pending approval, is: an attachment-rendering value resolves as
*attachment-claimed value if present, else daemon config*, read at composition
time from existing snapshots, never written to `domain.Config` or persisted.
`tabs.terminal-title` and `palette.anchor` are the worked examples above.

Explicitly out of scope: client-local clipboard policy, client-side theme
editing, per-attachment scrollback, per-attachment bindings, and any new
`domain.Config` field. Each would need its own approved contract.

## Tests mapped (to be added only after approval)

- `TestAttachmentRenderingPreferenceFallsBackToDaemonConfig` (daemon):
  attachment without claim renders with daemon `palette.anchor` /
  `tabs.terminal-title`.
- `TestAttachmentRenderingPreferenceClaimWins` (daemon): claimed value used,
  shared pane VT colors byte-identical to unclaimed control.
- `TestTwoAttachmentsIndependentRenderingPrefs` (daemon): A and B on one
  session render different anchors/titles; pane screens identical.
- `TestInvalidRenderingPreferenceWarnsAndKeepsPrior` (config + daemon):
  warn-and-continue path mirrors existing theme/scrollback tests.
- Existing preserved-behavior pins: `TestDefaults*`, `TestParse*` in
  `internal/adapters/config`, `config_test.go` snapshot tests,
  `picker_test.go` sort tests, `theme_integration_test.go`.

## Stop condition

P4.1 ends at this document. No preference scope change, no `domain.Config`
field, no wire field, and no client ownership move is authorized by this
plan alone.
