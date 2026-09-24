# Session navigation

## Session picker

Open it with `SSP` in the palette. It lists local sessions and, if you added hosts, remote sessions as `session@host`.

| Key | Action |
|---|---|
| ↑/↓ or `j`/`k` | move |
| Enter | open the session or tab |
| `/` | search (Escape leaves search) |
| `s` | switch sort: recent first, or named before numbered |
| `x` | kill the selected session |
| Escape or `q` | close and go back |

- Each row can expand to show its tabs, with a live preview of the selected one.
- Remote sessions that cannot be opened show a tag such as `down` or `broken`.
- While the picker is open, your keys never reach the shell underneath.

## Jumping between sessions

| Code / key | Action |
|---|---|
| `BCK` | back to the previous session |
| `JRS <rank>` | jump to a recent session by rank |
| Alt+a | jump to a session that needs attention (bell) |
| `CNS` | create a named session; asks where when several daemons are available |
| `CES` | create a numbered session |

Recent-session history belongs to one client and is not saved. A new client starts with an empty history.

## Move panes and tabs

- `MFP` moves the focused pane to another tab.
- `MAT` moves the current tab to another session.

Both open a small picker to choose the destination.
