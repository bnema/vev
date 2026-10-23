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
| Alt+a | jump to a tab that needs attention (bell), else to a finished agent you have not seen |
| `CNS` | create a named session; asks where when several daemons are available |
| `CES` | create a numbered session |

Recent-session history belongs to one client and is not saved. A new client starts with an empty history. A session that rings a bell also shows in the history with its bell, after the sessions this client visited, until any client sees it.

Alt+a goes to the oldest bell first: in the current session, then in other sessions, then on other daemons. With no bell left, it goes to the agent tab that finished first, current session first; finished agents on other daemons are not reached. The tab you are viewing is skipped, so pressing Alt+a again moves on.

A vev window only counts as seeing a bell while it has focus. With several vev windows open, a window on another workspace or behind another app never clears a bell, so the window you are using still shows it. This needs a terminal with focus reporting, such as kitty, foot, WezTerm, or Ghostty; other terminals keep treating every open window as seen. Focusing a vev window also resizes the session content to that window.

## Move panes and tabs

- `MFP` moves the focused pane to another tab.
- `MAT` moves the current tab to another session.

Both open a small picker to choose the destination.
