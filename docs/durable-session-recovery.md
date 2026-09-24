# Durable session recovery

Named sessions are saved to disk and come back after a daemon restart. This page explains what is saved, what can go wrong, and how to recover.

## What is saved

- Session name and working directory.
- Tabs and layout.
- Scrollback and the last visible screen, restored as copy-mode history.
- Allowed processes are relaunched (see `snapshot.restore_processes` in [configuration](configuration.md)).

The live process state, cursor, and terminal modes are not restored.

vev keeps the latest checkpoint and the one before it as a fallback.

## Session states in `vev ls`

| State | Meaning |
|---|---|
| `up` | Running. |
| `temporary` | Running numbered session. Not saved to disk. |
| `down` | Saved and stopped. Attaching restarts it. |
| `broken` | Saved state could not be loaded safely. Attach is refused until you recover it. |

## Recover a broken session

```sh
vev cmd -s NAME session-recovery discard
```

This deletes the saved tabs, layout, and history of `NAME`. The name and working directory are kept, so the next attach starts a fresh session.

## Upgrades

- A new vev release converts the old session catalogue automatically at startup. A backup is written next to it as `sessions.kv.pre-v6.bak`.
- To go back to an older vev, stop vev and restore that backup first.
- When the checkpoint format changes, old checkpoints are reset: names and working directories stay, but tabs, layout, and history are cleared.
- Installing a new binary does not restart a running daemon. Run `vev kill --all` to switch.

## When the daemon refuses to start

If the session catalogue cannot be read, the daemon stops instead of starting empty. This protects your data.

1. Do not edit or delete files in `~/.local/state/vev/`.
2. Look for `catalogue_validation_failed` in `~/.local/state/vev/vev-daemon.log`.
3. Fix the cause (disk full, wrong file owner), then start vev again.
4. For real corruption, back up the whole directory before trying anything else.

A `lifecycle_owner_wait` log line means another daemon is still starting or stopping. Wait for it to finish.

## Where files live

| What | Where |
|---|---|
| Catalogue, snapshots, logs | `~/.local/state/vev/` |
| Socket and lock | `$XDG_RUNTIME_DIR/vev/` |

Recovery logs never contain terminal content.
