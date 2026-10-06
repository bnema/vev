# Plan: mosh-like remote resilience

Goal: on a lossy or intermittent link (4G, roaming, sleep), the UI never
freezes, typed input is kept and delivered when the link returns, and SSH is
used only to authenticate the first QUIC handshake.

Reference: mosh (`/tmp/mosh`, `src/network/transportsender-impl.h`,
`src/frontend/terminaloverlay.cc`).

## Diagnosis (current main)

1. Input path blocks on the network: `sessionAttachmentWorker.send` waits for
   `SendClient`, which waits for the carriage write. While QUIC stalls, the
   worker loop stops reading keys, servicing overlays, and painting.
2. Every reconnect re-runs SSH: `_broker-mux-quic-proxy` serves one
   connection with one-time credentials and exits with it.
3. No connection migration: one UDP socket per connection, no `AddPath`.
4. Slow detection, early give-up: keepalive 15 s / idle 60 s; 5 resume
   attempts over a few seconds, then back to the picker.
5. `LinkState` is only ever `Offline` at teardown; `degraded`/`probing` are
   documented but never produced.
6. Daemon ignores `Input.InputSeq`, so input in flight at a drop is lost or
   would be duplicated by a replay.

## Phase 1 — non-blocking client send (done)

- Per-attachment ordered outbox drained by one sender goroutine; the worker
  loop enqueues and keeps running. Write failure settles the run as before.
- Bounded by count and bytes. An input delivery stays undecided until the
  outbox wrote it, so the pump reads no newer keys meanwhile (backpressure)
  and a link failure preserves exactly the unwritten keys for the resume.
- Lifecycle exits drain the outbox for at most 2 s.

## Phase 2 — patient resume (done, except status text)

- QUIC keepalive 2 s; idle timeout stays 60 s.
- A route that stays down keeps resuming for the 15-minute park window; a
  session that drops right after attaching still gives up after 5 flaps.
- During the resume backoff, Ctrl-C or a lone Esc returns to the picker;
  other keys are kept (bounded) and replayed into the resumed session.
- TODO: status bar "last contact Ns ago" while resuming.

## Phase 3 — exactly-once input across resume (protocol bump)

- Daemon tracks last applied `InputSeq` per attachment; drops `<=` seqs.
- `Welcome` on resume carries the last applied seq; client replays unacked
  input after it. Bump `protocol.Version`.

## Phase 4 — SSH only for first handshake (remote helpers, trust)

- Remote QUIC proxy outlives one connection for the park window and issues a
  resume ticket (bearer secret, bound to cert fingerprint + session, expiry,
  single active use, memory only).
- Reconnect dials QUIC directly with the ticket; SSH only on rejection/expiry.
- Picker host health uses the live link + a ticket-based QUIC probe, never
  SSH.
- Needs a security review (ticket theft, replay, expiry) before merge.

## Phase 5 — real link states and path migration

- Derive `degraded`/`probing` from RTT and last-packet age in the adapter.
- On stall or local address change, `AddPath` + probe + switch.

## Phase 6 — client-rendered palette on remote sessions

- The command palette is rendered by the serving daemon, so each keystroke
  costs a full round trip on a remote session. Render it client-side like
  the session picker, fed by the daemon's command catalogue, and send only
  the chosen command.

## Phase 7 — optional predictive local echo

- mosh-style overlay with underline-until-confirmed. Breaks the "thin client
  interprets nothing" rule; needs an explicit design decision.
