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

## Phase 1 — non-blocking client send (client only)

- Per-attachment ordered outbound queue drained by one sender goroutine;
  the worker loop enqueues and keeps running. Failure of the sender surfaces
  as one event into the loop, which settles as today.
- Bounded by bytes; when full, the loop stops *reading* terminal input
  (backpressure) instead of blocking UI work.
- Tests: a stalled `SendClient` must not block overlay/repaint/output
  handling; ordering preserved; error settles the run.

## Phase 2 — faster detection, patient resume (client + QUIC adapter)

- QUIC keepalive ~2 s; keep idle timeout long (resume covers longer gaps).
- Resume loop: retry with capped backoff until the remote park window
  (15 min) expires instead of 5 attempts; Ctrl-C/detach still exit.
- Status bar shows "last contact Ns ago" while resuming.

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

## Phase 6 — optional predictive local echo

- mosh-style overlay with underline-until-confirmed. Breaks the "thin client
  interprets nothing" rule; needs an explicit design decision.
