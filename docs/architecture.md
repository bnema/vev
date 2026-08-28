# Architecture

vev uses a hexagonal core with typed session messages at the client and daemon boundary. Binary framing and carriage selection stay outside the use cases.

## Package ownership

- `internal/domain`: pure shared values and invariants. `domain/terminalcap` owns terminal capability values and environment detection policy.
- `internal/protocol`: typed, transport-neutral client/daemon messages, protocol version, semantic validation, and handshake policy.
- `internal/protocol/catalogue`: independently versioned remote discovery JSON schema and bounded validation.
- `internal/protocol/wire`: message IDs, raw frames, strict codecs, compression, encoded limits, and raw `Transport`, `Dialer`, and `Listener` contracts.
- `internal/ports`: application-facing interfaces and the values required by those interfaces. It contains no codecs, raw frames, environment policy, or worker implementations.
- `internal/usecase`: client, daemon, and supporting application behavior. Production use cases may consume semantic protocol packages but never `protocol/wire` or concrete adapters.
- `internal/adapters/sessionwire`: translates between typed session connections and raw wire transports, including direction checks and decode-failure classification.
- `internal/adapters`: IPC, UDP, SSH stdio, PTY, terminal, persistence-facing, and observability implementations.
- `internal/app`: CLI parsing and composition. It selects local or remote carriage, wraps raw dialers with `sessionwire`, and injects typed ports into use cases.
- `pkg`: reusable packages that never import `internal`.

## Dependency direction

```text
main → app → usecase → ports
             │          │
             └────────→ protocol ← catalogue
                              ↑
                        protocol/wire
                              ↑
                    carriage adapters
```

`internal/app` may import every layer to compose the process. Adapters depend inward on ports, protocol, and domain. The existing `adapters/snapshot → usecase/snapshot` dependency is an explicit exception until the snapshot codec has its own owner. Production dependency rules and the separate test-import policy are executable in `boundary_test.go`.

## Session composition

```text
client or daemon use case
  ↕ ports.ClientConnection / ports.ServerConnection
adapters/sessionwire
  ↕ protocol/wire.Transport carrying wire.Frame
IPC, UDP, or SSH stdio adapter
```

Use cases exchange `protocol.ClientMessage` and `protocol.ServerMessage` values. `sessionwire` alone maps those values to message IDs and payload codecs. Blind proxies may forward raw frames, and the UDP adapter may inspect bounded frame classification for QoS, but neither path exposes bytes to a use case.

## Adding code

- Add application behavior to a use case and define any cross-layer interface in `internal/ports`.
- Add transport-neutral session meaning to `internal/protocol`.
- Add remote catalogue schema fields and validation to `internal/protocol/catalogue`.
- Add message IDs, binary layouts, strict decoding, compression, or raw carriage contracts to `internal/protocol/wire`.
- Implement I/O, queues, workers, environment integration, or technology selection in an adapter or `internal/app`.
- Keep protocol version `37` unless a negotiated wire layout changes.
