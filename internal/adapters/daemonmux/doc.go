// Package daemonmux multiplexes typed session connections over one authenticated
// physical daemon carriage.
//
// Its directional codec strictly scans bounded envelopes; the physical
// handshake binds an immutable daemon identity, incarnation, accepted policy,
// and negotiated ceilings. The stream engine enforces admission, per-stream
// identity, bounded queues, and terminal publication. A fair scheduler drains
// independently cancellable streams through one reader and one writer; a
// stream-local refusal does not terminate its siblings, while physical failure
// settles every stream before the connection reports completion.
//
// Broker connectors and daemon listeners adapt injected raw framed transports
// into logical typed connections. Accepted streams share one absolute session
// handshake deadline from admission through the daemon handshake. The aggregate
// listener isolates physical child failures so other transports keep serving.
// Unix IPC, QUIC, and SSH stdio carriage and authentication live in their own
// adapters; this package neither opens sockets nor owns session state.
package daemonmux

import "path/filepath"

// SocketPath is the canonical production daemonmux endpoint beneath ipc.SocketDir().
func SocketPath(socketDir string) string { return filepath.Join(socketDir, "daemonmux.sock") }
