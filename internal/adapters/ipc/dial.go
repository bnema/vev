package ipc

import (
	"context"
	"fmt"
	"net"
	"path/filepath"

	"github.com/bnema/vev/internal/protocol/wire"
)

// DialContext connects to the daemon's socket in dir (dir/daemon.sock) and
// returns a Transport speaking vev's framed protocol. A dial failure (no daemon
// listening, or a stale/absent socket) is reported as a wrapped error so
// callers can decide whether to spawn a daemon and retry.
func DialContext(ctx context.Context, dir string, opts ...Option) (wire.Transport, error) {
	sockPath := filepath.Join(dir, socketFileName)
	var dialer net.Dialer
	conn, err := dialer.DialContext(ctx, "unix", sockPath)
	if err != nil {
		return nil, fmt.Errorf("ipc: dial %s: %w", sockPath, err)
	}
	return NewTransport(conn, opts...), nil
}
