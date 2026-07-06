package dgram

import (
	"context"
	"log/slog"
	"net"
	"strings"
	"time"

	"github.com/bnema/vev/internal/ports"
)

const (
	maxBootstrapStderr  = 64 * 1024
	defaultProbeTimeout = 3 * time.Second
)

type limitedBuffer struct {
	buf []byte
}

func (b *limitedBuffer) Write(p []byte) (int, error) {
	if len(b.buf) < maxBootstrapStderr {
		keep := min(len(p), maxBootstrapStderr-len(b.buf))
		b.buf = append(b.buf, p[:keep]...)
	}
	return len(p), nil
}

func (b *limitedBuffer) String() string { return string(b.buf) }

// RemoteDialer connects to the authenticated datagram bootstrap for UDP remote
// attach. SSH stdio is selected only through the explicit stdio transport mode.
type RemoteDialer struct {
	Target       string
	Session      string
	ProbeTimeout time.Duration
	Log          *slog.Logger
}

func NewRemoteDialer(target, session string) RemoteDialer {
	return RemoteDialer{Target: target, Session: session, ProbeTimeout: defaultProbeTimeout}
}

func NewRemoteDialerWithLogger(target, session string, log *slog.Logger) RemoteDialer {
	return RemoteDialer{Target: target, Session: session, ProbeTimeout: defaultProbeTimeout, Log: log}
}

func (d RemoteDialer) Dial(ctx context.Context) (ports.Transport, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	return nil, ErrLinkDead
}

func sshTargetHost(target string) string {
	if at := strings.LastIndexByte(target, '@'); at >= 0 {
		target = target[at+1:]
	}
	if h, _, err := net.SplitHostPort(target); err == nil {
		return h
	}
	return target
}
