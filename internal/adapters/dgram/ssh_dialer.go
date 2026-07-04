package dgram

import (
	"context"
	"net"
	"strings"
	"time"

	"github.com/bnema/vev/internal/adapters/sshstdio"
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

// RemoteDialer tries the authenticated datagram bootstrap first and falls back
// to ssh stdio whenever bootstrap, UDP resolution, or the UDP probe fails.
type RemoteDialer struct {
	Target       string
	Session      string
	ProbeTimeout time.Duration
}

func NewRemoteDialer(target, session string) RemoteDialer {
	return RemoteDialer{Target: target, Session: session, ProbeTimeout: defaultProbeTimeout}
}

func (d RemoteDialer) Dial(ctx context.Context) (ports.Transport, error) {
	// The UDP bootstrap currently requires a long-lived remote proxy process. Do
	// not select it as the app's remote attach path until that process can detach
	// from SSH safely; stdio is the bounded fallback path.
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	return sshstdio.DialContext(ctx, d.Target, d.Session)
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
