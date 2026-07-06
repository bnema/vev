package remote

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/bnema/vev/internal/adapters/dgram"
	"github.com/bnema/vev/internal/adapters/sshstdio"
	"github.com/bnema/vev/internal/ports"
)

// DialerFactory selects the requested remote transport adapter.
type DialerFactory struct{}

func NewDialerFactory() DialerFactory { return DialerFactory{} }

func (DialerFactory) DialerForRemote(target string, session string, mode ports.RemoteTransportMode, log *slog.Logger) (ports.Dialer, error) {
	switch mode {
	case ports.RemoteTransportUDP:
		if log != nil {
			log.Info("remote transport selected", "mode", mode, "target", target, "session", session)
		}
		return dgram.NewRemoteDialerWithLogger(target, session, log), nil
	case ports.RemoteTransportStdio:
		if log != nil {
			log.Info("remote transport selected", "mode", mode, "target", target, "session", session)
		}
		return stdioDialer{target: target, session: session, log: log}, nil
	default:
		return nil, fmt.Errorf("vev: unsupported remote transport %q", mode)
	}
}

type stdioDialer struct {
	target  string
	session string
	log     *slog.Logger
}

func (d stdioDialer) Dial(ctx context.Context) (ports.Transport, error) {
	return sshstdio.DialContext(ctx, d.target, d.session, d.log)
}
