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
type DialerFactory struct {
	observer ports.SerializedRuntimeObserver
}

var _ ports.RemoteDialerFactory = DialerFactory{}

func NewDialerFactory() DialerFactory { return DialerFactory{} }

// NewDialerFactoryWithRuntimeObserver wires the one process-local sink into
// whichever concrete remote carriage adapter is selected.
func NewDialerFactoryWithRuntimeObserver(observer ports.SerializedRuntimeObserver) DialerFactory {
	return DialerFactory{observer: observer}
}

func (f DialerFactory) DialerForRemote(target string, session string, mode ports.RemoteTransportMode, log *slog.Logger) (ports.Dialer, error) {
	switch mode {
	case ports.RemoteTransportUDP:
		if log != nil {
			log.Info("remote transport selected", "mode", mode, "target", target, "session", session)
		}
		dialer := dgram.NewRemoteDialerWithLogger(target, session, log)
		dialer.RuntimeObserver = f.observer
		return dialer, nil
	case ports.RemoteTransportStdio:
		if log != nil {
			log.Info("remote transport selected", "mode", mode, "target", target, "session", session)
		}
		return stdioDialer{target: target, session: session, log: log, observer: f.observer}, nil
	default:
		return nil, fmt.Errorf("vev: unsupported remote transport %q", mode)
	}
}

type stdioDialer struct {
	target   string
	session  string
	log      *slog.Logger
	observer ports.SerializedRuntimeObserver
}

func (d stdioDialer) Dial(ctx context.Context) (ports.Transport, error) {
	return sshstdio.DialContextWithRuntimeObserver(ctx, d.target, d.session, d.log, sshstdio.WithRuntimeObserver(d.observer))
}
