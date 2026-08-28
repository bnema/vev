package remote

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/bnema/vev/internal/adapters/dgram"
	"github.com/bnema/vev/internal/adapters/sshstdio"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol/wire"
)

// TransportMode selects a concrete remote carriage after app validation.
type TransportMode string

const (
	TransportUDP   TransportMode = "udp"
	TransportStdio TransportMode = "stdio"
)

// DialerFactory constructs target-specific raw carriage dialers.
type DialerFactory struct {
	observer ports.SerializedRuntimeObserver
}

func NewDialerFactory() DialerFactory { return DialerFactory{} }

func NewDialerFactoryWithRuntimeObserver(observer ports.SerializedRuntimeObserver) DialerFactory {
	return DialerFactory{observer: observer}
}

func (f DialerFactory) DialerForRemote(target, session string, mode TransportMode, log *slog.Logger) (wire.Dialer, error) {
	switch mode {
	case TransportUDP:
		if log != nil {
			log.Info("remote transport selected", "mode", mode, "target", target, "session", session)
		}
		dialer := dgram.NewRemoteDialerWithLogger(target, "", log)
		dialer.RuntimeObserver = f.observer
		return dialer, nil
	case TransportStdio:
		if log != nil {
			log.Info("remote transport selected", "mode", mode, "target", target, "session", session)
		}
		return stdioDialer{target: target, log: log, observer: f.observer}, nil
	default:
		return nil, fmt.Errorf("vev: unsupported remote transport %q", mode)
	}
}

type stdioDialer struct {
	target   string
	log      *slog.Logger
	observer ports.SerializedRuntimeObserver
}

func (d stdioDialer) Dial(ctx context.Context) (wire.Transport, error) {
	return sshstdio.DialContextWithRuntimeObserver(ctx, d.target, "", d.log, sshstdio.WithRuntimeObserver(d.observer))
}
