package remote

import (
	"context"
	"errors"
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

// EndpointLaunch contains the absolute remote executable and complete child
// environment selected by an explicit UI-driver launch configuration.
type EndpointLaunch struct {
	Binary      string
	Root        string
	OwnerToken  string
	Environment []string
}

func NewDialerFactory() DialerFactory { return DialerFactory{} }

func NewDialerFactoryWithRuntimeObserver(observer ports.SerializedRuntimeObserver) DialerFactory {
	return DialerFactory{observer: observer}
}

func (f DialerFactory) DialerForRemote(target, session string, mode TransportMode, log *slog.Logger) (wire.Dialer, error) {
	return f.dialerForRemote(target, session, mode, log, nil)
}

// DialerForRemoteWithLaunch selects the same normal carriage while using the
// explicit endpoint executable/environment for remote child startup.
func (f DialerFactory) DialerForRemoteWithLaunch(target, session string, mode TransportMode, log *slog.Logger, launch *EndpointLaunch) (wire.Dialer, error) {
	if launch != nil && (launch.Binary == "" || launch.Root == "" || launch.OwnerToken == "") {
		return nil, errors.New("vev: isolated remote launch requires a binary, root, and owner")
	}
	return f.dialerForRemote(target, session, mode, log, launch)
}

func (f DialerFactory) dialerForRemote(target, session string, mode TransportMode, log *slog.Logger, launch *EndpointLaunch) (wire.Dialer, error) {
	switch mode {
	case TransportUDP:
		if log != nil {
			log.Info("remote transport selected", "mode", mode, "target", target, "session", session)
		}
		dialer := dgram.NewRemoteDialerWithLogger(target, "", log)
		dialer.RuntimeObserver = f.observer
		if launch != nil {
			dialer.BootstrapBinary = launch.Binary
			dialer.BootstrapRoot = launch.Root
			dialer.BootstrapOwnerToken = launch.OwnerToken
			dialer.BootstrapEnvironment = append([]string(nil), launch.Environment...)
		}
		return dialer, nil
	case TransportStdio:
		if log != nil {
			log.Info("remote transport selected", "mode", mode, "target", target, "session", session)
		}
		var selected *EndpointLaunch
		if launch != nil {
			copyLaunch := *launch
			copyLaunch.Environment = append([]string(nil), launch.Environment...)
			selected = &copyLaunch
		}
		return stdioDialer{target: target, log: log, observer: f.observer, launch: selected}, nil
	default:
		return nil, fmt.Errorf("vev: unsupported remote transport %q", mode)
	}
}

type stdioDialer struct {
	target   string
	log      *slog.Logger
	observer ports.SerializedRuntimeObserver
	launch   *EndpointLaunch
}

func (d stdioDialer) Dial(ctx context.Context) (wire.Transport, error) {
	if d.launch == nil {
		return sshstdio.DialContextWithRuntimeObserver(ctx, d.target, "", d.log, sshstdio.WithRuntimeObserver(d.observer))
	}
	return sshstdio.DialContextWithIsolatedLaunch(ctx, d.target, d.launch.Root, d.launch.OwnerToken, d.launch.Binary, d.launch.Environment, d.log, sshstdio.WithRuntimeObserver(d.observer))
}
