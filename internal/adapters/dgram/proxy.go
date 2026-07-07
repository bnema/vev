package dgram

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"time"

	"github.com/bnema/vev/internal/ports"
)

const (
	DefaultProxyIdleTTL      = 30 * time.Second
	DefaultProxyRetryBackoff = 100 * time.Millisecond
)

type ProxyRuntime struct {
	Client       ports.Transport
	Daemon       ports.Transport
	Log          *slog.Logger
	IdleTTL      time.Duration
	RetryBackoff time.Duration
	Clock        ports.Clock
}

type proxyCopyErrKind uint8

const (
	proxyClientRecv proxyCopyErrKind = iota
	proxyDaemonRecv
	proxyClientSend
	proxyDaemonSend
)

type proxyCopyErr struct {
	kind proxyCopyErrKind
	err  error
}

func (p ProxyRuntime) Run(ctx context.Context) error {
	defer func() {
		_ = p.Client.Close()
		_ = p.Daemon.Close()
	}()

	idleTTL := p.IdleTTL
	if idleTTL <= 0 {
		idleTTL = DefaultProxyIdleTTL
	}
	retryBackoff := p.RetryBackoff
	if retryBackoff <= 0 {
		retryBackoff = DefaultProxyRetryBackoff
	}
	clk := p.Clock
	if clk == nil {
		clk = realClock{}
	}

	errCh := make(chan proxyCopyErr, 2)
	startClientCopy := func() {
		go copyProxyFrames(p.Daemon, p.Client, proxyClientRecv, proxyDaemonSend, errCh)
	}
	startDaemonCopy := func() {
		go copyProxyFrames(p.Client, p.Daemon, proxyDaemonRecv, proxyClientSend, errCh)
	}
	startClientCopy()
	startDaemonCopy()

	var linkEvents <-chan ports.LinkEvent
	if reporter, ok := p.Client.(ports.LinkStateReporter); ok {
		linkEvents = reporter.LinkEvents()
	}
	var idle <-chan time.Time
	var timer ports.Timer
	defer func() {
		if timer != nil {
			timer.Stop()
		}
	}()
	startIdle := func() {
		if timer == nil {
			timer = clk.NewTimer(idleTTL)
		} else {
			timer.Reset(idleTTL)
		}
		idle = timer.C()
	}
	stopIdle := func() {
		if timer != nil {
			timer.Stop()
		}
		idle = nil
	}
	waitRetry := func() error {
		t := clk.NewTimer(retryBackoff)
		defer t.Stop()
		select {
		case <-t.C():
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case ev, ok := <-linkEvents:
			if !ok {
				linkEvents = nil
				continue
			}
			switch ev.State {
			case ports.LinkStateDegraded, ports.LinkStateProbing, ports.LinkStateOffline:
				startIdle()
			case ports.LinkStateConnected:
				stopIdle()
			case ports.LinkStateDead:
				return ErrLinkDead
			}
		case <-idle:
			return ErrLinkDead
		case ce := <-errCh:
			switch ce.kind {
			case proxyDaemonRecv:
				if errors.Is(ce.err, io.EOF) {
					return nil
				}
				return ce.err
			case proxyDaemonSend:
				return ce.err
			case proxyClientSend:
				if !recoverableClientLink(p.Client, ce.err) {
					return ce.err
				}
				if p.Log != nil {
					p.Log.Info("udp proxy restarting recoverable client send path", "err", ce.err)
				}
				startIdle()
				if err := waitRetry(); err != nil {
					return err
				}
				startDaemonCopy()
			case proxyClientRecv:
				if !recoverableClientLink(p.Client, ce.err) {
					return ce.err
				}
				if p.Log != nil {
					p.Log.Info("udp proxy restarting recoverable client receive path", "err", ce.err)
				}
				startIdle()
				if err := waitRetry(); err != nil {
					return err
				}
				startClientCopy()
			}
		}
	}
}

func copyProxyFrames(dst, src ports.Transport, recvKind, sendKind proxyCopyErrKind, errCh chan<- proxyCopyErr) {
	for {
		f, err := src.Recv()
		if err != nil {
			errCh <- proxyCopyErr{kind: recvKind, err: err}
			return
		}
		if err := dst.Send(f); err != nil {
			errCh <- proxyCopyErr{kind: sendKind, err: err}
			return
		}
	}
}

func recoverableClientLink(t ports.Transport, err error) bool {
	if errors.Is(err, ErrLinkDead) {
		return false
	}
	reporter, ok := t.(ports.LinkStateReporter)
	if !ok {
		return false
	}
	switch reporter.LinkState() {
	case ports.LinkStateDegraded, ports.LinkStateProbing, ports.LinkStateOffline:
		return true
	default:
		return false
	}
}
