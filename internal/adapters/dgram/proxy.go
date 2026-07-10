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

type proxyCopier struct {
	ctx          context.Context
	errCh        chan<- proxyCopyErr
	retryBackoff time.Duration
	clk          ports.Clock
}

type proxyCopyDirection struct {
	src              ports.Transport
	dst              ports.Transport
	recvKind         proxyCopyErrKind
	sendKind         proxyCopyErrKind
	retryRecoverable bool
	transform        func(ports.Frame) ports.Frame
}

func (p ProxyRuntime) Run(ctx context.Context) error {
	defer func() {
		_ = p.Client.Close()
		_ = p.Daemon.Close()
	}()
	copyCtx, cancelCopies := context.WithCancel(ctx)
	defer cancelCopies()

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
	copier := proxyCopier{ctx: copyCtx, errCh: errCh, retryBackoff: retryBackoff, clk: clk}
	clientToDaemon := proxyCopyDirection{
		src: p.Client, dst: p.Daemon,
		recvKind: proxyClientRecv, sendKind: proxyDaemonSend,
		transform: clampDatagramHelloOutputWindow,
	}
	daemonToClient := proxyCopyDirection{src: p.Daemon, dst: p.Client, recvKind: proxyDaemonRecv, sendKind: proxyClientSend, retryRecoverable: true}
	go copier.copyFrames(clientToDaemon)
	go copier.copyFrames(daemonToClient)

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
		if idle != nil {
			return
		}
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
			case ports.LinkStateDegraded, ports.LinkStateProbing, ports.LinkStateOffline, ports.LinkStateDead:
				startIdle()
			case ports.LinkStateConnected:
				stopIdle()
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
			case proxyClientRecv, proxyClientSend:
				if errors.Is(ce.err, ErrLinkDead) {
					startIdle()
					continue
				}
				return ce.err
			default:
				// Recoverable client-side errors are retried inside the copier
				// without dropping the frame; anything that surfaces here (a
				// daemon send failure, or a non-recoverable client send failure)
				// is fatal.
				return ce.err
			}
		}
	}
}

func (c proxyCopier) copyFrames(dir proxyCopyDirection) {
	for {
		f, err := dir.src.Recv()
		if err != nil {
			c.errCh <- proxyCopyErr{kind: dir.recvKind, err: err}
			return
		}
		if dir.transform != nil {
			f = dir.transform(f)
		}
		// Retry the same frame in place while the destination link error stays
		// recoverable (congestion, transient path loss). The frame is never
		// dropped — on a reliable stream a gap desynchronizes the client screen.
		for {
			err := dir.dst.Send(f)
			if err == nil {
				break
			}
			if dir.retryRecoverable && recoverableClientLink(dir.dst, err) {
				t := c.clk.NewTimer(c.retryBackoff)
				select {
				case <-t.C():
					t.Stop()
					continue
				case <-c.ctx.Done():
					t.Stop()
					return
				}
			}
			c.errCh <- proxyCopyErr{kind: dir.sendKind, err: err}
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
	case ports.LinkStateConnected:
		return errors.Is(err, ErrPendingFull)
	case ports.LinkStateDegraded, ports.LinkStateProbing, ports.LinkStateOffline:
		return true
	default:
		return false
	}
}

// clampDatagramHelloOutputWindow enforces the datagram-safe state window at
// the adapter boundary. Invalid Hello payloads pass through unchanged so the
// daemon remains the single authority for strict decoding and version checks.
func clampDatagramHelloOutputWindow(f ports.Frame) ports.Frame {
	if f.Type != ports.MsgHello {
		return f
	}
	hello, err := ports.UnmarshalHello(f.Payload)
	if err != nil {
		return f
	}
	hello.MaxOutputInFlight = 1
	f.Payload = ports.MarshalHello(hello)
	return f
}
