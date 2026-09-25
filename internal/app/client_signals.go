package app

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/bnema/vev/internal/usecase/client"
)

// clientSignalContext cancels an attached client on process signals. SIGHUP
// (terminal closed) and SIGTERM use client.ErrClientClosed as the cause so the
// attachment reports Detach{Closed} and the daemon applies
// ephemeral.close-on-exit. SIGINT keeps a plain cancellation.
func clientSignalContext(parent context.Context) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancelCause(parent)
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGHUP, syscall.SIGTERM, syscall.SIGINT)
	done := make(chan struct{})
	go func() {
		select {
		case sig := <-signals:
			if sig == syscall.SIGINT {
				cancel(context.Canceled)
				return
			}
			cancel(client.ErrClientClosed)
		case <-done:
		}
	}()
	return ctx, func() {
		signal.Stop(signals)
		close(done)
		cancel(context.Canceled)
	}
}
