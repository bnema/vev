package client

import (
	"context"
	"time"

	"github.com/bnema/vev-vt/protocol/terminalquery"

	"github.com/bnema/vev/internal/ports"
)

// terminalCapabilityProbeTimeout bounds the outer-terminal capability probe.
// DA1 answers first on any terminal, so a real terminal finishes far sooner.
const terminalCapabilityProbeTimeout = 150 * time.Millisecond

// terminalCapabilities is what the outer terminal declared to the probe.
type terminalCapabilities struct {
	KittyGraphics bool
	KittyKeyboard bool
}

// probeTerminalCapabilities performs one bounded probe of the outer terminal
// through the lifecycle's only input pump. It must run before any other
// consumer claims input. Probe responses are removed; every unrelated byte is
// kept, in order, for the next consumer.
func probeTerminalCapabilities(ctx context.Context, terminal ports.Terminal, clock ports.Clock, input *terminalInputPump) terminalCapabilities {
	if input == nil || supervisorNil(terminal) || supervisorNil(clock) {
		return terminalCapabilities{}
	}
	consumer, ok := input.tryClaim()
	if !ok {
		return terminalCapabilities{}
	}
	defer input.revoke(consumer)

	query := terminalquery.KittyGraphicsQuery + terminalquery.KittyKeyboardQuery + terminalquery.DeviceAttributesQuery
	if _, err := terminal.Out().Write([]byte(query)); err != nil {
		return terminalCapabilities{}
	}
	if err := terminal.Flush(); err != nil {
		return terminalCapabilities{}
	}

	probe := &terminalquery.Probe{}
	var replay []byte
	timer := clock.NewTimer(terminalCapabilityProbeTimeout)
	defer timer.Stop()
	// DA1 is the FIFO sentinel: once it arrives, the answers to the earlier
	// queries are definitive.
wait:
	for !probe.DA1() {
		select {
		case <-ctx.Done():
			break wait
		case <-timer.C():
			break wait
		case <-input.readyFor(consumer):
			result, ok := input.take(ctx, consumer)
			if !ok {
				if ctx.Err() != nil {
					break wait
				}
				continue
			}
			replay = append(replay, probe.Feed(result.data)...)
			input.ack(consumer)
			if result.err != nil {
				break wait
			}
		}
	}
	replay = append(replay, probe.Finish()...)
	// Preserve before revoke so the next consumer reads these bytes before any
	// later terminal read.
	input.preserveResidual(consumer, replay)
	if !probe.DA1() {
		return terminalCapabilities{}
	}
	return terminalCapabilities{KittyGraphics: probe.KittyGraphics(), KittyKeyboard: probe.KittyKeyboard()}
}
