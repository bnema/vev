package client

import (
	"context"
	"errors"
	"io"
	"sync"
)

// terminalReadResult is copied by terminalInputPump before the next Read so
// terminal input remains owned by one lifecycle reader across reconnects.
type terminalInputSource uint8

const (
	terminalInputHuman terminalInputSource = iota
	terminalInputReply
	terminalInputAutomation
)

type terminalReadResult struct {
	data       []byte
	err        error
	source     terminalInputSource
	generation uint64
	actionID   uint64
	endBatch   bool
	// keys/text carry the decoded ui-driver op for picker interception.
	// Physical input leaves both empty; the picker consumer then decodes the
	// raw bytes itself.
	keys []string
	text string
}

// terminalAutomationRequest is admitted only by the foreground scanner owner.
// Its reply is buffered so cancellation never holds up physical input.
type terminalAutomationRequest struct {
	ctx        context.Context
	owner      *UI
	consumer   uint64
	record     terminalReadResult
	admitted   chan bool
	dispatched chan bool
}

// terminalInputPump is the sole consumer of the caller-owned terminal reader.
// It is started once after raw mode is entered and is reused by each attach
// scanner. A bare io.Reader cannot be interrupted, so a Read may stay blocked
// after Run exits; suspend drops any result received while inactive and never
// closes caller-owned stdin. Reconnects and later runs never add another reader.
type terminalInputPump struct {
	in         io.Reader
	done       chan struct{}
	automation chan terminalAutomationRequest

	mu sync.Mutex
	// residual holds scanner bytes that were read but not delivered when an
	// attempt is cancelled. It is consumed before the next terminal Read so a
	// replacement scanner neither loses a partial marker nor replays bytes
	// whose callbacks already ran.
	residual           []byte
	pending            *terminalReadResult
	consumer           uint64
	delivering         uint64
	deliveringResidual bool
	nextID             uint64
	activation         uint64
	closed             bool
	active             bool
	// readCause stores the cause the terminal reader ended with. The reader
	// enqueues its final result for a consumer to report, but the exit watcher
	// can win that race, so the cause is recorded here as well: a read failure
	// must never be published as an orderly EOF.
	readCause    error
	readyMu      sync.Mutex
	ready        map[uint64]chan struct{}
	claimChanged chan struct{}
	space        chan struct{}
	state        chan struct{}
	exited       chan struct{}
	afterRevoke  func() // test synchronization hook
	startMu      sync.Once
	stopMu       sync.Once
}

func newTerminalInputPump(in io.Reader) *terminalInputPump {
	space := make(chan struct{}, 1)
	space <- struct{}{}
	return &terminalInputPump{
		in:           in,
		done:         make(chan struct{}),
		automation:   make(chan terminalAutomationRequest),
		activation:   1,
		active:       true,
		ready:        make(map[uint64]chan struct{}),
		claimChanged: make(chan struct{}, 1),
		space:        space,
		state:        make(chan struct{}, 1),
		exited:       make(chan struct{}),
	}
}

// tryClaim lets competing foreground owners decline admission without panicking.
func (p *terminalInputPump) tryClaim() (uint64, bool) {
	p.mu.Lock()
	if p.consumer != 0 {
		p.mu.Unlock()
		return 0, false
	}
	p.nextID++
	p.consumer = p.nextID
	consumer := p.consumer
	p.readyMu.Lock()
	p.ready[consumer] = make(chan struct{}, 1)
	p.readyMu.Unlock()
	ready := (len(p.residual) != 0 || p.pending != nil) && p.delivering == 0
	p.mu.Unlock()
	p.signalClaimChanged()
	if ready {
		p.signalReady(consumer)
	}
	return consumer, true
}

// revoke invalidates an attempt before its replacement is allowed to claim
// input. Pending bytes are deliberately retained for that replacement.
func (p *terminalInputPump) revoke(consumer uint64) {
	p.mu.Lock()
	if p.consumer == consumer {
		p.consumer = 0
	}
	if p.delivering == consumer {
		// Legacy attach-attempt replacement remains lossless. Autonomous
		// supervisor ownership uses ack-before-picker-handoff and
		// drop-before-attachment-handoff, so bytes cannot cross owner kinds.
		p.delivering = 0
		p.deliveringResidual = false
	}
	p.mu.Unlock()
	p.readyMu.Lock()
	delete(p.ready, consumer)
	p.readyMu.Unlock()
	p.signalClaimChanged()
	if p.afterRevoke != nil {
		p.afterRevoke()
	}
}

func (p *terminalInputPump) signalClaimChanged() {
	select {
	case p.claimChanged <- struct{}{}:
	default:
	}
}

func (p *terminalInputPump) readyFor(consumer uint64) <-chan struct{} {
	p.readyMu.Lock()
	defer p.readyMu.Unlock()
	return p.ready[consumer]
}

func (p *terminalInputPump) signalReady(consumer uint64) {
	if consumer == 0 {
		return
	}
	p.readyMu.Lock()
	ready := p.ready[consumer]
	p.readyMu.Unlock()
	if ready == nil {
		return
	}
	select {
	case ready <- struct{}{}:
	default:
	}
}

// enqueue retains at most one completed terminal read. The reader blocks
// before publishing another result, keeping handoff buffering bounded.
func (p *terminalInputPump) enqueue(result terminalReadResult, activation uint64) bool {
	select {
	case <-p.done:
		return false
	case <-p.space:
	}
	p.mu.Lock()
	if p.closed || !p.active || p.activation != activation {
		p.mu.Unlock()
		p.space <- struct{}{}
		return false
	}
	p.pending = &result
	consumer := p.consumer
	p.mu.Unlock()
	p.signalReady(consumer)
	return true
}

// take leases a ready result only to the current, non-cancelled consumer.
// The raw read remains pending until ack, so revocation or cancellation before
// scanner delivery leaves the exact bytes available to the next attempt.
func (p *terminalInputPump) take(ctx context.Context, consumer uint64) (terminalReadResult, bool) {
	p.mu.Lock()
	if ctx.Err() != nil || p.consumer != consumer || p.delivering != 0 {
		p.mu.Unlock()
		return terminalReadResult{}, false
	}
	if len(p.residual) != 0 {
		result := terminalReadResult{data: append([]byte(nil), p.residual...)}
		p.delivering = consumer
		p.deliveringResidual = true
		p.mu.Unlock()
		return result, true
	}
	if p.pending == nil {
		p.mu.Unlock()
		return terminalReadResult{}, false
	}
	result := *p.pending
	p.delivering = consumer
	p.deliveringResidual = false
	p.mu.Unlock()
	return result, true
}

// preserveResidual records the undecided marker prefix after callbacks for the
// rest of its read have completed. It is intentionally bounded by the marker
// scanner's fixed response prefixes and is replayed before later terminal
// reads on a replacement attempt.
func (p *terminalInputPump) preserveResidual(consumer uint64, data []byte) {
	if len(data) == 0 {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.consumer != consumer {
		return
	}
	p.residual = append(p.residual[:0], data...)
}

// ack commits scanner delivery of a leased read and lets the lifecycle reader
// accept the next result. Only the consumer that holds the lease can ack it.
// dropOwned disposes every undecided byte at an autonomous owner-class
// boundary. Picker-queued bytes are dropped before attachment claim, and
// attachment-queued bytes are dropped before picker claim. No delivery,
// pending read, or preserved residual may cross between those owner classes.
// revoke remains lossless for legacy attachment-to-attachment replacement.
func (p *terminalInputPump) dropOwned(consumer uint64) {
	p.mu.Lock()
	if p.consumer != consumer {
		p.mu.Unlock()
		return
	}
	hadPending := p.pending != nil
	p.pending = nil
	p.residual = nil
	if p.delivering == consumer {
		p.delivering = 0
		p.deliveringResidual = false
	}
	p.mu.Unlock()
	if hadPending {
		select {
		case p.space <- struct{}{}:
		default:
		}
	}
}

func (p *terminalInputPump) ack(consumer uint64) {
	p.mu.Lock()
	if p.consumer != consumer || p.delivering != consumer {
		p.mu.Unlock()
		return
	}
	wasResidual := p.deliveringResidual
	if wasResidual {
		p.residual = nil
	} else {
		p.pending = nil
	}
	p.delivering = 0
	p.deliveringResidual = false
	more := len(p.residual) != 0 || p.pending != nil
	p.mu.Unlock()
	if wasResidual {
		if more {
			p.signalReady(consumer)
		}
		return
	}
	p.space <- struct{}{}
}

func (p *terminalInputPump) finish() {
	p.mu.Lock()
	p.closed = true
	p.mu.Unlock()
	p.signalClaimChanged()
	p.signalState()
}

func (p *terminalInputPump) signalState() {
	select {
	case p.state <- struct{}{}:
	default:
	}
}

func (p *terminalInputPump) waitForActive() (uint64, bool) {
	for {
		p.mu.Lock()
		active, closed, activation := p.active, p.closed, p.activation
		p.mu.Unlock()
		if closed {
			return 0, false
		}
		if active {
			return activation, true
		}
		select {
		case <-p.done:
			return 0, false
		case <-p.state:
		}
	}
}

func (p *terminalInputPump) start() {
	p.startMu.Do(func() {
		go func() {
			defer close(p.exited)
			buf := make([]byte, stdinBufSize)
			for {
				activation, ok := p.waitForActive()
				if !ok {
					return
				}
				n, err := p.in.Read(buf)
				result := terminalReadResult{err: err}
				if n > 0 {
					result.data = append([]byte(nil), buf[:n]...)
				}
				if !p.enqueue(result, activation) {
					if p.isClosed() {
						return
					}
					continue
				}
				if err != nil {
					p.setReadCause(err)
					p.finish()
					return
				}
			}
		}()
	})
}

// setReadCause records the cause the terminal reader ended with. An orderly EOF
// is the absence of a failure and stays zero, so cause reports io.EOF for it.
func (p *terminalInputPump) setReadCause(err error) {
	if err == nil || errors.Is(err, io.EOF) {
		return
	}
	p.mu.Lock()
	if p.readCause == nil {
		p.readCause = err
	}
	p.mu.Unlock()
}

// cause reports why the terminal read ended: the read failure when there was
// one, otherwise an orderly EOF.
func (p *terminalInputPump) cause() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.readCause != nil {
		return p.readCause
	}
	return io.EOF
}

func (p *terminalInputPump) isClosed() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.closed
}

// suspend drops input completed after an attach ends and parks the lifecycle
// reader before its next Read. A later Run resumes this same reader, avoiding
// both input replay between runs and a competing read on caller-owned stdin.
func (p *terminalInputPump) suspend() {
	p.mu.Lock()
	p.active = false
	discarded := p.pending != nil
	p.residual = nil
	p.pending = nil
	p.delivering = 0
	p.deliveringResidual = false
	p.mu.Unlock()
	if discarded {
		select {
		case p.space <- struct{}{}:
		default:
		}
	}
	p.signalState()
	p.signalClaimChanged()
}

func (p *terminalInputPump) stop() {
	p.stopMu.Do(func() {
		// Mark closure while holding the same mutex that publishes a completed
		// Read. A Read which returns after stop therefore cannot publish even
		// when both done and space are selectable in enqueue.
		p.mu.Lock()
		p.closed = true
		p.mu.Unlock()
		close(p.done)
		p.signalClaimChanged()
		p.signalState()
	})
}
