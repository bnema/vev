package brokeripc

import (
	"io"
	"sync"
)

// streamPipe is the private ordered, bounded byte channel between one broker
// wire session and the sessionwire framing of one logical stream.
//
// The wire reader never blocks on a slow consumer: a chunk the peer sent is
// appended to a bounded queue and refused with ErrStreamBackpressure once the
// bound is reached, so only that stream is settled and every sibling and the
// physical connection stay untouched. The consumer never blocks the writer:
// Read serves already-queued chunks or parks on the wake channel, and Close
// releases both ends promptly. Write hands each byte slice to the injected
// outbound hook, which owns chunking it under the negotiated stream chunk
// ceiling and enqueueing the frames on the connection.
type streamPipe struct {
	mu       sync.Mutex
	queue    [][]byte
	queued   int
	closed   bool
	err      error
	maxChunk int
	maxBytes int

	wake     chan struct{}
	closedCh chan struct{}

	outbound func([]byte) error
}

// newStreamPipe builds one bounded stream carriage. outbound must not be nil.
func newStreamPipe(maxChunks int, maxBytes uint64, outbound func([]byte) error) *streamPipe {
	if maxChunks <= 0 {
		maxChunks = DefaultStreamInboundChunks
	}
	if maxBytes == 0 {
		maxBytes = DefaultStreamInboundBytes
	}
	return &streamPipe{
		maxChunk: maxChunks,
		maxBytes: int(maxBytes),
		wake:     make(chan struct{}, 1),
		closedCh: make(chan struct{}),
		outbound: outbound,
	}
}

// Read serves the next queued chunk, blocking until one arrives or the pipe
// closes. A partial copy retains the unread remainder for the next call.
func (p *streamPipe) Read(b []byte) (int, error) {
	if len(b) == 0 {
		return 0, nil
	}
	for {
		p.mu.Lock()
		if len(p.queue) > 0 {
			chunk := p.queue[0]
			n := copy(b, chunk)
			if n == len(chunk) {
				p.queue[0] = nil
				p.queue = p.queue[1:]
			} else {
				p.queue[0] = chunk[n:]
			}
			p.queued -= n
			p.mu.Unlock()
			return n, nil
		}
		if p.closed {
			err := p.err
			p.mu.Unlock()
			if err != nil {
				return 0, err
			}
			return 0, io.EOF
		}
		p.mu.Unlock()
		select {
		case <-p.wake:
		case <-p.closedCh:
		}
	}
}

// Write hands one byte slice to the outbound hook. It returns ErrClosed once the
// pipe reached its terminal outcome.
func (p *streamPipe) Write(b []byte) (int, error) {
	p.mu.Lock()
	closed := p.closed
	p.mu.Unlock()
	if closed {
		return 0, ErrStreamGone
	}
	if err := p.outbound(b); err != nil {
		return 0, err
	}
	return len(b), nil
}

// deliver accepts one inbound chunk from the connection reader. It returns
// ErrStreamBackpressure when the bound is reached and ErrStreamGone once the
// pipe is closed; the caller settles the affected stream in both cases.
func (p *streamPipe) deliver(chunk []byte) error {
	if len(chunk) == 0 {
		return nil
	}
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return ErrStreamGone
	}
	if len(p.queue) >= p.maxChunk || p.queued+len(chunk) > p.maxBytes {
		p.mu.Unlock()
		return ErrStreamBackpressure
	}
	owned := append([]byte(nil), chunk...)
	p.queue = append(p.queue, owned)
	p.queued += len(owned)
	p.mu.Unlock()
	select {
	case p.wake <- struct{}{}:
	default:
	}
	return nil
}

// closeWith settles the pipe exactly once with cause. A nil cause is an orderly
// close; every blocked Read observes it promptly.
func (p *streamPipe) closeWith(cause error) {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return
	}
	p.closed = true
	p.err = cause
	p.queue = nil
	p.queued = 0
	p.mu.Unlock()
	close(p.closedCh)
	select {
	case p.wake <- struct{}{}:
	default:
	}
}

// Close settles the pipe as an orderly close.
func (p *streamPipe) Close() error {
	p.closeWith(nil)
	return nil
}
