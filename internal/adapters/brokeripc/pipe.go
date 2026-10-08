package brokeripc

import (
	"io"
	"sync"

	"github.com/bnema/vev/internal/adapters/brokerwire"
)

// streamPipe is the private ordered, credit-controlled byte channel between one
// broker wire session and the sessionwire framing of one logical stream.
//
// Each direction of a stream has a brokerwire.StreamWindowBytes credit window.
// The sender spends brokerwire.StreamChunkCredit per data frame before writing
// it and waits while it has too little, so a slow consumer slows its own
// sender instead of failing the stream. The receiver returns the credit of
// every frame its consumer has fully read with one StreamWindowUpdate per
// batch. Because a conforming peer never exceeds the window, the wire reader
// never blocks: it appends each frame to the queue, and a frame beyond the
// granted credit is a peer violation (ErrStreamCredit) that settles only that
// stream. Close releases the reader, a writer waiting for credit, and the
// writer's peer promptly.
type streamPipe struct {
	mu     sync.Mutex
	queue  []pipeChunk
	closed bool
	err    error

	// recvUsed is the credit held by queued frames, recvReturn the credit of
	// fully read frames not yet granted back, and returnAt the batch size.
	recvUsed   uint64
	recvReturn uint64
	returnAt   uint64
	// sendCredit is what the peer still allows this side to send.
	sendCredit uint64
	creditWake chan struct{}

	wake     chan struct{}
	closedCh chan struct{}

	ceiling   int
	sendFrame func([]byte) error
	grant     func(uint64) error
}

// pipeChunk is one queued inbound frame and the credit it holds until it is
// fully read.
type pipeChunk struct {
	data []byte
	cost uint64
}

// newStreamPipe builds one stream carriage. Write splits outbound bytes into
// frames of at most ceiling bytes, the negotiated stream chunk limit, and hands
// each to sendFrame once its credit is reserved; grant returns consumed inbound
// credit to the peer. Neither hook may be nil.
func newStreamPipe(ceiling int, sendFrame func([]byte) error, grant func(uint64) error) *streamPipe {
	window := brokerwire.StreamWindowBytes
	return &streamPipe{
		returnAt:   streamCreditReturnAt(window),
		sendCredit: window,
		creditWake: make(chan struct{}),
		wake:       make(chan struct{}, 1),
		closedCh:   make(chan struct{}),
		ceiling:    ceiling,
		sendFrame:  sendFrame,
		grant:      grant,
	}
}

// streamCreditReturnAt is the consumed credit a receiver batches before it
// grants it back: half the window, lowered so a sender blocked on a maximum
// frame is always released. A sender waits only once more than
// window-StreamChunkCredit(max) is unreturned, so granting at or below that
// point can never deadlock.
func streamCreditReturnAt(window uint64) uint64 {
	cost := brokerwire.StreamChunkCredit(int(brokerwire.MaxStreamChunkBytes))
	return max(min(window/2, window-cost+1), 1)
}

// Read serves the next queued frame, blocking until one arrives or the pipe
// closes. A partial copy retains the unread remainder for the next call. The
// credit of each fully read frame is returned to the peer in batches.
func (p *streamPipe) Read(b []byte) (int, error) {
	if len(b) == 0 {
		return 0, nil
	}
	for {
		p.mu.Lock()
		if len(p.queue) > 0 {
			chunk := &p.queue[0]
			n := copy(b, chunk.data)
			var grant uint64
			if n == len(chunk.data) {
				p.recvUsed -= chunk.cost
				p.recvReturn += chunk.cost
				p.queue[0] = pipeChunk{}
				p.queue = p.queue[1:]
				if p.recvReturn >= p.returnAt && !p.closed {
					grant, p.recvReturn = p.recvReturn, 0
				}
			} else {
				chunk.data = chunk.data[n:]
			}
			p.mu.Unlock()
			if grant > 0 {
				// A failed grant means the connection itself is failing; its
				// terminal path settles this stream, so there is nothing to retry.
				_ = p.grant(grant)
			}
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

// Write sends b in order as frames under the chunk ceiling, waiting for the
// peer's credit before each one: a peer that is not reading slows this writer
// instead of losing the stream. It returns ErrStreamGone once the pipe reached
// its terminal outcome.
func (p *streamPipe) Write(b []byte) (int, error) {
	if p.ceiling <= 0 {
		return 0, ErrConfig
	}
	p.mu.Lock()
	closed := p.closed
	p.mu.Unlock()
	if closed {
		return 0, ErrStreamGone
	}
	for data := b; len(data) > 0; {
		n := min(len(data), p.ceiling)
		if err := p.reserve(n); err != nil {
			return 0, err
		}
		if err := p.sendFrame(data[:n]); err != nil {
			return 0, err
		}
		data = data[n:]
	}
	return len(b), nil
}

// reserve spends the send credit of one outbound frame of n bytes, waiting for
// the peer to grant more when it is short. It returns ErrStreamGone once the
// pipe closes, so a writer waiting for credit is always released.
func (p *streamPipe) reserve(n int) error {
	cost := brokerwire.StreamChunkCredit(n)
	for {
		p.mu.Lock()
		if p.closed {
			p.mu.Unlock()
			return ErrStreamGone
		}
		if p.sendCredit >= cost {
			p.sendCredit -= cost
			p.mu.Unlock()
			return nil
		}
		wake := p.creditWake
		p.mu.Unlock()
		select {
		case <-wake:
		case <-p.closedCh:
		}
	}
}

// granted adds credit the peer returned. Credit beyond the window is a peer
// flow-control violation.
func (p *streamPipe) granted(credit uint64) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return nil
	}
	if credit > brokerwire.StreamWindowBytes-p.sendCredit {
		return ErrStreamCredit
	}
	p.sendCredit += credit
	close(p.creditWake)
	p.creditWake = make(chan struct{})
	return nil
}

// deliver accepts one inbound frame from the connection reader. It returns
// ErrStreamCredit when the peer sent beyond the credit it was granted and
// ErrStreamGone once the pipe is closed; the caller settles the affected stream
// in both cases. The pipe takes ownership of chunk: callers pass freshly
// decoded bytes and never touch them again.
func (p *streamPipe) deliver(chunk []byte) error {
	if len(chunk) == 0 {
		return nil
	}
	cost := brokerwire.StreamChunkCredit(len(chunk))
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return ErrStreamGone
	}
	if p.recvUsed+p.recvReturn+cost > brokerwire.StreamWindowBytes {
		p.mu.Unlock()
		return ErrStreamCredit
	}
	p.queue = append(p.queue, pipeChunk{data: chunk, cost: cost})
	p.recvUsed += cost
	p.mu.Unlock()
	select {
	case p.wake <- struct{}{}:
	default:
	}
	return nil
}

// closeWith settles the pipe exactly once with cause. A nil cause is an orderly
// close; every blocked Read observes it promptly. On a nil cause, any frames
// already queued are kept: Read serves them first and only returns io.EOF once
// the queue is drained, so a reply that arrived just before an orderly peer
// close is never discarded underneath its reader. An error cause is a failure:
// the queue is discarded, since its content can no longer be trusted to reach
// a consumer that is being torn down for cause.
func (p *streamPipe) closeWith(cause error) {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return
	}
	p.closed = true
	p.err = cause
	if cause != nil {
		p.queue = nil
		p.recvUsed = 0
	}
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
