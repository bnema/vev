package client

import (
	"context"
	"errors"
	"sync"

	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol"
)

// Outbox bounds. Past either, enqueue blocks until the sender makes progress
// or the caller is cancelled. Input is acknowledged to the terminal pump only
// once written, so the pump stops reading new keys long before these limits;
// they bound acks, focus reports, and other control traffic.
const (
	outboxMaxPending = 4096
	outboxMaxBytes   = 4 << 20
)

// errOutboxStopped is returned by SendClient after the outbox stopped.
var errOutboxStopped = errors.New("client: attachment outbox stopped")

// attachmentOutbox decouples the attachment worker loop from the carriage:
// enqueue returns once the message is accepted, and one sender goroutine
// writes messages in order. A stalled link therefore no longer freezes key
// handling, overlays, or painting. The first write failure is sticky: it is
// reported on Failed and returned by every later enqueue.
type attachmentOutbox struct {
	ports.BrokerLogicalConnection

	mu      sync.Mutex
	queue   []protocol.ClientMessage
	bytes   int    // payload bytes queued
	queued  uint64 // messages ever accepted
	written uint64 // messages written successfully
	err     error
	stopped bool
	wake    chan struct{} // sender wake-up, capacity 1
	space   chan struct{} // closed-and-replaced after each write, closed on failure
	idle    chan struct{} // closed-and-replaced when the queue drains
	failed  chan struct{} // closed once on the first write failure
	done    chan struct{} // closed when the sender exits
}

func newAttachmentOutbox(stream ports.BrokerLogicalConnection) *attachmentOutbox {
	b := &attachmentOutbox{
		BrokerLogicalConnection: stream,
		wake:                    make(chan struct{}, 1),
		space:                   make(chan struct{}),
		idle:                    make(chan struct{}),
		failed:                  make(chan struct{}),
		done:                    make(chan struct{}),
	}
	close(b.idle)
	go b.run()
	return b
}

// outboxPayload is the variable-size part of a message counted against
// outboxMaxBytes.
func outboxPayload(message protocol.ClientMessage) int {
	switch m := message.(type) {
	case protocol.Input:
		return len(m.Data)
	case protocol.ImagePush:
		return len(m.Data)
	}
	return 0
}

// SendClient enqueues message without a cancellation source.
func (b *attachmentOutbox) SendClient(message protocol.ClientMessage) error {
	return b.enqueue(context.Background(), nil, message)
}

// enqueue accepts message for ordered transmission. It blocks only while the
// outbox is full, until space frees, ctx ends, or cancel closes. It returns
// the sticky failure once the sender failed.
func (b *attachmentOutbox) enqueue(ctx context.Context, cancel <-chan struct{}, message protocol.ClientMessage) error {
	size := outboxPayload(message)
	b.mu.Lock()
	for {
		if b.err != nil {
			err := b.err
			b.mu.Unlock()
			return err
		}
		if b.stopped {
			b.mu.Unlock()
			return errOutboxStopped
		}
		// An empty queue always admits, so one oversized message still goes.
		if len(b.queue) == 0 || (len(b.queue) < outboxMaxPending && b.bytes+size <= outboxMaxBytes) {
			break
		}
		space := b.space
		b.mu.Unlock()
		select {
		case <-space:
		case <-b.done:
		case <-ctx.Done():
			return ctx.Err()
		case <-cancel:
			return errAttachmentForegroundRevoked
		}
		b.mu.Lock()
	}
	if len(b.queue) == 0 {
		b.idle = make(chan struct{})
	}
	b.queue = append(b.queue, message)
	b.bytes += size
	b.queued++
	b.mu.Unlock()
	select {
	case b.wake <- struct{}{}:
	default:
	}
	return nil
}

// Close stops the sender and closes the underlying stream, which unblocks a
// write stalled on the link.
func (b *attachmentOutbox) Close() error {
	b.stop()
	return b.BrokerLogicalConnection.Close()
}

// stop ends the sender after its current write; queued messages are dropped.
// Session input among them was never acknowledged to the terminal pump, so
// the worker preserves it like a failed synchronous send.
func (b *attachmentOutbox) stop() {
	b.mu.Lock()
	b.stopped = true
	b.mu.Unlock()
	select {
	case b.wake <- struct{}{}:
	default:
	}
}

// unsentInput returns the session input bytes of accepted messages numbered
// (from, to] that were never written. It is final only after join.
func (b *attachmentOutbox) unsentInput(from, to uint64) []byte {
	b.mu.Lock()
	defer b.mu.Unlock()
	var data []byte
	// queue[i] is accepted message number written+1+i.
	for i, message := range b.queue {
		n := b.written + 1 + uint64(i)
		if n <= from || n > to {
			continue
		}
		if input, ok := message.(protocol.Input); ok {
			data = append(data, input.Data...)
		}
	}
	return data
}

// join stops the sender, closes the stream so a stalled write returns, and
// waits for the sender to exit. Afterwards writtenCount is final.
func (b *attachmentOutbox) join() {
	_ = b.Close()
	<-b.done
}

// accepted reports how many messages were ever accepted.
func (b *attachmentOutbox) accepted() uint64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.queued
}

// writtenCount reports how many messages were written successfully.
func (b *attachmentOutbox) writtenCount() uint64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.written
}

// Progress is closed after the next successful write or on failure.
func (b *attachmentOutbox) Progress() <-chan struct{} {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.space
}

// Failed is closed once a write failed; failure returns the cause.
func (b *attachmentOutbox) Failed() <-chan struct{} { return b.failed }

func (b *attachmentOutbox) failure() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.err
}

// Idle is closed while nothing is queued or being written.
func (b *attachmentOutbox) Idle() <-chan struct{} {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.idle
}

func (b *attachmentOutbox) run() {
	defer close(b.done)
	for {
		b.mu.Lock()
		if b.stopped {
			b.mu.Unlock()
			return
		}
		if len(b.queue) == 0 {
			b.mu.Unlock()
			<-b.wake
			continue
		}
		message := b.queue[0]
		b.mu.Unlock()

		err := b.BrokerLogicalConnection.SendClient(message)

		b.mu.Lock()
		if err != nil {
			// The queue is kept, failed write first, so unsentInput can
			// hand undelivered keys back to the terminal pump.
			b.err = err
			close(b.failed)
			close(b.idle)
			close(b.space)
			b.mu.Unlock()
			return
		}
		b.queue[0] = nil
		b.queue = b.queue[1:]
		b.bytes -= outboxPayload(message)
		b.written++
		close(b.space)
		b.space = make(chan struct{})
		if len(b.queue) == 0 {
			close(b.idle)
		}
		b.mu.Unlock()
	}
}
