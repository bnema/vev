package client

import (
	"errors"
	"sync"

	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol"
)

// outboxMaxPending bounds client messages accepted but not yet written. Past
// it SendClient blocks, which applies backpressure to the worker loop only
// after a large amount of input has piled up behind a stalled link.
const outboxMaxPending = 4096

// errOutboxStopped is returned by SendClient after the outbox stopped.
var errOutboxStopped = errors.New("client: attachment outbox stopped")

// attachmentOutbox decouples the attachment worker loop from the carriage:
// SendClient enqueues and returns, and one sender goroutine writes messages in
// order. A stalled link therefore no longer freezes key handling, overlays, or
// painting. The first write failure is sticky: it is reported on failed and
// returned by every later SendClient.
type attachmentOutbox struct {
	ports.BrokerLogicalConnection

	mu      sync.Mutex
	queue   []protocol.ClientMessage
	err     error
	stopped bool
	wake    chan struct{} // sender wake-up, capacity 1
	space   chan struct{} // closed-and-replaced when the queue shrinks
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

// SendClient enqueues message for ordered transmission. It blocks only while
// the queue is full, and returns the sticky failure once the sender failed.
func (b *attachmentOutbox) SendClient(message protocol.ClientMessage) error {
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
		if len(b.queue) < outboxMaxPending {
			break
		}
		space := b.space
		b.mu.Unlock()
		select {
		case <-space:
		case <-b.done:
		}
		b.mu.Lock()
	}
	if len(b.queue) == 0 {
		b.idle = make(chan struct{})
	}
	b.queue = append(b.queue, message)
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
func (b *attachmentOutbox) stop() {
	b.mu.Lock()
	b.stopped = true
	b.mu.Unlock()
	select {
	case b.wake <- struct{}{}:
	default:
	}
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
			b.err = err
			b.queue = nil
			close(b.failed)
			close(b.idle)
			close(b.space)
			b.mu.Unlock()
			return
		}
		b.queue[0] = nil
		b.queue = b.queue[1:]
		close(b.space)
		b.space = make(chan struct{})
		if len(b.queue) == 0 {
			close(b.idle)
		}
		b.mu.Unlock()
	}
}
