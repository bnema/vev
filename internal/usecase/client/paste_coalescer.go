package client

import (
	"bytes"
	"sync"
	"time"

	"github.com/bnema/vev/internal/ports"
)

// Bracketed-paste markers. A terminal in bracketed-paste mode brackets pasted
// text with these so the receiving application can tell a paste from typed
// input.
var (
	pasteOpenMarker  = []byte("\x1b[200~")
	pasteCloseMarker = []byte("\x1b[201~")
)

const (
	// pasteFlushDelay bounds how long a trailing strict prefix of the opening
	// marker is held before it is emitted as ordinary bytes. It only has to
	// bridge a local kernel read split: the rest of the marker is already in
	// the tty buffer and arrives on the next Read microseconds later. It is NOT
	// meant to bridge a network-scale gap — once the opening marker completes,
	// buffering mode holds the whole paste (up to the 2s safety timer), so a
	// close-marker split at any realistic latency is covered by whole-paste
	// framing rather than this timer. Kept well under the daemon router's
	// ESCDelay so a real lone Esc keypress still reaches the pane promptly.
	pasteFlushDelay = 20 * time.Millisecond
	// pasteSafetyDelay caps how long the coalescer waits for a closing marker
	// before flushing a partial paste, so a paste with a lost closing marker
	// degrades to today's per-frame behavior instead of stalling input.
	pasteSafetyDelay = 2 * time.Second
	// maxPasteBuffer caps buffered paste bytes. Beyond it the buffer is flushed
	// verbatim and normal scanning resumes; input is never dropped.
	maxPasteBuffer = 2 << 20 // 2 MiB
)

// pasteCoalescer reframes a bracketed paste that arrives split across input
// reads into a single emit call. Over a remote attach, stdin is read in fixed
// chunks and forwarded frame by frame; an input-frame boundary can land inside
// a paste marker (e.g. between the marker's ESC and its '['). When the daemon's
// keys router then sees a lone trailing ESC it retains it for ESCDelay and, if
// the next frame is late, forwards it as a standalone Esc keypress — breaking
// the opening marker so the pane's TUI never enters paste mode. Coalescing the
// whole paste into one emit keeps both markers contiguous on the wire.
//
// It is safe for concurrent use: Scan runs on the stdin pump goroutine while a
// flush/safety timer may fire on its own goroutine; both take the mutex, so
// emit calls are serialised.
type pasteCoalescer struct {
	clock ports.Clock
	emit  func([]byte)

	mu        sync.Mutex
	buffering bool   // inside a paste: accumulating until the closing marker
	buf       []byte // paste bytes accumulated in buffering mode
	pending   []byte // held trailing strict prefix of the opening marker

	flushTimer  ports.Timer
	flushDone   chan struct{}
	safetyTimer ports.Timer
	safetyDone  chan struct{}
	closed      bool
}

func newPasteCoalescer(clock ports.Clock, emit func([]byte)) *pasteCoalescer {
	return &pasteCoalescer{clock: clock, emit: emit}
}

// Scan consumes one run of ordinary input bytes, emitting either pass-through
// bytes immediately or a whole bracketed paste as a single emit call once its
// closing marker is seen.
func (c *pasteCoalescer) Scan(data []byte) {
	if len(data) == 0 {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.pending) > 0 {
		// New bytes arrived before the flush timer fired: the held prefix was
		// the start of this data, not a lone Esc. Recombine and rescan.
		c.stopFlushTimer()
		data = append(c.pending, data...)
		c.pending = nil
	}
	c.process(data)
}

// process runs the scan/buffer state machine with the mutex held.
func (c *pasteCoalescer) process(data []byte) {
	for {
		if c.buffering {
			c.buf = append(c.buf, data...)
			data = nil
			idx := bytes.Index(c.buf, pasteCloseMarker)
			if idx < 0 {
				if len(c.buf) > maxPasteBuffer {
					c.emit(c.buf)
					c.resetBuffering()
				}
				return
			}
			end := idx + len(pasteCloseMarker)
			c.emit(c.buf[:end])
			rest := append([]byte(nil), c.buf[end:]...)
			c.resetBuffering()
			data = rest
			// Fall through to scan any bytes trailing the paste.
		}

		if len(data) == 0 {
			return
		}

		if idx := bytes.Index(data, pasteOpenMarker); idx >= 0 {
			if idx > 0 {
				c.emit(data[:idx])
			}
			c.buffering = true
			c.startSafetyTimer()
			data = data[idx:] // marker onward; re-enter the buffering branch
			continue
		}

		if p := trailingOpenPrefixLen(data); p > 0 {
			if len(data) > p {
				c.emit(data[:len(data)-p])
			}
			c.pending = append([]byte(nil), data[len(data)-p:]...)
			c.startFlushTimer()
			return
		}

		c.emit(data)
		return
	}
}

// resetBuffering leaves paste mode and stops the safety timer.
func (c *pasteCoalescer) resetBuffering() {
	c.buffering = false
	c.buf = nil
	c.stopSafetyTimer()
}

// Buffering reports whether it is currently unsafe to reinterpret incoming
// bytes as ordinary keystrokes: either a bracketed paste is being
// accumulated, or a trailing strict prefix of the opening marker is being
// held across a read boundary (pending, awaiting either completion into a
// real paste or the flush timer proving it was not one). The clipboard
// Ctrl+V interceptor uses this to avoid treating bytes that are, or might
// turn out to be, part of an in-flight paste as a standalone keystroke —
// pasted text may legitimately contain 0x16.
func (c *pasteCoalescer) Buffering() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.buffering || len(c.pending) > 0
}

// Close stops any pending timers and their goroutines. Held bytes are dropped:
// Close runs only as the stdin pump unwinds on detach, when there is nowhere
// left to deliver them.
func (c *pasteCoalescer) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.closed = true
	c.stopFlushTimer()
	c.stopSafetyTimer()
}

func (c *pasteCoalescer) startFlushTimer() {
	c.stopFlushTimer()
	if c.closed {
		return
	}
	timer := c.clock.NewTimer(pasteFlushDelay)
	done := make(chan struct{})
	c.flushTimer = timer
	c.flushDone = done
	go func() {
		select {
		case <-timer.C():
		case <-done:
			return
		}
		c.mu.Lock()
		defer c.mu.Unlock()
		if c.flushTimer != timer || len(c.pending) == 0 {
			return
		}
		c.flushTimer = nil
		c.flushDone = nil
		held := c.pending
		c.pending = nil
		c.emit(held)
	}()
}

func (c *pasteCoalescer) stopFlushTimer() {
	if c.flushTimer != nil {
		c.flushTimer.Stop()
		c.flushTimer = nil
	}
	if c.flushDone != nil {
		close(c.flushDone)
		c.flushDone = nil
	}
}

func (c *pasteCoalescer) startSafetyTimer() {
	c.stopSafetyTimer()
	if c.closed {
		return
	}
	timer := c.clock.NewTimer(pasteSafetyDelay)
	done := make(chan struct{})
	c.safetyTimer = timer
	c.safetyDone = done
	go func() {
		select {
		case <-timer.C():
		case <-done:
			return
		}
		c.mu.Lock()
		defer c.mu.Unlock()
		if c.safetyTimer != timer || !c.buffering {
			return
		}
		buffered := c.buf
		c.buffering = false
		c.buf = nil
		c.safetyTimer = nil
		c.safetyDone = nil
		if len(buffered) > 0 {
			c.emit(buffered)
		}
	}()
}

func (c *pasteCoalescer) stopSafetyTimer() {
	if c.safetyTimer != nil {
		c.safetyTimer.Stop()
		c.safetyTimer = nil
	}
	if c.safetyDone != nil {
		close(c.safetyDone)
		c.safetyDone = nil
	}
}

// trailingOpenPrefixLen returns the length of the longest suffix of data that
// is a non-empty strict prefix of the opening marker (e.g. a trailing "\x1b" or
// "\x1b[20"). A full marker match is handled by the caller before this runs.
func trailingOpenPrefixLen(data []byte) int {
	max := len(pasteOpenMarker) - 1
	if max > len(data) {
		max = len(data)
	}
	for n := max; n >= 1; n-- {
		if bytes.Equal(data[len(data)-n:], pasteOpenMarker[:n]) {
			return n
		}
	}
	return 0
}
