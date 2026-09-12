package client

import (
	"bytes"
	"sync"
	"time"
	"unicode/utf8"
)

// This file owns user-input routing for a client-owned picker interaction.
//
// The attach loop is the single owner of the picker model: it publishes who
// owns user input through pickerConsumer, and the stdin pump only decodes
// batches into identified operations. The pump never mutates the model, so
// the two goroutines never share it.
//
// While an interaction owns input, every byte read from the terminal
// belongs to the picker: unknown or oversized sequences are consumed and
// dropped rather than forwarded to the session. There is deliberately no
// fallback to the PTY pipeline.

// pickerPendingLimit bounds a withheld escape-sequence prefix. A longer
// incomplete sequence is unrecognized input and is discarded.
const pickerPendingLimit = 8

// pickerEscapeDeadline bounds how long a lone ESC is withheld while the
// picker owns input, so ESC and the start of a CSI/SS3 sequence are told
// apart without ever falling back to the session pipeline.
const pickerEscapeDeadline = 20 * time.Millisecond

// Bracketed-paste delimiters. Their content is dropped: a paste must never
// become a run of modal commands, and the picker has no text field that
// wants pasted input.
const (
	pasteOpenMarker  = "\x1b[200~"
	pasteCloseMarker = "\x1b[201~"
)

// pickerInputState is the attach loop's publication of the current input
// owner: zero interaction with drain unset means the session owns input.
type pickerInputState struct {
	interaction uint64
	generation  uint64
	// drain consumes and drops every batch while the interaction is
	// retired but its authoritative repaint is still in flight.
	drain bool
}

// pickerEventKind distinguishes a command token from a typed rune. Events
// keep their arrival order: a batch must never reorder input.
type pickerEventKind uint8

const (
	pickerEventKey pickerEventKind = iota
	pickerEventRune
)

// pickerEvent is one decoded input event.
type pickerEvent struct {
	kind pickerEventKind
	key  string
	r    rune
}

// pickerConsumer is the slot shared between the attach loop and the stdin
// pump. It protects the published owner, the withheld escape or UTF-8
// prefix, and the paste state; it holds no picker model.
type pickerConsumer struct {
	mu        sync.Mutex
	state     pickerInputState
	pending   []byte
	pasting   bool
	pasteTail []byte
}

// setOwned publishes the interaction that now owns user input.
func (c *pickerConsumer) setOwned(interaction, generation uint64) {
	if c == nil {
		return
	}
	c.mu.Lock()
	c.state = pickerInputState{interaction: interaction, generation: generation}
	c.resetDecoderLocked()
	c.mu.Unlock()
}

// setDrain publishes the release window: the interaction is retired, its
// repaint is in flight, and every batch must be consumed and dropped so a
// picker-bound key can never reach the session.
func (c *pickerConsumer) setDrain(interaction, generation uint64) {
	if c == nil {
		return
	}
	c.mu.Lock()
	c.state = pickerInputState{interaction: interaction, generation: generation, drain: true}
	c.resetDecoderLocked()
	c.mu.Unlock()
}

// clear releases input back to the session and purges the decoder state.
func (c *pickerConsumer) clear() {
	if c == nil {
		return
	}
	c.mu.Lock()
	c.state = pickerInputState{}
	c.resetDecoderLocked()
	c.mu.Unlock()
}

// resetDecoderLocked drops a withheld prefix and any paste state. A retired
// owner must never leak a half-decoded sequence into the next one.
func (c *pickerConsumer) resetDecoderLocked() {
	c.pending = nil
	c.pasting = false
	c.pasteTail = nil
}

// consume decodes one read or admitted automation batch into an identified
// operation. The second result is false when the session owns input, so the
// caller keeps its normal pipeline. During the release window every batch is
// consumed and dropped.
func (c *pickerConsumer) consume(record terminalReadResult) (pickerConsumeOutcome, bool) {
	if c == nil {
		return pickerConsumeOutcome{}, false
	}
	c.mu.Lock()
	state := c.state
	if state.interaction == 0 && !state.drain {
		c.mu.Unlock()
		return pickerConsumeOutcome{}, false
	}
	outcome := pickerConsumeOutcome{
		consumed: true, actionID: record.actionID,
		generation: state.generation, interaction: state.interaction,
	}
	switch {
	case state.drain:
		// Retired interaction: consume and drop, including any prefix.
		c.resetDecoderLocked()
	case record.keys == nil && record.text == "":
		var batch pickerInputBatch
		c.pending, c.pasting, c.pasteTail = c.decodeLocked(record.data, &batch)
		outcome.events = batch.events
	default:
		outcome.events = automationEvents(record.keys, record.text)
	}
	c.mu.Unlock()
	return outcome, true
}

// flushPending resolves a withheld prefix once its disambiguation window
// expired. A bare escape becomes Escape; an incomplete escape sequence is
// dropped; an incomplete UTF-8 prefix is dropped. A paste in progress stays
// in progress: its content is never replayed as commands.
func (c *pickerConsumer) flushPending() (pickerConsumeOutcome, bool) {
	if c == nil {
		return pickerConsumeOutcome{}, false
	}
	c.mu.Lock()
	state, pending := c.state, c.pending
	c.pending = nil
	c.mu.Unlock()
	if len(pending) == 0 || state.drain {
		return pickerConsumeOutcome{}, false
	}
	outcome := pickerConsumeOutcome{consumed: true, generation: state.generation, interaction: state.interaction}
	if string(pending) == "\x1b" {
		outcome.events = []pickerEvent{{kind: pickerEventKey, key: "Escape"}}
	}
	return outcome, true
}

// hasPending reports whether an escape or UTF-8 prefix is withheld.
func (c *pickerConsumer) hasPending() bool {
	if c == nil {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.pending) != 0
}

// automationEvents converts one admitted ui-driver op into ordered events.
func automationEvents(keys []string, text string) []pickerEvent {
	events := make([]pickerEvent, 0, len(keys)+len(text))
	for _, key := range keys {
		events = append(events, pickerEvent{kind: pickerEventKey, key: key})
	}
	for _, r := range text {
		events = append(events, pickerEvent{kind: pickerEventRune, r: r})
	}
	return events
}

// decoder carries the paste state across reads: a paste's boundaries can
// span several terminal reads, and its content must never be interpreted.
type decoder struct {
	pending []byte
	pasting bool
	tail    []byte
}

// decodeLocked decodes bytes under the consumer lock and returns the next
// pending prefix, paste state, and paste tail.
func (c *pickerConsumer) decodeLocked(data []byte, batch *pickerInputBatch) ([]byte, bool, []byte) {
	d := decoder{pending: c.pending, pasting: c.pasting, tail: c.pasteTail}
	d.decode(data, batch)
	return d.pending, d.pasting, d.tail
}

// decode consumes one read into ordered events. A non-nil pending result is
// an incomplete escape or UTF-8 prefix to withhold until the next read.
func (d *decoder) decode(data []byte, batch *pickerInputBatch) {
	buf := data
	if len(d.pending) != 0 {
		buf = append(append([]byte(nil), d.pending...), data...)
		d.pending = nil
	}
	if d.pasting {
		buf = append(append([]byte(nil), d.tail...), buf...)
		d.tail = nil
	}
	for i := 0; i < len(buf); {
		if d.pasting {
			end := bytes.Index(buf[i:], []byte(pasteCloseMarker))
			if end < 0 {
				// Hold only enough trailing bytes to recognize a marker split
				// across reads; everything else in the paste is dropped.
				keep := len(pasteCloseMarker) - 1
				if len(buf)-i > keep {
					d.tail = append([]byte(nil), buf[len(buf)-keep:]...)
				} else {
					d.tail = append([]byte(nil), buf[i:]...)
				}
				return
			}
			i += end + len(pasteCloseMarker)
			d.pasting = false
			continue
		}
		switch b := buf[i]; {
		case b == '\r' || b == '\n':
			batch.events = append(batch.events, pickerEvent{kind: pickerEventKey, key: "Enter"})
			i++
		case b == 0x7f || b == 0x08:
			batch.events = append(batch.events, pickerEvent{kind: pickerEventKey, key: "Backspace"})
			i++
		case b == 0x03:
			batch.events = append(batch.events, pickerEvent{kind: pickerEventKey, key: "Ctrl+C"})
			i++
		case b == 0x1b:
			consumed, held, paste := decodeEscape(buf[i:], batch)
			if held != nil {
				d.pending = held
				return
			}
			if paste {
				d.pasting = true
			}
			i += consumed
		case b < 0x20:
			// Other control bytes carry no picker meaning; consume them so
			// they cannot reach the session.
			i++
		case b < utf8.RuneSelf:
			batch.events = append(batch.events, pickerEvent{kind: pickerEventKey, key: string(rune(b))})
			i++
		default:
			r, size := utf8.DecodeRune(buf[i:])
			if r == utf8.RuneError && size <= 1 {
				if !utf8PrefixIncomplete(buf[i:]) {
					i++ // Invalid UTF-8: consume the byte and keep scanning.
					continue
				}
				d.pending = append([]byte(nil), buf[i:]...)
				return
			}
			batch.events = append(batch.events, pickerEvent{kind: pickerEventRune, r: r})
			i += size
		}
	}
}

// decodeEscape consumes one escape-led sequence starting at buf[0]. It
// returns the number of bytes consumed; a non-nil result is an incomplete
// prefix to withhold until the next read. paste reports the bracketed-paste
// start marker.
func decodeEscape(buf []byte, batch *pickerInputBatch) (consumed int, held []byte, paste bool) {
	if len(buf) == 1 {
		return 0, append([]byte(nil), buf...), false
	}
	if buf[1] != '[' && buf[1] != 'O' {
		// ESC followed by an ordinary byte: the escape stands alone.
		batch.events = append(batch.events, pickerEvent{kind: pickerEventKey, key: "Escape"})
		return 1, nil, false
	}
	if len(buf) == 2 {
		return 0, append([]byte(nil), buf...), false
	}
	switch buf[2] {
	case 'A':
		batch.events = append(batch.events, pickerEvent{kind: pickerEventKey, key: "Up"})
		return 3, nil, false
	case 'B':
		batch.events = append(batch.events, pickerEvent{kind: pickerEventKey, key: "Down"})
		return 3, nil, false
	}
	// Any other CSI/SS3 sequence (mouse reports, probes, paste markers) is
	// consumed through its final byte and dropped.
	final := -1
	for i := 2; i < len(buf); i++ {
		if buf[i] >= 0x40 && buf[i] <= 0x7e {
			final = i
			break
		}
	}
	if final < 0 {
		if len(buf) > pickerPendingLimit {
			return len(buf), nil, false
		}
		return 0, append([]byte(nil), buf...), false
	}
	seq := buf[:final+1]
	if string(seq) == pasteOpenMarker {
		return final + 1, nil, true
	}
	return final + 1, nil, false
}

// utf8PrefixIncomplete reports whether b is a valid but incomplete UTF-8
// sequence start, which must be withheld until its continuation bytes
// arrive instead of being dropped.
func utf8PrefixIncomplete(b []byte) bool {
	if len(b) == 0 || !utf8.RuneStart(b[0]) {
		return false
	}
	need := 0
	switch {
	case b[0]&0xe0 == 0xc0:
		need = 2
	case b[0]&0xf0 == 0xe0:
		need = 3
	case b[0]&0xf8 == 0xf0:
		need = 4
	default:
		return false
	}
	if len(b) >= need {
		return false
	}
	for _, c := range b[1:] {
		if c&0xc0 != 0x80 {
			return false
		}
	}
	return true
}
