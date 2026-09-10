package client

import (
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

// pickerInputState is the attach loop's publication of the current input
// owner: zero interaction with drain unset means the session owns input.
type pickerInputState struct {
	interaction uint64
	generation  uint64
	// drain consumes and drops every batch while the interaction is
	// retired but its authoritative repaint is still in flight.
	drain bool
}

// pickerConsumer is the slot shared between the attach loop and the stdin
// pump. It protects the published owner and the withheld escape prefix; it
// holds no picker model.
type pickerConsumer struct {
	mu      sync.Mutex
	state   pickerInputState
	pending []byte
}

// ownsInput reports whether the picker, rather than the session, owns the
// terminal input right now.
func (c *pickerConsumer) ownsInput() bool {
	if c == nil {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.state.interaction != 0 || c.state.drain
}

// setOwned publishes the interaction that now owns user input.
func (c *pickerConsumer) setOwned(interaction, generation uint64) {
	if c == nil {
		return
	}
	c.mu.Lock()
	c.state = pickerInputState{interaction: interaction, generation: generation}
	c.pending = nil
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
	c.pending = nil
	c.mu.Unlock()
}

// clear releases input back to the session and purges any withheld prefix.
func (c *pickerConsumer) clear() {
	if c == nil {
		return
	}
	c.mu.Lock()
	c.state = pickerInputState{}
	c.pending = nil
	c.mu.Unlock()
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
		c.pending = nil
	case record.keys == nil && record.text == "":
		var batch pickerInputBatch
		c.pending = decodePickerInto(c.pending, record.data, &batch)
		outcome.keys, outcome.text = batch.keys, batch.text
	default:
		outcome.keys, outcome.text = record.keys, record.text
	}
	c.mu.Unlock()
	return outcome, true
}

// flushPending resolves a withheld escape prefix once its disambiguation
// window expired. A bare escape becomes Escape; any other withheld prefix
// is an incomplete sequence and is discarded.
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
		outcome.keys = []string{"Escape"}
	}
	return outcome, true
}

// hasPending reports whether an escape prefix is currently withheld.
func (c *pickerConsumer) hasPending() bool {
	if c == nil {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.pending) != 0
}

// pickerInputBatch is one decoded batch: command tokens plus typed runes.
type pickerInputBatch struct {
	keys []string
	text string
}

// decodePickerInto decodes terminal bytes into command tokens and typed
// runes, carrying an incomplete escape prefix into the next read. Nothing
// is ever left for the session: unrecognized, oversized and control
// sequences are consumed.
func decodePickerInto(pending, data []byte, batch *pickerInputBatch) []byte {
	buf := data
	if len(pending) != 0 {
		buf = append(append([]byte(nil), pending...), data...)
	}
	for i := 0; i < len(buf); {
		switch b := buf[i]; {
		case b == '\r' || b == '\n':
			batch.keys = append(batch.keys, "Enter")
			i++
		case b == 0x7f || b == 0x08:
			batch.keys = append(batch.keys, "Backspace")
			i++
		case b == 0x03:
			batch.keys = append(batch.keys, "Ctrl+C")
			i++
		case b == 0x1b:
			next, held := decodePickerEscape(buf[i:], batch)
			if held != nil {
				return held
			}
			i += next
		case b < 0x20:
			// Other control bytes carry no picker meaning; consume them so
			// they cannot reach the session.
			i++
		case b < utf8.RuneSelf:
			batch.keys = append(batch.keys, string(rune(b)))
			i++
		default:
			r, size := utf8.DecodeRune(buf[i:])
			if r == utf8.RuneError && size <= 1 {
				// Invalid UTF-8: consume the byte and keep scanning.
				i++
				continue
			}
			batch.text += string(r)
			i += size
		}
	}
	return nil
}

// decodePickerEscape consumes one escape-led sequence starting at buf[0].
// It returns the number of bytes consumed; a non-nil result is an
// incomplete prefix to withhold until the next read.
func decodePickerEscape(buf []byte, batch *pickerInputBatch) (int, []byte) {
	if len(buf) == 1 {
		return 0, append([]byte(nil), buf...)
	}
	if buf[1] != '[' && buf[1] != 'O' {
		// ESC followed by an ordinary byte: the escape stands alone.
		batch.keys = append(batch.keys, "Escape")
		return 1, nil
	}
	if len(buf) == 2 {
		return 0, append([]byte(nil), buf...)
	}
	switch buf[2] {
	case 'A':
		batch.keys = append(batch.keys, "Up")
		return 3, nil
	case 'B':
		batch.keys = append(batch.keys, "Down")
		return 3, nil
	}
	// Any other CSI/SS3 sequence (mouse reports, bracketed-paste markers,
	// probes) is consumed through its final byte and dropped.
	final := -1
	for i := 2; i < len(buf); i++ {
		if buf[i] >= 0x40 && buf[i] <= 0x7e {
			final = i
			break
		}
	}
	if final < 0 {
		if len(buf) > pickerPendingLimit {
			return len(buf), nil
		}
		return 0, append([]byte(nil), buf...)
	}
	return final + 1, nil
}
