package client

import (
	"bytes"
	"time"

	"github.com/bnema/vev/internal/usecase/keys/kittykey"
)

const resumeEscapeLimit = 256

// resumeEscapeDelay is how long a lone Esc stays undecided before it counts
// as a cancel rather than the start of an escape sequence.
const resumeEscapeDelay = 50 * time.Millisecond

// resumeInputDecoder retains only an undecided escape prefix. Held bytes stay
// in their original terminal encoding; paste content is never a cancel key.
type resumeInputDecoder struct {
	prefix []byte
	paste  bool
}

func (d *resumeInputDecoder) feed(data []byte) (held []byte, cancel bool) {
	for _, b := range data {
		if len(d.prefix) == 0 {
			if b == 0x1b {
				d.prefix = append(d.prefix, b)
				continue
			}
			if b == 3 && !d.paste {
				return held, true
			}
			held = append(held, b)
			continue
		}
		if b == 0x1b || (b < 0x20 && !d.paste) {
			held = append(held, d.prefix...)
			d.prefix = d.prefix[:0]
			if b == 3 {
				return held, true
			}
			if b == 0x1b {
				d.prefix = append(d.prefix, b)
			} else {
				held = append(held, b)
			}
			continue
		}
		d.prefix = append(d.prefix, b)
		if d.paste {
			end := []byte("\x1b[201~")
			if bytes.Equal(d.prefix, end) {
				d.paste = false
			} else if bytes.HasPrefix(end, d.prefix) {
				continue
			}
		} else {
			start := []byte("\x1b[200~")
			if bytes.Equal(d.prefix, start) {
				d.paste = true
			} else if bytes.HasPrefix(start, d.prefix) {
				continue
			} else {
				_, n, ok, partial := kittykey.Parse(d.prefix)
				if ok {
					translated := kittykey.Translate(d.prefix[:n], 0)
					if len(translated) == 1 && (translated[0] == 3 || translated[0] == 0x1b) {
						return held, true
					}
				} else if partial && len(d.prefix) < resumeEscapeLimit {
					continue
				} else if len(d.prefix) >= 2 && (d.prefix[1] == '[' || d.prefix[1] == 'O') && b >= 0x20 && b < 0x40 && len(d.prefix) < resumeEscapeLimit {
					continue
				}
			}
		}
		held = append(held, d.prefix...)
		d.prefix = d.prefix[:0]
	}
	return held, false
}

// undecided reports whether an escape prefix is still waiting for more bytes.
func (d *resumeInputDecoder) undecided() bool {
	return len(d.prefix) != 0
}

func (d *resumeInputDecoder) loneEscape() bool {
	return !d.paste && bytes.Equal(d.prefix, []byte{0x1b})
}

func (d *resumeInputDecoder) flush() []byte {
	data := d.prefix
	d.prefix = nil
	return data
}
