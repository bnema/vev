package ports

import (
	"sync"
)

// Logical stream admission window.
//
// One connection's logical stream IDs are allocated by its calling service and
// are strictly increasing, but two concurrent opens may be scheduled in either
// order: an ID can be abandoned, and a lower ID can legitimately arrive after a
// higher one was already admitted. A plain "not above the last admitted ID"
// check would refuse that valid concurrency, so every admission layer (the
// client, the listener, and the pool) applies one bounded anti-replay window
// instead.

const (
	// BrokerStreamWindowSize is the anti-replay window: an identity that trails
	// the newest admitted identity by BrokerStreamWindowSize or more is stale
	// and never admitted, while every unconsumed identity inside the window is
	// admitted exactly once.
	BrokerStreamWindowSize = 1024
	// brokerStreamWindowWords is the window's bitset size in 64-bit words.
	brokerStreamWindowWords = BrokerStreamWindowSize / 64
)

// BrokerStreamWindow is the bounded anti-replay admission window for the
// logical stream IDs of exactly one connection scope. Every identity is
// consumed at most once: a duplicate inside the window, and an identity evicted
// from it, are both refused as stale. It is safe for concurrent use and
// performs no I/O.
//
// The window is deliberately transport- and codec-independent: the broker IPC
// client, the broker IPC listener, and the broker core pool share this exact
// rule, so a request refused by one layer is refused by the next for the same
// reason rather than by a weaker monotone comparison.
type BrokerStreamWindow struct {
	mu      sync.Mutex
	highest BrokerStreamID
	// base is the lowest identity the window still represents. It is zero
	// before the first admission and at least one afterwards.
	base  BrokerStreamID
	words [brokerStreamWindowWords]uint64
}

// Admit consumes one stream identity for this connection. It reports
// BrokerAdmissionInvalid for a zero identity, and BrokerAdmissionStale for an
// identity already consumed or evicted from the window because it trails the
// newest admitted identity by BrokerStreamWindowSize or more. An admitted
// identity stays consumed even when the caller refuses the open afterwards, so
// a rejected request can never be replayed with the same identity.
func (w *BrokerStreamWindow) Admit(stream BrokerStreamID) error {
	if w == nil {
		return BrokerAdmissionInvalid
	}
	if stream == 0 {
		return BrokerAdmissionInvalid
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.highest == 0 {
		// The first admission opens a window anchored at one, so a lower
		// identity that a concurrent open legitimately allocated earlier can
		// still arrive afterwards.
		w.highest = stream
		w.base = windowBase(stream)
		w.setLocked(stream - w.base)
		return nil
	}
	if stream < w.base {
		return BrokerAdmissionStale
	}
	if stream <= w.highest {
		offset := stream - w.base
		if w.words[offset/64]&(1<<(offset%64)) != 0 {
			return BrokerAdmissionStale
		}
		w.setLocked(offset)
		return nil
	}
	// The incoming identity is the newest: slide the window forward. Only then
	// can an identity leave the window, which is what makes a very late open
	// stale instead of merely duplicate.
	next := windowBase(stream)
	if next > w.base {
		w.shiftLocked(next - w.base)
		w.base = next
	}
	w.highest = stream
	w.setLocked(stream - w.base)
	return nil
}

// windowBase returns the lowest identity a window whose newest admitted
// identity is highest still represents.
func windowBase(highest BrokerStreamID) BrokerStreamID {
	if highest <= BrokerStreamWindowSize {
		return 1
	}
	return highest - (BrokerStreamWindowSize - 1)
}

// setLocked marks one window offset as consumed.
func (w *BrokerStreamWindow) setLocked(offset BrokerStreamID) {
	w.words[offset/64] |= 1 << (offset % 64)
}

// shiftLocked drops the oldest shift identities from the window. A shift at or
// above the window size clears it entirely.
func (w *BrokerStreamWindow) shiftLocked(shift BrokerStreamID) {
	if shift >= BrokerStreamWindowSize {
		w.words = [brokerStreamWindowWords]uint64{}
		return
	}
	words := int(shift / 64)
	bits := uint(shift % 64)
	var shifted [brokerStreamWindowWords]uint64
	for i := range shifted {
		index := i + words
		if index >= brokerStreamWindowWords {
			break
		}
		// Go defines a shift at or above the operand width as zero, so a zero
		// bit shift needs no special case.
		var high uint64
		if index+1 < brokerStreamWindowWords {
			high = w.words[index+1] << (64 - bits)
		}
		shifted[i] = w.words[index]>>bits | high
	}
	w.words = shifted
}
