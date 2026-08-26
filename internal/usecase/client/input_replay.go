package client

import "sync"

// inputReplayLedger retains input accepted by the scanner until the sender
// confirms its transport write. On a link failure, queued input can be handed
// back to the lifecycle-owned reader without losing ordered keystrokes.
type inputReplayLedger struct {
	mu      sync.Mutex
	pending map[uint64][]byte
	order   []uint64
}

func newInputReplayLedger() *inputReplayLedger {
	return &inputReplayLedger{pending: make(map[uint64][]byte)}
}

func (l *inputReplayLedger) register(seq uint64, data []byte) {
	if l == nil || seq == 0 || len(data) == 0 {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if _, exists := l.pending[seq]; !exists {
		l.order = append(l.order, seq)
	}
	l.pending[seq] = append([]byte(nil), data...)
}

func (l *inputReplayLedger) markSent(seq uint64) {
	if l == nil || seq == 0 {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	delete(l.pending, seq)
	for index, queued := range l.order {
		if queued == seq {
			l.order = append(l.order[:index], l.order[index+1:]...)
			return
		}
	}
}

func (l *inputReplayLedger) takeUnsent() []byte {
	if l == nil {
		return nil
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	var data []byte
	for _, seq := range l.order {
		data = append(data, l.pending[seq]...)
	}
	l.pending = make(map[uint64][]byte)
	l.order = l.order[:0]
	return data
}
