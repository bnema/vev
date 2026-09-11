package client

import (
	"sync"

	"github.com/bnema/vev/internal/protocol"
)

type pickerControlOutcome struct {
	snapshot  *protocol.PickerSnapshot
	selection *protocol.PickerSelection
	target    *protocol.AttachTarget
	err       error
}

type pickerControlBox struct {
	mu       sync.Mutex
	outcomes []pickerControlOutcome
	wake     chan struct{}
}

func newPickerControlBox() *pickerControlBox { return &pickerControlBox{wake: make(chan struct{}, 1)} }
func (b *pickerControlBox) offer(outcome pickerControlOutcome) {
	b.mu.Lock()
	b.outcomes = append(b.outcomes, outcome)
	b.mu.Unlock()
	select {
	case b.wake <- struct{}{}:
	default:
	}
}
func (b *pickerControlBox) take() (pickerControlOutcome, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(b.outcomes) == 0 {
		return pickerControlOutcome{}, false
	}
	outcome := b.outcomes[0]
	b.outcomes = b.outcomes[1:]
	if len(b.outcomes) != 0 {
		select {
		case b.wake <- struct{}{}:
		default:
		}
	}
	return outcome, true
}
