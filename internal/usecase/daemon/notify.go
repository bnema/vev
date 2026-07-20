package daemon

import (
	"sync"

	"github.com/bnema/vev/internal/domain"
)

const (
	noticeHistoryCap = 200
	noticePendingCap = 32
)

// noticeCenter owns the daemon-wide notification history and the queue of
// global notices awaiting a first attached client. Its mu is leaf-level: no
// other lock is ever taken while holding it.
type noticeCenter struct {
	mu      sync.Mutex
	ring    []domain.Notification
	pending []domain.Notification
}

func newNoticeCenter() *noticeCenter { return &noticeCenter{} }

func (nc *noticeCenter) record(n domain.Notification) domain.Notification {
	if n.Count == 0 {
		n.Count = 1
	}
	nc.mu.Lock()
	defer nc.mu.Unlock()
	nc.ring = append(nc.ring, n)
	if len(nc.ring) > noticeHistoryCap {
		nc.ring = nc.ring[len(nc.ring)-noticeHistoryCap:]
	}
	return n
}

func (nc *noticeCenter) history() []domain.Notification {
	nc.mu.Lock()
	defer nc.mu.Unlock()
	out := make([]domain.Notification, len(nc.ring))
	for i, n := range nc.ring {
		out[len(nc.ring)-1-i] = n
	}
	return out
}

func (nc *noticeCenter) latest() (domain.Notification, bool) {
	nc.mu.Lock()
	defer nc.mu.Unlock()
	if len(nc.ring) == 0 {
		return domain.Notification{}, false
	}
	return nc.ring[len(nc.ring)-1], true
}

func (nc *noticeCenter) queueGlobal(n domain.Notification) {
	if n.Count == 0 {
		n.Count = 1
	}
	nc.mu.Lock()
	defer nc.mu.Unlock()
	for i := range nc.pending {
		if nc.pending[i].Code == n.Code {
			nc.pending[i].Count += n.Count
			nc.pending[i].Time = n.Time
			return
		}
	}
	if len(nc.pending) >= noticePendingCap {
		return // history already has it via record(); toast is dropped
	}
	nc.pending = append(nc.pending, n)
}

func (nc *noticeCenter) drainPending() []domain.Notification {
	nc.mu.Lock()
	defer nc.mu.Unlock()
	out := nc.pending
	nc.pending = nil
	return out
}
