package ui

import (
	"time"

	"github.com/bnema/vev/internal/domain"
)

// ToastQueueOptions bound one ToastQueue. Zero values select the defaults.
type ToastQueueOptions struct {
	// MaxVisible is how many toasts show at once (default 3).
	MaxVisible int
	// MaxPending is how many toasts wait for a free slot (default 32). When
	// full, the oldest waiting toast moves straight to history.
	MaxPending int
	// MaxHistory is how many dismissed or expired toasts are kept (default 50).
	MaxHistory int
}

const (
	defaultQueueVisible = 3
	defaultQueuePending = 32
	defaultQueueHistory = 50
)

// ToastQueue shows a bounded number of toasts and queues the rest, so a burst
// is displayed one slot at a time instead of being dropped or overdrawn.
// Toasts leaving the screen are kept in a bounded history. A toast pushed with
// the ID of a visible or waiting toast replaces it in place. Time comes only
// from the caller, so the queue is deterministic and needs no locking of its
// own; callers serialize access.
type ToastQueue struct {
	opts    ToastQueueOptions
	visible []ActiveToast // newest first
	pending []Toast       // oldest first
	history []ActiveToast // oldest first
}

// NewToastQueue returns an empty queue with opts, defaults filled in.
func NewToastQueue(opts ToastQueueOptions) *ToastQueue {
	if opts.MaxVisible <= 0 {
		opts.MaxVisible = defaultQueueVisible
	}
	if opts.MaxPending <= 0 {
		opts.MaxPending = defaultQueuePending
	}
	if opts.MaxHistory <= 0 {
		opts.MaxHistory = defaultQueueHistory
	}
	return &ToastQueue{opts: opts}
}

// Push shows toast at now when a slot is free, otherwise queues it.
func (q *ToastQueue) Push(now time.Time, toast Toast) {
	q.expire(now)
	if toast.ID != "" {
		for i := range q.visible {
			if q.visible[i].ID == toast.ID {
				q.visible = append(q.visible[:i], q.visible[i+1:]...)
				q.visible = append([]ActiveToast{{Toast: toast, ShownAt: now}}, q.visible...)
				return
			}
		}
		for i := range q.pending {
			if q.pending[i].ID == toast.ID {
				q.pending[i] = toast
				return
			}
		}
	}
	if len(q.visible) < q.opts.MaxVisible {
		q.visible = append([]ActiveToast{{Toast: toast, ShownAt: now}}, q.visible...)
		return
	}
	if len(q.pending) >= q.opts.MaxPending {
		q.remember(ActiveToast{Toast: q.pending[0]})
		q.pending = q.pending[1:]
	}
	q.pending = append(q.pending, toast)
}

// Visible expires toasts due at now, promotes waiting ones into the freed
// slots, and returns the displayed toasts newest first.
func (q *ToastQueue) Visible(now time.Time) []ActiveToast {
	q.expire(now)
	return append([]ActiveToast(nil), q.visible...)
}

// Pending reports how many toasts wait for a slot.
func (q *ToastQueue) Pending() int { return len(q.pending) }

// History returns dismissed and expired toasts, newest first.
func (q *ToastQueue) History() []ActiveToast {
	out := make([]ActiveToast, len(q.history))
	for i, t := range q.history {
		out[len(q.history)-1-i] = t
	}
	return out
}

// NextDeadline is the earliest instant a visible toast expires.
func (q *ToastQueue) NextDeadline() (time.Time, bool) {
	var next time.Time
	found := false
	for _, t := range q.visible {
		if t.Duration <= 0 {
			continue
		}
		due := t.ShownAt.Add(t.Duration)
		if !found || due.Before(next) {
			next, found = due, true
		}
	}
	return next, found
}

// Dismiss moves the toast with id to history and frees its slot at now.
func (q *ToastQueue) Dismiss(now time.Time, id string) {
	for i := range q.visible {
		if q.visible[i].ID == id {
			q.remember(q.visible[i])
			q.visible = append(q.visible[:i], q.visible[i+1:]...)
			break
		}
	}
	q.expire(now)
	q.promote(now)
}

// promote fills free slots with waiting toasts, shown from at.
func (q *ToastQueue) promote(at time.Time) {
	for len(q.visible) < q.opts.MaxVisible && len(q.pending) > 0 {
		next := q.pending[0]
		q.pending = q.pending[1:]
		q.visible = append([]ActiveToast{{Toast: next, ShownAt: at}}, q.visible...)
	}
}

// Clear drops visible and waiting toasts. History is kept.
func (q *ToastQueue) Clear() {
	for _, t := range q.visible {
		q.remember(t)
	}
	q.visible = nil
	q.pending = nil
}

// expire retires due toasts oldest first and fills each freed slot at the
// instant it was freed, so a burst advances at a steady pace even when the
// caller polls late.
func (q *ToastQueue) expire(now time.Time) {
	for {
		i, due := q.firstDue(now)
		if i < 0 {
			return
		}
		q.remember(q.visible[i])
		q.visible = append(q.visible[:i], q.visible[i+1:]...)
		q.promote(due)
	}
}

func (q *ToastQueue) firstDue(now time.Time) (int, time.Time) {
	idx := -1
	var first time.Time
	for i, t := range q.visible {
		if t.Duration <= 0 {
			continue
		}
		due := t.ShownAt.Add(t.Duration)
		if now.Before(due) {
			continue
		}
		// visible is newest first, so a later index wins a tie: the oldest
		// toast leaves first.
		if idx < 0 || !due.After(first) {
			idx, first = i, due
		}
	}
	return idx, first
}

func (q *ToastQueue) remember(t ActiveToast) {
	q.history = append(q.history, t)
	if len(q.history) > q.opts.MaxHistory {
		q.history = q.history[len(q.history)-q.opts.MaxHistory:]
	}
}

// ToastStackBounds lays toasts (newest first) out as a column at their shared
// anchor: the newest sits at the anchor and older ones step away from it,
// downward for top and middle anchors and upward for bottom anchors. Boxes that
// would leave base are omitted, so the result may be shorter than toasts.
func ToastStackBounds(base domain.Size, toasts []Toast) []domain.Rect {
	rects := make([]domain.Rect, 0, len(toasts))
	offset := 0
	for _, toast := range toasts {
		r := ToastBounds(base, toast)
		if r.Width <= 0 || r.Height <= 0 {
			break
		}
		if growsUp(toast.Anchor) {
			r.Y -= offset
		} else {
			r.Y += offset
		}
		if r.Y < 0 || r.Y+r.Height > base.Rows {
			break
		}
		rects = append(rects, r)
		offset += r.Height
	}
	return rects
}

func growsUp(anchor domain.Anchor) bool {
	switch anchor {
	case domain.AnchorBottomLeft, domain.AnchorBottom, domain.AnchorBottomRight:
		return true
	}
	return false
}
