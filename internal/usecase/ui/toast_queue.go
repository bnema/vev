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
	// full, the oldest waiting toast is dropped.
	MaxPending int
}

const (
	defaultQueueVisible = 3
	defaultQueuePending = 32
)

// QueuedToast is one entry of a ToastQueue. Value is the caller's payload.
type QueuedToast[T any] struct {
	// ID is the coalescing identity: pushing an ID that is visible or waiting
	// replaces that entry. An empty ID never matches.
	ID    string
	Value T
	// Duration is how long the entry stays visible; zero never expires.
	Duration time.Duration
	// ShownAt is when the entry became visible. Push sets it.
	ShownAt time.Time
}

// ToastQueue shows a bounded number of toasts and queues the rest, so a burst
// is displayed one slot at a time instead of being dropped or overdrawn.
// Time comes only
// from the caller, so the queue is deterministic; callers serialize access.
type ToastQueue[T any] struct {
	opts    ToastQueueOptions
	visible []QueuedToast[T] // newest first
	pending []QueuedToast[T] // oldest first
}

// NewToastQueue returns an empty queue with opts, defaults filled in.
func NewToastQueue[T any](opts ToastQueueOptions) *ToastQueue[T] {
	if opts.MaxVisible <= 0 {
		opts.MaxVisible = defaultQueueVisible
	}
	if opts.MaxPending <= 0 {
		opts.MaxPending = defaultQueuePending
	}
	return &ToastQueue[T]{opts: opts}
}

// Push shows toast at now when a slot is free, otherwise queues it. A visible
// entry with the same ID is replaced, moved to the front, and restarts its
// lifetime; a waiting one is replaced where it waits.
func (q *ToastQueue[T]) Push(now time.Time, toast QueuedToast[T]) {
	q.expire(now)
	toast.ShownAt = now
	if i := q.indexVisible(toast.ID); i >= 0 {
		q.visible = append(q.visible[:i], q.visible[i+1:]...)
		q.visible = append([]QueuedToast[T]{toast}, q.visible...)
		return
	}
	if i := q.indexPending(toast.ID); i >= 0 {
		q.pending[i] = toast
		return
	}
	if len(q.visible) < q.opts.MaxVisible {
		q.visible = append([]QueuedToast[T]{toast}, q.visible...)
		return
	}
	if len(q.pending) >= q.opts.MaxPending {
		q.pending = q.pending[1:]
	}
	q.pending = append(q.pending, toast)
}

// Get returns the visible or waiting entry with id.
func (q *ToastQueue[T]) Get(id string) (QueuedToast[T], bool) {
	if i := q.indexVisible(id); i >= 0 {
		return q.visible[i], true
	}
	if i := q.indexPending(id); i >= 0 {
		return q.pending[i], true
	}
	return QueuedToast[T]{}, false
}

// Visible expires entries due at now, promotes waiting ones into the freed
// slots, and returns the displayed entries newest first.
func (q *ToastQueue[T]) Visible(now time.Time) []QueuedToast[T] {
	q.expire(now)
	return append([]QueuedToast[T](nil), q.visible...)
}

// Peek returns the displayed entries newest first without expiring any, for
// readers that must not change the queue, such as a render snapshot.
func (q *ToastQueue[T]) Peek() []QueuedToast[T] {
	return append([]QueuedToast[T](nil), q.visible...)
}

// Pending reports how many entries wait for a slot.
func (q *ToastQueue[T]) Pending() int { return len(q.pending) }

// NextDeadline is the earliest instant a visible entry expires.
func (q *ToastQueue[T]) NextDeadline() (time.Time, bool) {
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

// Dismiss removes the visible or waiting entry with id and fills a
// freed slot at now. It reports whether an entry was removed.
func (q *ToastQueue[T]) Dismiss(now time.Time, id string) bool {
	removed := false
	if i := q.indexVisible(id); i >= 0 {
		q.visible = append(q.visible[:i], q.visible[i+1:]...)
		removed = true
	} else if i := q.indexPending(id); i >= 0 {
		q.pending = append(q.pending[:i], q.pending[i+1:]...)
		removed = true
	}
	q.expire(now)
	q.promote(now)
	return removed
}

func (q *ToastQueue[T]) indexVisible(id string) int {
	if id == "" {
		return -1
	}
	for i := range q.visible {
		if q.visible[i].ID == id {
			return i
		}
	}
	return -1
}

func (q *ToastQueue[T]) indexPending(id string) int {
	if id == "" {
		return -1
	}
	for i := range q.pending {
		if q.pending[i].ID == id {
			return i
		}
	}
	return -1
}

// expire retires due entries oldest first and fills each freed slot at the
// instant it was freed, so a burst advances at a steady pace even when the
// caller polls late.
func (q *ToastQueue[T]) expire(now time.Time) {
	for {
		i, due := q.firstDue(now)
		if i < 0 {
			return
		}
		q.visible = append(q.visible[:i], q.visible[i+1:]...)
		q.promote(due)
	}
}

// promote fills free slots with waiting entries, shown from at.
func (q *ToastQueue[T]) promote(at time.Time) {
	for len(q.visible) < q.opts.MaxVisible && len(q.pending) > 0 {
		next := q.pending[0]
		q.pending = q.pending[1:]
		next.ShownAt = at
		q.visible = append([]QueuedToast[T]{next}, q.visible...)
	}
}

func (q *ToastQueue[T]) firstDue(now time.Time) (int, time.Time) {
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
		// entry leaves first.
		if idx < 0 || !due.After(first) {
			idx, first = i, due
		}
	}
	return idx, first
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
