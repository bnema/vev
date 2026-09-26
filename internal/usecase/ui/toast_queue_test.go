package ui

import (
	"testing"
	"time"

	"github.com/bnema/vev/internal/domain"
	"github.com/stretchr/testify/require"
)

func TestToastQueue(t *testing.T) {
	start := time.Unix(100, 0)
	life := 4 * time.Second
	toast := func(id string) QueuedToast[string] { return QueuedToast[string]{ID: id, Value: id, Duration: life} }
	ids := func(ts []QueuedToast[string]) []string {
		out := make([]string, len(ts))
		for i, t := range ts {
			out[i] = t.ID
		}
		return out
	}

	tests := []struct {
		name        string
		opts        ToastQueueOptions
		push        []QueuedToast[string]
		at          time.Duration
		wantVisible []string
		wantPending int
	}{
		{name: "fills slots newest first", opts: ToastQueueOptions{MaxVisible: 3}, push: []QueuedToast[string]{toast("a"), toast("b")}, wantVisible: []string{"b", "a"}},
		{name: "overflow waits", opts: ToastQueueOptions{MaxVisible: 2}, push: []QueuedToast[string]{toast("a"), toast("b"), toast("c")}, wantVisible: []string{"b", "a"}, wantPending: 1},
		{name: "expiry promotes waiting toasts", opts: ToastQueueOptions{MaxVisible: 2}, push: []QueuedToast[string]{toast("a"), toast("b"), toast("c")}, at: life, wantVisible: []string{"c"}},
		{name: "late poll paces the burst", opts: ToastQueueOptions{MaxVisible: 1}, push: []QueuedToast[string]{toast("a"), toast("b"), toast("c")}, at: 2 * life, wantVisible: []string{"c"}},
		{name: "same id replaces in place", opts: ToastQueueOptions{MaxVisible: 3}, push: []QueuedToast[string]{toast("a"), toast("b"), toast("a")}, wantVisible: []string{"a", "b"}},
		{name: "same id replaces waiting", opts: ToastQueueOptions{MaxVisible: 1}, push: []QueuedToast[string]{toast("a"), toast("b"), toast("b")}, wantVisible: []string{"a"}, wantPending: 1},
		{name: "full queue drops oldest waiting", opts: ToastQueueOptions{MaxVisible: 1, MaxPending: 1}, push: []QueuedToast[string]{toast("a"), toast("b"), toast("c")}, wantVisible: []string{"a"}, wantPending: 1},
		{name: "persistent toast never expires", opts: ToastQueueOptions{}, push: []QueuedToast[string]{{ID: "p"}}, at: time.Hour, wantVisible: []string{"p"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q := NewToastQueue[string](tt.opts)
			for _, p := range tt.push {
				q.Push(start, p)
			}
			require.Equal(t, tt.wantVisible, nilIfEmpty(ids(q.Visible(start.Add(tt.at)))))
			require.Equal(t, tt.wantPending, q.Pending())
		})
	}
}

func TestToastQueueDeadlineAndDismiss(t *testing.T) {
	start := time.Unix(100, 0)
	q := NewToastQueue[string](ToastQueueOptions{MaxVisible: 1})
	_, ok := q.NextDeadline()
	require.False(t, ok)

	q.Push(start, QueuedToast[string]{ID: "a", Duration: time.Second})
	q.Push(start, QueuedToast[string]{ID: "b", Duration: time.Second})
	due, ok := q.NextDeadline()
	require.True(t, ok)
	require.Equal(t, start.Add(time.Second), due)

	require.True(t, q.Dismiss(start.Add(time.Millisecond), "a"))
	require.False(t, q.Dismiss(start.Add(time.Millisecond), "missing"))
	require.Equal(t, "b", q.Visible(start.Add(time.Millisecond))[0].ID)
	due, _ = q.NextDeadline()
	require.Equal(t, start.Add(time.Second+time.Millisecond), due, "a promoted toast gets its full lifetime")
}

func TestToastStackBounds(t *testing.T) {
	base := domain.Size{Cols: 80, Rows: 12}
	msg := "hello"
	stack := func(anchor domain.Anchor, n int) []Toast {
		out := make([]Toast, n)
		for i := range out {
			out[i] = Toast{Message: msg, Anchor: anchor}
		}
		return out
	}
	tests := []struct {
		name  string
		toast []Toast
		wantY []int
	}{
		{name: "top grows down", toast: stack(domain.AnchorTopRight, 3), wantY: []int{2, 5, 8}},
		{name: "bottom grows up", toast: stack(domain.AnchorBottomLeft, 2), wantY: []int{7, 4}},
		{name: "boxes leaving the frame are dropped", toast: stack(domain.AnchorTopRight, 5), wantY: []int{2, 5, 8}},
		{name: "empty", toast: nil, wantY: nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var got []int
			for _, r := range ToastStackBounds(base, tt.toast) {
				got = append(got, r.Y)
			}
			require.Equal(t, tt.wantY, got)
		})
	}
}

func nilIfEmpty(s []string) []string {
	if len(s) == 0 {
		return nil
	}
	return s
}
