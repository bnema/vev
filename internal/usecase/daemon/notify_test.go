package daemon

import (
	"fmt"
	"testing"
	"time"

	"github.com/bnema/vev/internal/domain"
)

func notice(code domain.NoticeCode, msg string, sid domain.SessionID) domain.Notification {
	return domain.Notification{Code: code, Severity: domain.NoticeError, Message: msg, Time: time.Unix(1, 0), SessionID: sid}
}

func TestNoticeCenterRingEviction(t *testing.T) {
	nc := newNoticeCenter()
	for i := 0; i < 205; i++ {
		nc.record(notice(domain.NoticeInternal, fmt.Sprintf("n%d", i), ""))
	}
	h := nc.history()
	if len(h) != 200 {
		t.Fatalf("history len = %d, want 200", len(h))
	}
	if h[0].Message != "n204" {
		t.Fatalf("history[0] = %q, want n204 (newest insert)", h[0].Message)
	}
	if h[199].Message != "n5" {
		t.Fatalf("history[199] = %q, want n5 (oldest retained after eviction)", h[199].Message)
	}
	// Every slot must hold exactly the expected message: strictly newest-first,
	// no gaps, and none of the evicted n0..n4 leaked back in.
	for i, n := range h {
		want := fmt.Sprintf("n%d", 204-i)
		if n.Message != want {
			t.Fatalf("history[%d] = %q, want %q", i, n.Message, want)
		}
	}
}

func TestNoticeCenterHistoryNewestFirst(t *testing.T) {
	nc := newNoticeCenter()
	nc.record(notice(domain.NoticePaneSpawn, "first", ""))
	nc.record(notice(domain.NoticeTabSpawn, "second", ""))
	h := nc.history()
	if h[0].Message != "second" || h[1].Message != "first" {
		t.Fatalf("history order = %q,%q; want second,first", h[0].Message, h[1].Message)
	}
	last, ok := nc.latest()
	if !ok || last.Message != "second" {
		t.Fatalf("latest = %v,%v", last.Message, ok)
	}
}

func TestNoticeCenterPendingQueue(t *testing.T) {
	t.Run("dedup coalesces by code without growing the queue", func(t *testing.T) {
		nc := newNoticeCenter()
		for i := 0; i < 40; i++ {
			nc.queueGlobal(notice(domain.NoticeSnapshotRestore, "restore failed", ""))
		}
		nc.queueGlobal(notice(domain.NoticePersistDisabled, "persistence disabled", ""))
		pending := nc.drainPending()
		if len(pending) != 2 {
			t.Fatalf("pending len = %d, want 2 (deduped by code)", len(pending))
		}
		if pending[0].Count != 40 {
			t.Fatalf("coalesced Count = %d, want 40", pending[0].Count)
		}
		if got := nc.drainPending(); len(got) != 0 {
			t.Fatalf("second drain len = %d, want 0", len(got))
		}
	})

	t.Run("cap overflow drops the 33rd distinct code", func(t *testing.T) {
		nc := newNoticeCenter()
		for i := 0; i < 33; i++ {
			nc.queueGlobal(notice(domain.NoticeCode(i), fmt.Sprintf("code %d", i), ""))
		}
		pending := nc.drainPending()
		if len(pending) != 32 {
			t.Fatalf("pending len = %d, want 32 (bounded at cap)", len(pending))
		}
		for i, n := range pending {
			if n.Code != domain.NoticeCode(i) {
				t.Fatalf("pending[%d].Code = %v, want %v (first 32 distinct codes retained, in order)", i, n.Code, domain.NoticeCode(i))
			}
		}
		for _, n := range pending {
			if n.Code == domain.NoticeCode(32) {
				t.Fatalf("pending contains code %v, which should have been dropped on overflow", domain.NoticeCode(32))
			}
		}
	})

	t.Run("dedup on an already-queued code still increments Count at the cap", func(t *testing.T) {
		nc := newNoticeCenter()
		for i := 0; i < 32; i++ {
			nc.queueGlobal(notice(domain.NoticeCode(i), fmt.Sprintf("code %d", i), ""))
		}
		// The queue is now full. Re-queuing an existing code must coalesce
		// in place, not hit the overflow-drop branch.
		nc.queueGlobal(notice(domain.NoticeCode(0), "code 0 again", ""))
		nc.queueGlobal(notice(domain.NoticeCode(0), "code 0 again", ""))
		pending := nc.drainPending()
		if len(pending) != 32 {
			t.Fatalf("pending len = %d, want 32 (dedup must not grow past cap)", len(pending))
		}
		if pending[0].Code != domain.NoticeCode(0) || pending[0].Count != 3 {
			t.Fatalf("pending[0] = {Code:%v Count:%d}, want {Code:%v Count:3}", pending[0].Code, pending[0].Count, domain.NoticeCode(0))
		}
	})
}
