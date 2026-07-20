package daemon

import (
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
		nc.record(notice(domain.NoticeInternal, "m", ""))
	}
	h := nc.history()
	if len(h) != 200 {
		t.Fatalf("history len = %d, want 200", len(h))
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
}
