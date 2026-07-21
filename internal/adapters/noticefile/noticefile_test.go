package noticefile

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/bnema/vev/internal/domain"
)

func TestStoreAppendDrainRoundTrip(t *testing.T) {
	t.Parallel()
	dir := filepath.Join(t.TempDir(), "vev")
	store := New(dir)

	first := domain.Notification{
		Code:      domain.NoticeSnapshotWrite,
		Severity:  domain.NoticeError,
		Message:   "session foo shut down without saving terminal state",
		Details:   "boom: disk full",
		Time:      time.Date(2026, 7, 20, 10, 0, 0, 0, time.UTC),
		Count:     1,
		SessionID: "sess-1",
	}
	second := domain.Notification{
		Code:     domain.NoticeSnapshotRestore,
		Severity: domain.NoticeWarn,
		Message:  "session bar could not be restored",
		Time:     time.Date(2026, 7, 20, 10, 5, 0, 0, time.UTC),
		Count:    2,
	}

	if err := store.Append(first); err != nil {
		t.Fatalf("Append first: %v", err)
	}
	if err := store.Append(second); err != nil {
		t.Fatalf("Append second: %v", err)
	}

	got, err := store.Drain()
	if err != nil {
		t.Fatalf("Drain: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("Drain len = %d, want 2", len(got))
	}
	if !got[0].Time.Equal(first.Time) || got[0].Message != first.Message || got[0].Code != first.Code ||
		got[0].Severity != first.Severity || got[0].Details != first.Details || got[0].Count != first.Count ||
		got[0].SessionID != first.SessionID {
		t.Fatalf("first notice round-trip mismatch: got %#v, want %#v", got[0], first)
	}
	if !got[1].Time.Equal(second.Time) || got[1].Message != second.Message || got[1].Code != second.Code ||
		got[1].Severity != second.Severity || got[1].Count != second.Count {
		t.Fatalf("second notice round-trip mismatch: got %#v, want %#v", got[1], second)
	}

	again, err := store.Drain()
	if err != nil {
		t.Fatalf("Drain again: %v", err)
	}
	if len(again) != 0 {
		t.Fatalf("Drain again len = %d, want 0", len(again))
	}
}

func TestStoreDrainMissingFileReturnsNil(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	store := New(dir)

	got, err := store.Drain()
	if err != nil {
		t.Fatalf("Drain: %v", err)
	}
	if got != nil {
		t.Fatalf("Drain on missing file = %#v, want nil", got)
	}
}

func TestStoreDrainSkipsGarbageLines(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	store := New(dir)
	path := filepath.Join(dir, "pending-notices.jsonl")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}

	good := `{"Code":9,"Severity":2,"Message":"first"}` + "\n"
	garbage := "not json at all\n"
	good2 := `{"Code":10,"Severity":1,"Message":"second"}` + "\n"
	if err := os.WriteFile(path, []byte(good+garbage+good2), 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	got, err := store.Drain()
	if err != nil {
		t.Fatalf("Drain: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("Drain len = %d, want 2 (garbage line skipped)", len(got))
	}
	if got[0].Message != "first" || got[1].Message != "second" {
		t.Fatalf("Drain messages = %q, %q; want first, second", got[0].Message, got[1].Message)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile after drain: %v", err)
	}
	if len(data) != 0 {
		t.Fatalf("file not truncated after Drain, got %d bytes", len(data))
	}
}

func TestStoreAppendPrivatizesDir(t *testing.T) {
	t.Parallel()
	dir := filepath.Join(t.TempDir(), "vev")
	store := New(dir)

	if err := store.Append(domain.Notification{Message: "x"}); err != nil {
		t.Fatalf("Append: %v", err)
	}
	info, err := os.Stat(dir)
	if err != nil {
		t.Fatalf("Stat dir: %v", err)
	}
	if got := info.Mode().Perm(); got != 0o700 {
		t.Fatalf("dir mode = %v, want 0700", got)
	}
	fi, err := os.Stat(filepath.Join(dir, "pending-notices.jsonl"))
	if err != nil {
		t.Fatalf("Stat file: %v", err)
	}
	if got := fi.Mode().Perm(); got != 0o600 {
		t.Fatalf("file mode = %v, want 0600", got)
	}
}
