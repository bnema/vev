package vt

import "testing"

func TestScreenRowIDsRotateWithRowsAndHistory(t *testing.T) {
	s := NewScreenWithHistory(4, 3, HistoryConfig{MaxRows: 8, ChunkRows: 2})
	initial := s.RowIDs()
	assertUniqueNonzeroRowIDs(t, initial)

	s.Write([]byte("AAAA\r\nBBBB\r\nCCCC\r\n"))

	live := s.RowIDs()
	if got, want := live, []RowID{initial[1], initial[2], live[2]}; len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("live row IDs = %v, want rows moved from %v with a fresh blank row", got, initial)
	}
	if live[2] == initial[0] || live[2] == initial[1] || live[2] == initial[2] {
		t.Fatalf("new blank row reused a row ID: live=%v initial=%v", live, initial)
	}
	assertUniqueNonzeroRowIDs(t, live)

	view := s.History().SealAndView()
	if got, want := view.RowID(0), initial[0]; got != want {
		t.Fatalf("evicted row ID = %d, want %d", got, want)
	}
	if got := view.FindRowID(initial[0]); got != 0 {
		t.Fatalf("FindRowID(%d) = %d, want 0", initial[0], got)
	}
}

func TestScreenRowIDsDoNotAppendInteriorOrAlternateScrollHistory(t *testing.T) {
	s := NewScreenWithHistory(4, 4, HistoryConfig{MaxRows: 8})
	s.Write([]byte("AAAA\r\nBBBB\r\nCCCC\r\nDDDD"))
	s.Write([]byte("\x1b[2;4r\x1b[4;1H\n"))
	if got := s.History().Len(); got != 0 {
		t.Fatalf("interior scroll history rows = %d, want 0", got)
	}
	assertUniqueNonzeroRowIDs(t, s.RowIDs())
	primary := s.RowIDs()

	s.Write([]byte("\x1b[?1049h1111\r\n2222\r\n3333\r\n4444\r\n"))
	if got := s.History().Len(); got != 0 {
		t.Fatalf("alternate scroll history rows = %d, want 0", got)
	}
	s.Write([]byte("\x1b[?1049l"))
	for i, id := range primary {
		if got := s.RowID(i); got != id {
			t.Fatalf("restored primary row %d ID = %d, want %d", i, got, id)
		}
	}
}

func TestScreenRowIDsRefreshOnClearResetAndAlternateClone(t *testing.T) {
	s := NewScreen(4, 3)
	before := s.RowIDs()
	s.Write([]byte("AAAA\x1b[2J"))
	cleared := s.RowIDs()
	assertFreshRowIDs(t, before, cleared)

	s.Write([]byte("\x1bc"))
	reset := s.RowIDs()
	assertFreshRowIDs(t, cleared, reset)

	s.Write([]byte("BBBB\x1b[?1049h"))
	alternate := s.RowIDs()
	assertFreshRowIDs(t, reset, alternate)
	s.Write([]byte("CCCC\x1b[?1049l"))
	for i, id := range reset {
		if got := s.RowID(i); got != id {
			t.Fatalf("cloned primary row %d ID = %d, want %d", i, got, id)
		}
	}
}

func TestResizeReflowCarriesSourceRowIDsIntoHistoryAndViewport(t *testing.T) {
	b := newBuffer(8, 3)
	copy(b.frame.Row(0), historyRow("abcdefgh"))
	copy(b.frame.Row(1), historyRow("ijkl"))
	copy(b.frame.Row(2), historyRow("mnop"))
	b.boundaries[0] = LineBound{End: 8, Soft: true}
	b.boundaries[1] = LineBound{End: 4}
	b.boundaries[2] = LineBound{End: 4}
	b.rowIDs = []RowID{11, 12, 13}

	active := &bufferCursor{row: 2, col: 4}
	evicted, bounds, ids := b.resize(4, 2, active, nil)

	if len(evicted) != 2 || bounds[0].End != 4 || ids[0] != 11 || ids[1] != 0 {
		t.Fatalf("reflow eviction = rows=%d bound=%v IDs=%v, want two rows with source ID 11 then a generated ID", len(evicted), bounds, ids)
	}
	if got, want := b.rowIDs, []RowID{0, 13}; got[1] != want[1] || len(got) != len(want) {
		t.Fatalf("reflow viewport IDs = %v, want generated continuation then source ID %d", got, want[1])
	}
}

func TestHistoryRowIDsRemainStableAcrossViewsAndEviction(t *testing.T) {
	h := NewHistory(HistoryConfig{MaxRows: 3, ChunkRows: 2})
	for i, text := range []string{"one", "two"} {
		if err := h.Append(historyRow(text), LineBound{End: len(text)}, RowID(i+1)); err != nil {
			t.Fatal(err)
		}
	}
	before := h.SealAndView()
	if got, want := before.RowID(0), RowID(1); got != want {
		t.Fatalf("sealed row ID = %d, want %d", got, want)
	}

	if err := h.Append(historyRow("three"), LineBound{End: 5}, RowID(3)); err != nil {
		t.Fatal(err)
	}
	if got := before.FindRowID(2); got != 1 {
		t.Fatalf("retained view FindRowID(2) = %d, want 1", got)
	}
	after := h.SealAndView()
	if got := after.FindRowID(3); got != 2 {
		t.Fatalf("new view FindRowID(3) = %d, want 2", got)
	}

	if err := h.Append(historyRow("four"), LineBound{End: 4}, RowID(4)); err != nil {
		t.Fatal(err)
	}
	if got := before.FindRowID(1); got != 0 {
		t.Fatalf("eviction mutated retained view FindRowID(1) = %d, want 0", got)
	}
	if got := h.View().FindRowID(1); got != -1 {
		t.Fatalf("evicted current FindRowID(1) = %d, want -1", got)
	}
}

func assertUniqueNonzeroRowIDs(t *testing.T, ids []RowID) {
	t.Helper()
	seen := make(map[RowID]struct{}, len(ids))
	for _, id := range ids {
		if id == 0 {
			t.Fatal("row ID is zero")
		}
		if _, ok := seen[id]; ok {
			t.Fatalf("duplicate row ID %d in %v", id, ids)
		}
		seen[id] = struct{}{}
	}
}

func assertFreshRowIDs(t *testing.T, before, after []RowID) {
	t.Helper()
	assertUniqueNonzeroRowIDs(t, after)
	seen := make(map[RowID]struct{}, len(before))
	for _, id := range before {
		seen[id] = struct{}{}
	}
	for _, id := range after {
		if _, ok := seen[id]; ok {
			t.Fatalf("row ID %d was reused: before=%v after=%v", id, before, after)
		}
	}
}
