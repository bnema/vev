package dgram

import (
	"bytes"
	"errors"
	"testing"
)

func testCodec(t *testing.T) *Codec {
	t.Helper()
	key := bytes.Repeat([]byte{7}, KeySize)
	c, err := NewCodec(key)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func TestCodecOverheadMatchesSealedPacketGrowth(t *testing.T) {
	c := testCodec(t)
	plaintext := []byte("hello")
	pkt := c.Seal(1, 1, plaintext, nil)
	if got, want := len(pkt), HeaderSize+len(plaintext)+c.Overhead(); got != want {
		t.Fatalf("sealed packet length = %d, want %d", got, want)
	}
}

func TestSealOpen(t *testing.T) {
	c := testCodec(t)
	pkt := c.Seal(1, 42, []byte("hello"), []byte("aad"))
	ctr, pt, err := c.Open(pkt, 1, []byte("aad"), NewReplayWindow())
	if err != nil {
		t.Fatal(err)
	}
	if ctr != 42 || string(pt) != "hello" {
		t.Fatalf("ctr=%d pt=%q", ctr, pt)
	}
	if _, _, err := c.Open(pkt, 2, []byte("aad"), nil); !errors.Is(err, ErrDirection) {
		t.Fatalf("wrong direction err=%v", err)
	}
}

func TestReplayReject(t *testing.T) {
	c := testCodec(t)
	rw := NewReplayWindow()
	pkt := c.Seal(1, 1, []byte("x"), nil)
	if _, _, err := c.Open(pkt, 1, nil, rw); err != nil {
		t.Fatal(err)
	}
	if _, _, err := c.Open(pkt, 1, nil, rw); !errors.Is(err, ErrReplay) {
		t.Fatalf("err=%v", err)
	}
	old := c.Seal(1, 0, []byte("old"), nil)
	if _, _, err := c.Open(old, 1, nil, rw); err != nil {
		t.Fatalf("within window should pass: %v", err)
	}
}

func TestReplayWindowAllowsLargeFragmentReordering(t *testing.T) {
	c := testCodec(t)
	rw := NewReplayWindow()
	last := uint64(maxFragmentCount)
	if _, _, err := c.Open(c.Seal(1, last, []byte("last"), nil), 1, nil, rw); err != nil {
		t.Fatal(err)
	}
	if _, _, err := c.Open(c.Seal(1, 1, []byte("first"), nil), 1, nil, rw); err != nil {
		t.Fatalf("fragment within max fragment window rejected: %v", err)
	}
	if _, _, err := c.Open(c.Seal(1, last+2, []byte("advance"), nil), 1, nil, rw); err != nil {
		t.Fatal(err)
	}
	tooOld := c.Seal(1, 0, []byte("old"), nil)
	if _, _, err := c.Open(tooOld, 1, nil, rw); !errors.Is(err, ErrReplay) {
		t.Fatalf("err=%v, want ErrReplay", err)
	}
}

func TestFragmentation(t *testing.T) {
	payload := bytes.Repeat([]byte("abcdef"), 400)
	frags, err := FragmentPayload(9, payload, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(frags) < 2 {
		t.Fatalf("got %d fragments", len(frags))
	}
	for _, f := range frags {
		b, err := MarshalFragment(f)
		if err != nil {
			t.Fatal(err)
		}
		g, err := UnmarshalFragment(b)
		if err != nil {
			t.Fatal(err)
		}
		if g.Seq != f.Seq || g.Index != f.Index || !bytes.Equal(g.Data, f.Data) {
			t.Fatalf("bad roundtrip")
		}
	}
}

func TestReassemblyDisorderDuplicates(t *testing.T) {
	payload := []byte("abcdefghijklmnopqrstuvwxyz")
	frags, err := FragmentPayload(3, payload, 30)
	if err != nil {
		t.Fatal(err)
	}
	r := NewReassembler()
	order := []int{1, 0, 1}
	var got []byte
	var done bool
	for _, idx := range order {
		out, ok, err := r.Add(frags[idx])
		if err != nil {
			t.Fatal(err)
		}
		if ok {
			got, done = out, true
		}
	}
	if !done || !bytes.Equal(got, payload) {
		t.Fatalf("done=%v got=%q", done, got)
	}
}

func TestReassemblyRejectsOversizedCountBeforeInsert(t *testing.T) {
	r := NewReassembler()
	if _, _, err := r.Add(Fragment{Seq: 99, Index: 0, Count: uint16(maxFragmentCount + 1), Data: []byte("x")}); !errors.Is(err, ErrFragment) {
		t.Fatalf("err=%v, want ErrFragment", err)
	}
	if len(r.inflight) != 0 || len(r.order) != 0 {
		t.Fatalf("oversized fragment was inserted: inflight=%d order=%d", len(r.inflight), len(r.order))
	}
}

func TestFragmentPayloadRejectsOverMaxFragmentCount(t *testing.T) {
	payload := bytes.Repeat([]byte("x"), maxFragmentCount+1)
	if _, err := FragmentPayload(1, payload, fragmentHdr+1); !errors.Is(err, ErrFragment) {
		t.Fatalf("err=%v, want ErrFragment", err)
	}
}

func TestReassemblyCountMismatchRemovesStaleAssembly(t *testing.T) {
	r := NewReassembler()
	if _, _, err := r.Add(Fragment{Seq: 7, Index: 0, Count: 2, Data: []byte("stale")}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := r.Add(Fragment{Seq: 7, Index: 0, Count: 3, Data: []byte("a")}); !errors.Is(err, ErrFragment) {
		t.Fatalf("err=%v, want ErrFragment", err)
	}
	for _, f := range []Fragment{
		{Seq: 7, Index: 0, Count: 3, Data: []byte("a")},
		{Seq: 7, Index: 1, Count: 3, Data: []byte("b")},
		{Seq: 7, Index: 2, Count: 3, Data: []byte("c")},
	} {
		out, ok, err := r.Add(f)
		if err != nil {
			t.Fatal(err)
		}
		if ok && string(out) != "abc" {
			t.Fatalf("out=%q", out)
		}
	}
}

func TestReassemblyInflightBounded(t *testing.T) {
	r := NewReassembler()
	for i := range uint64(maxReassemblyInflight + 10) {
		if _, _, err := r.Add(Fragment{Seq: i, Index: 0, Count: 2, Data: []byte("x")}); err != nil {
			t.Fatal(err)
		}
	}
	if len(r.inflight) > maxReassemblyInflight {
		t.Fatalf("inflight=%d, want <= %d", len(r.inflight), maxReassemblyInflight)
	}
}
