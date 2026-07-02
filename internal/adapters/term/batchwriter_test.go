package term

import (
	"errors"
	"testing"
)

// recordingWriter records each Write call it receives, copying the bytes
// so later mutations by the caller (e.g. buffer reuse) don't corrupt the
// recorded history.
type recordingWriter struct {
	writes   [][]byte
	failNext bool
}

func (r *recordingWriter) Write(p []byte) (int, error) {
	if r.failNext {
		r.failNext = false
		return 0, errors.New("boom")
	}
	cp := make([]byte, len(p))
	copy(cp, p)
	r.writes = append(r.writes, cp)
	return len(p), nil
}

func (r *recordingWriter) all() []byte {
	var out []byte
	for _, w := range r.writes {
		out = append(out, w...)
	}
	return out
}

func TestBatchWriter_BufferedUntilFlush(t *testing.T) {
	rw := &recordingWriter{}
	bw := newBatchWriter(rw, 64)

	n, err := bw.Write([]byte("hello"))
	if err != nil || n != 5 {
		t.Fatalf("Write() = %d, %v; want 5, nil", n, err)
	}
	if len(rw.writes) != 0 {
		t.Fatalf("expected no writes reaching sink before Flush, got %d", len(rw.writes))
	}
	if got := bw.Buffered(); got != 5 {
		t.Fatalf("Buffered() = %d, want 5", got)
	}

	if err := bw.Flush(); err != nil {
		t.Fatalf("Flush() = %v, want nil", err)
	}
	if got := string(rw.all()); got != "hello" {
		t.Fatalf("sink got %q, want %q", got, "hello")
	}
	if got := bw.Buffered(); got != 0 {
		t.Fatalf("Buffered() after Flush = %d, want 0", got)
	}
}

func TestBatchWriter_MultipleWritesCoalesceUntilFlush(t *testing.T) {
	rw := &recordingWriter{}
	bw := newBatchWriter(rw, 64)

	for _, s := range []string{"a", "b", "c"} {
		if _, err := bw.WriteString(s); err != nil {
			t.Fatalf("WriteString(%q): %v", s, err)
		}
	}
	if len(rw.writes) != 0 {
		t.Fatalf("expected 0 writes to sink before Flush, got %d", len(rw.writes))
	}

	if err := bw.Flush(); err != nil {
		t.Fatalf("Flush(): %v", err)
	}
	if got := string(rw.all()); got != "abc" {
		t.Fatalf("sink got %q, want %q", got, "abc")
	}
	// bufio.Writer.Flush issues a single underlying Write.
	if len(rw.writes) != 1 {
		t.Fatalf("expected exactly 1 write to sink on Flush, got %d", len(rw.writes))
	}
}

func TestBatchWriter_LargeWritePassesThrough(t *testing.T) {
	rw := &recordingWriter{}
	bw := newBatchWriter(rw, 8) // small buffer

	big := make([]byte, 64)
	for i := range big {
		big[i] = 'x'
	}

	n, err := bw.Write(big)
	if err != nil || n != len(big) {
		t.Fatalf("Write(big) = %d, %v; want %d, nil", n, err, len(big))
	}
	// A write larger than the buffer bypasses buffering entirely.
	if len(rw.writes) == 0 {
		t.Fatalf("expected large write to pass through immediately, got 0 sink writes")
	}
	if got := string(rw.all()); got != string(big) {
		t.Fatalf("sink content mismatch: got %d bytes, want %d bytes", len(got), len(big))
	}
}

func TestBatchWriter_FlushPropagatesError(t *testing.T) {
	rw := &recordingWriter{failNext: true}
	bw := newBatchWriter(rw, 64)

	if _, err := bw.WriteString("x"); err != nil {
		t.Fatalf("WriteString: %v", err)
	}
	if err := bw.Flush(); err == nil {
		t.Fatalf("expected Flush error, got nil")
	}
}

func TestBatchWriter_FlushOnEmptyBufferIsNoop(t *testing.T) {
	rw := &recordingWriter{}
	bw := newBatchWriter(rw, 64)

	if err := bw.Flush(); err != nil {
		t.Fatalf("Flush() on empty buffer: %v", err)
	}
	if len(rw.writes) != 0 {
		t.Fatalf("expected no sink writes, got %d", len(rw.writes))
	}
}
