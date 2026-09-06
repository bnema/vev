package term

import (
	"bufio"
	"io"
	"sync"

	"github.com/bnema/vev/internal/ports"
)

// batchWriter is a thread-safe buffered writer. Writes accumulate in an
// internal buffer until Flush is called; a single write larger than the
// buffer's capacity passes straight through to the underlying sink
// (bufio.Writer's standard behavior).
type batchWriter struct {
	mu   sync.Mutex
	bw   *bufio.Writer
	sink ports.UIObservationSink
}

type observedWriter struct {
	out  io.Writer
	sink ports.UIObservationSink
}

func (w observedWriter) Write(p []byte) (int, error) {
	n, err := w.out.Write(p)
	if n > 0 {
		w.sink.ObserveTerminalWrite(p[:n])
	}
	return n, err
}

// newBatchWriter wraps out with a buffer of the given size.
func newBatchWriter(out io.Writer, size int) *batchWriter {
	return newBatchWriterWithObservation(out, size, nil)
}

func newBatchWriterWithObservation(out io.Writer, size int, sink ports.UIObservationSink) *batchWriter {
	writer := out
	if sink != nil {
		writer = observedWriter{out: out, sink: sink}
	}
	return &batchWriter{bw: bufio.NewWriterSize(writer, size), sink: sink}
}

// Write buffers p, flushing to the underlying sink only as bufio.Writer
// requires (e.g. when p would overflow the buffer).
func (w *batchWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.bw.Write(p)
}

// WriteString is the string analogue of Write.
func (w *batchWriter) WriteString(s string) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.bw.WriteString(s)
}

// Flush writes any buffered data to the underlying sink.
func (w *batchWriter) Flush() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.bw.Flush(); err != nil {
		return err
	}
	if w.sink != nil {
		w.sink.ObserveTerminalFlush()
	}
	return nil
}

// Buffered reports the number of bytes currently buffered and not yet
// flushed to the underlying sink.
func (w *batchWriter) Buffered() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.bw.Buffered()
}
