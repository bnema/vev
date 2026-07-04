package logging

import (
	"os"
	"sync"
)

type rotatingWriter struct {
	mu      sync.Mutex
	path    string
	max     int64
	runtime bool
	file    *os.File
	size    int64
	closed  bool
}

func newRotatingWriter(path string, maxBytes int64, rotateAtRuntime bool) (*rotatingWriter, error) {
	if maxBytes <= 0 {
		maxBytes = DefaultMaxBytes
	}
	rw := &rotatingWriter{path: path, max: maxBytes, runtime: rotateAtRuntime}
	if err := rw.open(); err != nil {
		return nil, err
	}
	if !rotateAtRuntime && rw.size >= rw.max {
		rw.rotateLocked()
	}
	return rw, nil
}

func (w *rotatingWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return 0, os.ErrClosed
	}
	n, err := w.file.Write(p)
	w.size += int64(n)
	if err == nil && w.runtime && w.size >= w.max {
		w.rotateLocked()
	}
	return n, err
}

func (w *rotatingWriter) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return nil
	}
	w.closed = true
	return w.file.Close()
}

func (w *rotatingWriter) open() error {
	f, err := os.OpenFile(w.path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return err
	}
	info, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return err
	}
	w.file = f
	w.size = info.Size()
	return nil
}

func (w *rotatingWriter) rotateLocked() {
	old := w.file
	_ = os.Rename(w.path, w.path+".old")
	if err := w.open(); err != nil {
		w.file = old
		w.size = w.max
		return
	}
	_ = old.Close()
}
