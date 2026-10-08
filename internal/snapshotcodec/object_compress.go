package snapshotcodec

import (
	"bytes"
	"compress/zlib"
	"errors"
	"fmt"
	"io"
	"sync"
)

// Object payloads are stored zlib-compressed. History cells are highly
// repetitive, so a sealed 10k-row history shrinks several times on disk.
// BestSpeed keeps snapshot encoding cheap; Go's deflate output is
// deterministic for one toolchain and level, so content addressing stays
// stable between checkpoints of one build.

// inflateInitialCap bounds the up-front allocation for one payload, so a
// corrupt declared length cannot allocate more than the stream produces.
const inflateInitialCap = 1 << 20

var objectCompressorPool = sync.Pool{New: func() any {
	w, err := zlib.NewWriterLevel(io.Discard, zlib.BestSpeed)
	if err != nil {
		panic(err) // BestSpeed is a valid level
	}
	return w
}}

var objectDecompressorPool sync.Pool

// compressObjectPayload appends the zlib stream of payload to prefix.
func compressObjectPayload(prefix, payload []byte) ([]byte, error) {
	out := bytes.NewBuffer(prefix)
	out.Grow(len(payload)/4 + 64)
	w := objectCompressorPool.Get().(*zlib.Writer)
	w.Reset(out)
	defer func() {
		w.Reset(io.Discard)
		objectCompressorPool.Put(w)
	}()
	if _, err := w.Write(payload); err != nil {
		return nil, err
	}
	if err := w.Close(); err != nil {
		return nil, err
	}
	return out.Bytes(), nil
}

// inflateObjectPayload decodes exactly size bytes and requires a clean end of
// stream (which also verifies the zlib checksum) with no trailing data.
func inflateObjectPayload(compressed []byte, size uint32) ([]byte, error) {
	invalid := func(err error) error { return fmt.Errorf("%w: object payload: %v", ErrInvalidData, err) }
	source := bytes.NewReader(compressed)
	r, err := objectDecompressor(source)
	if err != nil {
		return nil, invalid(err)
	}
	defer objectDecompressorPool.Put(r)
	payload, err := readExact(r, int(size))
	if err != nil {
		return nil, invalid(err)
	}
	var extra [1]byte
	if n, err := r.Read(extra[:]); n != 0 || err != io.EOF {
		if err == nil || err == io.EOF {
			err = errors.New("payload longer than declared")
		}
		return nil, invalid(err)
	}
	if source.Len() != 0 {
		return nil, ErrTrailingBytes
	}
	return payload, nil
}

// readExact reads exactly size bytes. It doubles the buffer as data arrives,
// starting at inflateInitialCap and capping every step at size, so a corrupt
// declared length allocates at most about twice what the stream produces and
// the returned payload has no growth slack.
func readExact(r io.Reader, size int) ([]byte, error) {
	buf := make([]byte, min(size, inflateInitialCap))
	for n := 0; ; {
		m, err := io.ReadFull(r, buf[n:])
		n += m
		if err != nil {
			if err == io.EOF {
				err = io.ErrUnexpectedEOF
			}
			return nil, err
		}
		if n == size {
			return buf, nil
		}
		grown := make([]byte, min(2*len(buf), size))
		copy(grown, buf)
		buf = grown
	}
}

func objectDecompressor(source io.Reader) (io.ReadCloser, error) {
	if pooled, ok := objectDecompressorPool.Get().(io.ReadCloser); ok {
		if err := pooled.(zlib.Resetter).Reset(source, nil); err != nil {
			return nil, err
		}
		return pooled, nil
	}
	return zlib.NewReader(source)
}
