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
	out := bytes.NewBuffer(make([]byte, 0, min(int(size), inflateInitialCap)))
	if _, err := io.Copy(out, io.LimitReader(r, int64(size))); err != nil {
		return nil, invalid(err)
	}
	if out.Len() != int(size) {
		return nil, invalid(io.ErrUnexpectedEOF)
	}
	payload := out.Bytes()
	if cap(payload) > len(payload)+len(payload)/4 {
		// Restored payloads are retained; drop the growth slack.
		payload = bytes.Clone(payload)
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

func objectDecompressor(source io.Reader) (io.ReadCloser, error) {
	if pooled, ok := objectDecompressorPool.Get().(io.ReadCloser); ok {
		if err := pooled.(zlib.Resetter).Reset(source, nil); err != nil {
			return nil, err
		}
		return pooled, nil
	}
	return zlib.NewReader(source)
}
