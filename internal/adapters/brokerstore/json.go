package brokerstore

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"os"
	"syscall"
	"unicode/utf8"
)

func readBounded(path string) ([]byte, error) {
	fd, err := syscall.Open(path, syscall.O_RDONLY|syscall.O_CLOEXEC|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
	if err != nil {
		return nil, err
	}
	f := os.NewFile(uintptr(fd), path)
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if !st.Mode().IsRegular() || st.Size() > MaxFileBytes {
		return nil, errors.New("brokerstore: invalid file type or size")
	}
	b, err := io.ReadAll(io.LimitReader(f, MaxFileBytes+1))
	if len(b) > MaxFileBytes {
		return nil, errors.New("brokerstore: oversized input")
	}
	return b, err
}

// strict rejects duplicate keys as well as unknown fields, trailing JSON,
// invalid UTF-8 and excessive nesting. Byte bounds apply before decoding.
func strict(raw []byte, dst any) error {
	if len(raw) > MaxFileBytes || !utf8.Valid(raw) {
		return errors.New("brokerstore: invalid JSON size or encoding")
	}
	scan := json.NewDecoder(bytes.NewReader(raw))
	if err := scanValue(scan, 0); err != nil {
		return err
	}
	if _, err := scan.Token(); err != io.EOF {
		return errors.New("brokerstore: trailing JSON")
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	return dec.Decode(dst)
}
func scanValue(d *json.Decoder, depth int) error {
	if depth > 64 {
		return errors.New("brokerstore: JSON nesting limit")
	}
	t, err := d.Token()
	if err != nil {
		return err
	}
	delim, ok := t.(json.Delim)
	if !ok {
		return nil
	}
	switch delim {
	case '{':
		seen := map[string]bool{}
		for d.More() {
			key, err := d.Token()
			if err != nil {
				return err
			}
			k, ok := key.(string)
			if !ok || seen[k] {
				return errors.New("brokerstore: duplicate JSON key")
			}
			seen[k] = true
			if err := scanValue(d, depth+1); err != nil {
				return err
			}
		}
	case '[':
		for d.More() {
			if err := scanValue(d, depth+1); err != nil {
				return err
			}
		}
	default:
		return errors.New("brokerstore: unexpected delimiter")
	}
	_, err = d.Token()
	return err
}
