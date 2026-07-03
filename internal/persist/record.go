package persist

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
)

// Record is the persisted metadata for a named session.
type Record struct {
	Name      string
	Cwd       string
	CreatedAt int64
	UpdatedAt int64
}

var (
	errEmptyName       = errors.New("persist: empty session name")
	errMalformedRecord = errors.New("persist: malformed record")
)

func encodeRecordValue(r Record) ([]byte, error) {
	if r.Name == "" {
		return nil, errEmptyName
	}
	if uint64(len(r.Cwd)) > math.MaxUint32 {
		return nil, fmt.Errorf("persist: cwd too large")
	}

	buf := make([]byte, 4+len(r.Cwd)+8+8)
	binary.BigEndian.PutUint32(buf[:4], uint32(len(r.Cwd)))
	copy(buf[4:], r.Cwd)
	off := 4 + len(r.Cwd)
	binary.BigEndian.PutUint64(buf[off:off+8], uint64(r.CreatedAt))
	binary.BigEndian.PutUint64(buf[off+8:off+16], uint64(r.UpdatedAt))
	return buf, nil
}

func decodeRecordValue(name string, value []byte) (Record, error) {
	if name == "" {
		return Record{}, errEmptyName
	}
	if len(value) < 4 {
		return Record{}, errMalformedRecord
	}
	cwdLen := int(binary.BigEndian.Uint32(value[:4]))
	if cwdLen > len(value)-4 || len(value) != 4+cwdLen+8+8 {
		return Record{}, errMalformedRecord
	}
	off := 4 + cwdLen
	return Record{
		Name:      name,
		Cwd:       string(value[4:off]),
		CreatedAt: int64(binary.BigEndian.Uint64(value[off : off+8])),
		UpdatedAt: int64(binary.BigEndian.Uint64(value[off+8 : off+16])),
	}, nil
}
