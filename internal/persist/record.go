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
	TabNames  []string
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
	if uint64(len(r.TabNames)) > math.MaxUint32 {
		return nil, fmt.Errorf("persist: too many tab names")
	}
	tabNamesSize := 4
	for _, name := range r.TabNames {
		if uint64(len(name)) > math.MaxUint32 {
			return nil, fmt.Errorf("persist: tab name too large")
		}
		tabNamesSize += 4 + len(name)
	}

	buf := make([]byte, 4+len(r.Cwd)+8+8+tabNamesSize)
	binary.BigEndian.PutUint32(buf[:4], uint32(len(r.Cwd)))
	copy(buf[4:], r.Cwd)
	off := 4 + len(r.Cwd)
	binary.BigEndian.PutUint64(buf[off:off+8], uint64(r.CreatedAt))
	binary.BigEndian.PutUint64(buf[off+8:off+16], uint64(r.UpdatedAt))
	off += 16
	binary.BigEndian.PutUint32(buf[off:off+4], uint32(len(r.TabNames)))
	off += 4
	for _, name := range r.TabNames {
		binary.BigEndian.PutUint32(buf[off:off+4], uint32(len(name)))
		off += 4
		copy(buf[off:], name)
		off += len(name)
	}
	return buf, nil
}

func decodeRecordValue(name string, value []byte) (Record, error) {
	if name == "" {
		return Record{}, errEmptyName
	}
	if len(value) < 4 {
		return Record{}, errMalformedRecord
	}
	cwdLen32 := binary.BigEndian.Uint32(value[:4])
	remaining := len(value) - 4
	if uint64(cwdLen32) > uint64(remaining) {
		return Record{}, errMalformedRecord
	}
	cwdLen := int(cwdLen32)
	baseLen := 4 + cwdLen + 8 + 8
	if len(value) < baseLen {
		return Record{}, errMalformedRecord
	}
	off := 4 + cwdLen
	r := Record{
		Name:      name,
		Cwd:       string(value[4:off]),
		CreatedAt: int64(binary.BigEndian.Uint64(value[off : off+8])),
		UpdatedAt: int64(binary.BigEndian.Uint64(value[off+8 : off+16])),
	}
	off += 16
	if len(value) == baseLen {
		return r, nil
	}
	if len(value)-off < 4 {
		return Record{}, errMalformedRecord
	}
	tabCount32 := binary.BigEndian.Uint32(value[off : off+4])
	off += 4
	if uint64(tabCount32) > uint64(len(value)-off)/4 {
		return Record{}, errMalformedRecord
	}
	tabCount := int(tabCount32)
	if tabCount > 0 {
		r.TabNames = make([]string, 0, tabCount)
	}
	for range tabCount {
		if len(value)-off < 4 {
			return Record{}, errMalformedRecord
		}
		nameLen32 := binary.BigEndian.Uint32(value[off : off+4])
		off += 4
		if uint64(nameLen32) > uint64(len(value)-off) {
			return Record{}, errMalformedRecord
		}
		nameLen := int(nameLen32)
		r.TabNames = append(r.TabNames, string(value[off:off+nameLen]))
		off += nameLen
	}
	if off != len(value) {
		return Record{}, errMalformedRecord
	}
	return r, nil
}
