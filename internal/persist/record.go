package persist

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"

	"github.com/bnema/vev/internal/domain"
)

const catalogueRecordVersion uint16 = 1

var errMalformedRecord = errors.New("persist: malformed catalogue record")

var catalogueMagic = [4]byte{'V', 'E', 'V', 'C'}

// Record is the durable catalogue record used by daemon code.
type Record = domain.CatalogueRecord

func encodeRecordValue(record domain.CatalogueRecord) ([]byte, error) {
	if err := record.Validate(); err != nil {
		return nil, fmt.Errorf("persist: invalid catalogue record: %w", err)
	}
	buf := make([]byte, 0, 128)
	buf = append(buf, catalogueMagic[:]...)
	buf = binary.BigEndian.AppendUint16(buf, catalogueRecordVersion)
	buf = append(buf, record.IncarnationID[:]...)
	var err error
	buf, err = appendCheckedString(buf, record.Cwd)
	if err != nil {
		return nil, err
	}
	buf = binary.BigEndian.AppendUint64(buf, uint64(record.CreatedAt))
	buf = binary.BigEndian.AppendUint64(buf, uint64(record.UpdatedAt))
	buf = binary.BigEndian.AppendUint64(buf, record.LastUsedSeq)
	if uint64(len(record.TabNames)) > math.MaxUint32 {
		return nil, errors.New("persist: too many tab names")
	}
	buf = binary.BigEndian.AppendUint32(buf, uint32(len(record.TabNames)))
	for _, name := range record.TabNames {
		buf, err = appendCheckedString(buf, name)
		if err != nil {
			return nil, err
		}
	}
	buf = append(buf, byte(record.RecoveryState))
	buf = appendRef(buf, record.Committed)
	buf = appendRef(buf, record.Fallbacks[0])
	buf = appendRef(buf, record.Fallbacks[1])
	buf, err = appendCheckedString(buf, record.DegradedReason)
	return buf, err
}

func decodeRecordValue(name string, value []byte) (domain.CatalogueRecord, error) {
	if err := domain.ValidateSessionName(name); err != nil {
		return domain.CatalogueRecord{}, err
	}
	reader := valueReader{data: value}
	magic, ok := reader.take(4)
	if !ok || string(magic) != string(catalogueMagic[:]) {
		return domain.CatalogueRecord{}, errMalformedRecord
	}
	version, ok := reader.u16()
	if !ok || version != catalogueRecordVersion {
		return domain.CatalogueRecord{}, errMalformedRecord
	}
	id, ok := reader.take(16)
	if !ok {
		return domain.CatalogueRecord{}, errMalformedRecord
	}
	record := domain.CatalogueRecord{Name: name}
	copy(record.IncarnationID[:], id)
	var valid bool
	if record.Cwd, valid = reader.str(); !valid {
		return domain.CatalogueRecord{}, errMalformedRecord
	}
	created, ok := reader.u64()
	if !ok {
		return domain.CatalogueRecord{}, errMalformedRecord
	}
	record.CreatedAt = int64(created)
	updated, ok := reader.u64()
	if !ok {
		return domain.CatalogueRecord{}, errMalformedRecord
	}
	record.UpdatedAt = int64(updated)
	if record.LastUsedSeq, ok = reader.u64(); !ok {
		return domain.CatalogueRecord{}, errMalformedRecord
	}
	count, ok := reader.u32()
	if !ok || uint64(count) > uint64(reader.remaining())/4 {
		return domain.CatalogueRecord{}, errMalformedRecord
	}
	if count > 0 {
		record.TabNames = make([]string, 0, int(count))
	}
	for range count {
		tab, ok := reader.str()
		if !ok {
			return domain.CatalogueRecord{}, errMalformedRecord
		}
		record.TabNames = append(record.TabNames, tab)
	}
	state, ok := reader.byte()
	if !ok {
		return domain.CatalogueRecord{}, errMalformedRecord
	}
	record.RecoveryState = domain.RecoveryState(state)
	if record.Committed, ok = reader.ref(); !ok {
		return domain.CatalogueRecord{}, errMalformedRecord
	}
	if record.Fallbacks[0], ok = reader.ref(); !ok {
		return domain.CatalogueRecord{}, errMalformedRecord
	}
	if record.Fallbacks[1], ok = reader.ref(); !ok {
		return domain.CatalogueRecord{}, errMalformedRecord
	}
	if record.DegradedReason, ok = reader.str(); !ok || reader.remaining() != 0 {
		return domain.CatalogueRecord{}, errMalformedRecord
	}
	if err := record.Validate(); err != nil {
		return domain.CatalogueRecord{}, errors.Join(errMalformedRecord, fmt.Errorf("persist: invalid catalogue record: %w", err))
	}
	return record, nil
}

func appendString(buf []byte, value string) []byte {
	buf = binary.BigEndian.AppendUint32(buf, uint32(len(value)))
	return append(buf, value...)
}
func appendCheckedString(buf []byte, value string) ([]byte, error) {
	if uint64(len(value)) > math.MaxUint32 {
		return nil, errors.New("persist: string too large")
	}
	return appendString(buf, value), nil
}
func appendRef(buf []byte, ref *domain.CheckpointRef) []byte {
	if ref == nil {
		return append(buf, 0)
	}
	buf = append(buf, 1)
	buf = binary.BigEndian.AppendUint64(buf, ref.Generation)
	return append(buf, ref.ManifestDigest[:]...)
}

type valueReader struct {
	data []byte
	off  int
}

func (r *valueReader) remaining() int { return len(r.data) - r.off }
func (r *valueReader) take(n int) ([]byte, bool) {
	if n < 0 || n > r.remaining() {
		return nil, false
	}
	value := r.data[r.off : r.off+n]
	r.off += n
	return value, true
}
func (r *valueReader) byte() (byte, bool) {
	value, ok := r.take(1)
	if !ok {
		return 0, false
	}
	return value[0], true
}
func (r *valueReader) u16() (uint16, bool) {
	value, ok := r.take(2)
	if !ok {
		return 0, false
	}
	return binary.BigEndian.Uint16(value), true
}
func (r *valueReader) u32() (uint32, bool) {
	value, ok := r.take(4)
	if !ok {
		return 0, false
	}
	return binary.BigEndian.Uint32(value), true
}
func (r *valueReader) u64() (uint64, bool) {
	value, ok := r.take(8)
	if !ok {
		return 0, false
	}
	return binary.BigEndian.Uint64(value), true
}
func (r *valueReader) str() (string, bool) {
	size, ok := r.u32()
	if !ok || uint64(size) > uint64(r.remaining()) {
		return "", false
	}
	value, _ := r.take(int(size))
	return string(value), true
}
func (r *valueReader) ref() (*domain.CheckpointRef, bool) {
	present, ok := r.byte()
	if !ok {
		return nil, false
	}
	if present == 0 {
		return nil, true
	}
	if present != 1 {
		return nil, false
	}
	generation, ok := r.u64()
	if !ok {
		return nil, false
	}
	digest, ok := r.take(32)
	if !ok {
		return nil, false
	}
	ref := &domain.CheckpointRef{Generation: generation}
	copy(ref.ManifestDigest[:], digest)
	return ref, true
}
