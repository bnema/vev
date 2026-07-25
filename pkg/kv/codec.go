package kv

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"math"
)

const (
	opSet byte = iota
	opDel
	opBatch

	headerLen           = 8
	payloadPrefixLen    = 3
	batchEntryPrefixLen = 7
	compactThreshold    = 64
	compactWasteRatio   = 0.5
	maxKeyLen           = 1<<16 - 1
)

var (
	errBadRecord  = errors.New("bad record")
	ErrCorruptWAL = errors.New("kv: corrupt WAL")
)

type record struct {
	op    byte
	key   []byte
	value []byte
}

// BatchChange is one key mutation contained in a single atomic WAL record.
type BatchChange struct {
	Key    []byte
	Value  []byte
	Delete bool
}

func encodeRecord(op byte, key, value []byte) ([]byte, error) {
	if len(key) > maxKeyLen {
		return nil, errors.New("key too large")
	}
	if op != opSet && op != opDel {
		return nil, errBadRecord
	}
	payloadLen := payloadPrefixLen + len(key)
	if op == opSet {
		payloadLen += len(value)
	}
	if uint64(payloadLen) > math.MaxUint32 {
		return nil, errors.New("payload too large")
	}
	payload := make([]byte, payloadLen)
	payload[0] = op
	binary.BigEndian.PutUint16(payload[1:3], uint16(len(key)))
	copy(payload[3:], key)
	if op == opSet {
		copy(payload[3+len(key):], value)
	}
	return framePayload(payload), nil
}

func encodeBatch(changes []BatchChange) ([]byte, error) {
	if len(changes) == 0 {
		return nil, errors.New("kv: empty batch")
	}
	if uint64(len(changes)) > math.MaxUint32 {
		return nil, errors.New("kv: too many batch changes")
	}
	seen := make(map[string]struct{}, len(changes))
	payloadLen := 1 + 4
	for _, change := range changes {
		if len(change.Key) > maxKeyLen {
			return nil, errors.New("key too large")
		}
		if _, exists := seen[string(change.Key)]; exists {
			return nil, errors.New("kv: duplicate batch key")
		}
		seen[string(change.Key)] = struct{}{}
		valueLen := len(change.Value)
		if change.Delete {
			if valueLen != 0 {
				return nil, errors.New("kv: delete batch change has value")
			}
		}
		payloadLen += 1 + 2 + len(change.Key) + 4 + valueLen
		if uint64(payloadLen) > math.MaxUint32 {
			return nil, errors.New("payload too large")
		}
	}
	payload := make([]byte, payloadLen)
	payload[0] = opBatch
	binary.BigEndian.PutUint32(payload[1:5], uint32(len(changes)))
	off := 5
	for _, change := range changes {
		payload[off] = opSet
		if change.Delete {
			payload[off] = opDel
		}
		off++
		binary.BigEndian.PutUint16(payload[off:off+2], uint16(len(change.Key)))
		off += 2
		copy(payload[off:], change.Key)
		off += len(change.Key)
		binary.BigEndian.PutUint32(payload[off:off+4], uint32(len(change.Value)))
		off += 4
		copy(payload[off:], change.Value)
		off += len(change.Value)
	}
	return framePayload(payload), nil
}

func framePayload(payload []byte) []byte {
	buf := make([]byte, headerLen+len(payload))
	binary.BigEndian.PutUint32(buf[:4], uint32(len(payload)))
	binary.BigEndian.PutUint32(buf[4:8], crc32.ChecksumIEEE(payload))
	copy(buf[headerLen:], payload)
	return buf
}

func decodePayload(payload []byte) (record, error) {
	if len(payload) < payloadPrefixLen {
		return record{}, errBadRecord
	}
	op := payload[0]
	if op != opSet && op != opDel {
		return record{}, errBadRecord
	}
	keyLen := int(binary.BigEndian.Uint16(payload[1:3]))
	if len(payload) < payloadPrefixLen+keyLen {
		return record{}, errBadRecord
	}
	if op == opDel && len(payload) != payloadPrefixLen+keyLen {
		return record{}, errBadRecord
	}
	key := append([]byte(nil), payload[payloadPrefixLen:payloadPrefixLen+keyLen]...)
	var value []byte
	if op == opSet {
		value = append([]byte(nil), payload[payloadPrefixLen+keyLen:]...)
	}
	return record{op: op, key: key, value: value}, nil
}

func decodeBatch(payload []byte) ([]record, error) {
	if len(payload) < 5 || payload[0] != opBatch {
		return nil, errBadRecord
	}
	count := binary.BigEndian.Uint32(payload[1:5])
	if count == 0 {
		return nil, errBadRecord
	}
	off := 5
	maxCount := uint64(len(payload)-off) / batchEntryPrefixLen
	if uint64(count) > maxCount {
		return nil, errBadRecord
	}
	records := make([]record, 0, int(count))
	seen := make(map[string]struct{}, int(count))
	for range count {
		if len(payload)-off < 7 {
			return nil, errBadRecord
		}
		op := payload[off]
		off++
		if op != opSet && op != opDel {
			return nil, errBadRecord
		}
		keyLen := int(binary.BigEndian.Uint16(payload[off : off+2]))
		off += 2
		if keyLen > len(payload)-off-4 {
			return nil, errBadRecord
		}
		key := append([]byte(nil), payload[off:off+keyLen]...)
		off += keyLen
		valueLen := uint64(binary.BigEndian.Uint32(payload[off : off+4]))
		off += 4
		if valueLen > uint64(len(payload)-off) {
			return nil, errBadRecord
		}
		if op == opDel && valueLen != 0 {
			return nil, errBadRecord
		}
		value := append([]byte(nil), payload[off:off+int(valueLen)]...)
		off += int(valueLen)
		if _, exists := seen[string(key)]; exists {
			return nil, errBadRecord
		}
		seen[string(key)] = struct{}{}
		records = append(records, record{op: op, key: key, value: value})
	}
	if off != len(payload) {
		return nil, errBadRecord
	}
	return records, nil
}
