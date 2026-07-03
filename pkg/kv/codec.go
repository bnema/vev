package kv

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"math"
)

const (
	opSet byte = 0
	opDel byte = 1

	headerLen         = 8
	payloadPrefixLen  = 3
	compactThreshold  = 64
	compactWasteRatio = 0.5
	maxKeyLen         = 1<<16 - 1
)

var errBadRecord = errors.New("bad record")

type record struct {
	op    byte
	key   []byte
	value []byte
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

	buf := make([]byte, headerLen+payloadLen)
	payload := buf[headerLen:]
	payload[0] = op
	binary.BigEndian.PutUint16(payload[1:3], uint16(len(key)))
	copy(payload[3:], key)
	if op == opSet {
		copy(payload[3+len(key):], value)
	}

	binary.BigEndian.PutUint32(buf[0:4], uint32(payloadLen))
	binary.BigEndian.PutUint32(buf[4:8], crc32.ChecksumIEEE(payload))
	return buf, nil
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
	var val []byte
	if op == opSet {
		val = append([]byte(nil), payload[payloadPrefixLen+keyLen:]...)
	}
	return record{op: op, key: key, value: val}, nil
}
