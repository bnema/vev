package kv

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"math"
	"sort"
)

var ErrCorrupt = errors.New("kv: corrupt store file")

var fileMagic = [4]byte{'V', 'E', 'V', 'K'}

const (
	fileVersion    uint16 = 3
	filePrefixLen         = 4 + 2 + 4
	entryPrefixLen        = 4 + 4
	crcLen                = 4
)

// encodeFile writes magic | version | count | (keyLen key valLen val)* | crc32.
func encodeFile(data map[string][]byte) []byte {
	keys := make([]string, 0, len(data))
	for key := range data {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	size := filePrefixLen + crcLen
	for _, key := range keys {
		size += entryPrefixLen + len(key) + len(data[key])
	}
	buf := make([]byte, 0, size)
	buf = append(buf, fileMagic[:]...)
	buf = binary.BigEndian.AppendUint16(buf, fileVersion)
	buf = binary.BigEndian.AppendUint32(buf, uint32(len(keys)))
	for _, key := range keys {
		value := data[key]
		buf = binary.BigEndian.AppendUint32(buf, uint32(len(key)))
		buf = append(buf, key...)
		buf = binary.BigEndian.AppendUint32(buf, uint32(len(value)))
		buf = append(buf, value...)
	}
	return appendCRC(buf)
}

func appendCRC(payload []byte) []byte {
	return binary.BigEndian.AppendUint32(payload, crc32.ChecksumIEEE(payload))
}

// decodeFile validates the single trailing CRC and rejects every malformed
// existing file. Callers must not replace or truncate a file that fails here.
func decodeFile(raw []byte) (map[string][]byte, error) {
	if len(raw) < filePrefixLen+crcLen {
		return nil, ErrCorrupt
	}
	payload := raw[:len(raw)-crcLen]
	wantCRC := binary.BigEndian.Uint32(raw[len(raw)-crcLen:])
	if crc32.ChecksumIEEE(payload) != wantCRC {
		return nil, ErrCorrupt
	}
	if string(payload[:4]) != string(fileMagic[:]) || binary.BigEndian.Uint16(payload[4:6]) != fileVersion {
		return nil, ErrCorrupt
	}

	count := binary.BigEndian.Uint32(payload[6:10])
	off := filePrefixLen
	data := make(map[string][]byte)
	for range count {
		keyLen, next, ok := decodeLength(payload, off)
		if !ok {
			return nil, ErrCorrupt
		}
		off = next
		if keyLen > uint64(len(payload)-off) {
			return nil, ErrCorrupt
		}
		key := string(payload[off : off+int(keyLen)])
		off += int(keyLen)
		valueLen, next, ok := decodeLength(payload, off)
		if !ok {
			return nil, ErrCorrupt
		}
		off = next
		if valueLen > uint64(len(payload)-off) {
			return nil, ErrCorrupt
		}
		if _, duplicate := data[key]; duplicate {
			return nil, ErrCorrupt
		}
		data[key] = append([]byte(nil), payload[off:off+int(valueLen)]...)
		off += int(valueLen)
	}
	if off != len(payload) {
		return nil, ErrCorrupt
	}
	return data, nil
}

func decodeLength(payload []byte, off int) (uint64, int, bool) {
	if off < 0 || len(payload)-off < 4 {
		return 0, off, false
	}
	length := uint64(binary.BigEndian.Uint32(payload[off : off+4]))
	if length > uint64(math.MaxInt) {
		return 0, off, false
	}
	return length, off + 4, true
}
