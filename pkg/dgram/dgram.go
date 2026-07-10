// Package dgram provides stdlib-only helpers for authenticated datagram
// framing, replay protection, and MTU-sized fragmentation.
package dgram

import (
	"crypto/aes"
	"crypto/cipher"
	"encoding/binary"
	"errors"
	"fmt"
)

const (
	KeySize     = 32
	NonceSize   = 12
	HeaderSize  = 4 + 8
	DefaultMTU  = 1200
	fragmentHdr = 8 + 2 + 2 + 2 + 2 // seq, idx, count, total, reserved
)

var (
	ErrReplay    = errors.New("dgram: replayed packet")
	ErrDirection = errors.New("dgram: unexpected direction")
	ErrFragment  = errors.New("dgram: invalid fragment")
)

type Codec struct {
	gcm cipher.AEAD
}

func NewCodec(key []byte) (*Codec, error) {
	if len(key) != KeySize {
		return nil, fmt.Errorf("dgram: key must be %d bytes", KeySize)
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	return &Codec{gcm: gcm}, nil
}

func (c *Codec) Overhead() int { return c.gcm.Overhead() }

func (c *Codec) Seal(direction uint32, counter uint64, plaintext, aad []byte) []byte {
	var nonce [NonceSize]byte
	binary.BigEndian.PutUint32(nonce[0:4], direction)
	binary.BigEndian.PutUint64(nonce[4:12], counter)
	out := make([]byte, HeaderSize, HeaderSize+len(plaintext)+c.gcm.Overhead())
	binary.BigEndian.PutUint32(out[0:4], direction)
	binary.BigEndian.PutUint64(out[4:12], counter)
	return c.gcm.Seal(out, nonce[:], plaintext, aad)
}

func (c *Codec) Open(packet []byte, wantDirection uint32, aad []byte, replay *ReplayWindow) (uint64, []byte, error) {
	if len(packet) < HeaderSize+c.gcm.Overhead() {
		return 0, nil, errors.New("dgram: packet too short")
	}
	dir := binary.BigEndian.Uint32(packet[0:4])
	if dir != wantDirection {
		return 0, nil, ErrDirection
	}
	ctr := binary.BigEndian.Uint64(packet[4:12])
	if replay != nil && !replay.Check(ctr) {
		return 0, nil, ErrReplay
	}
	var nonce [NonceSize]byte
	copy(nonce[:], packet[:HeaderSize])
	pt, err := c.gcm.Open(nil, nonce[:], packet[HeaderSize:], aad)
	if err != nil {
		return 0, nil, err
	}
	if replay != nil {
		replay.Accept(ctr)
	}
	return ctr, pt, nil
}

const replayWindowSize = maxFragmentCount

type ReplayWindow struct {
	max  uint64
	seen map[uint64]struct{}
	init bool
}

func NewReplayWindow() *ReplayWindow { return &ReplayWindow{} }

func (w *ReplayWindow) Check(counter uint64) bool {
	if !w.init {
		return true
	}
	if counter > w.max {
		return true
	}
	if w.max-counter >= uint64(replayWindowSize) {
		return false
	}
	_, ok := w.seen[counter]
	return !ok
}

func (w *ReplayWindow) Accept(counter uint64) {
	if w.seen == nil {
		w.seen = make(map[uint64]struct{})
	}
	if !w.init {
		w.max, w.init = counter, true
	}
	if counter > w.max {
		w.max = counter
	}
	w.seen[counter] = struct{}{}
	for seen := range w.seen {
		if w.max-seen >= uint64(replayWindowSize) {
			delete(w.seen, seen)
		}
	}
}

type Fragment struct {
	Seq   uint64
	Index uint16
	Count uint16
	Data  []byte
}

func FragmentPayload(seq uint64, payload []byte, mtu int) ([]Fragment, error) {
	if mtu <= fragmentHdr {
		return nil, ErrFragment
	}
	maxData := mtu - fragmentHdr
	count := (len(payload) + maxData - 1) / maxData
	if count == 0 {
		count = 1
	}
	if count > maxFragmentCount {
		return nil, ErrFragment
	}
	out := make([]Fragment, count)
	for i := 0; i < count; i++ {
		start := i * maxData
		end := min(len(payload), start+maxData)
		out[i] = Fragment{Seq: seq, Index: uint16(i), Count: uint16(count), Data: append([]byte(nil), payload[start:end]...)}
	}
	return out, nil
}

func MarshalFragment(f Fragment) ([]byte, error) {
	if f.Count == 0 || f.Index >= f.Count {
		return nil, ErrFragment
	}
	b := make([]byte, fragmentHdr+len(f.Data))
	binary.BigEndian.PutUint64(b[0:8], f.Seq)
	binary.BigEndian.PutUint16(b[8:10], f.Index)
	binary.BigEndian.PutUint16(b[10:12], f.Count)
	binary.BigEndian.PutUint16(b[12:14], uint16(len(f.Data)))
	copy(b[fragmentHdr:], f.Data)
	return b, nil
}

func UnmarshalFragment(b []byte) (Fragment, error) {
	if len(b) < fragmentHdr {
		return Fragment{}, ErrFragment
	}
	f := Fragment{Seq: binary.BigEndian.Uint64(b[0:8]), Index: binary.BigEndian.Uint16(b[8:10]), Count: binary.BigEndian.Uint16(b[10:12])}
	n := int(binary.BigEndian.Uint16(b[12:14]))
	if f.Count == 0 || f.Index >= f.Count || len(b[fragmentHdr:]) != n {
		return Fragment{}, ErrFragment
	}
	f.Data = append([]byte(nil), b[fragmentHdr:]...)
	return f, nil
}

const (
	maxFragmentCount      = 1024
	maxReassemblyInflight = 1024
)

type Reassembler struct {
	inflight map[uint64]*assembly
	order    []uint64
}
type assembly struct {
	count uint16
	got   int
	parts [][]byte
}

func NewReassembler() *Reassembler { return &Reassembler{inflight: make(map[uint64]*assembly)} }

// Inflight returns the number of incomplete records currently being reassembled.
func (r *Reassembler) Inflight() int { return len(r.inflight) }

func (r *Reassembler) Add(f Fragment) ([]byte, bool, error) {
	if f.Count == 0 || int(f.Count) > maxFragmentCount || f.Index >= f.Count {
		return nil, false, ErrFragment
	}
	a := r.inflight[f.Seq]
	if a == nil {
		r.evictOldest()
		a = &assembly{count: f.Count, parts: make([][]byte, f.Count)}
		r.inflight[f.Seq] = a
		r.order = append(r.order, f.Seq)
	}
	if a.count != f.Count {
		r.remove(f.Seq)
		return nil, false, ErrFragment
	}
	if a.parts[f.Index] != nil {
		return nil, false, nil
	}
	a.parts[f.Index] = append([]byte(nil), f.Data...)
	a.got++
	if a.got != int(a.count) {
		return nil, false, nil
	}
	var total int
	for _, p := range a.parts {
		total += len(p)
	}
	out := make([]byte, 0, total)
	for _, p := range a.parts {
		out = append(out, p...)
	}
	r.remove(f.Seq)
	return out, true, nil
}

func (r *Reassembler) evictOldest() {
	for len(r.order) >= maxReassemblyInflight {
		seq := r.order[0]
		r.order = r.order[1:]
		if _, ok := r.inflight[seq]; ok {
			delete(r.inflight, seq)
			return
		}
	}
}

func (r *Reassembler) remove(seq uint64) {
	delete(r.inflight, seq)
	for i, s := range r.order {
		if s == seq {
			copy(r.order[i:], r.order[i+1:])
			r.order = r.order[:len(r.order)-1]
			return
		}
	}
}
