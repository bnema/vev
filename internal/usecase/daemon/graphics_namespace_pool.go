package daemon

import (
	"crypto/sha256"
	"encoding/binary"
)

type graphicsNamespaceQuarantine struct {
	base  uint64
	fence uint64
}

// graphicsNamespacePool reserves deterministic, attachment/session-scoped Kitty
// ID blocks. Once a block may have reached an outer terminal it remains in
// this bounded table for the daemon lifetime: side-effect Output frames have
// no terminal ACK. Pool exhaustion disables graphics for new attachments and
// leaves their ordinary text output intact.
//
// The pool is guarded by Daemon.mu and has no lock of its own: every method
// requires the caller to hold it. The zero value is usable; the maps are
// created lazily and a zero salt simply skips salting.
type graphicsNamespacePool struct {
	reserved    map[uint64]struct{}
	fences      map[uint64]uint64
	quarantines map[uint64]*graphicsNamespaceQuarantine
	// salt separates daemon lifetimes before attachment keys are hashed. Kitty
	// IDs are terminal-global, so a restarted or neighboring daemon must not
	// deterministically reopen the previous daemon's block.
	salt uint64
}

func newGraphicsNamespacePool() graphicsNamespacePool {
	p := graphicsNamespacePool{salt: newGraphicsNamespaceSalt()}
	p.init()
	return p
}

func (p *graphicsNamespacePool) init() {
	if p.reserved == nil {
		p.reserved = make(map[uint64]struct{})
	}
	if p.fences == nil {
		p.fences = make(map[uint64]uint64)
	}
	if p.quarantines == nil {
		p.quarantines = make(map[uint64]*graphicsNamespaceQuarantine)
	}
}

func (p *graphicsNamespacePool) reserveLease(key string) (uint64, uint64) {
	p.init()
	hashInput := []byte(key)
	if p.salt != 0 {
		salt := make([]byte, 8, 8+len(key))
		binary.BigEndian.PutUint64(salt, p.salt)
		hashInput = append(salt, hashInput...)
	}
	digest := sha256.Sum256(hashInput)
	preferred := binary.BigEndian.Uint64(digest[:8]) % graphicsIDNamespaceCount
	for offset := uint64(0); offset < graphicsIDNamespaceCount; offset++ {
		block := (preferred + offset) % graphicsIDNamespaceCount
		if _, exists := p.reserved[block]; exists {
			continue
		}
		fence := nextGraphicsNamespaceFence()
		p.reserved[block] = struct{}{}
		p.fences[block] = fence
		return block*graphicsIDNamespaceSize + 1, fence
	}
	return 0, 0
}

func (p *graphicsNamespacePool) releaseLease(state *graphicsOutputState) {
	if state == nil || state.namespaceBase == 0 || state.namespaceBase%graphicsIDNamespaceSize != 1 {
		return
	}
	block := (state.namespaceBase - 1) / graphicsIDNamespaceSize
	if _, quarantined := p.quarantines[block]; quarantined {
		return
	}
	if current := p.fences[block]; current != 0 && current != state.namespaceFence {
		return
	}
	delete(p.reserved, block)
	delete(p.fences, block)
}

func (p *graphicsNamespacePool) quarantine(state *graphicsOutputState) *graphicsNamespaceQuarantine {
	if state == nil || state.namespaceBase == 0 || state.namespaceBase%graphicsIDNamespaceSize != 1 {
		return nil
	}
	block := (state.namespaceBase - 1) / graphicsIDNamespaceSize
	p.init()
	if _, exists := p.quarantines[block]; exists {
		return nil
	}
	p.reserved[block] = struct{}{}
	p.fences[block] = state.namespaceFence
	q := &graphicsNamespaceQuarantine{base: state.namespaceBase, fence: state.namespaceFence}
	p.quarantines[block] = q
	return q
}
