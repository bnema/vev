package ports

import "crypto/sha256"

// StoreChange is one key mutation in an atomic store batch.
type StoreChange struct {
	Key    []byte
	Value  []byte
	Delete bool
}

// Store is a small byte-key/value persistence port.
//
// Implementations may buffer writes; Sync is the durability barrier.
type Store interface {
	// Get returns a stable copy of the value for key. The caller owns the
	// returned slice and may retain or mutate it.
	Get(key []byte) ([]byte, bool)
	Set(key, val []byte) error
	Delete(key []byte) error
	// Batch applies all changes atomically to in-memory state: an error leaves
	// every prior value unchanged. Implementations repair or discard any torn
	// WAL batch during replay so no partial batch becomes visible after restart.
	// Batch does not retain StoreChange key or value slices; the caller owns and
	// may mutate them after Batch returns.
	Batch([]StoreChange) error
	// Range iterates key/value pairs; fn returning false stops iteration early.
	// Each key and value is a stable copy owned by the caller, which may retain
	// or mutate either slice after Range returns.
	Range(fn func(k, v []byte) bool)
	Sync() error
	Close() error
}

// SnapshotDigest is the SHA-256 content address of a complete snapshot object.
type SnapshotDigest [sha256.Size]byte

type SnapshotObject struct {
	Digest SnapshotDigest
	Data   []byte
}
