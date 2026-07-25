package ports

import (
	"context"
	"crypto/sha256"
)

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

// LegacySnapshot is the pre-incremental named-session blob retained only for
// one-way migration. Data is caller-owned.
type LegacySnapshot struct {
	Name string
	Data []byte
}

// LegacySnapshotSource exposes the v3 bridge separately from the new write
// contract.
type LegacySnapshotSource interface {
	LoadLegacy(context.Context) ([]LegacySnapshot, error)
	// DeleteVerifiedLegacy removes precisely the legacy blob that was verified
	// after import. Implementations must persist its identity before unlinking.
	DeleteVerifiedLegacy(context.Context, LegacySnapshot) error
	// DeleteLegacy is reserved for explicit session purges, which intentionally
	// delete by name rather than as part of an import receipt.
	DeleteLegacy(context.Context, string) error
}
