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

// SnapshotPublication atomically publishes a complete VEVM manifest and any
// newly reachable content-addressed VEVO objects for one named generation.
type SnapshotPublication struct {
	Name       string
	Generation uint64
	Manifest   []byte
	Objects    []SnapshotObject
}

// SnapshotGeneration is the caller-owned material needed to restore one
// named generation. Fallback indicates that the repository selected an older
// valid generation after the requested/current generation was unavailable.
type SnapshotGeneration struct {
	Name       string
	Generation uint64
	Manifest   []byte
	Objects    map[SnapshotDigest][]byte
	Fallback   bool
}

// SnapshotRepository persists content-addressed incremental snapshot
// generations. All returned bytes are owned by the caller.
type SnapshotRepository interface {
	Publish(context.Context, SnapshotPublication) error
	List(context.Context) ([]string, error)
	Load(context.Context, string) (SnapshotGeneration, error)
	Delete(context.Context, string) error
	// Tombstone fences a named-session purge before either incremental or
	// legacy data is deleted. It must remain durable until DeleteTombstone.
	Tombstone(context.Context, string) error
	DeleteTombstone(context.Context, string) error
	Maintain(context.Context) error
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
