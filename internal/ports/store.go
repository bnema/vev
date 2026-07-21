package ports

// Store is a small byte-key/value persistence port.
//
// Implementations may buffer writes; Sync is the durability barrier.
type Store interface {
	// Get returns a stable copy of the value for key. The caller owns the
	// returned slice and may retain or mutate it.
	Get(key []byte) ([]byte, bool)
	Set(key, val []byte) error
	Delete(key []byte) error
	// Range iterates key/value pairs; fn returning false stops iteration early.
	// Each key and value is a stable copy owned by the caller, which may retain
	// or mutate either slice after Range returns.
	Range(fn func(k, v []byte) bool)
	Sync() error
	Close() error
}

// SnapshotBlob is a durable named session snapshot payload.
type SnapshotBlob struct {
	Name string
	Data []byte
}

// SnapshotStore persists encoded session snapshots by name.
type SnapshotStore interface {
	Write(name string, data []byte) error
	Load() ([]SnapshotBlob, error)
	Delete(name string) error
}

// SnapshotDigest is the SHA-256 content address of a complete snapshot object
// or manifest envelope. The fixed-width value cannot be mutated by a caller.
type SnapshotDigest [32]byte

// SnapshotObject is one complete, typed VEVO envelope and its content address.
// Data is caller-owned: repository implementations must not retain it after
// Publish returns, and Load returns a fresh copy that callers may mutate.
type SnapshotObject struct {
	Digest SnapshotDigest
	Data   []byte
}

// SnapshotPublication is the atomic data submitted for a named generation.
// Head and every object Data slice are caller-owned.
type SnapshotPublication struct {
	Name    string
	Head    []byte
	Objects []SnapshotObject
}

// SnapshotGeneration is a published head together with its manifest and all
// reachable objects. Every byte slice is caller-owned by the recipient.
type SnapshotGeneration struct {
	Publication SnapshotPublication
	Manifest    []byte
}

// SnapshotRepository persists content-addressed incremental snapshot
// generations. Load returns caller-owned copies, so callers may retain or
// mutate all returned bytes without affecting the repository.
type SnapshotRepository interface {
	Publish(SnapshotPublication) error
	Load() ([]SnapshotGeneration, error)
	Delete(name string) error
}

// LegacySnapshot is the pre-incremental named-session blob retained only for
// one-way migration. Data is caller-owned.
type LegacySnapshot struct {
	Name string
	Data []byte
}

// LegacySnapshotSource exposes v3 snapshot blobs without coupling the new
// repository to the legacy SnapshotStore lifecycle.
type LegacySnapshotSource interface {
	LoadLegacySnapshots() ([]LegacySnapshot, error)
}
