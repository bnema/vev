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
