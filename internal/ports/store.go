package ports

// Store is a small byte-key/value persistence port.
//
// Implementations may buffer writes; Sync is the durability barrier.
type Store interface {
	Get(key []byte) ([]byte, bool)
	Set(key, val []byte) error
	Delete(key []byte) error
	// Range iterates key/value pairs; fn returning false stops iteration early.
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
