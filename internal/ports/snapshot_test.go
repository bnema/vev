package ports

import "testing"

type snapshotStoreContract struct{}

func (snapshotStoreContract) Write(name string, data []byte) error { return nil }
func (snapshotStoreContract) Load() ([]SnapshotBlob, error)        { return nil, nil }
func (snapshotStoreContract) Delete(name string) error             { return nil }

func TestSnapshotStoreContract(t *testing.T) {
	var _ SnapshotStore = snapshotStoreContract{}

	blob := SnapshotBlob{Name: "named-session", Data: []byte("snapshot-bytes")}
	if blob.Name != "named-session" {
		t.Fatalf("SnapshotBlob.Name = %q, want %q", blob.Name, "named-session")
	}
	if string(blob.Data) != "snapshot-bytes" {
		t.Fatalf("SnapshotBlob.Data = %q, want %q", string(blob.Data), "snapshot-bytes")
	}
}
