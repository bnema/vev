package domain

import (
	"crypto/rand"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNewIncarnationID(t *testing.T) {
	id, err := NewIncarnationID(rand.Reader)
	require.NoError(t, err)
	require.NotEqual(t, IncarnationID{}, id)
	require.Len(t, id.String(), 32)
}

func TestIncarnationIDText(t *testing.T) {
	id := IncarnationID{0: 0xab, 15: 0xcd}
	text, err := id.MarshalText()
	require.NoError(t, err)
	require.Equal(t, "ab0000000000000000000000000000cd", string(text))
	var decoded IncarnationID
	require.NoError(t, decoded.UnmarshalText(text))
	require.Equal(t, id, decoded)
	require.Error(t, decoded.UnmarshalText([]byte("AB")))
}

func TestCatalogueRecordValidate(t *testing.T) {
	valid := CatalogueRecord{Name: "work", IncarnationID: IncarnationID{1}, RecoveryState: RecoveryFresh}
	require.NoError(t, valid.Validate())
	valid.RecoveryState = RecoveryHealthy
	valid.Committed = &CheckpointRef{Generation: 7, ManifestDigest: [32]byte{1}}
	require.NoError(t, valid.Validate())
	valid.Fallbacks[0] = &CheckpointRef{Generation: 7, ManifestDigest: [32]byte{2}}
	require.Error(t, valid.Validate(), "duplicate generations are malformed")

	validRef := &CheckpointRef{Generation: 7, ManifestDigest: [32]byte{1}}
	tests := []struct {
		name   string
		record CatalogueRecord
	}{
		{name: "invalid session name", record: CatalogueRecord{Name: "bad/name", IncarnationID: IncarnationID{1}, RecoveryState: RecoveryFresh}},
		{name: "zero incarnation", record: CatalogueRecord{Name: "work", RecoveryState: RecoveryFresh}},
		{name: "unknown recovery state", record: CatalogueRecord{Name: "work", IncarnationID: IncarnationID{1}, RecoveryState: RecoveryState(99)}},
		{name: "fresh with committed checkpoint", record: CatalogueRecord{Name: "work", IncarnationID: IncarnationID{1}, RecoveryState: RecoveryFresh, Committed: validRef}},
		{name: "healthy without committed checkpoint", record: CatalogueRecord{Name: "work", IncarnationID: IncarnationID{1}, RecoveryState: RecoveryHealthy}},
		{name: "degraded without committed checkpoint", record: CatalogueRecord{Name: "work", IncarnationID: IncarnationID{1}, RecoveryState: RecoveryDegraded, DegradedReason: "missing manifest"}},
		{name: "degraded without reason", record: CatalogueRecord{Name: "work", IncarnationID: IncarnationID{1}, RecoveryState: RecoveryDegraded, Committed: validRef}},
		{name: "healthy with degraded reason", record: CatalogueRecord{Name: "work", IncarnationID: IncarnationID{1}, RecoveryState: RecoveryHealthy, Committed: validRef, DegradedReason: "missing manifest"}},
		{name: "zero checkpoint generation", record: CatalogueRecord{Name: "work", IncarnationID: IncarnationID{1}, RecoveryState: RecoveryHealthy, Committed: &CheckpointRef{ManifestDigest: [32]byte{1}}}},
		{name: "zero checkpoint digest", record: CatalogueRecord{Name: "work", IncarnationID: IncarnationID{1}, RecoveryState: RecoveryHealthy, Committed: &CheckpointRef{Generation: 7}}},
		{name: "second fallback without first", record: CatalogueRecord{Name: "work", IncarnationID: IncarnationID{1}, RecoveryState: RecoveryHealthy, Committed: validRef, Fallbacks: [2]*CheckpointRef{nil, {Generation: 6, ManifestDigest: [32]byte{2}}}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Error(t, tt.record.Validate())
		})
	}
}

func TestDeletionTombstoneValidate(t *testing.T) {
	tests := []struct {
		name      string
		tombstone DeletionTombstone
		wantErr   bool
	}{
		{name: "valid", tombstone: DeletionTombstone{Name: "work", IncarnationID: IncarnationID{1}}},
		{name: "invalid name", tombstone: DeletionTombstone{Name: "bad/name", IncarnationID: IncarnationID{1}}, wantErr: true},
		{name: "zero incarnation", tombstone: DeletionTombstone{Name: "work"}, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.tombstone.Validate()
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
		})
	}
}
