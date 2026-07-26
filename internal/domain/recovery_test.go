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
	require.Error(t, decoded.UnmarshalText([]byte("AB0000000000000000000000000000CD")))

	_, err = (IncarnationID{}).MarshalText()
	require.Error(t, err)
	require.Error(t, decoded.UnmarshalText([]byte("00000000000000000000000000000000")))
	require.Equal(t, id, decoded, "rejected zero text must not mutate the receiver")
}

func TestCatalogueRecordValidate(t *testing.T) {
	validRef := &CheckpointRef{Generation: 7, ManifestDigest: [32]byte{1}}
	tests := []struct {
		name      string
		record    CatalogueRecord
		wantValid bool
	}{
		{name: "fresh valid", record: CatalogueRecord{Name: "work", IncarnationID: IncarnationID{1}, RecoveryState: RecoveryFresh}, wantValid: true},
		{name: "healthy with committed checkpoint valid", record: CatalogueRecord{Name: "work", IncarnationID: IncarnationID{1}, RecoveryState: RecoveryHealthy, Committed: &CheckpointRef{Generation: 7, ManifestDigest: [32]byte{1}}}, wantValid: true},
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
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.record.Validate()
			if tt.wantValid {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
		})
	}
}

func TestCatalogueRecordEqualUsesCheckpointValuesAndOrderedTabs(t *testing.T) {
	committed := CheckpointRef{Generation: 2, ManifestDigest: [32]byte{2}}
	left := CatalogueRecord{Name: "work", IncarnationID: IncarnationID{1}, TabNames: []string{"shell", "logs"}, RecoveryState: RecoveryHealthy, Committed: &committed}
	right := left
	right.TabNames = append([]string(nil), left.TabNames...)
	committedCopy := committed
	right.Committed = &committedCopy
	require.True(t, left.Equal(right))

	right.TabNames[0], right.TabNames[1] = right.TabNames[1], right.TabNames[0]
	require.False(t, left.Equal(right))
	require.True(t, (*CheckpointRef)(nil).Equal(nil))
	require.False(t, left.Committed.Equal(nil))
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
