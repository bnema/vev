package ports

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRemoteCatalogStateContract(t *testing.T) {
	require.Equal(t, uint16(2), RemoteCatalogSchemaVersion)

	for _, state := range []string{"up", "down", "broken"} {
		t.Run(state, func(t *testing.T) {
			err := ValidateRemoteCatalog(RemoteCatalog{
				ProtocolVersion: ProtocolVersion,
				Sessions:        []RemoteCatalogSession{{Name: "work", State: state, Tabs: 1}},
			})
			require.NoError(t, err)
		})
	}

	err := ValidateRemoteCatalog(RemoteCatalog{
		ProtocolVersion: ProtocolVersion,
		Sessions:        []RemoteCatalogSession{{Name: "work", State: "running", Tabs: 1}},
	})
	require.ErrorIs(t, err, ErrRemoteCatalogUnknownState)
}
