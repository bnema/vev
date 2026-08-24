package ports

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRemoteCatalogStateContract(t *testing.T) {
	require.Equal(t, uint16(3), RemoteCatalogSchemaVersion)

	for _, state := range []RemoteCatalogSessionState{RemoteCatalogSessionUp, RemoteCatalogSessionDown, RemoteCatalogSessionBroken} {
		t.Run(string(state), func(t *testing.T) {
			err := ValidateRemoteCatalog(RemoteCatalog{
				ProtocolVersion: ProtocolVersion,
				SchemaVersion:   RemoteCatalogSchemaVersion,
				Sessions: []RemoteCatalogSession{{
					LifecycleID: [16]byte{1}, Name: "work", State: state,
					Tabs: []RemoteCatalogTab{{ID: "tab-1"}},
				}},
			})
			require.NoError(t, err)
		})
	}

	err := ValidateRemoteCatalog(RemoteCatalog{
		ProtocolVersion: ProtocolVersion,
		SchemaVersion:   RemoteCatalogSchemaVersion,
		Sessions: []RemoteCatalogSession{{
			LifecycleID: [16]byte{1}, Name: "work", State: RemoteCatalogSessionState("running"),
			Tabs: []RemoteCatalogTab{{ID: "tab-1"}},
		}},
	})
	require.ErrorIs(t, err, ErrRemoteCatalogUnknownState)

	err = ValidateRemoteCatalog(RemoteCatalog{ProtocolVersion: ProtocolVersion, Sessions: []RemoteCatalogSession{}})
	var mismatch *RemoteCatalogVersionMismatchError
	require.ErrorAs(t, err, &mismatch)
	require.Equal(t, "catalog", mismatch.Kind)
	require.Equal(t, RemoteCatalogSchemaVersion, mismatch.Want)
}
