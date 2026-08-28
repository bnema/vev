package catalogue

import (
	"testing"
	"time"

	"github.com/bnema/vev/internal/protocol"
	"github.com/stretchr/testify/require"
)

func TestRemoteCatalogStateContract(t *testing.T) {
	require.Equal(t, uint16(3), RemoteCatalogSchemaVersion)

	for _, state := range []RemoteCatalogSessionState{RemoteCatalogSessionUp, RemoteCatalogSessionDown, RemoteCatalogSessionBroken} {
		t.Run(string(state), func(t *testing.T) {
			err := ValidateRemoteCatalog(RemoteCatalog{
				ProtocolVersion: protocol.Version,
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
		ProtocolVersion: protocol.Version,
		SchemaVersion:   RemoteCatalogSchemaVersion,
		Sessions: []RemoteCatalogSession{{
			LifecycleID: [16]byte{1}, Name: "work", State: RemoteCatalogSessionState("running"),
			Tabs: []RemoteCatalogTab{{ID: "tab-1"}},
		}},
	})
	require.ErrorIs(t, err, ErrRemoteCatalogUnknownState)

	err = ValidateRemoteCatalog(RemoteCatalog{ProtocolVersion: protocol.Version, Sessions: []RemoteCatalogSession{}})
	var mismatch *RemoteCatalogVersionMismatchError
	require.ErrorAs(t, err, &mismatch)
	require.Equal(t, "catalog", mismatch.Kind)
	require.Equal(t, RemoteCatalogSchemaVersion, mismatch.Want)
}

func TestValidateRemoteCatalogRejectsTerminalUnsafeText(t *testing.T) {
	for _, tt := range []struct{ name, value string }{
		{name: "bidi override", value: "\u202eoverride"},
		{name: "line separator", value: "\u2028line separator"},
		{name: "paragraph separator", value: "\u2029paragraph separator"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateRemoteCatalog(RemoteCatalog{
				ProtocolVersion: protocol.Version,
				SchemaVersion:   RemoteCatalogSchemaVersion,
				Sessions: []RemoteCatalogSession{{
					LifecycleID: [16]byte{1}, Name: "work", State: RemoteCatalogSessionUp,
					Tabs: []RemoteCatalogTab{{ID: "tab-1", Detail: tt.value}},
				}},
			})
			require.ErrorIs(t, err, ErrInvalidRemoteCatalog)
		})
	}
}

func TestValidateRemoteCatalogCacheEntriesRejectsTerminalUnsafeHost(t *testing.T) {
	err := ValidateRemoteCatalogCacheEntries([]RemoteCatalogCacheEntry{{
		Host: "host\u202eoverride", FetchedAt: time.Unix(1, 0), Sessions: []RemoteCatalogSession{},
	}})
	require.ErrorIs(t, err, ErrInvalidRemoteCatalog)
}

func TestValidateRemoteCatalogRejectsDuplicateSessionNames(t *testing.T) {
	err := ValidateRemoteCatalog(RemoteCatalog{
		ProtocolVersion: protocol.Version,
		SchemaVersion:   RemoteCatalogSchemaVersion,
		Sessions: []RemoteCatalogSession{
			{LifecycleID: [16]byte{1}, Name: "work", State: RemoteCatalogSessionUp, Tabs: []RemoteCatalogTab{}},
			{LifecycleID: [16]byte{2}, Name: "work", State: RemoteCatalogSessionDown, Tabs: []RemoteCatalogTab{}},
		},
	})
	require.ErrorIs(t, err, ErrInvalidRemoteCatalog)
	require.ErrorContains(t, err, "duplicate session name")
}
