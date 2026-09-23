package app

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/protocol/catalogue"
)

// Portable local-broker fixtures shared by Linux-only daemon tests and
// portable broker tests.

const (
	brokerLocalTestIdentity = "offline-local-daemon"
	brokerLocalTestEpoch    = ports.BrokerEpoch(11)
)

// testLocalCatalogue builds one exact-schema catalogue carrying the supplied
// sessions, marshalled exactly as the daemon's remote-catalog command does.
func testLocalCatalogue(t *testing.T, sessions ...catalogue.RemoteCatalogSession) string {
	t.Helper()
	encoded, err := json.Marshal(catalogue.RemoteCatalog{
		ProtocolVersion: protocol.Version,
		SchemaVersion:   catalogue.RemoteCatalogSchemaVersion,
		Sessions:        sessions,
	})
	require.NoError(t, err)
	return string(encoded) + "\n"
}
