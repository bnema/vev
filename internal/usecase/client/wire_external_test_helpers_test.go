package client_test

import (
	"github.com/bnema/vev/internal/adapters/sessionwire"
	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/protocol/wire"
)

// mustClientEnvelope wraps one semantic client message in the envelope the
// wire.Transport exchanges.
func mustClientEnvelope(message protocol.ClientMessage) wire.Envelope {
	raw, err := sessionwire.EncodeClientMessage(message)
	if err != nil {
		panic(err)
	}
	return wire.Envelope{Payload: raw}
}
