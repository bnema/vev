package client_test

import (
	"bnema.dev/vev/internal/adapters/sessionwire"
	"bnema.dev/vev/internal/protocol"
	"bnema.dev/vev/internal/protocol/wire"
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
