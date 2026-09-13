package ipc

import (
	"github.com/bnema/vev/internal/adapters/sessionwire"
	"github.com/bnema/vev/internal/protocol"
)

func mustEncodeClient(m protocol.ClientMessage) []byte {
	payload, err := sessionwire.EncodeClientMessage(m)
	if err != nil {
		panic(err)
	}
	return payload
}

func mustEncodeServer(m protocol.ServerMessage) []byte {
	payload, err := sessionwire.EncodeServerMessage(m)
	if err != nil {
		panic(err)
	}
	return payload
}
