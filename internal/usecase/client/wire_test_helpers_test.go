package client

import (
	"github.com/bnema/vev/internal/adapters/sessionwire"
	"github.com/bnema/vev/internal/protocol"
)

func mustMarshalOutput(m protocol.Output) []byte {
	// Replay-only fixtures still model valid semantic publications. Tests
	// of rejected/missing context bypass this convenience constructor.
	if m.New != 0 && m.Context == nil {
		context := testOutputView(m.Epoch<<32 | m.New)
		m.Context = &context
	}
	payload, err := sessionwire.EncodeServerMessage(m)
	if err != nil {
		panic(err)
	}
	return payload
}
