package client

import (
	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/protocol/wire"
)

func mustMarshalOutput(m protocol.Output) []byte {
	// Replay-only fixtures still model valid v41 semantic publications. Tests
	// of rejected/missing context bypass this convenience constructor.
	if m.New != 0 && m.Context == nil {
		context := testOutputView(m.Epoch<<32 | m.New)
		m.Context = &context
	}
	payload, err := wire.MarshalOutput(m)
	if err != nil {
		panic(err)
	}
	return payload
}
