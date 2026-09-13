package daemon

import (
	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/protocol/wire"
)

func mustMarshalOutput(m protocol.Output) wire.Envelope {
	return mustServerEnvelope(m)
}

func mustMarshalAck(m protocol.Ack) wire.Envelope {
	return mustClientEnvelope(m)
}

func mustMarshalResize(m protocol.Resize) wire.Envelope {
	return mustClientEnvelope(m)
}
