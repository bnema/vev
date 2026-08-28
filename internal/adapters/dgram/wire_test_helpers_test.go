package dgram

import (
	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/protocol/wire"
)

func mustMarshalOutput(m protocol.Output) []byte {
	payload, err := wire.MarshalOutput(m)
	if err != nil {
		panic(err)
	}
	return payload
}

func mustMarshalAck(m protocol.Ack) []byte {
	payload, err := wire.MarshalAck(m)
	if err != nil {
		panic(err)
	}
	return payload
}
