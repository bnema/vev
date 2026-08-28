package daemon

import (
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol"
)

func mustMarshalOutput(m protocol.Output) []byte {
	payload, err := ports.MarshalOutput(m)
	if err != nil {
		panic(err)
	}
	return payload
}

func mustMarshalAck(m protocol.Ack) []byte {
	payload, err := ports.MarshalAck(m)
	if err != nil {
		panic(err)
	}
	return payload
}

func mustMarshalResize(m protocol.Resize) []byte {
	payload, err := ports.MarshalResize(m)
	if err != nil {
		panic(err)
	}
	return payload
}
