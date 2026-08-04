package dgram

import "github.com/bnema/vev/internal/ports"

func mustMarshalOutput(m ports.Output) []byte {
	payload, err := ports.MarshalOutput(m)
	if err != nil {
		panic(err)
	}
	return payload
}

func mustMarshalAck(m ports.Ack) []byte {
	payload, err := ports.MarshalAck(m)
	if err != nil {
		panic(err)
	}
	return payload
}
