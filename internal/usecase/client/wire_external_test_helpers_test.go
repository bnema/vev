package client_test

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
