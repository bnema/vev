package client_test

import "github.com/bnema/vev/internal/ports"

func mustMarshalOutput(m ports.Output) []byte {
	payload, err := ports.MarshalOutput(m)
	if err != nil {
		panic(err)
	}
	return payload
}
