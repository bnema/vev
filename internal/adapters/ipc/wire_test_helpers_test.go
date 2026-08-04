package ipc

import "github.com/bnema/vev/internal/ports"

func mustMarshalResize(m ports.Resize) []byte {
	payload, err := ports.MarshalResize(m)
	if err != nil {
		panic(err)
	}
	return payload
}
