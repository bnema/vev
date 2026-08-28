package ipc

import (
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol"
)

func mustMarshalResize(m protocol.Resize) []byte {
	payload, err := ports.MarshalResize(m)
	if err != nil {
		panic(err)
	}
	return payload
}
