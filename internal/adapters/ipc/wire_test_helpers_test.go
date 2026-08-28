package ipc

import (
	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/protocol/wire"
)

func mustMarshalResize(m protocol.Resize) []byte {
	payload, err := wire.MarshalResize(m)
	if err != nil {
		panic(err)
	}
	return payload
}
