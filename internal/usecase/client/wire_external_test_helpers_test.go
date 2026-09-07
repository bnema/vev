package client_test

import (
	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/protocol/wire"
)

func mustMarshalOutput(m protocol.Output) []byte {
	// Replay-only fixtures still model valid v41 semantic publications.
	if m.New != 0 && m.Context == nil {
		m.Context = &protocol.ViewContext{
			Publication: m.Epoch<<32 | m.New,
			Route:       protocol.CommittedRouteIdentity{Target: protocol.ExactSessionTarget{LifecycleID: domain.SessionLifecycleID{1}, SessionName: "work"}},
			TabID:       "tab-1", FocusedPaneID: "pane-1",
		}
	}
	payload, err := wire.MarshalOutput(m)
	if err != nil {
		panic(err)
	}
	return payload
}
