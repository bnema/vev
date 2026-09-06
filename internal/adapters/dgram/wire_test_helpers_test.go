package dgram

import (
	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/protocol/wire"
)

func mustMarshalOutput(m protocol.Output) []byte {
	if m.New != 0 && m.Context == nil {
		m.Context = &protocol.ViewContext{
			Publication: m.New,
			Route:       protocol.CommittedRouteIdentity{Target: protocol.ExactSessionTarget{LifecycleID: domain.SessionLifecycleID{1}, SessionName: "transport-test"}},
			TabID:       "tab-1", FocusedPaneID: "pane-1",
		}
	}
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
