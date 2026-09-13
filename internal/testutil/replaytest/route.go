package replaytest

import (
	"errors"

	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/protocol/wire"
)

// committedRouteFromWire decodes the replay route without importing a
// concrete adapter: the transcript stays value-only with generated types.
func committedRouteFromWire(message *wire.CommittedRouteIdentity) (protocol.CommittedRouteIdentity, error) {
	var identity protocol.CommittedRouteIdentity
	if message == nil || message.GetTarget() == nil {
		return identity, errors.New("replaytest: missing committed route")
	}
	lifecycleValue := message.GetTarget().GetLifecycleId().GetValue()
	if len(lifecycleValue) != len(identity.Target.LifecycleID) {
		return identity, errors.New("replaytest: bad lifecycle length")
	}
	copy(identity.Target.LifecycleID[:], lifecycleValue)
	identity.Target.SessionName = message.GetTarget().GetSessionName()
	identity.Ephemeral = message.GetEphemeral()
	if err := identity.Validate(); err != nil {
		return protocol.CommittedRouteIdentity{}, err
	}
	return identity, nil
}
