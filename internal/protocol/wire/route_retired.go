package wire

import "github.com/bnema/vev/internal/protocol"

func MarshalRouteRetired(message protocol.RouteRetired) ([]byte, error) {
	if message.Ref.IsZero() || message.Ref.Validate() != nil || message.Target.Validate() != nil {
		return nil, protocol.ErrInvalidRouteWire
	}
	w := payloadWriter{}
	marshalRouteRef(&w, message.Ref)
	marshalExactSessionTarget(&w, message.Target)
	return w.b, nil
}

func UnmarshalRouteRetired(b []byte) (protocol.RouteRetired, error) {
	r := payloadReader{b: b}
	ref, err := unmarshalRouteRef(&r)
	if err != nil || ref.IsZero() {
		return protocol.RouteRetired{}, protocol.ErrInvalidRouteWire
	}
	target, err := unmarshalExactSessionTarget(&r)
	if err != nil {
		return protocol.RouteRetired{}, err
	}
	if err := r.done(); err != nil {
		return protocol.RouteRetired{}, err
	}
	return protocol.RouteRetired{Ref: ref, Target: target}, nil
}
