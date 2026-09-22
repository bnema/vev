package brokerwire

// Preview wire converters (broker preview slice 1).
//
// StartPreview/CancelPreview travel client-to-server; PreviewPublication
// travels server-to-client with exactly one of its preview/error result
// members set. Conversion mirrors the port preview types losslessly and
// validates every narrowing numeric cast before it happens, following the
// shared converter patterns in convert.go: wire uint32 taxonomy fields
// never truncate into a smaller semantic enum, identity lengths are exact,
// and semantic validation failures map to ErrInvalidMessage.
//
// The preview viewport (RemotePreviewRequest/RemotePreview) reuses the
// terminal wire shapes owned by the session conversation; the broker
// conversation carries them as nested messages under its own envelope
// tags. Repeated authority is scope/generation plus the preview target
// identity response; the route names the observed daemon only, and the
// broker allocates the observation stream itself.
//
// The field-by-field RemotePreviewRequest/RemotePreview conversion logic
// lives in internal/adapters/protoconv, the canonical implementation shared
// with sessionwire. The wrappers below adapt protoconv's shared
// ErrOutOfRange to this package's errConvertRange, keeping range failures
// distinct from a semantic protocol validation failure (which maps to
// ErrInvalidMessage, or ErrTooLarge for an oversized preview) exactly as
// before extraction.

import (
	"errors"

	"github.com/bnema/vev/internal/adapters/protoconv"
	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/protocol/wire"
)

func remotePreviewRequestToWire(message protocol.RemotePreviewRequest) (*wire.RemotePreviewRequest, error) {
	out, err := protoconv.RemotePreviewRequestToWire(message)
	if err != nil {
		return nil, ErrInvalidMessage
	}
	return out, nil
}

func remotePreviewRequestFromWire(message *wire.RemotePreviewRequest) (protocol.RemotePreviewRequest, error) {
	if message == nil {
		return protocol.RemotePreviewRequest{}, ErrInvalidMessage
	}
	request, err := protoconv.RemotePreviewRequestFromWire(message)
	if err != nil {
		if errors.Is(err, protoconv.ErrOutOfRange) {
			return protocol.RemotePreviewRequest{}, errConvertRange
		}
		return protocol.RemotePreviewRequest{}, ErrInvalidMessage
	}
	return request, nil
}

func remotePreviewToWire(message protocol.RemotePreview) (*wire.RemotePreview, error) {
	out, err := protoconv.RemotePreviewToWire(message)
	if err != nil {
		if errors.Is(err, protocol.ErrRemotePreviewTooLarge) {
			return nil, ErrTooLarge
		}
		if errors.Is(err, protoconv.ErrOutOfRange) {
			return nil, errConvertRange
		}
		return nil, ErrInvalidMessage
	}
	return out, nil
}

func remotePreviewFromWire(message *wire.RemotePreview) (protocol.RemotePreview, error) {
	if message == nil {
		return protocol.RemotePreview{}, ErrInvalidMessage
	}
	preview, err := protoconv.RemotePreviewFromWire(message)
	if err != nil {
		if errors.Is(err, protocol.ErrRemotePreviewTooLarge) {
			return protocol.RemotePreview{}, ErrTooLarge
		}
		if errors.Is(err, protoconv.ErrOutOfRange) {
			return protocol.RemotePreview{}, errConvertRange
		}
		return protocol.RemotePreview{}, ErrInvalidMessage
	}
	return preview, nil
}

// previewRequest mirrors one StartPreview as the ports request the preview
// contract gates, so encode and decode refuse exactly what
// ports.BrokerPreviewRequest.Validate refuses.
func previewRequest(m StartPreview) ports.BrokerPreviewRequest {
	return ports.BrokerPreviewRequest{Epoch: m.Epoch, Connection: m.Connection, Generation: m.Generation, Route: m.Route, Preview: m.Preview}
}

func previewRouteToWire(route ports.BrokerPreviewRoute) *wire.PreviewRoute {
	out := &wire.PreviewRoute{Local: route.Local, Endpoint: route.Endpoint, Policy: policyToWire(route.Policy)}
	if !route.Local {
		out.Registration = registrationToWire(route.Registration)
	}
	return out
}

func previewRouteFromWire(message *wire.PreviewRoute) (ports.BrokerPreviewRoute, error) {
	if message == nil {
		return ports.BrokerPreviewRoute{}, ErrInvalidMessage
	}
	policy, err := policyFromWire(message.GetPolicy())
	if err != nil {
		return ports.BrokerPreviewRoute{}, ErrInvalidMessage
	}
	route := ports.BrokerPreviewRoute{Local: message.GetLocal(), Endpoint: message.GetEndpoint(), Policy: policy}
	if route.Local {
		if message.GetRegistration() != nil {
			return ports.BrokerPreviewRoute{}, ErrInvalidMessage
		}
	} else if route.Registration, err = registrationFromWire(message.GetRegistration()); err != nil {
		return ports.BrokerPreviewRoute{}, ErrInvalidMessage
	}
	if route.Validate() != nil {
		return ports.BrokerPreviewRoute{}, ErrInvalidMessage
	}
	return route, nil
}

func startPreviewToWire(m StartPreview) (*wire.StartPreview, error) {
	preview, err := remotePreviewRequestToWire(m.Preview)
	if err != nil {
		return nil, err
	}
	if err := previewRequest(m).Validate(); err != nil {
		return nil, ErrInvalidMessage
	}
	return &wire.StartPreview{
		Scope:      scopeToWire(m.Epoch, m.Connection),
		Generation: uint64(m.Generation),
		Route:      previewRouteToWire(m.Route),
		Preview:    preview,
	}, nil
}

func startPreviewFromWire(message *wire.StartPreview) (StartPreview, error) {
	if message == nil {
		return StartPreview{}, ErrInvalidMessage
	}
	epoch, connection, err := scopeFromWire(message.GetScope())
	if err != nil {
		return StartPreview{}, ErrInvalidMessage
	}
	route, err := previewRouteFromWire(message.GetRoute())
	if err != nil {
		return StartPreview{}, err
	}
	preview, err := remotePreviewRequestFromWire(message.GetPreview())
	if err != nil {
		return StartPreview{}, err
	}
	candidate := StartPreview{Epoch: epoch, Connection: connection, Generation: ports.BrokerPreviewGeneration(message.GetGeneration()), Route: route, Preview: preview}
	if err := previewRequest(candidate).Validate(); err != nil {
		return StartPreview{}, ErrInvalidMessage
	}
	return candidate, nil
}

func cancelPreviewToWire(m CancelPreview) (*wire.CancelPreview, error) {
	if m.Epoch == 0 {
		return nil, ErrInvalidMessage
	}
	if err := m.Connection.Validate(); err != nil {
		return nil, ErrInvalidMessage
	}
	if err := m.Generation.Validate(); err != nil {
		return nil, ErrInvalidMessage
	}
	return &wire.CancelPreview{
		Scope:      scopeToWire(m.Epoch, m.Connection),
		Generation: uint64(m.Generation),
	}, nil
}

func cancelPreviewFromWire(message *wire.CancelPreview) (CancelPreview, error) {
	var out CancelPreview
	if message == nil {
		return out, ErrInvalidMessage
	}
	epoch, connection, err := scopeFromWire(message.GetScope())
	if err != nil {
		return CancelPreview{}, ErrInvalidMessage
	}
	generation := ports.BrokerPreviewGeneration(message.GetGeneration())
	if err := generation.Validate(); err != nil {
		return CancelPreview{}, ErrInvalidMessage
	}
	return CancelPreview{Epoch: epoch, Connection: connection, Generation: generation}, nil
}

func previewPublicationToWire(m PreviewPublication) (*wire.PreviewPublication, error) {
	if m.Epoch == 0 {
		return nil, ErrInvalidMessage
	}
	if err := m.Connection.Validate(); err != nil {
		return nil, ErrInvalidMessage
	}
	if err := m.Generation.Validate(); err != nil {
		return nil, ErrInvalidMessage
	}
	out := &wire.PreviewPublication{
		Scope:      scopeToWire(m.Epoch, m.Connection),
		Generation: uint64(m.Generation),
	}
	// Exactly one of preview/error travels: a failed publication carries
	// only its typed error, a successful one only its preview.
	if m.HasError {
		if err := m.Error.validate(); err != nil {
			return nil, err
		}
		if previewCarriesViewport(m.Preview) {
			return nil, ErrInvalidMessage
		}
		out.Result = &wire.PreviewPublication_Error{Error: errorDetailToWire(m.Error)}
		return out, nil
	}
	if m.Error != (ErrorDetail{}) {
		return nil, ErrInvalidMessage
	}
	preview, err := remotePreviewToWire(m.Preview)
	if err != nil {
		return nil, err
	}
	out.Result = &wire.PreviewPublication_Preview{Preview: preview}
	return out, nil
}

// previewCarriesViewport reports whether a preview value carries any
// viewport authority. A failed publication must carry none.
func previewCarriesViewport(preview protocol.RemotePreview) bool {
	return preview.Version != 0 || preview.Status != 0 ||
		preview.LifecycleID != (domain.SessionLifecycleID{}) || preview.TabID != "" ||
		preview.Revision != 0 || preview.Width != 0 || preview.Height != 0 ||
		len(preview.Cells) != 0
}

func previewPublicationFromWire(message *wire.PreviewPublication) (PreviewPublication, error) {
	var out PreviewPublication
	if message == nil {
		return out, ErrInvalidMessage
	}
	epoch, connection, err := scopeFromWire(message.GetScope())
	if err != nil {
		return PreviewPublication{}, ErrInvalidMessage
	}
	generation := ports.BrokerPreviewGeneration(message.GetGeneration())
	if err := generation.Validate(); err != nil {
		return PreviewPublication{}, ErrInvalidMessage
	}
	result := PreviewPublication{Epoch: epoch, Connection: connection, Generation: generation}
	switch payload := message.GetResult().(type) {
	case *wire.PreviewPublication_Preview:
		if payload.Preview == nil {
			return PreviewPublication{}, ErrInvalidMessage
		}
		preview, err := remotePreviewFromWire(payload.Preview)
		if err != nil {
			return PreviewPublication{}, err
		}
		result.Preview = preview
	case *wire.PreviewPublication_Error:
		if payload.Error == nil {
			return PreviewPublication{}, ErrInvalidMessage
		}
		detail, err := errorDetailFromWire(payload.Error)
		if err != nil {
			return PreviewPublication{}, ErrInvalidMessage
		}
		result.Error = detail
		result.HasError = true
	default:
		// An absent result is malformed: exactly one member is required.
		return PreviewPublication{}, ErrInvalidMessage
	}
	return result, nil
}
