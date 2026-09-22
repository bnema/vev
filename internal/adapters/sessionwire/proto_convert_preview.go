package sessionwire

// RemotePreviewWatch/RemotePreview converters delegate their field-by-field
// logic to internal/adapters/protoconv, the canonical implementation shared
// with brokerwire. sessionwire's policy is to collapse every failure -
// whether a narrowing range failure or a semantic validation failure - into
// one fixed sentinel per direction (protocol.ErrInvalidRemotePreviewWatch
// or protocol.ErrInvalidRemotePreview), except that remotePreviewToWire
// still distinguishes protoconv's own ErrOutOfRange (encoded as
// errProtoConvertRange) from a protocol validation failure (passed through
// unchanged), matching this file's pre-extraction behavior exactly.

import (
	"errors"
	"time"

	"github.com/bnema/vev/internal/adapters/protoconv"
	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/protocol/wire"
)

// remotePreviewWatchToWire carries the watch interval as whole milliseconds.
func remotePreviewWatchToWire(message protocol.RemotePreviewWatch) (*wire.RemotePreviewWatch, error) {
	if protocol.ValidateRemotePreviewWatch(message) != nil {
		return nil, protocol.ErrInvalidRemotePreviewWatch
	}
	request, err := protoconv.RemotePreviewRequestToWire(message.Request)
	if err != nil {
		return nil, protocol.ErrInvalidRemotePreviewWatch
	}
	return &wire.RemotePreviewWatch{Request: request, MinIntervalMs: uint32(message.MinInterval / time.Millisecond)}, nil
}

func remotePreviewWatchFromWire(message *wire.RemotePreviewWatch) (protocol.RemotePreviewWatch, error) {
	if message == nil || message.GetRequest() == nil {
		return protocol.RemotePreviewWatch{}, protocol.ErrInvalidRemotePreviewWatch
	}
	request, err := protoconv.RemotePreviewRequestFromWire(message.GetRequest())
	if err != nil {
		return protocol.RemotePreviewWatch{}, protocol.ErrInvalidRemotePreviewWatch
	}
	interval := message.GetMinIntervalMs()
	if interval > uint32(protocol.RemotePreviewWatchMaxInterval/time.Millisecond) {
		return protocol.RemotePreviewWatch{}, protocol.ErrInvalidRemotePreviewWatch
	}
	watch := protocol.RemotePreviewWatch{Request: request, MinInterval: time.Duration(interval) * time.Millisecond}
	if protocol.ValidateRemotePreviewWatch(watch) != nil {
		return protocol.RemotePreviewWatch{}, protocol.ErrInvalidRemotePreviewWatch
	}
	return watch, nil
}

func remotePreviewToWire(message protocol.RemotePreview) (*wire.RemotePreview, error) {
	out, err := protoconv.RemotePreviewToWire(message)
	if err != nil {
		if errors.Is(err, protoconv.ErrOutOfRange) {
			return nil, errProtoConvertRange
		}
		return nil, err
	}
	return out, nil
}

func remotePreviewFromWire(message *wire.RemotePreview) (protocol.RemotePreview, error) {
	if message == nil {
		return protocol.RemotePreview{}, protocol.ErrInvalidRemotePreview
	}
	preview, err := protoconv.RemotePreviewFromWire(message)
	if err != nil {
		return protocol.RemotePreview{}, protocol.ErrInvalidRemotePreview
	}
	return preview, nil
}
