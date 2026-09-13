package sessionwire

// Directional ceiling enforcement (P2.2/P3.3, P5.1 limits table).
//
// The preamble negotiates one immutable pair of ceilings per connection:
// maxReceiveEnvelopeBytes bounds every envelope this side accepts, and
// outputDataLimit bounds the uncompressed terminal bytes of one Output
// message. Envelope ceilings are category-aware:
//
//   - ordinary control envelopes never exceed wire.ControlEnvelopeLimit;
//   - payload-bearing Input, Output, ImagePush, PickerSnapshot, and
//     NavigationInventoryResponse variants may use the full negotiated
//     envelope ceiling, which
//     the preamble already bounds by wire.AbsoluteEnvelopeLimit.
//
// A negotiated ceiling below the applicable category ceiling always wins.
// Category detection is centralized here and derives from the serialized
// payload, so every typed send (normal, async, owned-synchronous) and every
// typed receive shares one classification; unknown or malformed payloads
// fall back to control, the strictest category. Carriage framing limits
// stay owned by the concrete transports.

import (
	"errors"

	"google.golang.org/protobuf/encoding/protowire"

	"github.com/bnema/vev/internal/protocol/wire"
)

// errOutputDataExceedsLimit reports one Output whose uncompressed terminal
// data is above the negotiated output-data ceiling.
var errOutputDataExceedsLimit = errors.New("sessionwire: output exceeds negotiated data limit")

// envelopeCategory selects the serialized-size ceiling for one directional
// envelope.
type envelopeCategory uint8

const (
	// categoryControl covers every envelope that is neither Output nor
	// ImagePush; it never exceeds wire.ControlEnvelopeLimit.
	categoryControl envelopeCategory = iota
	// categoryOutput covers server Output; its serialized envelope rides
	// the negotiated envelope ceiling and its uncompressed data is bounded
	// separately by outputDataLimit.
	categoryOutput
	// categoryBulk covers bounded application payloads whose semantic limits
	// exceed the ordinary 1 MiB control ceiling.
	categoryBulk
)

// Envelope variant field numbers that select the ceiling-exempt categories;
// see schema/envelope.proto. Every other variant, known or not, is control.
const (
	clientInputField               = 2
	clientImagePushField           = 10
	serverOutputField              = 3
	serverNavigationInventoryField = 19
	serverPickerSnapshotField      = 23
)

// envelopeLimitFor returns the serialized-size ceiling for one categorized
// envelope: control is capped at wire.ControlEnvelopeLimit while bulk
// payloads may use the full negotiated ceiling. A negotiated
// ceiling below the category ceiling always wins.
func envelopeLimitFor(category envelopeCategory, negotiated uint64) uint64 {
	if category == categoryControl && negotiated > wire.ControlEnvelopeLimit {
		return wire.ControlEnvelopeLimit
	}
	return negotiated
}

// checkEnvelopeCeiling rejects one serialized envelope above ceiling bytes.
func checkEnvelopeCeiling(payload []byte, ceiling uint64) error {
	if uint64(len(payload)) > ceiling {
		return wire.ErrScanLength
	}
	return nil
}

// checkCategoryCeiling classifies one serialized directional payload and
// applies its category ceiling. Sends and receives share this single
// enforcement point.
func checkCategoryCeiling(payload []byte, category envelopeCategory, negotiated uint64) error {
	return checkEnvelopeCeiling(payload, envelopeLimitFor(category, negotiated))
}

// clientEnvelopeCategory classifies one serialized client envelope before
// decode. Input and ImagePush are exempt from wire.ControlEnvelopeLimit; every
// other variant, including unknown and malformed payloads, stays control so
// the strictest ceiling applies.
func clientEnvelopeCategory(payload []byte) envelopeCategory {
	switch leadingField(payload) {
	case clientInputField, clientImagePushField:
		return categoryBulk
	}
	return categoryControl
}

// serverEnvelopeCategory classifies one serialized server envelope before
// decode. Output and the semantically 4 MiB-bounded inventory/picker snapshots
// are exempt from wire.ControlEnvelopeLimit; every other variant, including
// unknown and malformed payloads, stays control.
func serverEnvelopeCategory(payload []byte) envelopeCategory {
	switch leadingField(payload) {
	case serverOutputField:
		return categoryOutput
	case serverNavigationInventoryField, serverPickerSnapshotField:
		return categoryBulk
	}
	return categoryControl
}

// leadingField returns the field number of payload's leading tag when it is
// length-delimited, the wire type every envelope variant uses. It returns 0
// for empty, truncated, or non-length-delimited payloads, which then
// classify as control.
func leadingField(payload []byte) protowire.Number {
	number, wireType, consumed := protowire.ConsumeTag(payload)
	if consumed <= 0 || wireType != protowire.BytesType {
		return 0
	}
	return number
}

// checkOutputDataCeiling rejects one serialized Output envelope whose
// declared uncompressed length is above the negotiated output-data ceiling.
// It runs before decompression so hostile lengths never allocate.
func checkOutputDataCeiling(envelope *wire.ServerEnvelope, limit uint64) error {
	if envelope == nil {
		return nil
	}
	output, ok := envelope.Payload.(*wire.ServerEnvelope_Output)
	if !ok || output.Output == nil {
		return nil
	}
	if output.Output.GetUncompressedLength() > limit {
		return errOutputDataExceedsLimit
	}
	return nil
}

// sendEnvelopeWithin marshals one server envelope and enforces the
// negotiated output-data and category envelope ceilings before any
// transport call. All server send modes (normal, async, owned synchronous)
// share it.
func sendEnvelopeWithin(envelope *wire.ServerEnvelope, ceilings protoCeilings) (wire.Envelope, error) {
	if err := checkOutputDataCeiling(envelope, ceilings.outputDataLimit); err != nil {
		return wire.Envelope{}, err
	}
	raw, err := marshalServerEnvelope(envelope)
	if err != nil {
		return wire.Envelope{}, err
	}
	if err := checkCategoryCeiling(raw.Payload, serverEnvelopeCategory(raw.Payload), ceilings.maxReceiveEnvelopeBytes); err != nil {
		return wire.Envelope{}, err
	}
	return raw, nil
}
