// Preamble and envelope limits for vev's Protobuf wire contract. The
// directional envelope and preamble shapes live in schema/envelope.proto;
// generated *.pb.go files are never edited.
package wire

// ProtocolEpoch identifies the current wire epoch; see schema/envelope.proto.
// Bumped only for an intentional clean break; QUIC ALPN binds to it.
const ProtocolEpoch uint32 = 1

// PreambleMagic opens every preamble ("VEV1"); see schema/envelope.proto.
const PreambleMagic uint32 = 0x56455631

// AbsoluteEnvelopeLimit is the 16 MiB absolute ceiling for one serialized
// envelope, carried over from MaxFrameLen minus the legacy type byte.
const AbsoluteEnvelopeLimit = 16 << 20

// PreambleLimit bounds the serialized preamble envelope.
const PreambleLimit = 4 << 10

// ControlEnvelopeLimit bounds non-Output, non-image envelopes.
const ControlEnvelopeLimit = 1 << 20
