package protocol

import "fmt"

// DecodeCategory classifies encoded input rejected before it reaches a use case.
type DecodeCategory uint8

const (
	DecodeMalformed DecodeCategory = iota + 1
	DecodeUnknownType
	DecodeWrongDirection
)

// DecodeMessageKind identifies message families whose malformed prefixes need
// compatibility responses before a typed message can be produced.
type DecodeMessageKind uint8

const (
	DecodeMessageUnknown DecodeMessageKind = iota
	DecodeMessageHello
	DecodeMessageCommand
	DecodeMessageKill
	DecodeMessageRemotePreview
)

// DecodeFailure describes rejected encoded input without retaining payload bytes.
type DecodeFailure struct {
	Category  DecodeCategory
	Kind      DecodeMessageKind
	Type      uint8
	Version   uint16
	RequestID uint64
	Err       error
}

func (e *DecodeFailure) Error() string {
	if e == nil {
		return "protocol: decode failure"
	}
	return fmt.Sprintf("protocol: decode message type %d: %v", e.Type, e.Err)
}

func (e *DecodeFailure) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}
