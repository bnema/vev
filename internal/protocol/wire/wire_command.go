package wire

import (
	"encoding/binary"
	"errors"
	"math"

	"github.com/bnema/vev/internal/protocol"
)

// ErrTooManyCommandArgs reports that a request exceeds the wire count limit.
var ErrTooManyCommandArgs = errors.New("ports: too many command arguments")

// PeekCommandVersion returns the leading protocol version from a
// CommandRequest payload.
func PeekCommandVersion(b []byte) (uint16, bool) {
	if len(b) < 2 {
		return 0, false
	}
	return binary.BigEndian.Uint16(b), true
}

// PeekCommandRequestID returns the request ID that follows the leading
// protocol version in a CommandRequest payload.
func PeekCommandRequestID(b []byte) (uint64, bool) {
	if len(b) < 10 {
		return 0, false
	}
	return binary.BigEndian.Uint64(b[2:10]), true
}

// MarshalCommandRequest encodes m into a CommandRequest payload.
func MarshalCommandRequest(m protocol.CommandRequest) ([]byte, error) {
	if len(m.Args) > math.MaxUint16 {
		return nil, ErrTooManyCommandArgs
	}

	w := payloadWriter{}
	w.putUint16(m.Version)
	w.putUint64(m.RequestID)
	w.putBool(m.Attached)
	w.putBool(m.Self)
	w.putString(m.Slug)
	w.putUint16(uint16(len(m.Args)))
	for _, arg := range m.Args {
		w.putLongString(arg)
	}
	w.putString(m.TargetSession)
	w.putString(m.TargetTab)
	w.putString(m.TargetPane)
	w.putBool(m.JSON)
	return w.b, nil
}

// UnmarshalCommandRequest decodes a strict CommandRequest payload.
func UnmarshalCommandRequest(b []byte) (protocol.CommandRequest, error) {
	r := payloadReader{b: b}
	var m protocol.CommandRequest
	var err error

	if m.Version, err = r.getUint16(); err != nil {
		return protocol.CommandRequest{}, err
	}
	if m.RequestID, err = r.getUint64(); err != nil {
		return protocol.CommandRequest{}, err
	}
	if m.Attached, err = r.getBool(); err != nil {
		return protocol.CommandRequest{}, err
	}
	if m.Self, err = r.getBool(); err != nil {
		return protocol.CommandRequest{}, err
	}
	if m.Slug, err = r.getString(); err != nil {
		return protocol.CommandRequest{}, err
	}
	argCount, err := r.getUint16()
	if err != nil {
		return protocol.CommandRequest{}, err
	}
	// Every argument requires at least a uint32 length prefix. Reject an
	// impossible count before allocating from untrusted input.
	if uint64(argCount) > uint64(len(r.b)/4) {
		return protocol.CommandRequest{}, errShortPayload
	}
	if argCount != 0 {
		m.Args = make([]string, 0, int(argCount))
		for range int(argCount) {
			arg, err := r.getLongString()
			if err != nil {
				return protocol.CommandRequest{}, err
			}
			m.Args = append(m.Args, arg)
		}
	}
	if m.TargetSession, err = r.getString(); err != nil {
		return protocol.CommandRequest{}, err
	}
	if m.TargetTab, err = r.getString(); err != nil {
		return protocol.CommandRequest{}, err
	}
	if m.TargetPane, err = r.getString(); err != nil {
		return protocol.CommandRequest{}, err
	}
	if m.JSON, err = r.getBool(); err != nil {
		return protocol.CommandRequest{}, err
	}
	if err := r.done(); err != nil {
		return protocol.CommandRequest{}, err
	}
	return m, nil
}

// MarshalCommandResult encodes m into a CommandResult payload.
func MarshalCommandResult(m protocol.CommandResult) []byte {
	w := payloadWriter{}
	w.putUint64(m.RequestID)
	w.putBool(m.OK)
	w.putUint16(m.Code)
	w.putString(m.Text)
	w.putLongString(m.Output)
	return w.b
}

// UnmarshalCommandResult decodes a strict CommandResult payload.
func UnmarshalCommandResult(b []byte) (protocol.CommandResult, error) {
	r := payloadReader{b: b}
	var m protocol.CommandResult
	var err error

	if m.RequestID, err = r.getUint64(); err != nil {
		return protocol.CommandResult{}, err
	}
	if m.OK, err = r.getBool(); err != nil {
		return protocol.CommandResult{}, err
	}
	if m.Code, err = r.getUint16(); err != nil {
		return protocol.CommandResult{}, err
	}
	if m.Text, err = r.getString(); err != nil {
		return protocol.CommandResult{}, err
	}
	if m.Output, err = r.getLongString(); err != nil {
		return protocol.CommandResult{}, err
	}
	if err := r.done(); err != nil {
		return protocol.CommandResult{}, err
	}
	return m, nil
}
