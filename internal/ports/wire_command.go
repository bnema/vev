package ports

import (
	"encoding/binary"
	"errors"
	"math"
)

// ErrTooManyCommandArgs reports that a request exceeds the wire count limit.
var ErrTooManyCommandArgs = errors.New("ports: too many command arguments")

// CommandRequest asks the daemon to run one control command. Version must
// stay first so a future payload layout can still be rejected cleanly.
type CommandRequest struct {
	Version       uint16
	Self          bool
	Slug          string
	Args          []string
	TargetSession string
	TargetTab     string
	TargetPane    string
	JSON          bool
}

// CommandResult reports a control command's outcome.
type CommandResult struct {
	OK     bool
	Code   uint16
	Text   string
	Output string
}

// PeekCommandVersion returns the leading protocol version from a
// CommandRequest payload.
func PeekCommandVersion(b []byte) (uint16, bool) {
	if len(b) < 2 {
		return 0, false
	}
	return binary.BigEndian.Uint16(b), true
}

// MarshalCommandRequest encodes m into a CommandRequest payload.
func MarshalCommandRequest(m CommandRequest) ([]byte, error) {
	if len(m.Args) > math.MaxUint16 {
		return nil, ErrTooManyCommandArgs
	}

	w := payloadWriter{}
	w.putUint16(m.Version)
	if m.Self {
		w.putUint8(1)
	} else {
		w.putUint8(0)
	}
	w.putString(m.Slug)
	w.putUint16(uint16(len(m.Args)))
	for _, arg := range m.Args {
		w.putLongString(arg)
	}
	w.putString(m.TargetSession)
	w.putString(m.TargetTab)
	w.putString(m.TargetPane)
	if m.JSON {
		w.putUint8(1)
	} else {
		w.putUint8(0)
	}
	return w.b, nil
}

// UnmarshalCommandRequest decodes a strict CommandRequest payload.
func UnmarshalCommandRequest(b []byte) (CommandRequest, error) {
	r := payloadReader{b: b}
	var m CommandRequest
	var err error

	if m.Version, err = r.getUint16(); err != nil {
		return CommandRequest{}, err
	}
	selfFlag, err := r.getUint8()
	if err != nil {
		return CommandRequest{}, err
	}
	m.Self = selfFlag != 0
	if m.Slug, err = r.getString(); err != nil {
		return CommandRequest{}, err
	}
	argCount, err := r.getUint16()
	if err != nil {
		return CommandRequest{}, err
	}
	// Every argument requires at least a uint32 length prefix. Reject an
	// impossible count before allocating from untrusted input.
	if uint64(argCount) > uint64(len(r.b)/4) {
		return CommandRequest{}, errShortPayload
	}
	if argCount != 0 {
		m.Args = make([]string, 0, int(argCount))
		for range int(argCount) {
			arg, err := r.getLongString()
			if err != nil {
				return CommandRequest{}, err
			}
			m.Args = append(m.Args, arg)
		}
	}
	if m.TargetSession, err = r.getString(); err != nil {
		return CommandRequest{}, err
	}
	if m.TargetTab, err = r.getString(); err != nil {
		return CommandRequest{}, err
	}
	if m.TargetPane, err = r.getString(); err != nil {
		return CommandRequest{}, err
	}
	jsonFlag, err := r.getUint8()
	if err != nil {
		return CommandRequest{}, err
	}
	m.JSON = jsonFlag != 0
	if err := r.done(); err != nil {
		return CommandRequest{}, err
	}
	return m, nil
}

// MarshalCommandResult encodes m into a CommandResult payload.
func MarshalCommandResult(m CommandResult) []byte {
	w := payloadWriter{}
	if m.OK {
		w.putUint8(1)
	} else {
		w.putUint8(0)
	}
	w.putUint16(m.Code)
	w.putString(m.Text)
	w.putLongString(m.Output)
	return w.b
}

// UnmarshalCommandResult decodes a strict CommandResult payload.
func UnmarshalCommandResult(b []byte) (CommandResult, error) {
	r := payloadReader{b: b}
	var m CommandResult
	var err error

	okFlag, err := r.getUint8()
	if err != nil {
		return CommandResult{}, err
	}
	m.OK = okFlag != 0
	if m.Code, err = r.getUint16(); err != nil {
		return CommandResult{}, err
	}
	if m.Text, err = r.getString(); err != nil {
		return CommandResult{}, err
	}
	if m.Output, err = r.getLongString(); err != nil {
		return CommandResult{}, err
	}
	if err := r.done(); err != nil {
		return CommandResult{}, err
	}
	return m, nil
}
