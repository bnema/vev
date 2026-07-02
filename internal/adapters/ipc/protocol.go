package ipc

import (
	"encoding/binary"
	"errors"

	"github.com/bnema/vev/internal/domain"
)

// errShortPayload is returned when a payload ends before a required field
// has been fully read.
var errShortPayload = errors.New("ipc: payload too short")

// errTrailingBytes is returned when a fixed-shape payload has bytes left
// over after every field has been consumed. Protocol strictness here
// catches version drift early instead of silently ignoring extra data.
var errTrailingBytes = errors.New("ipc: unexpected trailing bytes in payload")

// Intent values carried by Hello, describing what the client wants to do.
const (
	IntentEphemeral uint8 = 0
	IntentNew       uint8 = 1
	IntentAttach    uint8 = 2
)

// ErrorMsg codes.
const (
	ErrVersionMismatch uint16 = 1
	ErrNoSuchSession   uint16 = 2
	ErrNameTaken       uint16 = 3
	ErrInternal        uint16 = 255
)

// Detached reasons.
const (
	ReasonDetach         uint8 = 0
	ReasonSessionKilled  uint8 = 1
	ReasonServerShutdown uint8 = 2
)

// Hello is sent by the client immediately after connecting.
type Hello struct {
	Version uint16
	Intent  uint8
	Name    string
	Size    domain.Size
	TermEnv string
}

// Input carries raw bytes typed/pasted by the client, destined for the PTY.
type Input struct {
	Data []byte
}

// Resize notifies the daemon of a client-side terminal size change.
type Resize struct {
	Size domain.Size
}

// Detach asks the daemon to detach the current client without killing the
// session.
type Detach struct{}

// Ping is a keepalive/liveness probe sent by either side.
type Ping struct{}

// Pong answers a Ping.
type Pong struct{}

// Welcome is the daemon's reply to a successful Hello.
type Welcome struct {
	SessionID   string
	SessionName string
	Ephemeral   bool
}

// ErrorMsg reports a protocol- or session-level failure to the client.
type ErrorMsg struct {
	Code uint16
	Text string
}

// Output carries raw PTY output, destined for the client's terminal.
type Output struct {
	Data []byte
}

// Detached tells a client it has been disconnected from its session and why.
type Detached struct {
	Reason uint8
}

// payloadWriter builds a message payload by appending fields in wire order.
type payloadWriter struct {
	b []byte
}

func (w *payloadWriter) putUint8(v uint8) {
	w.b = append(w.b, v)
}

func (w *payloadWriter) putUint16(v uint16) {
	var tmp [2]byte
	binary.BigEndian.PutUint16(tmp[:], v)
	w.b = append(w.b, tmp[:]...)
}

func (w *payloadWriter) putString(s string) {
	w.putUint16(uint16(len(s)))
	w.b = append(w.b, s...)
}

// payloadReader consumes a message payload field by field in wire order,
// erroring instead of panicking on any short read.
type payloadReader struct {
	b []byte
}

func (r *payloadReader) getUint8() (uint8, error) {
	if len(r.b) < 1 {
		return 0, errShortPayload
	}
	v := r.b[0]
	r.b = r.b[1:]
	return v, nil
}

func (r *payloadReader) getUint16() (uint16, error) {
	if len(r.b) < 2 {
		return 0, errShortPayload
	}
	v := binary.BigEndian.Uint16(r.b)
	r.b = r.b[2:]
	return v, nil
}

func (r *payloadReader) getString() (string, error) {
	n, err := r.getUint16()
	if err != nil {
		return "", err
	}
	if len(r.b) < int(n) {
		return "", errShortPayload
	}
	s := string(r.b[:n])
	r.b = r.b[n:]
	return s, nil
}

// rest consumes and returns all remaining bytes, copied so the result is
// independent of the reader's backing array.
func (r *payloadReader) rest() []byte {
	b := append([]byte(nil), r.b...)
	r.b = nil
	return b
}

// done reports an error if any bytes remain unconsumed.
func (r *payloadReader) done() error {
	if len(r.b) != 0 {
		return errTrailingBytes
	}
	return nil
}

// MarshalHello encodes h into a Hello message payload.
func MarshalHello(h Hello) []byte {
	w := payloadWriter{}
	w.putUint16(h.Version)
	w.putUint8(h.Intent)
	w.putString(h.Name)
	w.putUint16(uint16(h.Size.Cols))
	w.putUint16(uint16(h.Size.Rows))
	w.putString(h.TermEnv)
	return w.b
}

// UnmarshalHello decodes a Hello message payload.
func UnmarshalHello(b []byte) (Hello, error) {
	r := payloadReader{b: b}
	var h Hello
	var err error

	if h.Version, err = r.getUint16(); err != nil {
		return Hello{}, err
	}
	if h.Intent, err = r.getUint8(); err != nil {
		return Hello{}, err
	}
	if h.Name, err = r.getString(); err != nil {
		return Hello{}, err
	}
	cols, err := r.getUint16()
	if err != nil {
		return Hello{}, err
	}
	rows, err := r.getUint16()
	if err != nil {
		return Hello{}, err
	}
	h.Size = domain.Size{Cols: int(cols), Rows: int(rows)}
	if h.TermEnv, err = r.getString(); err != nil {
		return Hello{}, err
	}
	if err := r.done(); err != nil {
		return Hello{}, err
	}
	return h, nil
}

// MarshalInput encodes m into an Input message payload.
func MarshalInput(m Input) []byte {
	return append([]byte(nil), m.Data...)
}

// UnmarshalInput decodes an Input message payload. The whole payload is the
// data; there is no separate length prefix.
func UnmarshalInput(b []byte) (Input, error) {
	r := payloadReader{b: b}
	return Input{Data: r.rest()}, nil
}

// MarshalResize encodes m into a Resize message payload.
func MarshalResize(m Resize) []byte {
	w := payloadWriter{}
	w.putUint16(uint16(m.Size.Cols))
	w.putUint16(uint16(m.Size.Rows))
	return w.b
}

// UnmarshalResize decodes a Resize message payload.
func UnmarshalResize(b []byte) (Resize, error) {
	r := payloadReader{b: b}
	cols, err := r.getUint16()
	if err != nil {
		return Resize{}, err
	}
	rows, err := r.getUint16()
	if err != nil {
		return Resize{}, err
	}
	if err := r.done(); err != nil {
		return Resize{}, err
	}
	return Resize{Size: domain.Size{Cols: int(cols), Rows: int(rows)}}, nil
}

// MarshalDetach encodes a Detach message payload (always empty).
func MarshalDetach(Detach) []byte {
	return nil
}

// UnmarshalDetach decodes a Detach message payload.
func UnmarshalDetach(b []byte) (Detach, error) {
	r := payloadReader{b: b}
	if err := r.done(); err != nil {
		return Detach{}, err
	}
	return Detach{}, nil
}

// MarshalPing encodes a Ping message payload (always empty).
func MarshalPing(Ping) []byte {
	return nil
}

// UnmarshalPing decodes a Ping message payload.
func UnmarshalPing(b []byte) (Ping, error) {
	r := payloadReader{b: b}
	if err := r.done(); err != nil {
		return Ping{}, err
	}
	return Ping{}, nil
}

// MarshalPong encodes a Pong message payload (always empty).
func MarshalPong(Pong) []byte {
	return nil
}

// UnmarshalPong decodes a Pong message payload.
func UnmarshalPong(b []byte) (Pong, error) {
	r := payloadReader{b: b}
	if err := r.done(); err != nil {
		return Pong{}, err
	}
	return Pong{}, nil
}

// MarshalWelcome encodes m into a Welcome message payload.
func MarshalWelcome(m Welcome) []byte {
	w := payloadWriter{}
	w.putString(m.SessionID)
	w.putString(m.SessionName)
	if m.Ephemeral {
		w.putUint8(1)
	} else {
		w.putUint8(0)
	}
	return w.b
}

// UnmarshalWelcome decodes a Welcome message payload.
func UnmarshalWelcome(b []byte) (Welcome, error) {
	r := payloadReader{b: b}
	var m Welcome
	var err error

	if m.SessionID, err = r.getString(); err != nil {
		return Welcome{}, err
	}
	if m.SessionName, err = r.getString(); err != nil {
		return Welcome{}, err
	}
	eph, err := r.getUint8()
	if err != nil {
		return Welcome{}, err
	}
	m.Ephemeral = eph != 0
	if err := r.done(); err != nil {
		return Welcome{}, err
	}
	return m, nil
}

// MarshalErrorMsg encodes m into an ErrorMsg message payload.
func MarshalErrorMsg(m ErrorMsg) []byte {
	w := payloadWriter{}
	w.putUint16(m.Code)
	w.putString(m.Text)
	return w.b
}

// UnmarshalErrorMsg decodes an ErrorMsg message payload.
func UnmarshalErrorMsg(b []byte) (ErrorMsg, error) {
	r := payloadReader{b: b}
	var m ErrorMsg
	var err error

	if m.Code, err = r.getUint16(); err != nil {
		return ErrorMsg{}, err
	}
	if m.Text, err = r.getString(); err != nil {
		return ErrorMsg{}, err
	}
	if err := r.done(); err != nil {
		return ErrorMsg{}, err
	}
	return m, nil
}

// MarshalOutput encodes m into an Output message payload.
func MarshalOutput(m Output) []byte {
	return append([]byte(nil), m.Data...)
}

// UnmarshalOutput decodes an Output message payload. The whole payload is
// the data; there is no separate length prefix.
func UnmarshalOutput(b []byte) (Output, error) {
	r := payloadReader{b: b}
	return Output{Data: r.rest()}, nil
}

// MarshalDetached encodes m into a Detached message payload.
func MarshalDetached(m Detached) []byte {
	return []byte{m.Reason}
}

// UnmarshalDetached decodes a Detached message payload.
func UnmarshalDetached(b []byte) (Detached, error) {
	r := payloadReader{b: b}
	reason, err := r.getUint8()
	if err != nil {
		return Detached{}, err
	}
	if err := r.done(); err != nil {
		return Detached{}, err
	}
	return Detached{Reason: reason}, nil
}
