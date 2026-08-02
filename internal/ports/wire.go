// Wire codecs for vev's IPC message payloads. They live in ports (alongside
// Frame and the MsgType constants) because wire encoding is protocol
// surface, not I/O: everything here is pure, stdlib-only byte marshalling.
// Adapters keep what actually performs I/O (framing over a connection,
// listening on a socket) and import these types for the payloads they carry.

package ports

import (
	"encoding/binary"
	"errors"
	"math"
	"unicode/utf8"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/pkg/renderer"
)

// errShortPayload is returned when a payload ends before a required field
// has been fully read.
var errShortPayload = errors.New("ports: payload too short")

// errTrailingBytes is returned when a fixed-shape payload has bytes left
// over after every field has been consumed. Protocol strictness here
// catches version drift early instead of silently ignoring extra data.
var errTrailingBytes = errors.New("ports: unexpected trailing bytes in payload")

var errInvalidBoolean = errors.New("ports: invalid boolean flag")

// ErrInvalidSessionMeta reports metadata that cannot be represented by the
// protocol's ordered uint16-indexed tab layout.
var ErrInvalidSessionMeta = errors.New("ports: invalid session metadata")

// ErrSessionMetaStringTooLong reports metadata containing a string too large
// for the protocol's uint16 string encoding.
var ErrSessionMetaStringTooLong = errors.New("ports: session metadata string too long")

// Intent values carried by Hello, describing what the client wants to do.
const (
	IntentEphemeral uint8 = 0
	IntentNew       uint8 = 1
	IntentAttach    uint8 = 2
	IntentResume    uint8 = 3
)

// Capability bits advertised in Welcome.
const (
	CapabilityResume  uint32 = 1 << 0
	CapabilityUDP     uint32 = 1 << 1
	CapabilityPredict uint32 = 1 << 2
	CapabilityProxied uint32 = 1 << 3
)

// ErrorMsg codes.
const (
	ErrVersionMismatch    uint16 = 1
	ErrNoSuchSession      uint16 = 2
	ErrNameTaken          uint16 = 3
	ErrServerShutdown     uint16 = 4
	ErrInvalidSessionName uint16 = 5
	ErrUnknownCommand     uint16 = 6
	ErrNotScriptable      uint16 = 7
	ErrInvalidCommandArgs uint16 = 8
	ErrNoSuchTarget       uint16 = 9
	ErrAmbiguousTarget    uint16 = 10
	ErrInternal           uint16 = 255
)

// Detached reasons.
const (
	ReasonDetach         uint8 = 0
	ReasonSessionKilled  uint8 = 1
	ReasonServerShutdown uint8 = 2
	ReasonReplaced       uint8 = 3
)

// Hello is sent by the client immediately after connecting.
type Hello struct {
	Version     uint16
	Intent      uint8
	Proxied     bool
	ClientID    [16]byte
	ResumeToken uint64
	Name        string
	Size        domain.Size
	TermEnv     string
	Cwd         string
	TrueColor   bool
	// MaxOutputInFlight is the requested maximum number of unacknowledged
	// state-bearing output frames.
	MaxOutputInFlight uint8
	// Env is the complete exported client environment for future PTY children.
	// Entries are opaque strings so their ordering and contents are preserved.
	Env []string
}

// Input carries raw bytes typed/pasted by the client, destined for the PTY.
type Input struct {
	InputSeq uint64
	Data     []byte
}

// Resize notifies the daemon of a client-side terminal size change.
type Resize struct {
	Size domain.Size
}

// Theme reports the client's terminal foreground/background colors, ANSI
// palette entries, and whether the client terminal supports truecolor.
type Theme struct {
	HasForeground bool
	Foreground    renderer.RGB
	HasBackground bool
	Background    renderer.RGB
	TrueColor     bool
	SchemeKnown   bool
	Light         bool
	PaletteKnown  uint16
	Palette       [16]renderer.RGB
}

// ImagePush carries a clipboard image from a remote client, to be written to
// a temp file and injected into the focused pane's PTY as a path (see
// docs/superpowers/specs/2026-07-04-clipboard-image-transfer-design.md).
type ImagePush struct {
	InputSeq uint64
	Mime     string
	Data     []byte
}

// ClientNotice reports a fixed client-side event for daemon-rendered user
// feedback. Action values are deliberately closed so a client cannot inject
// arbitrary display text into a shared session.
type ClientNotice struct {
	Action uint8
}

const (
	ClientNoticeClipboardFallback uint8 = 1
	ClientNoticeClipboardTooLarge uint8 = 2
	ClientNoticeLinkDegraded      uint8 = 3
	ClientNoticeLinkConnected     uint8 = 4
)

func validClientNoticeAction(action uint8) bool {
	switch action {
	case ClientNoticeClipboardFallback, ClientNoticeClipboardTooLarge, ClientNoticeLinkDegraded, ClientNoticeLinkConnected:
		return true
	default:
		return false
	}
}

// Detach asks the daemon to detach the current client without killing the
// session.
type Detach struct{}

// Ping is a keepalive/liveness probe sent by either side.
type Ping struct{}

// Pong answers a Ping.
type Pong struct{}

// Ack acknowledges receipt/application of an output state number.
type Ack struct {
	AckedStateNum uint64
}

// Welcome is the daemon's reply to a successful Hello.
type Welcome struct {
	SessionID    string
	SessionName  string
	Ephemeral    bool
	ResumeToken  uint64
	Capabilities uint32
}

// ErrorMsg reports a protocol- or session-level failure to the client.
type ErrorMsg struct {
	Code uint16
	Text string
}

// Output carries raw PTY output, destined for the client's terminal.
type Output struct {
	BaseStateNum uint64
	NewStateNum  uint64
	EchoAck      uint64
	Data         []byte
}

// Detached tells a client it has been disconnected from its session and why.
type Detached struct {
	Reason uint8
}

// List asks the daemon to enumerate its live sessions. It carries no
// fields; the request is fully described by its message type.
type List struct{}

// Kill asks the daemon to terminate one named session or all sessions.
type Kill struct {
	Name string
	All  bool
}

// SessionState is the client-visible lifecycle state of a session.
type SessionState uint8

const (
	SessionRunning SessionState = iota
	SessionStopped
	SessionBroken
)

// SessionInfo describes one session in a Sessions listing.
type SessionInfo struct {
	SessionID string
	Name      string
	State     SessionState
	Ephemeral bool
	Tabs      uint16
	Attached  bool
}

// Sessions is the daemon's reply to a List, enumerating live sessions.
type Sessions struct {
	Sessions []SessionInfo
}

// OutputResetRequest asks a proxied daemon to rebase its output stream.
type OutputResetRequest struct{}

// SessionTabMeta describes one tab in a proxied session's authoritative
// metadata snapshot.
type SessionTabMeta struct {
	Index     uint16
	Name      string
	Attention bool
}

// SessionMeta is the authoritative tab metadata sent to a proxied client.
type SessionMeta struct {
	SessionName string
	Active      uint16
	Tabs        []SessionTabMeta
}

// payloadWriter builds a message payload by appending fields in wire order.
type payloadWriter struct {
	b []byte
}

func (w *payloadWriter) putUint8(v uint8) {
	w.b = append(w.b, v)
}

// putBool writes the single byte payloadReader.getBool accepts: 1 for true and
// 0 for false.
func (w *payloadWriter) putBool(v bool) {
	if v {
		w.putUint8(1)
		return
	}
	w.putUint8(0)
}

func (w *payloadWriter) putUint16(v uint16) {
	var tmp [2]byte
	binary.BigEndian.PutUint16(tmp[:], v)
	w.b = append(w.b, tmp[:]...)
}

func (w *payloadWriter) putUint32(v uint32) {
	var tmp [4]byte
	binary.BigEndian.PutUint32(tmp[:], v)
	w.b = append(w.b, tmp[:]...)
}

func (w *payloadWriter) putUint64(v uint64) {
	var tmp [8]byte
	binary.BigEndian.PutUint64(tmp[:], v)
	w.b = append(w.b, tmp[:]...)
}

func (w *payloadWriter) putBytes(b []byte) {
	w.b = append(w.b, b...)
}

func (w *payloadWriter) putString(s string) {
	w.putUint16(uint16(len(s)))
	w.b = append(w.b, s...)
}

func (w *payloadWriter) putLongString(s string) {
	w.putUint32(uint32(len(s)))
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

func (r *payloadReader) getBool() (bool, error) {
	v, err := r.getUint8()
	if err != nil {
		return false, err
	}
	if v > 1 {
		return false, errInvalidBoolean
	}
	return v == 1, nil
}

func (r *payloadReader) getUint16() (uint16, error) {
	if len(r.b) < 2 {
		return 0, errShortPayload
	}
	v := binary.BigEndian.Uint16(r.b)
	r.b = r.b[2:]
	return v, nil
}

func (r *payloadReader) getUint32() (uint32, error) {
	if len(r.b) < 4 {
		return 0, errShortPayload
	}
	v := binary.BigEndian.Uint32(r.b)
	r.b = r.b[4:]
	return v, nil
}

func (r *payloadReader) getUint64() (uint64, error) {
	if len(r.b) < 8 {
		return 0, errShortPayload
	}
	v := binary.BigEndian.Uint64(r.b)
	r.b = r.b[8:]
	return v, nil
}

func (r *payloadReader) getBytes(n int) ([]byte, error) {
	if len(r.b) < n {
		return nil, errShortPayload
	}
	b := append([]byte(nil), r.b[:n]...)
	r.b = r.b[n:]
	return b, nil
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

func (r *payloadReader) getLongString() (string, error) {
	n, err := r.getUint32()
	if err != nil {
		return "", err
	}
	if uint64(n) > uint64(len(r.b)) {
		return "", errShortPayload
	}
	s := string(r.b[:int(n)])
	r.b = r.b[int(n):]
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

// PeekHelloVersion returns the leading protocol version from a Hello payload.
func PeekHelloVersion(b []byte) (uint16, bool) {
	if len(b) < 2 {
		return 0, false
	}
	return binary.BigEndian.Uint16(b), true
}

// MarshalHello encodes h into a Hello message payload.
func MarshalHello(h Hello) []byte {
	w := payloadWriter{}
	w.putUint16(h.Version)
	w.putUint8(h.Intent)
	w.putBool(h.Proxied)
	w.putBytes(h.ClientID[:])
	w.putUint64(h.ResumeToken)
	w.putString(h.Name)
	w.putUint16(uint16(h.Size.Cols))
	w.putUint16(uint16(h.Size.Rows))
	w.putString(h.TermEnv)
	w.putString(h.Cwd)
	w.putBool(h.TrueColor)
	w.putUint8(h.MaxOutputInFlight)
	w.putUint32(uint32(len(h.Env)))
	for _, entry := range h.Env {
		w.putLongString(entry)
	}
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
	if h.Proxied, err = r.getBool(); err != nil {
		return Hello{}, err
	}
	clientID, err := r.getBytes(len(h.ClientID))
	if err != nil {
		return Hello{}, err
	}
	copy(h.ClientID[:], clientID)
	if h.ResumeToken, err = r.getUint64(); err != nil {
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
	if h.Cwd, err = r.getString(); err != nil {
		return Hello{}, err
	}
	trueColor, err := r.getUint8()
	if err != nil {
		return Hello{}, err
	}
	h.TrueColor = trueColor != 0
	if h.MaxOutputInFlight, err = r.getUint8(); err != nil {
		return Hello{}, err
	}
	envCount, err := r.getUint32()
	if err != nil {
		return Hello{}, err
	}
	// Each entry has at least its uint32 byte length. Check that before
	// allocating so a malformed count cannot force an excessive allocation.
	if uint64(envCount) > uint64(len(r.b)/4) {
		return Hello{}, errShortPayload
	}
	if envCount != 0 {
		h.Env = make([]string, 0, int(envCount))
		for range int(envCount) {
			entry, err := r.getLongString()
			if err != nil {
				return Hello{}, err
			}
			h.Env = append(h.Env, entry)
		}
	}
	if err := r.done(); err != nil {
		return Hello{}, err
	}
	return h, nil
}

// MarshalInput encodes m into an Input message payload.
func MarshalInput(m Input) []byte {
	w := payloadWriter{}
	w.putUint64(m.InputSeq)
	w.putBytes(m.Data)
	return w.b
}

// UnmarshalInput decodes an Input message payload. After the fixed input
// sequence, the rest of the payload is data; there is no length prefix.
func UnmarshalInput(b []byte) (Input, error) {
	r := payloadReader{b: b}
	seq, err := r.getUint64()
	if err != nil {
		return Input{}, err
	}
	return Input{InputSeq: seq, Data: r.rest()}, nil
}

// MarshalImagePush encodes m into an ImagePush message payload.
func MarshalImagePush(m ImagePush) []byte {
	w := payloadWriter{}
	w.putUint64(m.InputSeq)
	w.putString(m.Mime)
	w.putBytes(m.Data)
	return w.b
}

// UnmarshalImagePush decodes an ImagePush message payload. After the fixed
// input sequence and length-prefixed mime string, the rest of the payload is
// data; there is no length prefix for it.
func UnmarshalImagePush(b []byte) (ImagePush, error) {
	r := payloadReader{b: b}
	seq, err := r.getUint64()
	if err != nil {
		return ImagePush{}, err
	}
	mime, err := r.getString()
	if err != nil {
		return ImagePush{}, err
	}
	return ImagePush{InputSeq: seq, Mime: mime, Data: r.rest()}, nil
}

// MarshalClientNotice encodes a fixed one-byte client notice action.
func MarshalClientNotice(m ClientNotice) []byte {
	return []byte{m.Action}
}

// UnmarshalClientNotice decodes a fixed client notice action and rejects both
// unknown values and any trailing bytes.
func UnmarshalClientNotice(b []byte) (ClientNotice, error) {
	r := payloadReader{b: b}
	action, err := r.getUint8()
	if err != nil {
		return ClientNotice{}, err
	}
	if !validClientNoticeAction(action) {
		return ClientNotice{}, errors.New("ports: unknown client notice action")
	}
	if err := r.done(); err != nil {
		return ClientNotice{}, err
	}
	return ClientNotice{Action: action}, nil
}

// MarshalResize encodes m into a Resize message payload.
func MarshalResize(m Resize) []byte {
	w := payloadWriter{}
	w.putUint16(uint16(m.Size.Cols))
	w.putUint16(uint16(m.Size.Rows))
	return w.b
}

// MarshalTheme encodes m into a 57-byte fixed-width Theme message payload.
func MarshalTheme(m Theme) []byte {
	var flags uint8
	if m.HasForeground {
		flags |= 0x01
	}
	if m.HasBackground {
		flags |= 0x02
	}
	if m.TrueColor {
		flags |= 0x04
	}
	if m.SchemeKnown {
		flags |= 0x08
	}
	if m.Light {
		flags |= 0x10
	}

	w := payloadWriter{b: make([]byte, 0, 57)}
	w.putUint8(flags)
	w.putUint8(m.Foreground.R)
	w.putUint8(m.Foreground.G)
	w.putUint8(m.Foreground.B)
	w.putUint8(m.Background.R)
	w.putUint8(m.Background.G)
	w.putUint8(m.Background.B)
	w.putUint16(m.PaletteKnown)
	for _, color := range m.Palette {
		w.putUint8(color.R)
		w.putUint8(color.G)
		w.putUint8(color.B)
	}
	return w.b
}

// UnmarshalTheme decodes a 57-byte fixed-width Theme message payload.
func UnmarshalTheme(b []byte) (Theme, error) {
	r := payloadReader{b: b}
	flags, err := r.getUint8()
	if err != nil {
		return Theme{}, err
	}
	fgR, err := r.getUint8()
	if err != nil {
		return Theme{}, err
	}
	fgG, err := r.getUint8()
	if err != nil {
		return Theme{}, err
	}
	fgB, err := r.getUint8()
	if err != nil {
		return Theme{}, err
	}
	bgR, err := r.getUint8()
	if err != nil {
		return Theme{}, err
	}
	bgG, err := r.getUint8()
	if err != nil {
		return Theme{}, err
	}
	bgB, err := r.getUint8()
	if err != nil {
		return Theme{}, err
	}
	paletteKnown, err := r.getUint16()
	if err != nil {
		return Theme{}, err
	}

	m := Theme{
		HasForeground: flags&0x01 != 0,
		Foreground:    renderer.RGB{R: fgR, G: fgG, B: fgB},
		HasBackground: flags&0x02 != 0,
		Background:    renderer.RGB{R: bgR, G: bgG, B: bgB},
		TrueColor:     flags&0x04 != 0,
		SchemeKnown:   flags&0x08 != 0,
		Light:         flags&0x10 != 0,
		PaletteKnown:  paletteKnown,
	}
	for i := range m.Palette {
		if m.Palette[i].R, err = r.getUint8(); err != nil {
			return Theme{}, err
		}
		if m.Palette[i].G, err = r.getUint8(); err != nil {
			return Theme{}, err
		}
		if m.Palette[i].B, err = r.getUint8(); err != nil {
			return Theme{}, err
		}
	}
	if err := r.done(); err != nil {
		return Theme{}, err
	}
	return m, nil
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

// MarshalAck encodes m into an Ack message payload.
func MarshalAck(m Ack) []byte {
	w := payloadWriter{}
	w.putUint64(m.AckedStateNum)
	return w.b
}

// UnmarshalAck decodes an Ack message payload.
func UnmarshalAck(b []byte) (Ack, error) {
	r := payloadReader{b: b}
	acked, err := r.getUint64()
	if err != nil {
		return Ack{}, err
	}
	if err := r.done(); err != nil {
		return Ack{}, err
	}
	return Ack{AckedStateNum: acked}, nil
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
	w.putUint64(m.ResumeToken)
	w.putUint32(m.Capabilities)
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
	if m.ResumeToken, err = r.getUint64(); err != nil {
		return Welcome{}, err
	}
	if m.Capabilities, err = r.getUint32(); err != nil {
		return Welcome{}, err
	}
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
	w := payloadWriter{}
	w.putUint64(m.BaseStateNum)
	w.putUint64(m.NewStateNum)
	w.putUint64(m.EchoAck)
	w.putBytes(m.Data)
	return w.b
}

// UnmarshalOutput decodes an Output message payload. After fixed state
// numbers, the rest of the payload is data; there is no length prefix.
func UnmarshalOutput(b []byte) (Output, error) {
	r := payloadReader{b: b}
	base, err := r.getUint64()
	if err != nil {
		return Output{}, err
	}
	newState, err := r.getUint64()
	if err != nil {
		return Output{}, err
	}
	echoAck, err := r.getUint64()
	if err != nil {
		return Output{}, err
	}
	return Output{BaseStateNum: base, NewStateNum: newState, EchoAck: echoAck, Data: r.rest()}, nil
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

// MarshalList encodes a List message payload (always empty).
func MarshalList(List) []byte {
	return nil
}

// UnmarshalList decodes a List message payload.
func UnmarshalList(b []byte) (List, error) {
	r := payloadReader{b: b}
	if err := r.done(); err != nil {
		return List{}, err
	}
	return List{}, nil
}

// MarshalOutputResetRequest encodes an OutputResetRequest payload (always
// empty).
func MarshalOutputResetRequest(OutputResetRequest) []byte {
	return nil
}

// UnmarshalOutputResetRequest decodes a strict empty OutputResetRequest
// payload.
func UnmarshalOutputResetRequest(b []byte) (OutputResetRequest, error) {
	r := payloadReader{b: b}
	if err := r.done(); err != nil {
		return OutputResetRequest{}, err
	}
	return OutputResetRequest{}, nil
}

// MarshalSessionMeta encodes m when it satisfies the protocol's authoritative
// ordered-tab constraints.
func MarshalSessionMeta(m SessionMeta) ([]byte, error) {
	if len(m.Tabs) == 0 || len(m.Tabs) > math.MaxUint16 || int(m.Active) >= len(m.Tabs) {
		return nil, ErrInvalidSessionMeta
	}
	if len(m.SessionName) > math.MaxUint16 {
		return nil, ErrSessionMetaStringTooLong
	}
	for i, tab := range m.Tabs {
		if tab.Index != uint16(i) {
			return nil, ErrInvalidSessionMeta
		}
		if len(tab.Name) > math.MaxUint16 {
			return nil, ErrSessionMetaStringTooLong
		}
	}

	w := payloadWriter{}
	w.putString(m.SessionName)
	w.putUint16(m.Active)
	w.putUint16(uint16(len(m.Tabs)))
	for _, tab := range m.Tabs {
		w.putUint16(tab.Index)
		w.putString(tab.Name)
		w.putBool(tab.Attention)
	}
	return w.b, nil
}

// UnmarshalSessionMeta decodes a strict authoritative metadata snapshot.
func UnmarshalSessionMeta(b []byte) (SessionMeta, error) {
	r := payloadReader{b: b}
	var m SessionMeta
	var err error
	if m.SessionName, err = r.getString(); err != nil {
		return SessionMeta{}, err
	}
	if m.Active, err = r.getUint16(); err != nil {
		return SessionMeta{}, err
	}
	count, err := r.getUint16()
	if err != nil {
		return SessionMeta{}, err
	}
	if count == 0 || int(m.Active) >= int(count) {
		return SessionMeta{}, ErrInvalidSessionMeta
	}
	if uint64(count) > uint64(len(r.b)/5) {
		return SessionMeta{}, errShortPayload
	}
	m.Tabs = make([]SessionTabMeta, 0, int(count))
	for i := range int(count) {
		var tab SessionTabMeta
		if tab.Index, err = r.getUint16(); err != nil {
			return SessionMeta{}, err
		}
		if tab.Index != uint16(i) {
			return SessionMeta{}, ErrInvalidSessionMeta
		}
		if tab.Name, err = r.getString(); err != nil {
			return SessionMeta{}, err
		}
		if tab.Attention, err = r.getBool(); err != nil {
			return SessionMeta{}, err
		}
		m.Tabs = append(m.Tabs, tab)
	}
	if err := r.done(); err != nil {
		return SessionMeta{}, err
	}
	return m, nil
}

// MarshalKill encodes m into a Kill message payload.
func MarshalKill(m Kill) []byte {
	w := payloadWriter{}
	w.putString(m.Name)
	if m.All {
		w.putUint8(1)
	}
	return w.b
}

// UnmarshalKill decodes a Kill message payload.
func UnmarshalKill(b []byte) (Kill, error) {
	r := payloadReader{b: b}
	name, err := r.getString()
	if err != nil {
		return Kill{}, err
	}
	var all uint8
	if len(r.b) > 0 {
		all, err = r.getUint8()
		if err != nil {
			return Kill{}, err
		}
	}
	if err := r.done(); err != nil {
		return Kill{}, err
	}
	return Kill{Name: name, All: all != 0}, nil
}

// MarshalSessions encodes m into a Sessions message payload: a uint16 count
// followed by that many session records.
func MarshalSessions(m Sessions) []byte {
	w := payloadWriter{}
	w.putUint16(uint16(len(m.Sessions)))
	for _, s := range m.Sessions {
		w.putString(s.SessionID)
		w.putString(s.Name)
		if s.Ephemeral {
			w.putUint8(1)
		} else {
			w.putUint8(0)
		}
		w.putUint16(s.Tabs)
		if s.Attached {
			w.putUint8(1)
		} else {
			w.putUint8(0)
		}
		w.putUint8(uint8(s.State))
	}
	return w.b
}

// UnmarshalSessions decodes a Sessions message payload.
func UnmarshalSessions(b []byte) (Sessions, error) {
	r := payloadReader{b: b}
	count, err := r.getUint16()
	if err != nil {
		return Sessions{}, err
	}
	sessions := make([]SessionInfo, 0, count)
	for range int(count) {
		var s SessionInfo
		if s.SessionID, err = r.getString(); err != nil {
			return Sessions{}, err
		}
		if s.Name, err = r.getString(); err != nil {
			return Sessions{}, err
		}
		eph, err := r.getUint8()
		if err != nil {
			return Sessions{}, err
		}
		s.Ephemeral = eph != 0
		if s.Tabs, err = r.getUint16(); err != nil {
			return Sessions{}, err
		}
		att, err := r.getUint8()
		if err != nil {
			return Sessions{}, err
		}
		s.Attached = att != 0
		state, err := r.getUint8()
		if err != nil {
			return Sessions{}, err
		}
		s.State = SessionState(state)
		if s.State > SessionBroken {
			return Sessions{}, errors.New("ports: invalid session state")
		}
		sessions = append(sessions, s)
	}
	if err := r.done(); err != nil {
		return Sessions{}, err
	}
	return Sessions{Sessions: sessions}, nil
}

// ScreenUpdateKind selects whether an update replaces the complete screen or
// applies a delta to the previously committed screen.
type ScreenUpdateKind uint8

const (
	ScreenUpdateSnapshot ScreenUpdateKind = 1
	ScreenUpdateDelta    ScreenUpdateKind = 2
)

// ScreenCursor is the absolute cursor state carried by each screen update.
type ScreenCursor struct {
	Row, Col uint16
	Style    uint8
	Visible  bool
	StyleSet bool
}

// ScreenScroll describes a full-width upward scroll applied before spans.
type ScreenScroll struct {
	Top, Height, Count uint16
}

// ScreenSpan identifies owned cells beginning at a horizontal position.
type ScreenSpan struct {
	Y, X  uint16
	Cells []renderer.Cell
}

// ScreenUpdate is the semantic structured screen message exchanged by proxy
// daemons. Wire style tables and run IDs remain private to its codec.
type ScreenUpdate struct {
	BaseStateNum uint64
	NewStateNum  uint64
	EchoAck      uint64
	Kind         ScreenUpdateKind
	Size         domain.Size
	Cursor       ScreenCursor
	Scroll       *ScreenScroll
	Spans        []ScreenSpan
}

var (
	// ErrInvalidScreenUpdate reports a malformed or non-canonical screen
	// update. It is deliberately shared by marshal and unmarshal validation.
	ErrInvalidScreenUpdate = errors.New("ports: invalid screen update")
	// ErrScreenUpdateTooLarge reports a payload that cannot fit in a frame.
	ErrScreenUpdateTooLarge = errors.New("ports: screen update too large")
)

const (
	// MaxScreenCells bounds the area of a screen update.
	MaxScreenCells = 1 << 18

	screenHeaderLen = 40
	screenStyleLen  = 18
	// MaxScreenSpans bounds the number of spans in a screen update.
	MaxScreenSpans = 4096

	// The frame length includes MsgScreenUpdate's type byte. The codec takes
	// only the payload, so one byte is reserved here.
	screenPayloadLimit = MaxFrameLen - 1

	screenFlagCursorVisible  = 1 << 0
	screenFlagCursorStyleSet = 1 << 1
	screenKnownCursorFlags   = screenFlagCursorVisible | screenFlagCursorStyleSet

	styleBoldBit              = 1 << 0
	styleItalicBit            = 1 << 1
	styleInverseBit           = 1 << 2
	styleDimBit               = 1 << 3
	styleUnderlineBit         = 1 << 4
	styleBlinkBit             = 1 << 5
	styleStrikethroughBit     = 1 << 6
	styleForegroundRGBBit     = 1 << 7
	styleBackgroundRGBBit     = 1 << 8
	styleUnderlineColorBit    = 1 << 9
	styleUnderlineColorRGBBit = 1 << 10
	styleKnownBits            = (1 << 11) - 1
)

// MarshalScreenUpdate encodes a canonical structured screen update.
func MarshalScreenUpdate(m ScreenUpdate) ([]byte, error) {
	if err := validateScreenUpdate(m); err != nil {
		return nil, err
	}

	styles := make([]renderer.Style, 0)
	styleIDs := make(map[renderer.Style]uint16)
	for _, span := range m.Spans {
		for _, cell := range span.Cells {
			style := canonicalScreenStyle(cell.Style)
			if _, ok := styleIDs[style]; ok {
				continue
			}
			if len(styles) == math.MaxUint16 {
				return nil, ErrInvalidScreenUpdate
			}
			styleIDs[style] = uint16(len(styles))
			styles = append(styles, style)
		}
	}
	w := screenWriter{b: make([]byte, 0, screenHeaderLen+len(styles)*screenStyleLen)}
	w.u64(m.BaseStateNum)
	w.u64(m.NewStateNum)
	w.u64(m.EchoAck)
	w.u8(uint8(m.Kind))
	w.u16(uint16(m.Size.Cols))
	w.u16(uint16(m.Size.Rows))
	w.u16(m.Cursor.Row)
	w.u16(m.Cursor.Col)
	w.u8(m.Cursor.Style)
	var cursorFlags uint8
	if m.Cursor.Visible {
		cursorFlags |= screenFlagCursorVisible
	}
	if m.Cursor.StyleSet {
		cursorFlags |= screenFlagCursorStyleSet
	}
	w.u8(cursorFlags)
	if m.Scroll != nil {
		w.u8(1)
	} else {
		w.u8(0)
	}
	w.u16(uint16(len(styles)))
	w.u16(uint16(len(m.Spans)))
	if m.Scroll != nil {
		w.u16(m.Scroll.Top)
		w.u16(m.Scroll.Height)
		w.u16(m.Scroll.Count)
	}
	for _, style := range styles {
		appendScreenStyle(&w, style)
	}
	for _, span := range m.Spans {
		w.u16(span.Y)
		w.u16(span.X)
		w.u16(uint16(len(span.Cells)))
		runs := screenRunCount(span.Cells)
		w.u16(uint16(runs))
		for start := 0; start < len(span.Cells); {
			style := canonicalScreenStyle(span.Cells[start].Style)
			id := styleIDs[style]
			end := start + 1
			for end < len(span.Cells) && styleIDs[canonicalScreenStyle(span.Cells[end].Style)] == id {
				end++
			}
			w.u16(id)
			w.u16(uint16(end - start))
			for _, cell := range span.Cells[start:end] {
				if cell.Continuation {
					w.uvarint(0)
				} else {
					w.uvarint(uint64(cell.Rune) + 1)
				}
			}
			start = end
		}
	}
	if w.tooLarge {
		return nil, ErrScreenUpdateTooLarge
	}
	return w.b, nil
}

// UnmarshalScreenUpdate strictly decodes one screen-update payload. All
// decoded slices own their data; no input-backed slice is retained.
func UnmarshalScreenUpdate(data []byte) (ScreenUpdate, error) {
	if len(data) > screenPayloadLimit {
		return ScreenUpdate{}, ErrScreenUpdateTooLarge
	}
	if len(data) < screenHeaderLen {
		return ScreenUpdate{}, ErrInvalidScreenUpdate
	}
	r := screenReader{b: data}
	var m ScreenUpdate
	var err error
	if m.BaseStateNum, err = r.u64(); err != nil {
		return ScreenUpdate{}, ErrInvalidScreenUpdate
	}
	if m.NewStateNum, err = r.u64(); err != nil {
		return ScreenUpdate{}, ErrInvalidScreenUpdate
	}
	if m.EchoAck, err = r.u64(); err != nil {
		return ScreenUpdate{}, ErrInvalidScreenUpdate
	}
	kind, err := r.u8()
	if err != nil || (ScreenUpdateKind(kind) != ScreenUpdateSnapshot && ScreenUpdateKind(kind) != ScreenUpdateDelta) {
		return ScreenUpdate{}, ErrInvalidScreenUpdate
	}
	m.Kind = ScreenUpdateKind(kind)
	cols, err := r.u16()
	if err != nil {
		return ScreenUpdate{}, ErrInvalidScreenUpdate
	}
	rows, err := r.u16()
	if err != nil || cols == 0 || rows == 0 || !screenAreaWithinLimit(domain.Size{Cols: int(cols), Rows: int(rows)}) {
		return ScreenUpdate{}, ErrInvalidScreenUpdate
	}
	m.Size = domain.Size{Cols: int(cols), Rows: int(rows)}
	if m.Cursor.Row, err = r.u16(); err != nil {
		return ScreenUpdate{}, ErrInvalidScreenUpdate
	}
	if m.Cursor.Col, err = r.u16(); err != nil {
		return ScreenUpdate{}, ErrInvalidScreenUpdate
	}
	if m.Cursor.Style, err = r.u8(); err != nil {
		return ScreenUpdate{}, ErrInvalidScreenUpdate
	}
	cursorFlags, err := r.u8()
	if err != nil || cursorFlags&^uint8(screenKnownCursorFlags) != 0 || m.Cursor.Row >= rows || m.Cursor.Col >= cols {
		return ScreenUpdate{}, ErrInvalidScreenUpdate
	}
	m.Cursor.Visible = cursorFlags&screenFlagCursorVisible != 0
	m.Cursor.StyleSet = cursorFlags&screenFlagCursorStyleSet != 0
	if (!m.Cursor.StyleSet && m.Cursor.Style != 0) || (m.Cursor.StyleSet && m.Cursor.Style > 6) {
		return ScreenUpdate{}, ErrInvalidScreenUpdate
	}
	scrollPresent, err := r.u8()
	if err != nil || scrollPresent > 1 {
		return ScreenUpdate{}, ErrInvalidScreenUpdate
	}
	styleCount, err := r.u16()
	if err != nil || uint64(styleCount) > uint64(len(r.b))/screenStyleLen {
		return ScreenUpdate{}, ErrInvalidScreenUpdate
	}
	spanCount, err := r.u16()
	if err != nil || spanCount > MaxScreenSpans || uint64(spanCount) > uint64(len(r.b))/8 {
		return ScreenUpdate{}, ErrInvalidScreenUpdate
	}
	if scrollPresent != 0 {
		if m.Kind != ScreenUpdateDelta || len(r.b) < 6 {
			return ScreenUpdate{}, ErrInvalidScreenUpdate
		}
		m.Scroll = new(ScreenScroll)
		if m.Scroll.Top, err = r.u16(); err != nil {
			return ScreenUpdate{}, ErrInvalidScreenUpdate
		}
		if m.Scroll.Height, err = r.u16(); err != nil {
			return ScreenUpdate{}, ErrInvalidScreenUpdate
		}
		if m.Scroll.Count, err = r.u16(); err != nil {
			return ScreenUpdate{}, ErrInvalidScreenUpdate
		}
		if !validScreenScroll(*m.Scroll, rows) {
			return ScreenUpdate{}, ErrInvalidScreenUpdate
		}
	} else {
		m.Scroll = nil
	}
	// styleCount was checked against the bytes still available before this
	// allocation. Each style is copied field-by-field below.
	styles := make([]renderer.Style, styleCount)
	seenStyles := make(map[renderer.Style]struct{}, styleCount)
	for i := range styles {
		style, ok := readScreenStyle(&r)
		if !ok {
			return ScreenUpdate{}, ErrInvalidScreenUpdate
		}
		if _, duplicate := seenStyles[style]; duplicate {
			return ScreenUpdate{}, ErrInvalidScreenUpdate
		}
		seenStyles[style] = struct{}{}
		styles[i] = style
	}
	m.Spans = make([]ScreenSpan, spanCount)
	usedStyles := make([]bool, styleCount)
	nextStyleID := uint16(0)
	var previousEnd uint32
	var previousY uint16
	for i := range m.Spans {
		y, errY := r.u16()
		x, errX := r.u16()
		cellCount, errCells := r.u16()
		runCount, errRuns := r.u16()
		if errY != nil || errX != nil || errCells != nil || errRuns != nil || cellCount == 0 || runCount == 0 || runCount > cellCount || uint64(runCount) > uint64(len(r.b))/4 {
			return ScreenUpdate{}, ErrInvalidScreenUpdate
		}
		if y >= rows || x >= cols || uint64(x)+uint64(cellCount) > uint64(cols) {
			return ScreenUpdate{}, ErrInvalidScreenUpdate
		}
		if i > 0 {
			if y < previousY || (y == previousY && uint32(x) < previousEnd) {
				return ScreenUpdate{}, ErrInvalidScreenUpdate
			}
		}
		// Ordered non-overlapping spans have at most one cell per screen cell,
		// so the area check above bounds aggregate decoded cells before each
		// per-span allocation. Every cell has at least one token and every run
		// has a four-byte descriptor; check both before allocating the slice.
		minimum := uint64(runCount)*4 + uint64(cellCount)
		if minimum > uint64(len(r.b)) {
			return ScreenUpdate{}, ErrInvalidScreenUpdate
		}
		cells := make([]renderer.Cell, cellCount)
		filled := 0
		previousStyleID := uint16(math.MaxUint16)
		for range runCount {
			styleID, e1 := r.u16()
			runCells, e2 := r.u16()
			if e1 != nil || e2 != nil || runCells == 0 || styleID >= styleCount || styleID == previousStyleID || uint32(filled)+uint32(runCells) > uint32(cellCount) {
				return ScreenUpdate{}, ErrInvalidScreenUpdate
			}
			if !usedStyles[styleID] {
				if styleID != nextStyleID {
					return ScreenUpdate{}, ErrInvalidScreenUpdate
				}
				nextStyleID++
				usedStyles[styleID] = true
			}
			previousStyleID = styleID
			for range runCells {
				token, ok := r.uvarint()
				if !ok {
					return ScreenUpdate{}, ErrInvalidScreenUpdate
				}
				if token == 0 {
					cells[filled] = renderer.Cell{Style: styles[styleID], Continuation: true}
				} else {
					runeValue, valid := screenRune(token)
					if !valid {
						return ScreenUpdate{}, ErrInvalidScreenUpdate
					}
					cells[filled] = renderer.Cell{Rune: runeValue, Style: styles[styleID]}
				}
				filled++
			}
		}
		if filled != int(cellCount) {
			return ScreenUpdate{}, ErrInvalidScreenUpdate
		}
		m.Spans[i] = ScreenSpan{Y: y, X: x, Cells: cells}
		previousY = y
		previousEnd = uint32(x) + uint32(cellCount)
	}
	if len(r.b) != 0 || nextStyleID != styleCount {
		return ScreenUpdate{}, ErrInvalidScreenUpdate
	}
	if err := validateScreenUpdate(m); err != nil {
		return ScreenUpdate{}, err
	}
	return m, nil
}

// ValidateScreenUpdate validates the shared semantic and canonical screen
// update contract used by the wire codec and proxy consumers.
func ValidateScreenUpdate(m ScreenUpdate) error {
	return validateScreenUpdate(m)
}

func validateScreenUpdate(m ScreenUpdate) error {
	switch m.Kind {
	case ScreenUpdateSnapshot:
		if m.BaseStateNum != 0 || m.NewStateNum == 0 {
			return ErrInvalidScreenUpdate
		}
	case ScreenUpdateDelta:
		if m.BaseStateNum == 0 || m.BaseStateNum == math.MaxUint64 || m.NewStateNum != m.BaseStateNum+1 {
			return ErrInvalidScreenUpdate
		}
	default:
		return ErrInvalidScreenUpdate
	}
	if m.Size.Cols <= 0 || m.Size.Rows <= 0 || m.Size.Cols > math.MaxUint16 || m.Size.Rows > math.MaxUint16 ||
		!screenAreaWithinLimit(m.Size) || m.Cursor.Row >= uint16(m.Size.Rows) || m.Cursor.Col >= uint16(m.Size.Cols) {
		return ErrInvalidScreenUpdate
	}
	if (!m.Cursor.StyleSet && m.Cursor.Style != 0) || (m.Cursor.StyleSet && m.Cursor.Style > 6) {
		return ErrInvalidScreenUpdate
	}
	if m.Kind == ScreenUpdateSnapshot && m.Scroll != nil {
		return ErrInvalidScreenUpdate
	}
	if m.Scroll != nil && !validScreenScroll(*m.Scroll, uint16(m.Size.Rows)) {
		return ErrInvalidScreenUpdate
	}
	if len(m.Spans) > MaxScreenSpans || len(m.Spans) > math.MaxUint16 || (m.Kind == ScreenUpdateSnapshot && len(m.Spans) == 0) {
		return ErrInvalidScreenUpdate
	}
	return validateScreenSpans(m)
}

func screenAreaWithinLimit(size domain.Size) bool {
	return size.Cols > 0 && size.Rows > 0 && size.Rows <= MaxScreenCells/size.Cols
}

func validScreenScroll(scroll ScreenScroll, rows uint16) bool {
	return scroll.Height != 0 && scroll.Count != 0 && scroll.Count < scroll.Height &&
		scroll.Top < rows && uint64(scroll.Top)+uint64(scroll.Height) <= uint64(rows)
}

func validateScreenSpans(m ScreenUpdate) error {
	cols, rows := uint32(m.Size.Cols), uint32(m.Size.Rows)
	var previousY uint16
	var previousEnd uint32
	for i, span := range m.Spans {
		if len(span.Cells) == 0 || len(span.Cells) > math.MaxUint16 || uint32(span.Y) >= rows || uint32(span.X) >= cols || uint64(span.X)+uint64(len(span.Cells)) > uint64(cols) {
			return ErrInvalidScreenUpdate
		}
		if i > 0 && (span.Y < previousY || (span.Y == previousY && uint32(span.X) < previousEnd)) {
			return ErrInvalidScreenUpdate
		}
		for _, cell := range span.Cells {
			if (cell.Continuation && cell.Rune != 0) || (!cell.Continuation && !utf8.ValidRune(cell.Rune)) {
				return ErrInvalidScreenUpdate
			}
			if err := validateScreenStyle(cell.Style); err != nil {
				return err
			}
		}
		previousY = span.Y
		previousEnd = uint32(span.X) + uint32(len(span.Cells))
	}
	if m.Kind == ScreenUpdateSnapshot {
		if len(m.Spans) != m.Size.Rows {
			return ErrInvalidScreenUpdate
		}
		for y, span := range m.Spans {
			if int(span.Y) != y || span.X != 0 || len(span.Cells) != m.Size.Cols {
				return ErrInvalidScreenUpdate
			}
		}
	}
	return nil
}

func validateScreenStyle(style renderer.Style) error {
	if style.Attrs&^(renderer.AttrDim|renderer.AttrUnderline|renderer.AttrBlink|renderer.AttrStrikethrough) != 0 || style.UnderlineStyle > renderer.UnderlineDashed {
		return ErrInvalidScreenUpdate
	}
	if !style.HasForegroundRGB && (style.Foreground < -1 || style.Foreground > math.MaxUint8 || style.ForegroundRGB != (renderer.RGB{})) {
		return ErrInvalidScreenUpdate
	}
	if !style.HasBackgroundRGB && (style.Background < -1 || style.Background > math.MaxUint8 || style.BackgroundRGB != (renderer.RGB{})) {
		return ErrInvalidScreenUpdate
	}
	if style.HasUnderlineColorRGB && style.HasUnderlineColor {
		return ErrInvalidScreenUpdate
	} else if style.HasUnderlineColorRGB {
		// The indexed value is inactive when RGB is selected.
	} else if style.HasUnderlineColor {
		if style.UnderlineColor < 0 || style.UnderlineColor > math.MaxUint8 || style.UnderlineColorRGB != (renderer.RGB{}) {
			return ErrInvalidScreenUpdate
		}
	} else if style.UnderlineColorRGB != (renderer.RGB{}) {
		return ErrInvalidScreenUpdate
	}
	return nil
}

func canonicalScreenStyle(style renderer.Style) renderer.Style {
	if style.HasForegroundRGB {
		style.Foreground = -1
	} else {
		style.ForegroundRGB = renderer.RGB{}
	}
	if style.HasBackgroundRGB {
		style.Background = -1
	} else {
		style.BackgroundRGB = renderer.RGB{}
	}
	if style.HasUnderlineColorRGB {
		style.UnderlineColor = -1
	} else if style.HasUnderlineColor {
		style.UnderlineColorRGB = renderer.RGB{}
	} else {
		style.UnderlineColor = -1
		style.UnderlineColorRGB = renderer.RGB{}
	}
	return style
}

func screenStyleBits(style renderer.Style) uint16 {
	var bits uint16
	if style.Bold {
		bits |= styleBoldBit
	}
	if style.Italic {
		bits |= styleItalicBit
	}
	if style.Inverse {
		bits |= styleInverseBit
	}
	if style.Attrs&renderer.AttrDim != 0 {
		bits |= styleDimBit
	}
	if style.Attrs&renderer.AttrUnderline != 0 {
		bits |= styleUnderlineBit
	}
	if style.Attrs&renderer.AttrBlink != 0 {
		bits |= styleBlinkBit
	}
	if style.Attrs&renderer.AttrStrikethrough != 0 {
		bits |= styleStrikethroughBit
	}
	if style.HasForegroundRGB {
		bits |= styleForegroundRGBBit
	}
	if style.HasBackgroundRGB {
		bits |= styleBackgroundRGBBit
	}
	if style.HasUnderlineColor {
		bits |= styleUnderlineColorBit
	}
	if style.HasUnderlineColorRGB {
		bits |= styleUnderlineColorRGBBit
	}
	return bits
}

func appendScreenStyle(w *screenWriter, style renderer.Style) {
	style = canonicalScreenStyle(style)
	w.u16(screenStyleBits(style))
	w.u16(screenColorIndex(style.Foreground))
	w.u16(screenColorIndex(style.Background))
	w.u16(screenColorIndex(style.UnderlineColor))
	w.u8(style.ForegroundRGB.R)
	w.u8(style.ForegroundRGB.G)
	w.u8(style.ForegroundRGB.B)
	w.u8(style.BackgroundRGB.R)
	w.u8(style.BackgroundRGB.G)
	w.u8(style.BackgroundRGB.B)
	w.u8(style.UnderlineColorRGB.R)
	w.u8(style.UnderlineColorRGB.G)
	w.u8(style.UnderlineColorRGB.B)
	w.u8(uint8(style.UnderlineStyle))
}

func screenColorIndex(index int) uint16 {
	if index < 0 {
		return math.MaxUint16
	}
	return uint16(index)
}

func screenColorValue(index uint16) int {
	if index == math.MaxUint16 {
		return -1
	}
	return int(index)
}

func readScreenStyle(r *screenReader) (renderer.Style, bool) {
	bits, ok := r.u16ok()
	if !ok || bits&^uint16(styleKnownBits) != 0 {
		return renderer.Style{}, false
	}
	foregroundIndex, ok := r.u16ok()
	if !ok {
		return renderer.Style{}, false
	}
	backgroundIndex, ok := r.u16ok()
	if !ok {
		return renderer.Style{}, false
	}
	underlineIndex, ok := r.u16ok()
	if !ok {
		return renderer.Style{}, false
	}
	fgRGB, ok := r.rgb()
	if !ok {
		return renderer.Style{}, false
	}
	bgRGB, ok := r.rgb()
	if !ok {
		return renderer.Style{}, false
	}
	underlineRGB, ok := r.rgb()
	if !ok {
		return renderer.Style{}, false
	}
	underlineStyle, ok := r.u8ok()
	if !ok || underlineStyle > uint8(renderer.UnderlineDashed) {
		return renderer.Style{}, false
	}
	style := renderer.Style{
		Bold:                 bits&styleBoldBit != 0,
		Italic:               bits&styleItalicBit != 0,
		Inverse:              bits&styleInverseBit != 0,
		UnderlineStyle:       renderer.UnderlineStyle(underlineStyle),
		Foreground:           screenColorValue(foregroundIndex),
		Background:           screenColorValue(backgroundIndex),
		HasForegroundRGB:     bits&styleForegroundRGBBit != 0,
		ForegroundRGB:        fgRGB,
		HasBackgroundRGB:     bits&styleBackgroundRGBBit != 0,
		BackgroundRGB:        bgRGB,
		HasUnderlineColor:    bits&styleUnderlineColorBit != 0,
		UnderlineColor:       screenColorValue(underlineIndex),
		HasUnderlineColorRGB: bits&styleUnderlineColorRGBBit != 0,
		UnderlineColorRGB:    underlineRGB,
	}
	if style.HasUnderlineColor && style.HasUnderlineColorRGB {
		return renderer.Style{}, false
	}
	style.Attrs = 0
	if bits&styleDimBit != 0 {
		style.Attrs |= renderer.AttrDim
	}
	if bits&styleUnderlineBit != 0 {
		style.Attrs |= renderer.AttrUnderline
	}
	if bits&styleBlinkBit != 0 {
		style.Attrs |= renderer.AttrBlink
	}
	if bits&styleStrikethroughBit != 0 {
		style.Attrs |= renderer.AttrStrikethrough
	}
	if style.HasForegroundRGB {
		if foregroundIndex != math.MaxUint16 {
			return renderer.Style{}, false
		}
		style.Foreground = -1
	} else if (foregroundIndex != math.MaxUint16 && foregroundIndex > math.MaxUint8) || fgRGB != (renderer.RGB{}) {
		return renderer.Style{}, false
	}
	if style.HasBackgroundRGB {
		if backgroundIndex != math.MaxUint16 {
			return renderer.Style{}, false
		}
		style.Background = -1
	} else if (backgroundIndex != math.MaxUint16 && backgroundIndex > math.MaxUint8) || bgRGB != (renderer.RGB{}) {
		return renderer.Style{}, false
	}
	if style.HasUnderlineColorRGB {
		if underlineIndex != math.MaxUint16 {
			return renderer.Style{}, false
		}
		style.UnderlineColor = -1
	} else if style.HasUnderlineColor {
		if underlineIndex == math.MaxUint16 || underlineIndex > math.MaxUint8 || underlineRGB != (renderer.RGB{}) {
			return renderer.Style{}, false
		}
	} else if underlineIndex != math.MaxUint16 || underlineRGB != (renderer.RGB{}) {
		return renderer.Style{}, false
	}
	return style, true
}

func screenRunCount(cells []renderer.Cell) int {
	if len(cells) == 0 {
		return 0
	}
	n := 1
	previous := canonicalScreenStyle(cells[0].Style)
	for _, cell := range cells[1:] {
		style := canonicalScreenStyle(cell.Style)
		if style != previous {
			n++
			previous = style
		}
	}
	return n
}

func screenRune(token uint64) (rune, bool) {
	if token == 0 || token > uint64(utf8.MaxRune)+1 {
		return 0, false
	}
	r := rune(token - 1)
	return r, utf8.ValidRune(r)
}

type screenWriter struct {
	b        []byte
	tooLarge bool
}

func (w *screenWriter) append(p ...byte) {
	if w.tooLarge || len(w.b) > screenPayloadLimit-len(p) {
		w.tooLarge = true
		return
	}
	w.b = append(w.b, p...)
}
func (w *screenWriter) u8(v uint8)   { w.append(v) }
func (w *screenWriter) u16(v uint16) { w.append(byte(v>>8), byte(v)) }
func (w *screenWriter) u64(v uint64) {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], v)
	w.append(b[:]...)
}
func (w *screenWriter) uvarint(v uint64) {
	var b [10]byte
	n := binary.PutUvarint(b[:], v)
	w.append(b[:n]...)
}

type screenReader struct{ b []byte }

func (r *screenReader) take(n int) ([]byte, bool) {
	if n < 0 || len(r.b) < n {
		return nil, false
	}
	p := r.b[:n]
	r.b = r.b[n:]
	return p, true
}
func (r *screenReader) u8() (uint8, error) {
	v, ok := r.u8ok()
	if !ok {
		return 0, ErrInvalidScreenUpdate
	}
	return v, nil
}
func (r *screenReader) u8ok() (uint8, bool) {
	p, ok := r.take(1)
	if !ok {
		return 0, false
	}
	return p[0], true
}
func (r *screenReader) u16() (uint16, error) {
	p, ok := r.take(2)
	if !ok {
		return 0, ErrInvalidScreenUpdate
	}
	return binary.BigEndian.Uint16(p), nil
}
func (r *screenReader) u16ok() (uint16, bool) {
	p, ok := r.take(2)
	if !ok {
		return 0, false
	}
	return binary.BigEndian.Uint16(p), true
}
func (r *screenReader) u64() (uint64, error) {
	p, ok := r.take(8)
	if !ok {
		return 0, ErrInvalidScreenUpdate
	}
	return binary.BigEndian.Uint64(p), nil
}
func (r *screenReader) rgb() (renderer.RGB, bool) {
	p, ok := r.take(3)
	if !ok {
		return renderer.RGB{}, false
	}
	return renderer.RGB{R: p[0], G: p[1], B: p[2]}, true
}
func (r *screenReader) uvarint() (uint64, bool) {
	v, n := binary.Uvarint(r.b)
	if n <= 0 {
		return 0, false
	}
	var canonical [binary.MaxVarintLen64]byte
	if binary.PutUvarint(canonical[:], v) != n {
		return 0, false
	}
	r.b = r.b[n:]
	return v, true
}
