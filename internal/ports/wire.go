// Wire codecs for vev's IPC message payloads. They live in ports (alongside
// Frame and the MsgType constants) because wire encoding is protocol
// surface, not I/O: everything here is pure, stdlib-only byte marshalling.
// Adapters keep what actually performs I/O (framing over a connection,
// listening on a socket) and import these types for the payloads they carry.

package ports

import (
	"encoding/binary"
	"errors"

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
)

// ErrorMsg codes.
const (
	ErrVersionMismatch    uint16 = 1
	ErrNoSuchSession      uint16 = 2
	ErrNameTaken          uint16 = 3
	ErrServerShutdown     uint16 = 4
	ErrInvalidSessionName uint16 = 5
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

// SessionInfo describes one session in a Sessions listing.
type SessionInfo struct {
	SessionID string
	Name      string
	Ephemeral bool
	Tabs      uint16
	Attached  bool
	Stopped   bool
}

// Sessions is the daemon's reply to a List, enumerating live sessions.
type Sessions struct {
	Sessions []SessionInfo
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
	w.putBytes(h.ClientID[:])
	w.putUint64(h.ResumeToken)
	w.putString(h.Name)
	w.putUint16(uint16(h.Size.Cols))
	w.putUint16(uint16(h.Size.Rows))
	w.putString(h.TermEnv)
	w.putString(h.Cwd)
	if h.TrueColor {
		w.putUint8(1)
	} else {
		w.putUint8(0)
	}
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
		if s.Stopped {
			w.putUint8(1)
		} else {
			w.putUint8(0)
		}
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
		stopped, err := r.getUint8()
		if err != nil {
			return Sessions{}, err
		}
		s.Stopped = stopped != 0
		sessions = append(sessions, s)
	}
	if err := r.done(); err != nil {
		return Sessions{}, err
	}
	return Sessions{Sessions: sessions}, nil
}
