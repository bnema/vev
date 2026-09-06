// Package wire owns vev's binary frame representation and strict payload
// codecs. It performs no I/O; carriage adapters transport Frame values.

package wire

import (
	"bytes"
	"compress/zlib"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"sync"

	renderer "github.com/bnema/vev-vt"
	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/protocol"
)

// errShortPayload is returned when a payload ends before a required field
// has been fully read.
var errShortPayload = errors.New("ports: payload too short")

// errTrailingBytes is returned when a fixed-shape payload has bytes left
// over after every field has been consumed. Protocol strictness here
// catches version drift early instead of silently ignoring extra data.
var errTrailingBytes = errors.New("ports: unexpected trailing bytes in payload")

var errInvalidBoolean = errors.New("ports: invalid boolean flag")
var errInvalidEnum = errors.New("ports: invalid enum value")

const (
	outputHeaderLen            = 5*8 + 2*2 + 2
	outputCompressionHeaderLen = 1 + 4
	outputPayloadOverhead      = outputHeaderLen + outputCompressionHeaderLen + 4
	// outputCompressionThreshold keeps small snapshots on the canonical path.
	outputCompressionThreshold = 1024
)

const (
	outputCompressionNone byte = iota
	outputCompressionZlib
)

var outputCompressorPool = sync.Pool{New: func() any {
	writer, err := zlib.NewWriterLevel(io.Discard, zlib.BestSpeed)
	if err != nil {
		panic(err)
	}
	return writer
}}

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

func (w *payloadWriter) putLongBytes(b []byte) {
	w.putUint32(uint32(len(b)))
	w.b = append(w.b, b...)
}

// marshalRemoteTargetSection writes the required v27 target/policy section.
// A target may be absent for direct local attaches, but its presence marker and
// environment policy are never optional: v27 has no extension or legacy tail
// inside this section; the exact-target section follows it.
func marshalRemoteTargetSection(w *payloadWriter, target *domain.RemoteSessionTarget, policy protocol.EnvironmentPolicy) {
	if target == nil {
		w.putBool(false)
	} else {
		w.putBool(true)
		w.putString(target.Endpoint)
		w.putString(target.DisplayOrigin)
		w.putBytes(target.LifecycleID[:])
		w.putString(target.SessionName)
		w.putBool(target.Stopped)
		w.putString(string(target.LiveTabID))
		w.putUint8(uint8(target.StoppedTab.Kind))
		switch target.StoppedTab.Kind {
		case domain.TabSelectorByStableID:
			w.putString(string(target.StoppedTab.StableID))
		case domain.TabSelectorByOrdinal:
			w.putUint16(target.StoppedTab.Ordinal)
			w.putString(target.StoppedTab.RawName)
			w.putUint16(target.StoppedTab.ExpectedCount)
		}
	}
	w.putUint8(uint8(policy))
}

func skipRemoteTargetSection(r *payloadReader) error {
	present, err := r.getBool()
	if err != nil {
		return err
	}
	if present {
		if err := r.skipString(); err != nil {
			return err
		}
		if err := r.skipString(); err != nil {
			return err
		}
		if _, err := r.getBytes(16); err != nil {
			return err
		}
		if err := r.skipString(); err != nil {
			return err
		}
		if _, err := r.getBool(); err != nil {
			return err
		}
		if err := r.skipString(); err != nil {
			return err
		}
		kind, err := r.getUint8()
		if err != nil {
			return err
		}
		switch domain.TabSelectorKind(kind) {
		case domain.TabSelectorByStableID:
			if err := r.skipString(); err != nil {
				return err
			}
		case domain.TabSelectorByOrdinal:
			if _, err := r.getUint16(); err != nil {
				return err
			}
			if err := r.skipString(); err != nil {
				return err
			}
			if _, err := r.getUint16(); err != nil {
				return err
			}
		case 0:
		default:
			return errInvalidEnum
		}
	}
	policy, err := r.getUint8()
	if err != nil {
		return err
	}
	if protocol.ValidateEnvironmentPolicy(protocol.EnvironmentPolicy(policy)) != nil {
		return errInvalidEnum
	}
	return nil
}

func unmarshalRemoteTarget(r *payloadReader) (*domain.RemoteSessionTarget, error) {
	present, err := r.getBool()
	if err != nil {
		return nil, err
	}
	if !present {
		return nil, nil
	}
	target := &domain.RemoteSessionTarget{}
	if target.Endpoint, err = r.getString(); err != nil {
		return nil, err
	}
	if target.DisplayOrigin, err = r.getString(); err != nil {
		return nil, err
	}
	id, err := r.getBytes(16)
	if err != nil {
		return nil, err
	}
	copy(target.LifecycleID[:], id)
	if target.SessionName, err = r.getString(); err != nil {
		return nil, err
	}
	if target.Stopped, err = r.getBool(); err != nil {
		return nil, err
	}
	liveID, err := r.getString()
	if err != nil {
		return nil, err
	}
	target.LiveTabID = domain.TabStableID(liveID)
	kind, err := r.getUint8()
	if err != nil {
		return nil, err
	}
	target.StoppedTab.Kind = domain.TabSelectorKind(kind)
	switch target.StoppedTab.Kind {
	case domain.TabSelectorByStableID:
		stableID, err := r.getString()
		if err != nil {
			return nil, err
		}
		target.StoppedTab.StableID = domain.TabStableID(stableID)
	case domain.TabSelectorByOrdinal:
		if target.StoppedTab.Ordinal, err = r.getUint16(); err != nil {
			return nil, err
		}
		if target.StoppedTab.RawName, err = r.getString(); err != nil {
			return nil, err
		}
		if target.StoppedTab.ExpectedCount, err = r.getUint16(); err != nil {
			return nil, err
		}
	case 0:
	default:
		return nil, errInvalidEnum
	}
	return target, nil
}

func unmarshalRemoteTargetSection(r *payloadReader) (*domain.RemoteSessionTarget, protocol.EnvironmentPolicy, error) {
	target, err := unmarshalRemoteTarget(r)
	if err != nil {
		return nil, 0, err
	}
	policy, err := r.getUint8()
	if err != nil {
		return nil, 0, err
	}
	if protocol.ValidateEnvironmentPolicy(protocol.EnvironmentPolicy(policy)) != nil {
		return nil, 0, errInvalidEnum
	}
	return target, protocol.EnvironmentPolicy(policy), nil
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

func (r *payloadReader) getLongBytes() ([]byte, error) {
	n, err := r.getUint32()
	if err != nil {
		return nil, err
	}
	if uint64(n) > uint64(len(r.b)) {
		return nil, errShortPayload
	}
	b := append([]byte(nil), r.b[:int(n)]...)
	r.b = r.b[int(n):]
	return b, nil
}

func (r *payloadReader) skipString() error {
	n, err := r.getUint16()
	if err != nil {
		return err
	}
	if int(n) > len(r.b) {
		return errShortPayload
	}
	r.b = r.b[n:]
	return nil
}

func (r *payloadReader) skipLongString() error {
	n, err := r.getUint32()
	if err != nil {
		return err
	}
	if uint64(n) > uint64(len(r.b)) {
		return errShortPayload
	}
	r.b = r.b[n:]
	return nil
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

// ValidateHello validates Hello semantics without retaining or allocating any
// payload data.

// ValidateOutput validates the final output state before data allocation.

// ValidateAck validates an epoch-scoped output acknowledgement.

// ValidateAttachTarget validates a client-owned route handoff. An empty
// Endpoint selects another session on the currently connected daemon; a
// non-empty Endpoint selects a discovered remote daemon.

func validateWireRemoteTarget(target domain.RemoteSessionTarget) error {
	if len(target.Endpoint) > math.MaxUint16 || len(target.DisplayOrigin) > math.MaxUint16 || len(target.SessionName) > math.MaxUint16 || len(target.LiveTabID) > math.MaxUint16 || len(target.StoppedTab.RawName) > math.MaxUint16 {
		return errors.New("remote target string too long")
	}
	return target.Validate()
}

// MarshalHello encodes h into a Hello message payload.
func MarshalHello(h protocol.Hello) []byte {
	if err := protocol.ValidateHello(h); err != nil {
		return nil
	}
	w := payloadWriter{}
	w.putUint16(h.Version)
	w.putUint8(h.Intent)
	w.putBytes(h.ClientID[:])
	w.putUint64(h.ResumeToken)
	w.putString(h.Name)
	w.putUint16(uint16(h.Size.Cols))
	w.putUint16(uint16(h.Size.Rows))
	w.putUint16(uint16(h.PixelWidth))
	w.putUint16(uint16(h.PixelHeight))
	w.putString(h.TermEnv)
	w.putString(h.Cwd)
	w.putBool(h.TrueColor)
	w.putUint8(h.MaxOutputInFlight)
	w.putUint32(uint32(len(h.Env)))
	for _, entry := range h.Env {
		w.putLongString(entry)
	}
	marshalRemoteTargetSection(&w, h.RemoteTarget, h.EnvironmentPolicy)
	marshalExactTargetSection(&w, h.ExactTarget)
	w.putString(string(h.PreferredTabID))
	w.putUint8(uint8(h.NavigationCapabilities))
	w.putUint8(uint8(h.StartupOverlay))
	w.putBool(h.Remote)
	w.putBool(h.KittyDirectGraphics)
	return w.b
}

func preflightHello(b []byte) error {
	r := payloadReader{b: b}
	version, err := r.getUint16()
	if err != nil {
		return err
	}
	if version == 21 {
		return protocol.ErrInvalidHello
	}
	if _, err := r.getUint8(); err != nil {
		return err
	}
	if len(r.b) < 16 {
		return errShortPayload
	}
	r.b = r.b[16:]
	if _, err := r.getUint64(); err != nil {
		return err
	}
	if err := r.skipString(); err != nil {
		return err
	}
	if _, err := r.getUint16(); err != nil {
		return err
	}
	if _, err := r.getUint16(); err != nil {
		return err
	}
	if _, err := r.getUint16(); err != nil {
		return err
	}
	if _, err := r.getUint16(); err != nil {
		return err
	}
	if err := r.skipString(); err != nil {
		return err
	}
	if err := r.skipString(); err != nil {
		return err
	}
	if _, err := r.getBool(); err != nil {
		return err
	}
	if _, err := r.getUint8(); err != nil {
		return err
	}
	envCount, err := r.getUint32()
	if err != nil {
		return err
	}
	if uint64(envCount) > uint64(len(r.b)/4) {
		return errShortPayload
	}
	for range int(envCount) {
		if err := r.skipLongString(); err != nil {
			return err
		}
	}
	if err := skipRemoteTargetSection(&r); err != nil {
		return err
	}
	if err := skipExactTargetSection(&r); err != nil {
		return err
	}
	if err := r.skipString(); err != nil {
		return err
	}
	if _, err := r.getUint8(); err != nil {
		return err
	}
	if _, err := r.getUint8(); err != nil {
		return err
	}
	if _, err := r.getBool(); err != nil {
		return err
	}
	if _, err := r.getBool(); err != nil {
		return err
	}
	return r.done()
}

// UnmarshalHello decodes a Hello message payload.
func UnmarshalHello(b []byte) (protocol.Hello, error) {
	if len(b) > MaxFrameLen-1 {
		return protocol.Hello{}, protocol.ErrInvalidHello
	}
	if err := preflightHello(b); err != nil {
		return protocol.Hello{}, err
	}
	r := payloadReader{b: b}
	var h protocol.Hello
	var err error

	if h.Version, err = r.getUint16(); err != nil {
		return protocol.Hello{}, err
	}
	if h.Intent, err = r.getUint8(); err != nil {
		return protocol.Hello{}, err
	}
	clientID, err := r.getBytes(len(h.ClientID))
	if err != nil {
		return protocol.Hello{}, err
	}
	copy(h.ClientID[:], clientID)
	if h.ResumeToken, err = r.getUint64(); err != nil {
		return protocol.Hello{}, err
	}
	if h.Name, err = r.getString(); err != nil {
		return protocol.Hello{}, err
	}
	cols, err := r.getUint16()
	if err != nil {
		return protocol.Hello{}, err
	}
	rows, err := r.getUint16()
	if err != nil {
		return protocol.Hello{}, err
	}
	pixelWidth, err := r.getUint16()
	if err != nil {
		return protocol.Hello{}, err
	}
	pixelHeight, err := r.getUint16()
	if err != nil {
		return protocol.Hello{}, err
	}
	h.Size = domain.Size{Cols: int(cols), Rows: int(rows)}
	h.PixelWidth = int(pixelWidth)
	h.PixelHeight = int(pixelHeight)
	if h.TermEnv, err = r.getString(); err != nil {
		return protocol.Hello{}, err
	}
	if h.Cwd, err = r.getString(); err != nil {
		return protocol.Hello{}, err
	}
	if h.TrueColor, err = r.getBool(); err != nil {
		return protocol.Hello{}, err
	}
	if h.MaxOutputInFlight, err = r.getUint8(); err != nil {
		return protocol.Hello{}, err
	}
	envCount, err := r.getUint32()
	if err != nil {
		return protocol.Hello{}, err
	}
	// Each entry has at least its uint32 byte length. Check that before
	// allocating so a malformed count cannot force an excessive allocation.
	if uint64(envCount) > uint64(len(r.b)/4) {
		return protocol.Hello{}, errShortPayload
	}
	if envCount != 0 {
		h.Env = make([]string, 0, int(envCount))
		for range int(envCount) {
			entry, err := r.getLongString()
			if err != nil {
				return protocol.Hello{}, err
			}
			h.Env = append(h.Env, entry)
		}
	}
	if h.RemoteTarget, h.EnvironmentPolicy, err = unmarshalRemoteTargetSection(&r); err != nil {
		return protocol.Hello{}, err
	}
	if h.ExactTarget, err = unmarshalExactTargetSection(&r); err != nil {
		return protocol.Hello{}, err
	}
	preferredTabID, err := r.getString()
	if err != nil {
		return protocol.Hello{}, err
	}
	h.PreferredTabID = domain.TabStableID(preferredTabID)
	capabilities, err := r.getUint8()
	if err != nil {
		return protocol.Hello{}, err
	}
	h.NavigationCapabilities = protocol.NavigationCapabilities(capabilities)
	overlay, err := r.getUint8()
	if err != nil {
		return protocol.Hello{}, err
	}
	h.StartupOverlay = protocol.StartupOverlay(overlay)
	if h.Remote, err = r.getBool(); err != nil {
		return protocol.Hello{}, err
	}
	if h.KittyDirectGraphics, err = r.getBool(); err != nil {
		return protocol.Hello{}, err
	}
	if err := r.done(); err != nil {
		return protocol.Hello{}, err
	}
	if err := protocol.ValidateHello(h); err != nil {
		return protocol.Hello{}, err
	}
	return h, nil
}

// MarshalInput encodes m into an Input message payload.
func MarshalInput(m protocol.Input) []byte {
	w := payloadWriter{}
	w.putUint64(m.InputSeq)
	w.putUint64(m.ActionID)
	w.putBytes(m.Data)
	return w.b
}

// UnmarshalInput decodes an Input message payload. After the fixed input
// sequence, the rest of the payload is data; there is no length prefix.
func UnmarshalInput(b []byte) (protocol.Input, error) {
	r := payloadReader{b: b}
	seq, err := r.getUint64()
	if err != nil {
		return protocol.Input{}, err
	}
	actionID, err := r.getUint64()
	if err != nil {
		return protocol.Input{}, err
	}
	return protocol.Input{InputSeq: seq, ActionID: actionID, Data: r.rest()}, nil
}

// MarshalImagePush encodes m into an ImagePush message payload.
func MarshalImagePush(m protocol.ImagePush) []byte {
	w := payloadWriter{}
	w.putUint64(m.InputSeq)
	w.putString(m.Mime)
	w.putBytes(m.Data)
	return w.b
}

// UnmarshalImagePush decodes an ImagePush message payload. After the fixed
// input sequence and length-prefixed mime string, the rest of the payload is
// data; there is no length prefix for it.
func UnmarshalImagePush(b []byte) (protocol.ImagePush, error) {
	r := payloadReader{b: b}
	seq, err := r.getUint64()
	if err != nil {
		return protocol.ImagePush{}, err
	}
	mime, err := r.getString()
	if err != nil {
		return protocol.ImagePush{}, err
	}
	return protocol.ImagePush{InputSeq: seq, Mime: mime, Data: r.rest()}, nil
}

// MarshalClientNotice encodes a fixed one-byte client notice action.
func MarshalClientNotice(m protocol.ClientNotice) []byte {
	return []byte{m.Action}
}

// UnmarshalClientNotice decodes a fixed client notice action and rejects both
// unknown values and any trailing bytes.
func UnmarshalClientNotice(b []byte) (protocol.ClientNotice, error) {
	r := payloadReader{b: b}
	action, err := r.getUint8()
	if err != nil {
		return protocol.ClientNotice{}, err
	}
	if protocol.ValidateClientNotice(protocol.ClientNotice{Action: action}) != nil {
		return protocol.ClientNotice{}, errors.New("ports: unknown client notice action")
	}
	if err := r.done(); err != nil {
		return protocol.ClientNotice{}, err
	}
	return protocol.ClientNotice{Action: action}, nil
}

// MarshalResize encodes m into a Resize message payload.
func MarshalResize(m protocol.Resize) ([]byte, error) {
	if err := protocol.ValidateGeometry(domain.Geometry{Size: m.Size, PixelWidth: m.PixelWidth, PixelHeight: m.PixelHeight}); err != nil {
		return nil, err
	}
	w := payloadWriter{}
	w.putUint16(uint16(m.Size.Cols))
	w.putUint16(uint16(m.Size.Rows))
	w.putUint16(uint16(m.PixelWidth))
	w.putUint16(uint16(m.PixelHeight))
	return w.b, nil
}

// MarshalTheme encodes m into a 57-byte fixed-width Theme message payload.
func MarshalTheme(m protocol.Theme) []byte {
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
func UnmarshalTheme(b []byte) (protocol.Theme, error) {
	r := payloadReader{b: b}
	flags, err := r.getUint8()
	if err != nil {
		return protocol.Theme{}, err
	}
	if flags&^uint8(0x1f) != 0 {
		return protocol.Theme{}, errInvalidEnum
	}
	fgR, err := r.getUint8()
	if err != nil {
		return protocol.Theme{}, err
	}
	fgG, err := r.getUint8()
	if err != nil {
		return protocol.Theme{}, err
	}
	fgB, err := r.getUint8()
	if err != nil {
		return protocol.Theme{}, err
	}
	bgR, err := r.getUint8()
	if err != nil {
		return protocol.Theme{}, err
	}
	bgG, err := r.getUint8()
	if err != nil {
		return protocol.Theme{}, err
	}
	bgB, err := r.getUint8()
	if err != nil {
		return protocol.Theme{}, err
	}
	paletteKnown, err := r.getUint16()
	if err != nil {
		return protocol.Theme{}, err
	}

	m := protocol.Theme{
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
			return protocol.Theme{}, err
		}
		if m.Palette[i].G, err = r.getUint8(); err != nil {
			return protocol.Theme{}, err
		}
		if m.Palette[i].B, err = r.getUint8(); err != nil {
			return protocol.Theme{}, err
		}
	}
	if err := r.done(); err != nil {
		return protocol.Theme{}, err
	}
	return m, nil
}

// UnmarshalResize decodes a Resize message payload.
func UnmarshalResize(b []byte) (protocol.Resize, error) {
	r := payloadReader{b: b}
	cols, err := r.getUint16()
	if err != nil {
		return protocol.Resize{}, err
	}
	rows, err := r.getUint16()
	if err != nil {
		return protocol.Resize{}, err
	}
	pixelWidth, err := r.getUint16()
	if err != nil {
		return protocol.Resize{}, err
	}
	pixelHeight, err := r.getUint16()
	if err != nil {
		return protocol.Resize{}, err
	}
	if err := r.done(); err != nil {
		return protocol.Resize{}, err
	}
	m := protocol.Resize{
		Size:        domain.Size{Cols: int(cols), Rows: int(rows)},
		PixelWidth:  int(pixelWidth),
		PixelHeight: int(pixelHeight),
	}
	if err := protocol.ValidateGeometry(domain.Geometry{Size: m.Size, PixelWidth: m.PixelWidth, PixelHeight: m.PixelHeight}); err != nil {
		return protocol.Resize{}, err
	}
	return m, nil
}

// MarshalDetach encodes a Detach message payload (always empty).
func MarshalDetach(protocol.Detach) []byte {
	return nil
}

// UnmarshalDetach decodes a Detach message payload.
func UnmarshalDetach(b []byte) (protocol.Detach, error) {
	r := payloadReader{b: b}
	if err := r.done(); err != nil {
		return protocol.Detach{}, err
	}
	return protocol.Detach{}, nil
}

// MarshalPing encodes a Ping message payload (always empty).
func MarshalPing(protocol.Ping) []byte {
	return nil
}

// UnmarshalPing decodes a Ping message payload.
func UnmarshalPing(b []byte) (protocol.Ping, error) {
	r := payloadReader{b: b}
	if err := r.done(); err != nil {
		return protocol.Ping{}, err
	}
	return protocol.Ping{}, nil
}

// MarshalPong encodes a Pong message payload (always empty).
func MarshalPong(protocol.Pong) []byte {
	return nil
}

// UnmarshalPong decodes a Pong message payload.
func UnmarshalPong(b []byte) (protocol.Pong, error) {
	r := payloadReader{b: b}
	if err := r.done(); err != nil {
		return protocol.Pong{}, err
	}
	return protocol.Pong{}, nil
}

// MarshalAck encodes m into an epoch/state Ack payload.
func MarshalAck(m protocol.Ack) ([]byte, error) {
	if err := protocol.ValidateAck(m); err != nil {
		return nil, err
	}
	w := payloadWriter{}
	w.putUint64(m.Epoch)
	w.putUint64(m.State)
	return w.b, nil
}

// UnmarshalAck decodes an epoch/state Ack payload.
func UnmarshalAck(b []byte) (protocol.Ack, error) {
	r := payloadReader{b: b}
	var m protocol.Ack
	var err error
	if m.Epoch, err = r.getUint64(); err != nil {
		return protocol.Ack{}, err
	}
	if m.State, err = r.getUint64(); err != nil {
		return protocol.Ack{}, err
	}
	if err := r.done(); err != nil {
		return protocol.Ack{}, err
	}
	if err := protocol.ValidateAck(m); err != nil {
		return protocol.Ack{}, err
	}
	return m, nil
}

// MarshalWelcome encodes m into a Welcome message payload.
func MarshalWelcome(m protocol.Welcome) []byte {
	if m.CommittedIdentity != nil && (m.SessionName != m.CommittedIdentity.Target.SessionName || m.Ephemeral != m.CommittedIdentity.Ephemeral) {
		return nil
	}
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
	if !marshalCommittedIdentitySection(&w, m.CommittedIdentity) {
		return nil
	}
	return w.b
}

// UnmarshalWelcome decodes a Welcome message payload.
func UnmarshalWelcome(b []byte) (protocol.Welcome, error) {
	r := payloadReader{b: b}
	var m protocol.Welcome
	var err error

	if m.SessionID, err = r.getString(); err != nil {
		return protocol.Welcome{}, err
	}
	if m.SessionName, err = r.getString(); err != nil {
		return protocol.Welcome{}, err
	}
	var ephemeral bool
	if ephemeral, err = r.getBool(); err != nil {
		return protocol.Welcome{}, err
	}
	m.Ephemeral = ephemeral
	if m.ResumeToken, err = r.getUint64(); err != nil {
		return protocol.Welcome{}, err
	}
	if m.Capabilities, err = r.getUint32(); err != nil {
		return protocol.Welcome{}, err
	}
	if m.CommittedIdentity, err = unmarshalCommittedIdentitySection(&r); err != nil {
		return protocol.Welcome{}, err
	}
	if m.CommittedIdentity != nil && (m.CommittedIdentity.Target.SessionName != m.SessionName || m.CommittedIdentity.Ephemeral != m.Ephemeral) {
		return protocol.Welcome{}, fmt.Errorf("%w: committed identity does not match welcome", protocol.ErrInvalidRouteWire)
	}
	if err := r.done(); err != nil {
		return protocol.Welcome{}, err
	}
	return m, nil
}

// MarshalErrorMsg encodes m into an ErrorMsg message payload.
func MarshalErrorMsg(m protocol.ErrorMsg) []byte {
	w := payloadWriter{}
	w.putUint16(m.Code)
	w.putString(m.Text)
	return w.b
}

// UnmarshalErrorMsg decodes an ErrorMsg message payload.
func UnmarshalErrorMsg(b []byte) (protocol.ErrorMsg, error) {
	r := payloadReader{b: b}
	var m protocol.ErrorMsg
	var err error

	if m.Code, err = r.getUint16(); err != nil {
		return protocol.ErrorMsg{}, err
	}
	if m.Text, err = r.getString(); err != nil {
		return protocol.ErrorMsg{}, err
	}
	if err := r.done(); err != nil {
		return protocol.ErrorMsg{}, err
	}
	return m, nil
}

// MarshalOutput encodes m into the epoch/base/new/echo/viewRevision/
// size/full/compression/decoded-length/data message layout. Compression is
// limited to large, full snapshots and is retained only when it saves bytes.
func MarshalOutput(m protocol.Output) ([]byte, error) {
	if err := protocol.ValidateOutput(m); err != nil {
		return nil, err
	}
	kind, data, err := compressOutputData(m)
	if err != nil {
		return nil, err
	}
	w := payloadWriter{}
	w.putUint64(m.Epoch)
	w.putUint64(m.Base)
	w.putUint64(m.New)
	w.putUint64(m.Echo)
	w.putUint64(m.ViewRevision)
	w.putUint16(uint16(m.Size.Cols))
	w.putUint16(uint16(m.Size.Rows))
	w.putBool(m.Full)
	w.putBool(m.Context != nil)
	if m.Context != nil {
		if err := marshalViewContext(&w, *m.Context); err != nil {
			return nil, err
		}
	}
	w.putUint8(kind)
	w.putUint32(uint32(len(m.Data)))
	w.putLongBytes(data)
	return w.b, nil
}

func compressOutputData(m protocol.Output) (byte, []byte, error) {
	if !m.Full || len(m.Data) < outputCompressionThreshold {
		return outputCompressionNone, m.Data, nil
	}
	var compressed bytes.Buffer
	writer := outputCompressorPool.Get().(*zlib.Writer)
	writer.Reset(&compressed)
	defer func() {
		writer.Reset(io.Discard)
		outputCompressorPool.Put(writer)
	}()
	if _, err := writer.Write(m.Data); err != nil {
		return 0, nil, err
	}
	if err := writer.Close(); err != nil {
		return 0, nil, err
	}
	if compressed.Len()+outputCompressionHeaderLen >= len(m.Data) {
		return outputCompressionNone, m.Data, nil
	}
	return outputCompressionZlib, compressed.Bytes(), nil
}

// UnmarshalOutput strictly decodes one final Output payload. Header semantics
// are validated before its bounded decoded data allocation.
func UnmarshalOutput(b []byte) (protocol.Output, error) {
	if len(b) > MaxFrameLen-1 {
		return protocol.Output{}, protocol.ErrInvalidOutput
	}
	if len(b) < outputPayloadOverhead {
		return protocol.Output{}, errShortPayload
	}
	r := payloadReader{b: b}
	var m protocol.Output
	var err error
	if m.Epoch, err = r.getUint64(); err != nil {
		return protocol.Output{}, err
	}
	if m.Base, err = r.getUint64(); err != nil {
		return protocol.Output{}, err
	}
	if m.New, err = r.getUint64(); err != nil {
		return protocol.Output{}, err
	}
	if m.Echo, err = r.getUint64(); err != nil {
		return protocol.Output{}, err
	}
	if m.ViewRevision, err = r.getUint64(); err != nil {
		return protocol.Output{}, err
	}
	cols, err := r.getUint16()
	if err != nil {
		return protocol.Output{}, err
	}
	rows, err := r.getUint16()
	if err != nil {
		return protocol.Output{}, err
	}
	m.Size = domain.Size{Cols: int(cols), Rows: int(rows)}
	if m.Full, err = r.getBool(); err != nil {
		return protocol.Output{}, err
	}
	hasContext, err := r.getBool()
	if err != nil {
		return protocol.Output{}, err
	}
	if hasContext {
		context, contextErr := unmarshalViewContext(&r)
		if contextErr != nil {
			return protocol.Output{}, contextErr
		}
		m.Context = &context
	}
	if err := protocol.ValidateOutput(m); err != nil {
		return protocol.Output{}, err
	}
	kind, err := r.getUint8()
	if err != nil {
		return protocol.Output{}, err
	}
	decodedLen, err := r.getUint32()
	if err != nil {
		return protocol.Output{}, err
	}
	if int64(decodedLen) > int64(protocol.MaxOutputDataLen) {
		return protocol.Output{}, protocol.ErrInvalidOutput
	}
	data, err := r.getLongBytes()
	if err != nil {
		return protocol.Output{}, err
	}
	if err := r.done(); err != nil {
		return protocol.Output{}, err
	}
	switch kind {
	case outputCompressionNone:
		if uint32(len(data)) != decodedLen {
			return protocol.Output{}, protocol.ErrInvalidOutput
		}
		m.Data = data
	case outputCompressionZlib:
		if !m.Full {
			return protocol.Output{}, protocol.ErrInvalidOutput
		}
		m.Data, err = decompressOutputData(data, int(decodedLen))
		if err != nil {
			return protocol.Output{}, protocol.ErrInvalidOutput
		}
	default:
		return protocol.Output{}, errInvalidEnum
	}
	if err := protocol.ValidateOutput(m); err != nil {
		return protocol.Output{}, err
	}
	return m, nil
}

func decompressOutputData(data []byte, decodedLen int) ([]byte, error) {
	source := bytes.NewReader(data)
	reader, err := zlib.NewReader(source)
	if err != nil {
		return nil, err
	}
	decoded := make([]byte, decodedLen)
	if _, err := io.ReadFull(reader, decoded); err != nil {
		_ = reader.Close()
		return nil, err
	}
	var extra [1]byte
	if n, err := reader.Read(extra[:]); n != 0 || err != io.EOF {
		_ = reader.Close()
		if err == nil {
			return nil, errors.New("ports: compressed output exceeds declared length")
		}
		return nil, err
	}
	if err := reader.Close(); err != nil {
		return nil, err
	}
	if source.Len() != 0 {
		return nil, errors.New("ports: compressed output has trailing bytes")
	}
	return decoded, nil
}

// MarshalAttachTarget encodes a strict server attach-target payload.
func MarshalAttachTarget(m protocol.AttachTarget) []byte {
	if err := protocol.ValidateAttachTarget(m); err != nil {
		return nil
	}
	w := payloadWriter{}
	w.putUint64(m.RequestID)
	w.putString(m.Endpoint)
	w.putString(m.Session)
	w.putUint8(m.Intent)
	marshalRemoteTargetSection(&w, m.RemoteTarget, m.EnvironmentPolicy)
	marshalExactTargetSection(&w, m.ExactTarget)
	w.putBool(m.SamePeer)
	w.putString(string(m.PreferredTabID))
	w.putUint64(m.CauseActionID)
	return w.b
}

// UnmarshalAttachTarget decodes a strict server attach-target payload.
func UnmarshalAttachTarget(b []byte) (protocol.AttachTarget, error) {
	// Preflight lengths and intent before getString can allocate either value.
	probe := payloadReader{b: b}
	if _, err := probe.getUint64(); err != nil {
		return protocol.AttachTarget{}, protocol.ErrInvalidAttachTarget
	}
	endpointLen, err := probe.getUint16()
	if err != nil || int(endpointLen) > len(probe.b) {
		return protocol.AttachTarget{}, protocol.ErrInvalidAttachTarget
	}
	probe.b = probe.b[endpointLen:]
	sessionLen, err := probe.getUint16()
	if err != nil || sessionLen == 0 || int(sessionLen) > len(probe.b) {
		return protocol.AttachTarget{}, protocol.ErrInvalidAttachTarget
	}
	probe.b = probe.b[sessionLen:]
	intent, err := probe.getUint8()
	if err != nil || (intent != protocol.IntentEphemeral && intent != protocol.IntentNew && intent != protocol.IntentAttach && intent != protocol.IntentResume) {
		return protocol.AttachTarget{}, protocol.ErrInvalidAttachTarget
	}
	if err := skipRemoteTargetSection(&probe); err != nil {
		return protocol.AttachTarget{}, protocol.ErrInvalidAttachTarget
	}
	if err := skipExactTargetSection(&probe); err != nil {
		return protocol.AttachTarget{}, protocol.ErrInvalidAttachTarget
	}
	if _, err := probe.getBool(); err != nil {
		return protocol.AttachTarget{}, protocol.ErrInvalidAttachTarget
	}
	if err := probe.skipString(); err != nil {
		return protocol.AttachTarget{}, protocol.ErrInvalidAttachTarget
	}
	if _, err := probe.getUint64(); err != nil {
		return protocol.AttachTarget{}, protocol.ErrInvalidAttachTarget
	}
	if err := probe.done(); err != nil {
		return protocol.AttachTarget{}, protocol.ErrInvalidAttachTarget
	}

	r := payloadReader{b: b}
	var m protocol.AttachTarget
	if m.RequestID, err = r.getUint64(); err != nil {
		return protocol.AttachTarget{}, err
	}
	if m.Endpoint, err = r.getString(); err != nil {
		return protocol.AttachTarget{}, err
	}
	if m.Session, err = r.getString(); err != nil {
		return protocol.AttachTarget{}, err
	}
	if m.Intent, err = r.getUint8(); err != nil {
		return protocol.AttachTarget{}, err
	}
	if m.RemoteTarget, m.EnvironmentPolicy, err = unmarshalRemoteTargetSection(&r); err != nil {
		return protocol.AttachTarget{}, err
	}
	if m.ExactTarget, err = unmarshalExactTargetSection(&r); err != nil {
		return protocol.AttachTarget{}, err
	}
	if m.SamePeer, err = r.getBool(); err != nil {
		return protocol.AttachTarget{}, err
	}
	preferredTabID, err := r.getString()
	if err != nil {
		return protocol.AttachTarget{}, err
	}
	m.PreferredTabID = domain.TabStableID(preferredTabID)
	if m.CauseActionID, err = r.getUint64(); err != nil {
		return protocol.AttachTarget{}, err
	}
	if err := r.done(); err != nil {
		return protocol.AttachTarget{}, err
	}
	if err := protocol.ValidateAttachTarget(m); err != nil {
		return protocol.AttachTarget{}, err
	}
	return m, nil
}

// MarshalDetached encodes m into a Detached message payload.
func MarshalDetached(m protocol.Detached) []byte {
	return []byte{m.Reason}
}

// UnmarshalDetached decodes a Detached message payload.
func UnmarshalDetached(b []byte) (protocol.Detached, error) {
	r := payloadReader{b: b}
	reason, err := r.getUint8()
	if err != nil {
		return protocol.Detached{}, err
	}
	if protocol.ValidateDetached(protocol.Detached{Reason: reason}) != nil {
		return protocol.Detached{}, errInvalidEnum
	}
	if err := r.done(); err != nil {
		return protocol.Detached{}, err
	}
	return protocol.Detached{Reason: reason}, nil
}

// MarshalList encodes a List message payload (always empty).
func MarshalList(protocol.List) []byte {
	return nil
}

// UnmarshalList decodes a List message payload.
func UnmarshalList(b []byte) (protocol.List, error) {
	r := payloadReader{b: b}
	if err := r.done(); err != nil {
		return protocol.List{}, err
	}
	return protocol.List{}, nil
}

// MarshalOutputResetRequest encodes an OutputResetRequest payload (always
// empty).
func MarshalOutputResetRequest(protocol.OutputResetRequest) []byte {
	return nil
}

// UnmarshalOutputResetRequest decodes a strict empty OutputResetRequest
// payload.
func UnmarshalOutputResetRequest(b []byte) (protocol.OutputResetRequest, error) {
	r := payloadReader{b: b}
	if err := r.done(); err != nil {
		return protocol.OutputResetRequest{}, err
	}
	return protocol.OutputResetRequest{}, nil
}

// MarshalKill encodes m into a Kill message payload.
func MarshalKill(m protocol.Kill) []byte {
	w := payloadWriter{}
	w.putString(m.Name)
	if m.Scope != protocol.KillSession {
		w.putUint8(uint8(m.Scope))
	}
	return w.b
}

// UnmarshalKill decodes a Kill message payload.
func UnmarshalKill(b []byte) (protocol.Kill, error) {
	r := payloadReader{b: b}
	name, err := r.getString()
	if err != nil {
		return protocol.Kill{}, err
	}
	var scope protocol.KillScope
	if len(r.b) > 0 {
		value, readErr := r.getUint8()
		if readErr != nil {
			return protocol.Kill{}, readErr
		}
		scope = protocol.KillScope(value)
		if scope > protocol.KillAll {
			return protocol.Kill{}, errInvalidEnum
		}
	}
	if err := r.done(); err != nil {
		return protocol.Kill{}, err
	}
	return protocol.Kill{Name: name, Scope: scope}, nil
}

// MarshalSessions encodes m into a Sessions message payload: a uint16 count
// followed by that many session records.
func MarshalSessions(m protocol.Sessions) []byte {
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

// A session record needs two empty length-prefixed strings, two flags, a tab
// count, and a state byte before either string contributes data.
const sessionRecordMinLen = 9

// UnmarshalSessions decodes a Sessions message payload.
func UnmarshalSessions(b []byte) (protocol.Sessions, error) {
	r := payloadReader{b: b}
	count, err := r.getUint16()
	if err != nil {
		return protocol.Sessions{}, err
	}
	if int(count) > len(r.b)/sessionRecordMinLen {
		return protocol.Sessions{}, errShortPayload
	}
	sessions := make([]protocol.SessionInfo, 0, count)
	for range int(count) {
		var s protocol.SessionInfo
		if s.SessionID, err = r.getString(); err != nil {
			return protocol.Sessions{}, err
		}
		if s.Name, err = r.getString(); err != nil {
			return protocol.Sessions{}, err
		}
		eph, err := r.getBool()
		if err != nil {
			return protocol.Sessions{}, err
		}
		s.Ephemeral = eph
		if s.Tabs, err = r.getUint16(); err != nil {
			return protocol.Sessions{}, err
		}
		att, err := r.getBool()
		if err != nil {
			return protocol.Sessions{}, err
		}
		s.Attached = att
		state, err := r.getUint8()
		if err != nil {
			return protocol.Sessions{}, err
		}
		s.State = protocol.SessionState(state)
		if s.State > protocol.SessionBroken {
			return protocol.Sessions{}, errors.New("ports: invalid session state")
		}
		sessions = append(sessions, s)
	}
	if err := r.done(); err != nil {
		return protocol.Sessions{}, err
	}
	return protocol.Sessions{Sessions: sessions}, nil
}
func putInt16(w *payloadWriter, n int) { w.putUint16(uint16(int16(n))) }

func getInt16(r *payloadReader) (int, error) {
	n, err := r.getUint16()
	return int(int16(n)), err
}

func putRGB(w *payloadWriter, c renderer.RGB) {
	w.putUint8(c.R)
	w.putUint8(c.G)
	w.putUint8(c.B)
}

func getRGB(r *payloadReader) (renderer.RGB, error) {
	red, err := r.getUint8()
	if err != nil {
		return renderer.RGB{}, err
	}
	green, err := r.getUint8()
	if err != nil {
		return renderer.RGB{}, err
	}
	blue, err := r.getUint8()
	if err != nil {
		return renderer.RGB{}, err
	}
	return renderer.RGB{R: red, G: green, B: blue}, nil
}

func putPreviewStyle(w *payloadWriter, s renderer.Style) {
	var flags uint8
	if s.Bold {
		flags |= 1 << 0
	}
	if s.Italic {
		flags |= 1 << 1
	}
	if s.Inverse {
		flags |= 1 << 2
	}
	if s.HasForegroundRGB {
		flags |= 1 << 3
	}
	if s.HasBackgroundRGB {
		flags |= 1 << 4
	}
	if s.HasUnderlineColor {
		flags |= 1 << 5
	}
	if s.HasUnderlineColorRGB {
		flags |= 1 << 6
	}
	w.putUint8(flags)
	w.putUint16(uint16(s.Attrs))
	putInt16(w, s.Foreground)
	putInt16(w, s.Background)
	w.putUint8(uint8(s.UnderlineStyle))
	putInt16(w, s.UnderlineColor)
	putRGB(w, s.ForegroundRGB)
	putRGB(w, s.BackgroundRGB)
	putRGB(w, s.UnderlineColorRGB)
}

func getPreviewStyle(r *payloadReader) (renderer.Style, error) {
	flags, err := r.getUint8()
	if err != nil {
		return renderer.Style{}, err
	}
	if flags&0x80 != 0 {
		return renderer.Style{}, protocol.ErrRemotePreviewUnsupportedStyle
	}
	attrs, err := r.getUint16()
	if err != nil {
		return renderer.Style{}, err
	}
	fg, err := getInt16(r)
	if err != nil {
		return renderer.Style{}, err
	}
	bg, err := getInt16(r)
	if err != nil {
		return renderer.Style{}, err
	}
	ul, err := r.getUint8()
	if err != nil {
		return renderer.Style{}, err
	}
	ulc, err := getInt16(r)
	if err != nil {
		return renderer.Style{}, err
	}
	fgrgb, err := getRGB(r)
	if err != nil {
		return renderer.Style{}, err
	}
	bgrgb, err := getRGB(r)
	if err != nil {
		return renderer.Style{}, err
	}
	ulrgb, err := getRGB(r)
	if err != nil {
		return renderer.Style{}, err
	}
	return renderer.Style{Bold: flags&1 != 0, Italic: flags&(1<<1) != 0, Inverse: flags&(1<<2) != 0,
		HasForegroundRGB: flags&(1<<3) != 0, HasBackgroundRGB: flags&(1<<4) != 0,
		HasUnderlineColor: flags&(1<<5) != 0, HasUnderlineColorRGB: flags&(1<<6) != 0,
		Attrs: renderer.StyleAttrs(attrs), Foreground: fg, Background: bg, UnderlineStyle: renderer.UnderlineStyle(ul), UnderlineColor: ulc,
		ForegroundRGB: fgrgb, BackgroundRGB: bgrgb, UnderlineColorRGB: ulrgb}, nil
}

func MarshalRemotePreviewRequest(request protocol.RemotePreviewRequest) []byte {
	if protocol.ValidateRemotePreviewRequest(request) != nil {
		return nil
	}
	w := payloadWriter{}
	w.putUint16(request.Version)
	w.putBytes(request.Target.LifecycleID[:])
	w.putString(request.Target.Endpoint)
	w.putString(request.Target.DisplayOrigin)
	w.putString(request.Target.SessionName)
	w.putString(string(request.Target.LiveTabID))
	w.putUint16(request.Width)
	w.putUint16(request.Height)
	return w.b
}

func UnmarshalRemotePreviewRequest(data []byte) (protocol.RemotePreviewRequest, error) {
	if len(data) > protocol.RemotePreviewMaxBytes {
		return protocol.RemotePreviewRequest{}, protocol.ErrInvalidRemotePreviewRequest
	}
	r := payloadReader{b: data}
	var q protocol.RemotePreviewRequest
	var err error
	if q.Version, err = r.getUint16(); err != nil {
		return q, err
	}
	id, err := r.getBytes(16)
	if err != nil {
		return q, err
	}
	copy(q.Target.LifecycleID[:], id)
	if q.Target.Endpoint, err = r.getString(); err != nil {
		return q, err
	}
	if q.Target.DisplayOrigin, err = r.getString(); err != nil {
		return q, err
	}
	if q.Target.SessionName, err = r.getString(); err != nil {
		return q, err
	}
	tab, err := r.getString()
	if err != nil {
		return q, err
	}
	q.Target.LiveTabID = domain.TabStableID(tab)
	if q.Width, err = r.getUint16(); err != nil {
		return q, err
	}
	if q.Height, err = r.getUint16(); err != nil {
		return q, err
	}
	if err := r.done(); err != nil {
		return q, err
	}
	if err := protocol.ValidateRemotePreviewRequest(q); err != nil {
		return q, err
	}
	return q, nil
}

const previewCellWireSize = 4 + 1 + 1 + 2 + 2 + 2 + 1 + 2 + 3 + 3 + 3

func MarshalRemotePreview(preview protocol.RemotePreview) []byte {
	if protocol.ValidateRemotePreview(preview) != nil {
		return nil
	}
	w := payloadWriter{}
	w.putUint16(preview.Version)
	w.putUint8(uint8(preview.Status))
	w.putBytes(preview.LifecycleID[:])
	w.putString(string(preview.TabID))
	w.putUint64(preview.Revision)
	w.putUint16(preview.Width)
	w.putUint16(preview.Height)
	w.putUint32(uint32(len(preview.Cells)))
	for _, cell := range preview.Cells {
		var flags uint8
		if cell.Continuation {
			flags = 1
		}
		w.putUint32(uint32(cell.Rune))
		w.putUint8(flags)
		putPreviewStyle(&w, cell.Style)
	}
	return w.b
}

func UnmarshalRemotePreview(data []byte) (protocol.RemotePreview, error) {
	if len(data) > protocol.RemotePreviewMaxBytes {
		return protocol.RemotePreview{}, protocol.ErrRemotePreviewTooLarge
	}
	r := payloadReader{b: data}
	var p protocol.RemotePreview
	var err error
	if p.Version, err = r.getUint16(); err != nil {
		return p, err
	}
	status, err := r.getUint8()
	if err != nil {
		return p, err
	}
	p.Status = protocol.RemotePreviewStatus(status)
	id, err := r.getBytes(16)
	if err != nil {
		return p, err
	}
	copy(p.LifecycleID[:], id)
	tab, err := r.getString()
	if err != nil {
		return p, err
	}
	p.TabID = domain.TabStableID(tab)
	if p.Revision, err = r.getUint64(); err != nil {
		return p, err
	}
	if p.Width, err = r.getUint16(); err != nil {
		return p, err
	}
	if p.Height, err = r.getUint16(); err != nil {
		return p, err
	}
	count, err := r.getUint32()
	if err != nil {
		return p, err
	}
	if count > protocol.RemotePreviewMaxCells || uint64(count) > uint64(len(r.b)/previewCellWireSize) {
		return p, protocol.ErrRemotePreviewTooLarge
	}
	if count != 0 {
		p.Cells = make([]renderer.Cell, 0, int(count))
		for range int(count) {
			runeValue, e := r.getUint32()
			if e != nil {
				return p, e
			}
			flags, e := r.getUint8()
			if e != nil {
				return p, e
			}
			if flags&^uint8(1) != 0 {
				return p, protocol.ErrInvalidRemotePreview
			}
			style, e := getPreviewStyle(&r)
			if e != nil {
				return p, e
			}
			p.Cells = append(p.Cells, renderer.Cell{Rune: rune(runeValue), Continuation: flags&1 != 0, Style: style})
		}
	}
	if err := r.done(); err != nil {
		return p, err
	}
	if err := protocol.ValidateRemotePreview(p); err != nil {
		return p, err
	}
	return p, nil
}

// MarshalNavigationDirective encodes one bounded navigation directive.
func MarshalNavigationDirective(directive protocol.NavigationDirective) []byte {
	if directive.Action != protocol.NavigationOpenHomePicker && directive.Action != protocol.NavigationBack {
		return nil
	}
	if directive.Action == protocol.NavigationOpenHomePicker && directive.LeaseID.IsZero() || directive.Action == protocol.NavigationBack && !directive.LeaseID.IsZero() {
		return nil
	}
	w := payloadWriter{}
	w.putUint8(uint8(directive.Action))
	w.putBytes(directive.LeaseID[:])
	w.putUint64(directive.CauseActionID)
	return w.b
}

// UnmarshalNavigationDirective decodes one strict navigation directive.
func UnmarshalNavigationDirective(b []byte) (protocol.NavigationDirective, error) {
	r := payloadReader{b: b}
	value, err := r.getUint8()
	if err != nil {
		return protocol.NavigationDirective{}, protocol.ErrInvalidNavigation
	}
	lease, err := r.getBytes(len(protocol.ParkedRouteLeaseID{}))
	if err != nil {
		return protocol.NavigationDirective{}, protocol.ErrInvalidNavigation
	}
	causeActionID, err := r.getUint64()
	if err != nil {
		return protocol.NavigationDirective{}, protocol.ErrInvalidNavigation
	}
	if err := r.done(); err != nil {
		return protocol.NavigationDirective{}, protocol.ErrInvalidNavigation
	}
	directive := protocol.NavigationDirective{Action: protocol.NavigationAction(value), CauseActionID: causeActionID}
	copy(directive.LeaseID[:], lease)
	if MarshalNavigationDirective(directive) == nil {
		return protocol.NavigationDirective{}, protocol.ErrInvalidNavigation
	}
	return directive, nil
}

// ValidateParkedRouteRequest enforces the closed action/target shape before a
// request reaches either side's route state machine.

// MarshalParkedRouteRequest encodes one retained-route operation.
func MarshalParkedRouteRequest(request protocol.ParkedRouteRequest) []byte {
	if protocol.ValidateParkedRouteRequest(request) != nil {
		return nil
	}
	w := payloadWriter{}
	w.putUint64(request.RequestID)
	w.putBytes(request.LeaseID[:])
	w.putUint8(uint8(request.Action))
	marshalRemoteTargetSection(&w, request.Target, protocol.EnvironmentPolicyDaemonOwned)
	return w.b
}

// UnmarshalParkedRouteRequest decodes one strict retained-route operation.
func UnmarshalParkedRouteRequest(b []byte) (protocol.ParkedRouteRequest, error) {
	r := payloadReader{b: b}
	requestID, err := r.getUint64()
	if err != nil {
		return protocol.ParkedRouteRequest{}, protocol.ErrInvalidNavigation
	}
	lease, err := r.getBytes(len(protocol.ParkedRouteLeaseID{}))
	if err != nil {
		return protocol.ParkedRouteRequest{}, protocol.ErrInvalidNavigation
	}
	action, err := r.getUint8()
	if err != nil {
		return protocol.ParkedRouteRequest{}, protocol.ErrInvalidNavigation
	}
	target, policy, err := unmarshalRemoteTargetSection(&r)
	if err != nil || policy != protocol.EnvironmentPolicyDaemonOwned || r.done() != nil {
		return protocol.ParkedRouteRequest{}, protocol.ErrInvalidNavigation
	}
	request := protocol.ParkedRouteRequest{RequestID: requestID, Action: protocol.ParkedRouteAction(action), Target: target}
	copy(request.LeaseID[:], lease)
	if protocol.ValidateParkedRouteRequest(request) != nil {
		return protocol.ParkedRouteRequest{}, protocol.ErrInvalidNavigation
	}
	return request, nil
}

// MarshalParkedRouteResponse encodes one correlated retained-route outcome.
func MarshalParkedRouteResponse(response protocol.ParkedRouteResponse) []byte {
	if response.RequestID == 0 || response.Status < protocol.ParkedRouteReady || response.Status > protocol.ParkedRouteStaleTarget {
		return nil
	}
	w := payloadWriter{}
	w.putUint64(response.RequestID)
	w.putUint8(uint8(response.Status))
	return w.b
}

// UnmarshalParkedRouteResponse decodes one strict retained-route outcome.
func UnmarshalParkedRouteResponse(b []byte) (protocol.ParkedRouteResponse, error) {
	r := payloadReader{b: b}
	requestID, err := r.getUint64()
	if err != nil {
		return protocol.ParkedRouteResponse{}, protocol.ErrInvalidNavigation
	}
	status, err := r.getUint8()
	if err != nil || r.done() != nil {
		return protocol.ParkedRouteResponse{}, protocol.ErrInvalidNavigation
	}
	response := protocol.ParkedRouteResponse{RequestID: requestID, Status: protocol.ParkedRouteStatus(status)}
	if MarshalParkedRouteResponse(response) == nil {
		return protocol.ParkedRouteResponse{}, protocol.ErrInvalidNavigation
	}
	return response, nil
}
