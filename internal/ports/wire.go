// Wire codecs for vev's IPC message payloads. They live in ports (alongside
// Frame and the MsgType constants) because wire encoding is protocol
// surface, not I/O: everything here is pure, stdlib-only byte marshalling.
// Adapters keep what actually performs I/O (framing over a connection,
// listening on a socket) and import these types for the payloads they carry.

package ports

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
	outputHeaderLen            = 5*8 + 2*2 + 1
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

// ErrInvalidSize reports a terminal size that cannot be represented by the
// protocol or would exceed the bounded screen area.
var ErrInvalidSize = errors.New("ports: invalid terminal size")

// ErrInvalidHello reports a malformed Hello semantic value.
var ErrInvalidHello = errors.New("ports: invalid hello")

// ErrInvalidOutput reports a malformed Output semantic value.
var ErrInvalidOutput = errors.New("ports: invalid output")

// ErrInvalidAck reports a malformed Ack semantic value.
var ErrInvalidAck = errors.New("ports: invalid ack")

// ErrInvalidAttachTarget reports a malformed attach target.
var ErrInvalidAttachTarget = errors.New("ports: invalid attach target")

// ErrInvalidNavigation reports an unknown or impossible navigation value.
var ErrInvalidNavigation = errors.New("ports: invalid navigation")

// Intent values carried by Hello, describing what the client wants to do.
const (
	IntentEphemeral uint8 = 0
	IntentNew       uint8 = 1
	IntentAttach    uint8 = 2
	IntentResume    uint8 = 3
)

// EnvironmentPolicy controls which side owns the environment used for future
// PTY children. Direct CLI attaches keep the historical client-owned policy;
// picker handoffs use daemon ownership so a local environment cannot leak into
// a remote daemon.
type EnvironmentPolicy uint8

const (
	EnvironmentPolicyClientOwned EnvironmentPolicy = iota
	EnvironmentPolicyDaemonOwned
)

func validEnvironmentPolicy(policy EnvironmentPolicy) bool {
	return policy == EnvironmentPolicyClientOwned || policy == EnvironmentPolicyDaemonOwned
}

func validNavigationCapabilities(capabilities NavigationCapabilities) bool {
	return capabilities&^(NavigationCapabilityHomePicker|NavigationCapabilityBack) == 0
}

func validStartupOverlay(overlay StartupOverlay) bool {
	return overlay == StartupOverlayNone || overlay == StartupOverlaySessionPicker
}

// ValidateNavigation validates route capabilities independently of the rest of
// the Hello payload. homePickerRoute indicates that the route may open its
// home/session picker (for example, a remote target or daemon-owned route).
func ValidateNavigation(capabilities NavigationCapabilities, overlay StartupOverlay, homePickerRoute bool) error {
	if !validNavigationCapabilities(capabilities) || !validStartupOverlay(overlay) {
		return ErrInvalidNavigation
	}
	home := capabilities&NavigationCapabilityHomePicker != 0
	back := capabilities&NavigationCapabilityBack != 0
	if (home && !homePickerRoute) || back != (overlay == StartupOverlaySessionPicker) {
		return ErrInvalidNavigation
	}
	if homePickerRoute && back {
		return ErrInvalidNavigation
	}
	return nil
}

func validateHelloNavigation(h Hello) error {
	if h.Intent != IntentAttach && h.Intent != IntentResume {
		if h.NavigationCapabilities != 0 || h.StartupOverlay != StartupOverlayNone {
			return ErrInvalidNavigation
		}
		return nil
	}
	return ValidateNavigation(h.NavigationCapabilities, h.StartupOverlay, h.RemoteTarget != nil || h.EnvironmentPolicy == EnvironmentPolicyDaemonOwned)
}

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
	ClientID    [16]byte
	ResumeToken uint64
	Name        string
	Size        domain.Size
	PixelWidth  int
	PixelHeight int
	TermEnv     string
	Cwd         string
	TrueColor   bool
	// MaxOutputInFlight is the requested maximum number of unacknowledged
	// state-bearing output frames.
	MaxOutputInFlight uint8
	// Env is the complete exported client environment for future PTY children.
	// Entries are opaque strings so their ordering and contents are preserved.
	Env []string
	// RemoteTarget is present only for a picker-selected remote attach. It is
	// carried through reconnects and is validated by the owning daemon before
	// it creates or publishes any session resources.
	RemoteTarget *domain.RemoteSessionTarget
	// ExactTarget is an optional lifecycle/name identity selected by the client
	// ledger. It is transport-neutral and never replaces the daemon's authority
	// to validate or commit the target.
	ExactTarget *ExactSessionTarget
	// PreferredTabID is a client-owned route cursor. The daemon treats it as an
	// attachment-local hint and falls back when the stable tab no longer exists.
	PreferredTabID         domain.TabStableID
	EnvironmentPolicy      EnvironmentPolicy
	NavigationCapabilities NavigationCapabilities
	StartupOverlay         StartupOverlay
	// Remote identifies a direct remote carriage even when no exact picker
	// target is present. It is daemon-facing so remote rendering backends can
	// be disabled consistently for both direct and picker attaches.
	Remote bool
}

// Input carries raw bytes typed/pasted by the client, destined for the PTY.
type Input struct {
	InputSeq uint64
	Data     []byte
}

// Geometry returns the complete controlling-terminal geometry carried by the
// handshake. Pixel dimensions are optional as a pair.
func (h Hello) Geometry() domain.Geometry {
	return domain.Geometry{Size: h.Size, PixelWidth: h.PixelWidth, PixelHeight: h.PixelHeight}.NormalizePixels()
}

// Resize notifies the daemon of a client-side terminal geometry change.
type Resize struct {
	Size        domain.Size
	PixelWidth  int
	PixelHeight int
}

// Geometry returns the complete controlling-terminal geometry carried by the
// resize message. Pixel dimensions are optional as a pair.
func (m Resize) Geometry() domain.Geometry {
	return domain.Geometry{Size: m.Size, PixelWidth: m.PixelWidth, PixelHeight: m.PixelHeight}.NormalizePixels()
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

func validDetachedReason(reason uint8) bool {
	switch reason {
	case ReasonDetach, ReasonSessionKilled, ReasonServerShutdown, ReasonReplaced:
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

// Ack acknowledges receipt/application of an output state in one output epoch.
type Ack struct {
	Epoch uint64
	State uint64
}

// Welcome is the daemon's reply to a successful Hello.
type Welcome struct {
	SessionID         string
	SessionName       string
	Ephemeral         bool
	ResumeToken       uint64
	Capabilities      uint32
	CommittedIdentity *CommittedRouteIdentity
}

// ErrorMsg reports a protocol- or session-level failure to the client.
type ErrorMsg struct {
	Code uint16
	Text string
}

// Output carries one ordered output publication for an attachment.
type Output struct {
	Epoch        uint64
	Base         uint64
	New          uint64
	Echo         uint64
	ViewRevision uint64
	Size         domain.Size
	Full         bool
	Data         []byte
}

// AttachTarget identifies the endpoint and exact session/tab selected for a
// new attachment. Endpoint and Session remain the compatibility route fields;
// RemoteTarget is the authoritative picker identity when non-nil.
type AttachTarget struct {
	Endpoint          string
	Session           string
	Intent            uint8
	RemoteTarget      *domain.RemoteSessionTarget
	ExactTarget       *ExactSessionTarget
	EnvironmentPolicy EnvironmentPolicy
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
	SessionUp SessionState = iota
	SessionDown
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

// OutputResetRequest asks a remote output stream to rebase.
type OutputResetRequest struct{}

// SessionTabMeta describes one tab in a remote session's metadata snapshot.
// ID is stable for the lifetime of its remote tab and ordered only for display.
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
func marshalRemoteTargetSection(w *payloadWriter, target *domain.RemoteSessionTarget, policy EnvironmentPolicy) {
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
	if !validEnvironmentPolicy(EnvironmentPolicy(policy)) {
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

func unmarshalRemoteTargetSection(r *payloadReader) (*domain.RemoteSessionTarget, EnvironmentPolicy, error) {
	target, err := unmarshalRemoteTarget(r)
	if err != nil {
		return nil, 0, err
	}
	policy, err := r.getUint8()
	if err != nil {
		return nil, 0, err
	}
	if !validEnvironmentPolicy(EnvironmentPolicy(policy)) {
		return nil, 0, errInvalidEnum
	}
	return target, EnvironmentPolicy(policy), nil
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
func ValidateHello(h Hello) error {
	if h.Intent != IntentEphemeral && h.Intent != IntentNew && h.Intent != IntentAttach && h.Intent != IntentResume {
		return ErrInvalidHello
	}
	if err := ValidateGeometry(domain.Geometry{Size: h.Size, PixelWidth: h.PixelWidth, PixelHeight: h.PixelHeight}); err != nil {
		return fmt.Errorf("%w: geometry", ErrInvalidHello)
	}
	if !validEnvironmentPolicy(h.EnvironmentPolicy) {
		return ErrInvalidHello
	}
	if err := validateHelloNavigation(h); err != nil {
		return fmt.Errorf("%w: navigation: %w", ErrInvalidHello, err)
	}
	if len(h.Name) > math.MaxUint16 || len(h.TermEnv) > math.MaxUint16 || len(h.Cwd) > math.MaxUint16 || len(h.Env) > math.MaxUint32 {
		return ErrInvalidHello
	}
	for _, entry := range h.Env {
		if uint64(len(entry)) > math.MaxUint32 {
			return ErrInvalidHello
		}
	}
	if h.PreferredTabID != "" {
		if h.Intent != IntentAttach && h.Intent != IntentResume {
			return ErrInvalidHello
		}
		if err := domain.ValidateTabStableID(h.PreferredTabID); err != nil {
			return fmt.Errorf("%w: preferred tab: %v", ErrInvalidHello, err)
		}
	}
	if h.ExactTarget != nil {
		if h.Intent != IntentAttach && h.Intent != IntentResume {
			return ErrInvalidHello
		}
		if err := h.ExactTarget.Validate(); err != nil {
			return fmt.Errorf("%w: exact target: %v", ErrInvalidHello, err)
		}
		if h.Name != h.ExactTarget.SessionName {
			return ErrInvalidHello
		}
		if h.RemoteTarget != nil && (h.RemoteTarget.LifecycleID != h.ExactTarget.LifecycleID || h.RemoteTarget.SessionName != h.ExactTarget.SessionName) {
			return ErrInvalidHello
		}
	}
	if h.RemoteTarget == nil {
		if h.EnvironmentPolicy != EnvironmentPolicyClientOwned && h.EnvironmentPolicy != EnvironmentPolicyDaemonOwned {
			return ErrInvalidHello
		}
		return nil
	}
	if h.EnvironmentPolicy != EnvironmentPolicyDaemonOwned {
		return ErrInvalidHello
	}
	if h.Intent != IntentAttach && h.Intent != IntentResume {
		return ErrInvalidHello
	}
	if err := validateWireRemoteTarget(*h.RemoteTarget); err != nil {
		return fmt.Errorf("%w: remote target: %v", ErrInvalidHello, err)
	}
	if h.Name != h.RemoteTarget.SessionName {
		return ErrInvalidHello
	}
	return nil
}

// ValidateOutput validates the final output state before data allocation.
func ValidateOutput(m Output) error {
	if m.Epoch == 0 {
		return ErrInvalidOutput
	}
	if m.New == 0 {
		if m.Base != 0 || m.Full {
			return ErrInvalidOutput
		}
	} else if (m.Base == 0 && !m.Full) || (m.Base != 0 && (m.Full || m.New != m.Base+1)) {
		return ErrInvalidOutput
	}
	if err := ValidateSize(m.Size); err != nil {
		return fmt.Errorf("%w: size", ErrInvalidOutput)
	}
	if uint64(len(m.Data)) > math.MaxUint32 || len(m.Data) > MaxFrameLen-1-outputPayloadOverhead {
		return ErrInvalidOutput
	}
	return nil
}

// ValidateAck validates an epoch-scoped output acknowledgement.
func ValidateAck(m Ack) error {
	if m.Epoch == 0 {
		return ErrInvalidAck
	}
	return nil
}

// ValidateAttachTarget validates a client-owned route handoff. An empty
// Endpoint selects another session on the currently connected daemon; a
// non-empty Endpoint selects a discovered remote daemon.
func ValidateAttachTarget(m AttachTarget) error {
	if m.Session == "" || len(m.Endpoint) > math.MaxUint16 || len(m.Session) > math.MaxUint16 {
		return ErrInvalidAttachTarget
	}
	if err := domain.ValidateSessionName(m.Session); err != nil {
		return ErrInvalidAttachTarget
	}
	if m.Intent != IntentEphemeral && m.Intent != IntentNew && m.Intent != IntentAttach && m.Intent != IntentResume {
		return ErrInvalidAttachTarget
	}
	if m.Intent == IntentResume {
		return ErrInvalidAttachTarget
	}
	if !validEnvironmentPolicy(m.EnvironmentPolicy) {
		return ErrInvalidAttachTarget
	}
	if m.ExactTarget != nil && (m.ExactTarget.SessionName != m.Session || m.ExactTarget.Validate() != nil) {
		return ErrInvalidAttachTarget
	}
	if m.RemoteTarget == nil {
		return nil
	}
	if m.EnvironmentPolicy != EnvironmentPolicyDaemonOwned {
		return ErrInvalidAttachTarget
	}
	if m.Intent != IntentAttach {
		return ErrInvalidAttachTarget
	}
	if err := validateWireRemoteTarget(*m.RemoteTarget); err != nil {
		return fmt.Errorf("%w: remote target: %v", ErrInvalidAttachTarget, err)
	}
	if m.Endpoint != m.RemoteTarget.Endpoint || m.Session != m.RemoteTarget.SessionName {
		return ErrInvalidAttachTarget
	}
	return nil
}

func validateWireRemoteTarget(target domain.RemoteSessionTarget) error {
	if len(target.Endpoint) > math.MaxUint16 || len(target.DisplayOrigin) > math.MaxUint16 || len(target.SessionName) > math.MaxUint16 || len(target.LiveTabID) > math.MaxUint16 || len(target.StoppedTab.RawName) > math.MaxUint16 {
		return errors.New("remote target string too long")
	}
	return target.Validate()
}

// MarshalHello encodes h into a Hello message payload.
func MarshalHello(h Hello) []byte {
	if err := ValidateHello(h); err != nil {
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
	return w.b
}

func preflightHello(b []byte) error {
	r := payloadReader{b: b}
	version, err := r.getUint16()
	if err != nil {
		return err
	}
	if version == 21 {
		return ErrInvalidHello
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
	return r.done()
}

// UnmarshalHello decodes a Hello message payload.
func UnmarshalHello(b []byte) (Hello, error) {
	if len(b) > MaxFrameLen-1 {
		return Hello{}, ErrInvalidHello
	}
	if err := preflightHello(b); err != nil {
		return Hello{}, err
	}
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
	pixelWidth, err := r.getUint16()
	if err != nil {
		return Hello{}, err
	}
	pixelHeight, err := r.getUint16()
	if err != nil {
		return Hello{}, err
	}
	h.Size = domain.Size{Cols: int(cols), Rows: int(rows)}
	h.PixelWidth = int(pixelWidth)
	h.PixelHeight = int(pixelHeight)
	if h.TermEnv, err = r.getString(); err != nil {
		return Hello{}, err
	}
	if h.Cwd, err = r.getString(); err != nil {
		return Hello{}, err
	}
	if h.TrueColor, err = r.getBool(); err != nil {
		return Hello{}, err
	}
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
	if h.RemoteTarget, h.EnvironmentPolicy, err = unmarshalRemoteTargetSection(&r); err != nil {
		return Hello{}, err
	}
	if h.ExactTarget, err = unmarshalExactTargetSection(&r); err != nil {
		return Hello{}, err
	}
	preferredTabID, err := r.getString()
	if err != nil {
		return Hello{}, err
	}
	h.PreferredTabID = domain.TabStableID(preferredTabID)
	capabilities, err := r.getUint8()
	if err != nil {
		return Hello{}, err
	}
	h.NavigationCapabilities = NavigationCapabilities(capabilities)
	overlay, err := r.getUint8()
	if err != nil {
		return Hello{}, err
	}
	h.StartupOverlay = StartupOverlay(overlay)
	if h.Remote, err = r.getBool(); err != nil {
		return Hello{}, err
	}
	if err := r.done(); err != nil {
		return Hello{}, err
	}
	if err := ValidateHello(h); err != nil {
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
func MarshalResize(m Resize) ([]byte, error) {
	if err := ValidateGeometry(domain.Geometry{Size: m.Size, PixelWidth: m.PixelWidth, PixelHeight: m.PixelHeight}); err != nil {
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
	if flags&^uint8(0x1f) != 0 {
		return Theme{}, errInvalidEnum
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
	pixelWidth, err := r.getUint16()
	if err != nil {
		return Resize{}, err
	}
	pixelHeight, err := r.getUint16()
	if err != nil {
		return Resize{}, err
	}
	if err := r.done(); err != nil {
		return Resize{}, err
	}
	m := Resize{
		Size:        domain.Size{Cols: int(cols), Rows: int(rows)},
		PixelWidth:  int(pixelWidth),
		PixelHeight: int(pixelHeight),
	}
	if err := ValidateGeometry(domain.Geometry{Size: m.Size, PixelWidth: m.PixelWidth, PixelHeight: m.PixelHeight}); err != nil {
		return Resize{}, err
	}
	return m, nil
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

// MarshalAck encodes m into an epoch/state Ack payload.
func MarshalAck(m Ack) ([]byte, error) {
	if err := ValidateAck(m); err != nil {
		return nil, err
	}
	w := payloadWriter{}
	w.putUint64(m.Epoch)
	w.putUint64(m.State)
	return w.b, nil
}

// UnmarshalAck decodes an epoch/state Ack payload.
func UnmarshalAck(b []byte) (Ack, error) {
	r := payloadReader{b: b}
	var m Ack
	var err error
	if m.Epoch, err = r.getUint64(); err != nil {
		return Ack{}, err
	}
	if m.State, err = r.getUint64(); err != nil {
		return Ack{}, err
	}
	if err := r.done(); err != nil {
		return Ack{}, err
	}
	if err := ValidateAck(m); err != nil {
		return Ack{}, err
	}
	return m, nil
}

// MarshalWelcome encodes m into a Welcome message payload.
func MarshalWelcome(m Welcome) []byte {
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
	var ephemeral bool
	if ephemeral, err = r.getBool(); err != nil {
		return Welcome{}, err
	}
	m.Ephemeral = ephemeral
	if m.ResumeToken, err = r.getUint64(); err != nil {
		return Welcome{}, err
	}
	if m.Capabilities, err = r.getUint32(); err != nil {
		return Welcome{}, err
	}
	if m.CommittedIdentity, err = unmarshalCommittedIdentitySection(&r); err != nil {
		return Welcome{}, err
	}
	if m.CommittedIdentity != nil && (m.CommittedIdentity.Target.SessionName != m.SessionName || m.CommittedIdentity.Ephemeral != m.Ephemeral) {
		return Welcome{}, fmt.Errorf("%w: committed identity does not match welcome", ErrInvalidRouteWire)
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

// MarshalOutput encodes m into the epoch/base/new/echo/viewRevision/
// size/full/compression/decoded-length/data message layout. Compression is
// limited to large, full snapshots and is retained only when it saves bytes.
func MarshalOutput(m Output) ([]byte, error) {
	if err := ValidateOutput(m); err != nil {
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
	w.putUint8(kind)
	w.putUint32(uint32(len(m.Data)))
	w.putLongBytes(data)
	return w.b, nil
}

func compressOutputData(m Output) (byte, []byte, error) {
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
func UnmarshalOutput(b []byte) (Output, error) {
	if len(b) > MaxFrameLen-1 {
		return Output{}, ErrInvalidOutput
	}
	if len(b) < outputPayloadOverhead {
		return Output{}, errShortPayload
	}
	r := payloadReader{b: b}
	var m Output
	var err error
	if m.Epoch, err = r.getUint64(); err != nil {
		return Output{}, err
	}
	if m.Base, err = r.getUint64(); err != nil {
		return Output{}, err
	}
	if m.New, err = r.getUint64(); err != nil {
		return Output{}, err
	}
	if m.Echo, err = r.getUint64(); err != nil {
		return Output{}, err
	}
	if m.ViewRevision, err = r.getUint64(); err != nil {
		return Output{}, err
	}
	cols, err := r.getUint16()
	if err != nil {
		return Output{}, err
	}
	rows, err := r.getUint16()
	if err != nil {
		return Output{}, err
	}
	m.Size = domain.Size{Cols: int(cols), Rows: int(rows)}
	if m.Full, err = r.getBool(); err != nil {
		return Output{}, err
	}
	if err := ValidateOutput(m); err != nil {
		return Output{}, err
	}
	kind, err := r.getUint8()
	if err != nil {
		return Output{}, err
	}
	decodedLen, err := r.getUint32()
	if err != nil {
		return Output{}, err
	}
	if int64(decodedLen) > int64(MaxFrameLen-1-outputPayloadOverhead) {
		return Output{}, ErrInvalidOutput
	}
	data, err := r.getLongBytes()
	if err != nil {
		return Output{}, err
	}
	if err := r.done(); err != nil {
		return Output{}, err
	}
	switch kind {
	case outputCompressionNone:
		if uint32(len(data)) != decodedLen {
			return Output{}, ErrInvalidOutput
		}
		m.Data = data
	case outputCompressionZlib:
		if !m.Full {
			return Output{}, ErrInvalidOutput
		}
		m.Data, err = decompressOutputData(data, int(decodedLen))
		if err != nil {
			return Output{}, ErrInvalidOutput
		}
	default:
		return Output{}, errInvalidEnum
	}
	if err := ValidateOutput(m); err != nil {
		return Output{}, err
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
func MarshalAttachTarget(m AttachTarget) []byte {
	if err := ValidateAttachTarget(m); err != nil {
		return nil
	}
	w := payloadWriter{}
	w.putString(m.Endpoint)
	w.putString(m.Session)
	w.putUint8(m.Intent)
	marshalRemoteTargetSection(&w, m.RemoteTarget, m.EnvironmentPolicy)
	marshalExactTargetSection(&w, m.ExactTarget)
	return w.b
}

// UnmarshalAttachTarget decodes a strict server attach-target payload.
func UnmarshalAttachTarget(b []byte) (AttachTarget, error) {
	// Preflight lengths and intent before getString can allocate either value.
	probe := payloadReader{b: b}
	endpointLen, err := probe.getUint16()
	if err != nil || int(endpointLen) > len(probe.b) {
		return AttachTarget{}, ErrInvalidAttachTarget
	}
	probe.b = probe.b[endpointLen:]
	sessionLen, err := probe.getUint16()
	if err != nil || sessionLen == 0 || int(sessionLen) > len(probe.b) {
		return AttachTarget{}, ErrInvalidAttachTarget
	}
	probe.b = probe.b[sessionLen:]
	intent, err := probe.getUint8()
	if err != nil || (intent != IntentEphemeral && intent != IntentNew && intent != IntentAttach && intent != IntentResume) {
		return AttachTarget{}, ErrInvalidAttachTarget
	}
	if err := skipRemoteTargetSection(&probe); err != nil {
		return AttachTarget{}, ErrInvalidAttachTarget
	}
	if err := skipExactTargetSection(&probe); err != nil {
		return AttachTarget{}, ErrInvalidAttachTarget
	}
	if err := probe.done(); err != nil {
		return AttachTarget{}, ErrInvalidAttachTarget
	}

	r := payloadReader{b: b}
	var m AttachTarget
	if m.Endpoint, err = r.getString(); err != nil {
		return AttachTarget{}, err
	}
	if m.Session, err = r.getString(); err != nil {
		return AttachTarget{}, err
	}
	if m.Intent, err = r.getUint8(); err != nil {
		return AttachTarget{}, err
	}
	if m.RemoteTarget, m.EnvironmentPolicy, err = unmarshalRemoteTargetSection(&r); err != nil {
		return AttachTarget{}, err
	}
	if m.ExactTarget, err = unmarshalExactTargetSection(&r); err != nil {
		return AttachTarget{}, err
	}
	if err := r.done(); err != nil {
		return AttachTarget{}, err
	}
	if err := ValidateAttachTarget(m); err != nil {
		return AttachTarget{}, err
	}
	return m, nil
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
	if !validDetachedReason(reason) {
		return Detached{}, errInvalidEnum
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
	var all bool
	if len(r.b) > 0 {
		all, err = r.getBool()
		if err != nil {
			return Kill{}, err
		}
	}
	if err := r.done(); err != nil {
		return Kill{}, err
	}
	return Kill{Name: name, All: all}, nil
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
		eph, err := r.getBool()
		if err != nil {
			return Sessions{}, err
		}
		s.Ephemeral = eph
		if s.Tabs, err = r.getUint16(); err != nil {
			return Sessions{}, err
		}
		att, err := r.getBool()
		if err != nil {
			return Sessions{}, err
		}
		s.Attached = att
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
		return renderer.Style{}, ErrRemotePreviewUnsupportedStyle
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

func MarshalRemotePreviewRequest(request RemotePreviewRequest) []byte {
	if ValidateRemotePreviewRequest(request) != nil {
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

func UnmarshalRemotePreviewRequest(data []byte) (RemotePreviewRequest, error) {
	if len(data) > RemotePreviewMaxBytes {
		return RemotePreviewRequest{}, ErrInvalidRemotePreviewRequest
	}
	r := payloadReader{b: data}
	var q RemotePreviewRequest
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
	if err := ValidateRemotePreviewRequest(q); err != nil {
		return q, err
	}
	return q, nil
}

const previewCellWireSize = 4 + 1 + 1 + 2 + 2 + 2 + 1 + 2 + 3 + 3 + 3

func MarshalRemotePreview(preview RemotePreview) []byte {
	if ValidateRemotePreview(preview) != nil {
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

func UnmarshalRemotePreview(data []byte) (RemotePreview, error) {
	if len(data) > RemotePreviewMaxBytes {
		return RemotePreview{}, ErrRemotePreviewTooLarge
	}
	r := payloadReader{b: data}
	var p RemotePreview
	var err error
	if p.Version, err = r.getUint16(); err != nil {
		return p, err
	}
	status, err := r.getUint8()
	if err != nil {
		return p, err
	}
	p.Status = RemotePreviewStatus(status)
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
	if count > RemotePreviewMaxCells || uint64(count) > uint64(len(r.b)/previewCellWireSize) {
		return p, ErrRemotePreviewTooLarge
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
				return p, ErrInvalidRemotePreview
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
	if err := ValidateRemotePreview(p); err != nil {
		return p, err
	}
	return p, nil
}

// MarshalNavigationDirective encodes one bounded navigation directive.
func MarshalNavigationDirective(directive NavigationDirective) []byte {
	if directive.Action != NavigationOpenHomePicker && directive.Action != NavigationBack {
		return nil
	}
	if directive.Action == NavigationOpenHomePicker && directive.LeaseID.IsZero() || directive.Action == NavigationBack && !directive.LeaseID.IsZero() {
		return nil
	}
	w := payloadWriter{}
	w.putUint8(uint8(directive.Action))
	w.putBytes(directive.LeaseID[:])
	return w.b
}

// UnmarshalNavigationDirective decodes one strict navigation directive.
func UnmarshalNavigationDirective(b []byte) (NavigationDirective, error) {
	r := payloadReader{b: b}
	value, err := r.getUint8()
	if err != nil {
		return NavigationDirective{}, ErrInvalidNavigation
	}
	lease, err := r.getBytes(len(ParkedRouteLeaseID{}))
	if err != nil {
		return NavigationDirective{}, ErrInvalidNavigation
	}
	if err := r.done(); err != nil {
		return NavigationDirective{}, ErrInvalidNavigation
	}
	directive := NavigationDirective{Action: NavigationAction(value)}
	copy(directive.LeaseID[:], lease)
	if MarshalNavigationDirective(directive) == nil {
		return NavigationDirective{}, ErrInvalidNavigation
	}
	return directive, nil
}

// ValidateParkedRouteRequest enforces the closed action/target shape before a
// request reaches either side's route state machine.
func ValidateParkedRouteRequest(request ParkedRouteRequest) error {
	if request.RequestID == 0 || request.LeaseID.IsZero() {
		return ErrInvalidNavigation
	}
	switch request.Action {
	case ParkedRoutePrepare, ParkedRouteResume:
		if request.Target != nil {
			return ErrInvalidNavigation
		}
	case ParkedRouteSwitch:
		if request.Target == nil || validateWireRemoteTarget(*request.Target) != nil {
			return ErrInvalidNavigation
		}
	default:
		return ErrInvalidNavigation
	}
	return nil
}

// MarshalParkedRouteRequest encodes one retained-route operation.
func MarshalParkedRouteRequest(request ParkedRouteRequest) []byte {
	if ValidateParkedRouteRequest(request) != nil {
		return nil
	}
	w := payloadWriter{}
	w.putUint64(request.RequestID)
	w.putBytes(request.LeaseID[:])
	w.putUint8(uint8(request.Action))
	marshalRemoteTargetSection(&w, request.Target, EnvironmentPolicyDaemonOwned)
	return w.b
}

// UnmarshalParkedRouteRequest decodes one strict retained-route operation.
func UnmarshalParkedRouteRequest(b []byte) (ParkedRouteRequest, error) {
	r := payloadReader{b: b}
	requestID, err := r.getUint64()
	if err != nil {
		return ParkedRouteRequest{}, ErrInvalidNavigation
	}
	lease, err := r.getBytes(len(ParkedRouteLeaseID{}))
	if err != nil {
		return ParkedRouteRequest{}, ErrInvalidNavigation
	}
	action, err := r.getUint8()
	if err != nil {
		return ParkedRouteRequest{}, ErrInvalidNavigation
	}
	target, policy, err := unmarshalRemoteTargetSection(&r)
	if err != nil || policy != EnvironmentPolicyDaemonOwned || r.done() != nil {
		return ParkedRouteRequest{}, ErrInvalidNavigation
	}
	request := ParkedRouteRequest{RequestID: requestID, Action: ParkedRouteAction(action), Target: target}
	copy(request.LeaseID[:], lease)
	if ValidateParkedRouteRequest(request) != nil {
		return ParkedRouteRequest{}, ErrInvalidNavigation
	}
	return request, nil
}

// MarshalParkedRouteResponse encodes one correlated retained-route outcome.
func MarshalParkedRouteResponse(response ParkedRouteResponse) []byte {
	if response.RequestID == 0 || response.Status < ParkedRouteReady || response.Status > ParkedRouteStaleTarget {
		return nil
	}
	w := payloadWriter{}
	w.putUint64(response.RequestID)
	w.putUint8(uint8(response.Status))
	return w.b
}

// UnmarshalParkedRouteResponse decodes one strict retained-route outcome.
func UnmarshalParkedRouteResponse(b []byte) (ParkedRouteResponse, error) {
	r := payloadReader{b: b}
	requestID, err := r.getUint64()
	if err != nil {
		return ParkedRouteResponse{}, ErrInvalidNavigation
	}
	status, err := r.getUint8()
	if err != nil || r.done() != nil {
		return ParkedRouteResponse{}, ErrInvalidNavigation
	}
	response := ParkedRouteResponse{RequestID: requestID, Status: ParkedRouteStatus(status)}
	if MarshalParkedRouteResponse(response) == nil {
		return ParkedRouteResponse{}, ErrInvalidNavigation
	}
	return response, nil
}
