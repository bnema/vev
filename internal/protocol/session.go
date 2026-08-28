// Package protocol defines vev's typed, transport-neutral client/daemon
// contract. Binary framing and codecs live outside this package.
package protocol

import (
	"errors"
	"fmt"
	"math"
	"time"

	renderer "github.com/bnema/vev-vt"
	"github.com/bnema/vev/internal/domain"
)

// Version is the negotiated vev session protocol version.
const Version uint16 = 37

// HandshakeTimeout bounds every transport handshake from connect through the
// first committed publication.
const HandshakeTimeout = 15 * time.Second

// MaxOutputDataLen is the largest decoded terminal byte stream accepted by one
// output publication in protocol version 37.
const MaxOutputDataLen = (16 << 20) - 55

// ConnectionCapabilities describes transport behavior relevant to session
// flow without naming a concrete carriage.
type ConnectionCapabilities struct {
	OutputDataLimit       int
	PreferredOutputWindow uint8
	AsyncSend             bool
	OwnedSynchronousSend  bool
	LinkState             bool
}

var (
	ErrInvalidSize              = errors.New("ports: invalid terminal size")
	ErrInvalidHello             = errors.New("ports: invalid hello")
	ErrInvalidOutput            = errors.New("ports: invalid output")
	ErrInvalidAck               = errors.New("ports: invalid ack")
	ErrInvalidAttachTarget      = errors.New("ports: invalid attach target")
	ErrInvalidNavigation        = errors.New("ports: invalid navigation")
	ErrInvalidEnvironmentPolicy = errors.New("ports: invalid environment policy")
	ErrInvalidClientNotice      = errors.New("ports: invalid client notice")
	ErrInvalidDetached          = errors.New("ports: invalid detached reason")
)

const (
	IntentEphemeral uint8 = 0
	IntentNew       uint8 = 1
	IntentAttach    uint8 = 2
	IntentResume    uint8 = 3
)

type EnvironmentPolicy uint8

const (
	EnvironmentPolicyClientOwned EnvironmentPolicy = iota
	EnvironmentPolicyDaemonOwned
)

const (
	CapabilityResume  uint32 = 1 << 0
	CapabilityUDP     uint32 = 1 << 1
	CapabilityPredict uint32 = 1 << 2
)

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
	// KittyDirectGraphics is an explicit declaration that the client has
	// probed its direct outer terminal and it accepts Kitty graphics output.
	KittyDirectGraphics    bool
	MaxOutputInFlight      uint8
	Env                    []string
	RemoteTarget           *domain.RemoteSessionTarget
	ExactTarget            *ExactSessionTarget
	PreferredTabID         domain.TabStableID
	EnvironmentPolicy      EnvironmentPolicy
	NavigationCapabilities NavigationCapabilities
	StartupOverlay         StartupOverlay
	Remote                 bool
}

func (h Hello) Geometry() domain.Geometry {
	return domain.Geometry{Size: h.Size, PixelWidth: h.PixelWidth, PixelHeight: h.PixelHeight}.NormalizePixels()
}

type Input struct {
	InputSeq uint64
	Data     []byte
}

type Resize struct {
	Size        domain.Size
	PixelWidth  int
	PixelHeight int
}

func (m Resize) Geometry() domain.Geometry {
	return domain.Geometry{Size: m.Size, PixelWidth: m.PixelWidth, PixelHeight: m.PixelHeight}.NormalizePixels()
}

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

type ImagePush struct {
	InputSeq uint64
	Mime     string
	Data     []byte
}

type ClientNotice struct{ Action uint8 }

const (
	ClientNoticeClipboardFallback uint8 = 1
	ClientNoticeClipboardTooLarge uint8 = 2
	ClientNoticeLinkDegraded      uint8 = 3
	ClientNoticeLinkConnected     uint8 = 4
)

type Detach struct{}
type Ping struct{}
type Pong struct{}

type Ack struct {
	Epoch uint64
	State uint64
}

type Welcome struct {
	SessionID         string
	SessionName       string
	Ephemeral         bool
	ResumeToken       uint64
	Capabilities      uint32
	CommittedIdentity *CommittedRouteIdentity
}

type ErrorMsg struct {
	Code uint16
	Text string
}

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

type AttachTarget struct {
	Endpoint          string
	Session           string
	Intent            uint8
	RemoteTarget      *domain.RemoteSessionTarget
	ExactTarget       *ExactSessionTarget
	EnvironmentPolicy EnvironmentPolicy
	SamePeer          bool
}

type Detached struct{ Reason uint8 }
type List struct{}

type Kill struct {
	Name string
	All  bool
}

type SessionState uint8

const (
	SessionUp SessionState = iota
	SessionDown
	SessionBroken
)

type SessionInfo struct {
	SessionID string
	Name      string
	State     SessionState
	Ephemeral bool
	Tabs      uint16
	Attached  bool
}

type Sessions struct{ Sessions []SessionInfo }
type OutputResetRequest struct{}

func validEnvironmentPolicy(policy EnvironmentPolicy) bool {
	return policy == EnvironmentPolicyClientOwned || policy == EnvironmentPolicyDaemonOwned
}

func ValidateEnvironmentPolicy(policy EnvironmentPolicy) error {
	if !validEnvironmentPolicy(policy) {
		return ErrInvalidEnvironmentPolicy
	}
	return nil
}

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

func ValidateClientNotice(m ClientNotice) error {
	if !validClientNoticeAction(m.Action) {
		return ErrInvalidClientNotice
	}
	return nil
}

func ValidateDetached(m Detached) error {
	if !validDetachedReason(m.Reason) {
		return ErrInvalidDetached
	}
	return nil
}

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
		return nil
	}
	if h.EnvironmentPolicy != EnvironmentPolicyDaemonOwned || (h.Intent != IntentAttach && h.Intent != IntentResume) {
		return ErrInvalidHello
	}
	if err := validateRemoteTarget(*h.RemoteTarget); err != nil {
		return fmt.Errorf("%w: remote target: %v", ErrInvalidHello, err)
	}
	if h.Name != h.RemoteTarget.SessionName {
		return ErrInvalidHello
	}
	return nil
}

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
	if uint64(len(m.Data)) > math.MaxUint32 || len(m.Data) > MaxOutputDataLen {
		return ErrInvalidOutput
	}
	return nil
}

func ValidateAck(m Ack) error {
	if m.Epoch == 0 {
		return ErrInvalidAck
	}
	return nil
}

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
	if m.Intent == IntentResume || !validEnvironmentPolicy(m.EnvironmentPolicy) {
		return ErrInvalidAttachTarget
	}
	if m.ExactTarget != nil && (m.ExactTarget.SessionName != m.Session || m.ExactTarget.Validate() != nil) {
		return ErrInvalidAttachTarget
	}
	if m.SamePeer && (m.Endpoint != "" || m.RemoteTarget != nil || m.ExactTarget == nil) {
		return ErrInvalidAttachTarget
	}
	if m.RemoteTarget == nil {
		return nil
	}
	if m.EnvironmentPolicy != EnvironmentPolicyDaemonOwned || m.Intent != IntentAttach {
		return ErrInvalidAttachTarget
	}
	if err := validateRemoteTarget(*m.RemoteTarget); err != nil {
		return fmt.Errorf("%w: remote target: %v", ErrInvalidAttachTarget, err)
	}
	if m.Endpoint != m.RemoteTarget.Endpoint || m.Session != m.RemoteTarget.SessionName {
		return ErrInvalidAttachTarget
	}
	return nil
}

func validateRemoteTarget(target domain.RemoteSessionTarget) error {
	if len(target.Endpoint) > math.MaxUint16 || len(target.DisplayOrigin) > math.MaxUint16 || len(target.SessionName) > math.MaxUint16 || len(target.LiveTabID) > math.MaxUint16 || len(target.StoppedTab.RawName) > math.MaxUint16 {
		return errors.New("remote target string too long")
	}
	return target.Validate()
}
