package ports

import (
	"time"

	renderer "github.com/bnema/vev-vt"
	"github.com/bnema/vev/internal/domain"
)

// HandshakeTimeout bounds every transport handshake from connect through the
// first committed publication.
const HandshakeTimeout = 15 * time.Second

// ProtocolVersion is the current vev IPC wire protocol version.
const ProtocolVersion uint16 = 30

// MaxFrameLen is the largest permitted frame length, including the type byte
// and excluding the four-byte length prefix.
const MaxFrameLen = 16 << 20

// MsgType identifies the kind of payload carried by a Frame.
type MsgType uint8

// Frame message types use non-contiguous allocations: the legacy client range
// is 1–13 and 15, and newer client subscriptions use 32. Server-originated
// messages occupy 16–23 and 25–31. Values 14 and 24 remain reserved for future
// extensions.
const (
	MsgHello                      MsgType = 1
	MsgInput                      MsgType = 2
	MsgResize                     MsgType = 3
	MsgDetach                     MsgType = 4
	MsgPing                       MsgType = 5
	MsgList                       MsgType = 6
	MsgKill                       MsgType = 7
	MsgTheme                      MsgType = 8
	MsgAck                        MsgType = 9
	MsgImagePush                  MsgType = 10
	MsgClientNotice               MsgType = 11
	MsgCommand                    MsgType = 12
	MsgOutputResetRequest         MsgType = 13
	MsgRemotePreviewRequest       MsgType = 15
	MsgRouteAttentionSubscription MsgType = 32

	MsgWelcome               MsgType = 16
	MsgError                 MsgType = 17
	MsgOutput                MsgType = 18
	MsgDetached              MsgType = 19
	MsgPong                  MsgType = 20
	MsgSessions              MsgType = 21
	MsgCommandResult         MsgType = 22
	MsgNavigationAction      MsgType = 23
	MsgAttachTarget          MsgType = 25
	MsgRemotePreviewResponse MsgType = 26
	// Route navigation metadata/control frames occupy the post-26 server range;
	// 14 and 24 remain reserved for compatibility with older peers.
	MsgCommittedRouteIdentity MsgType = 27
	MsgRecentRouteSnapshot    MsgType = 28
	MsgNavigateRecentRoute    MsgType = 29
	MsgRouteNavigationFailure MsgType = 30
	MsgRoutePosition          MsgType = 31
)

// Frame is the unit of exchange over a Transport: a typed, length-delimited
// message. It lives in ports (rather than an ipc adapter) so usecases and
// adapters can share it without either depending on a concrete transport
// implementation.
type Frame struct {
	Type    MsgType
	Payload []byte
}

// NavigationCapabilities advertises the bounded client routes available to a
// directly attached daemon.
type NavigationCapabilities uint8

const (
	NavigationCapabilityHomePicker NavigationCapabilities = 1 << iota
	NavigationCapabilityBack
)

// StartupOverlay selects the one startup overlay opened by a navigation route.
type StartupOverlay uint8

const (
	StartupOverlayNone StartupOverlay = iota
	StartupOverlaySessionPicker
)

// NavigationAction is a server request for one bounded client route transition.
type NavigationAction uint8

const (
	NavigationOpenHomePicker NavigationAction = 1
	NavigationBack           NavigationAction = 2
)

// RemotePreviewSchemaVersion is independent from the attachment IPC version.
const RemotePreviewSchemaVersion uint16 = 1

// RemotePreviewStatus is a closed response taxonomy. Terminal contents are
// never carried in an error response.
type RemotePreviewStatus uint8

const (
	RemotePreviewOK RemotePreviewStatus = iota
	RemotePreviewUnavailable
	RemotePreviewNoSuchTarget
	RemotePreviewStale
	RemotePreviewMalformed
	RemotePreviewTooLarge
)

// RemotePreviewRequest asks the owning daemon for an in-memory live viewport.
type RemotePreviewRequest struct {
	Version uint16
	Target  domain.RemoteSessionTarget
	Width   uint16
	Height  uint16
}

// RemotePreview is a bounded row-major styled-cell viewport. It is a
// process-memory DTO and is never persisted, logged, or traced.
type RemotePreview struct {
	Version     uint16
	Status      RemotePreviewStatus
	LifecycleID domain.SessionLifecycleID
	TabID       domain.TabStableID
	Revision    uint64
	Width       uint16
	Height      uint16
	Cells       []renderer.Cell
}
