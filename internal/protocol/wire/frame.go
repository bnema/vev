package wire

// MaxFrameLen is the largest permitted frame length, including the type byte
// and excluding the four-byte length prefix.
const MaxFrameLen = 16 << 20

// MsgType identifies the kind of payload carried by a Frame.
type MsgType uint8

// Frame message types use non-contiguous allocations: the legacy client range
// is 1–13 and 15, and newer client controls use 32–33, 35, and 37. Server-originated
// messages occupy 16–23, 25–31, 34, 36, and 38. Values 14 and 24 remain reserved for
// future extensions.
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
	MsgSamePeerSwitchRequest      MsgType = 33
	MsgParkedRouteRequest         MsgType = 35
	MsgSessionCreationFailure     MsgType = 37

	MsgWelcome                MsgType = 16
	MsgError                  MsgType = 17
	MsgOutput                 MsgType = 18
	MsgDetached               MsgType = 19
	MsgPong                   MsgType = 20
	MsgSessions               MsgType = 21
	MsgCommandResult          MsgType = 22
	MsgNavigationAction       MsgType = 23
	MsgAttachTarget           MsgType = 25
	MsgRemotePreviewResponse  MsgType = 26
	MsgCommittedRouteIdentity MsgType = 27
	MsgRecentRouteSnapshot    MsgType = 28
	MsgNavigateRecentRoute    MsgType = 29
	MsgRouteNavigationFailure MsgType = 30
	MsgRoutePosition          MsgType = 31
	MsgSamePeerSwitchFailure  MsgType = 34
	MsgParkedRouteResponse    MsgType = 36
	MsgRouteCreateSession     MsgType = 38
)

// Frame is the unit of exchange over a Transport: a typed, length-delimited
// message.
type Frame struct {
	Type    MsgType
	Payload []byte
}
