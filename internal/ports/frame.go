package ports

import "time"

// HandshakeTimeout bounds every transport handshake from connect through the
// first committed publication.
const HandshakeTimeout = 15 * time.Second

// ProtocolVersion is the current vev IPC wire protocol version.
const ProtocolVersion uint16 = 24

// MaxFrameLen is the largest permitted frame length, including the type byte
// and excluding the four-byte length prefix.
const MaxFrameLen = 16 << 20

// MsgType identifies the kind of payload carried by a Frame.
type MsgType uint8

// Frame message types. Client-originated messages are numbered from 1
// through 15, server-originated messages from 16, with reserved values kept
// for future extensions.
const (
	MsgHello                MsgType = 1
	MsgInput                MsgType = 2
	MsgResize               MsgType = 3
	MsgDetach               MsgType = 4
	MsgPing                 MsgType = 5
	MsgList                 MsgType = 6
	MsgKill                 MsgType = 7
	MsgTheme                MsgType = 8
	MsgAck                  MsgType = 9
	MsgImagePush            MsgType = 10
	MsgClientNotice         MsgType = 11
	MsgCommand              MsgType = 12
	MsgOutputResetRequest   MsgType = 13
	MsgRemotePreviewRequest MsgType = 14

	MsgWelcome               MsgType = 16
	MsgError                 MsgType = 17
	MsgOutput                MsgType = 18
	MsgDetached              MsgType = 19
	MsgPong                  MsgType = 20
	MsgSessions              MsgType = 21
	MsgCommandResult         MsgType = 22
	MsgSessionMeta           MsgType = 23
	MsgRemotePreviewResponse MsgType = 24
	MsgAttachTarget          MsgType = 25
)

// Frame is the unit of exchange over a Transport: a typed, length-delimited
// message. It lives in ports (rather than an ipc adapter) so usecases and
// adapters can share it without either depending on a concrete transport
// implementation.
type Frame struct {
	Type    MsgType
	Payload []byte
}
