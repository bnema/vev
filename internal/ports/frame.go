package ports

// ProtocolVersion is the current vev IPC wire protocol version.
const ProtocolVersion uint16 = 1

// MsgType identifies the kind of payload carried by a Frame.
type MsgType uint8

// Frame message types. Client-originated messages are numbered from 1,
// server-originated messages from 16, leaving room for growth in each band.
const (
	MsgHello  MsgType = 1
	MsgInput  MsgType = 2
	MsgResize MsgType = 3
	MsgDetach MsgType = 4
	MsgPing   MsgType = 5

	MsgWelcome  MsgType = 16
	MsgError    MsgType = 17
	MsgOutput   MsgType = 18
	MsgDetached MsgType = 19
	MsgPong     MsgType = 20
)

// Frame is the unit of exchange over a Transport: a typed, length-delimited
// message. It lives in ports (rather than an ipc adapter) so usecases and
// adapters can share it without either depending on a concrete transport
// implementation.
type Frame struct {
	Type    MsgType
	Payload []byte
}
