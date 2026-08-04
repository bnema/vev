package daemon

import (
	"testing"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/stretchr/testify/require"
)

func newProxyOutputAttachment(t *testing.T, proxy *proxySession, id byte) (*attachedClient, *proxyTestTransport) {
	t.Helper()
	transport := newProxyTestTransport()
	ac := &attachedClient{tr: transport, output: newOutputStateStream(), size: proxy.contentSize}
	ac.output.attachment = ac
	ac.clientID[0] = id
	ac.setSession(proxy)
	proxy.sessionCore.mu.Lock()
	require.True(t, proxy.registerAttachmentLocked(ac))
	proxy.sessionCore.mu.Unlock()
	return ac, transport
}

func TestProxyOutputRejectsStaleStateAndRequestsOneReset(t *testing.T) {
	tests := []struct {
		name  string
		state ports.Output
		stale ports.Output
	}{
		{
			name:  "duplicate full",
			state: ports.Output{Epoch: 3, Base: 0, New: 4, Full: true},
			stale: ports.Output{Epoch: 3, Base: 0, New: 4, Full: true, Data: []byte("duplicate")},
		},
		{
			name:  "older epoch",
			state: ports.Output{Epoch: 3, Base: 0, New: 4, Full: true},
			stale: ports.Output{Epoch: 2, Base: 0, New: 5, Full: true, Data: []byte("older")},
		},
		{
			name:  "base-zero non-full",
			state: ports.Output{Epoch: 3, Base: 0, New: 4, Full: true},
			stale: ports.Output{Epoch: 3, Base: 0, New: 5, Data: []byte("not full")},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			proxy, err := newProxySession(domain.RemoteSessionKey{Host: "host", Name: "session"}, domain.Size{Cols: 80, Rows: 24})
			require.NoError(t, err)
			proxy.linkGeneration = 1
			ack, reset, changed := proxy.applyOutputForGeneration(1, tt.state)
			require.Equal(t, uint64(4), ack)
			require.False(t, reset)
			require.Equal(t, len(tt.state.Data) != 0, changed)

			ack, reset, changed = proxy.applyOutputForGeneration(1, tt.stale)
			require.Zero(t, ack)
			require.True(t, reset)
			require.False(t, changed)
			ack, reset, changed = proxy.applyOutputForGeneration(1, tt.stale)
			require.Zero(t, ack)
			require.False(t, reset)
			require.False(t, changed)
		})
	}
}

func TestProxyOutputFansOutToEveryLiveAttachment(t *testing.T) {
	proxy, err := newProxySession(domain.RemoteSessionKey{Host: "host", Name: "session"}, domain.Size{Cols: 80, Rows: 24})
	require.NoError(t, err)
	remote := newProxyTestTransport()
	proxy.transport = remote
	proxy.linkGeneration = 1
	d := &Daemon{}

	attachments := make([]*proxyTestTransport, 0, 2)
	for id := byte(1); id <= 2; id++ {
		_, clientTransport := newProxyOutputAttachment(t, proxy, id)
		attachments = append(attachments, clientTransport)
	}

	result, err := d.handleLinkFrame(proxy, 1, ports.Frame{
		Type:    ports.MsgOutput,
		Payload: mustMarshalOutput(ports.Output{Epoch: 3, Base: 0, New: 1, Size: domain.Size{Cols: 1, Rows: 1}, Full: true, Data: []byte("full")}),
	})
	require.NoError(t, err)
	require.Equal(t, proxyLinkResume, result)
	for _, transport := range attachments {
		frame := awaitTestValue(t, transport.sent, "proxy attachment did not receive output")
		require.Equal(t, ports.MsgOutput, frame.Type)
		out, err := ports.UnmarshalOutput(frame.Payload)
		require.NoError(t, err)
		require.Zero(t, out.New, "local proxy forwarding must not create a second ACK path")
		require.Equal(t, []byte("full"), out.Data)
	}
	ackFrame := awaitTestValue(t, remote.sent, "proxy remote did not receive output acknowledgement")
	require.Equal(t, ports.MsgAck, ackFrame.Type)
}

func TestProxyOutputAttachAfterStreamReceivesCurrentFullOutput(t *testing.T) {
	proxy, err := newProxySession(domain.RemoteSessionKey{Host: "host", Name: "session"}, domain.Size{Cols: 80, Rows: 24})
	require.NoError(t, err)
	remote := newProxyTestTransport()
	proxy.transport = remote
	proxy.linkGeneration = 1
	d := &Daemon{}

	_, firstTransport := newProxyOutputAttachment(t, proxy, 1)

	handle := func(output ports.Output) {
		t.Helper()
		result, err := d.handleLinkFrame(proxy, 1, ports.Frame{Type: ports.MsgOutput, Payload: mustMarshalOutput(output)})
		require.NoError(t, err)
		require.Equal(t, proxyLinkResume, result)
	}
	handle(ports.Output{Epoch: 3, Base: 0, New: 1, Size: domain.Size{Cols: 1, Rows: 1}, Full: true, Data: []byte("before attach")})
	_ = awaitTestValue(t, firstTransport.sent, "first proxy attachment did not receive output")
	_ = awaitTestValue(t, remote.sent, "proxy remote did not receive first acknowledgement")

	_, secondTransport := newProxyOutputAttachment(t, proxy, 2)

	handle(ports.Output{Epoch: 4, Base: 0, New: 1, Size: domain.Size{Cols: 1, Rows: 1}, Full: true, Data: []byte("current full")})
	for _, transport := range []*proxyTestTransport{firstTransport, secondTransport} {
		frame := awaitTestValue(t, transport.sent, "proxy attachment did not receive current output")
		require.Equal(t, ports.MsgOutput, frame.Type)
		out, err := ports.UnmarshalOutput(frame.Payload)
		require.NoError(t, err)
		require.Zero(t, out.New)
		require.False(t, out.Full, "local proxy side effects must remain stateless")
		require.Equal(t, []byte("current full"), out.Data)
	}
	ackFrame := awaitTestValue(t, remote.sent, "proxy remote did not receive current acknowledgement")
	require.Equal(t, ports.MsgAck, ackFrame.Type)
}

func TestProxyOutputInvalidStateNeverForwardsBytes(t *testing.T) {
	proxy, err := newProxySession(domain.RemoteSessionKey{Host: "host", Name: "session"}, domain.Size{Cols: 80, Rows: 24})
	require.NoError(t, err)
	remote := newProxyTestTransport()
	proxy.transport = remote
	proxy.linkGeneration = 1
	_, clientTransport := newProxyOutputAttachment(t, proxy, 1)
	d := &Daemon{}

	_, err = d.handleLinkFrame(proxy, 1, ports.Frame{
		Type:    ports.MsgOutput,
		Payload: mustMarshalOutput(ports.Output{Epoch: 3, Base: 0, New: 1, Size: domain.Size{Cols: 1, Rows: 1}, Full: true, Data: []byte("full")}),
	})
	require.NoError(t, err)
	_ = awaitTestValue(t, clientTransport.sent, "proxy attachment did not receive initial output")
	_ = awaitTestValue(t, remote.sent, "proxy remote did not receive initial acknowledgement")

	result, err := d.handleLinkFrame(proxy, 1, ports.Frame{
		Type:    ports.MsgOutput,
		Payload: mustMarshalOutput(ports.Output{Epoch: 3, Base: 0, New: 1, Size: domain.Size{Cols: 1, Rows: 1}, Full: true, Data: []byte("stale")}),
	})
	require.NoError(t, err)
	require.Equal(t, proxyLinkResume, result)
	select {
	case frame := <-clientTransport.sent:
		t.Fatalf("stale proxy output forwarded as %v", frame.Type)
	default:
	}
	reset := awaitTestValue(t, remote.sent, "proxy remote did not receive reset request")
	require.Equal(t, ports.MsgOutputResetRequest, reset.Type)
}
