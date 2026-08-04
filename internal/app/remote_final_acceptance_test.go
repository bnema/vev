//go:build linux

package app

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"net"
	"sync"
	"testing"

	"github.com/bnema/vev/internal/adapters/sshstdio"
	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/usecase/client"
	"github.com/stretchr/testify/require"
)

type acceptanceRemoteFactory struct {
	mu      sync.Mutex
	calls   []string
	handoff map[string]ports.AttachTarget
	outputs chan string
}

func (f *acceptanceRemoteFactory) DialerForRemote(target string, _ string, _ ports.RemoteTransportMode, _ *slog.Logger) (ports.Dialer, error) {
	f.mu.Lock()
	f.calls = append(f.calls, target)
	targetHandoff := f.handoff[target]
	f.mu.Unlock()
	return acceptanceRemoteDialer{target: target, handoff: targetHandoff, outputs: f.outputs}, nil
}

type acceptanceRemoteDialer struct {
	target  string
	handoff ports.AttachTarget
	outputs chan<- string
}

func (d acceptanceRemoteDialer) Dial(context.Context) (ports.Transport, error) {
	clientConn, serverConn := net.Pipe()
	clientTransport := sshstdio.NewTransport(clientConn, clientConn, clientConn.Close)
	serverTransport := sshstdio.NewTransport(serverConn, serverConn, serverConn.Close)
	go serveAcceptanceRemote(serverTransport, d.target, d.handoff, d.outputs)
	return clientTransport, nil
}

func serveAcceptanceRemote(tr ports.Transport, target string, handoff ports.AttachTarget, outputs chan<- string) {
	defer func() { _ = tr.Close() }()
	welcomed := false
	for {
		frame, err := tr.Recv()
		if err != nil {
			return
		}
		if !welcomed {
			if frame.Type != ports.MsgHello {
				continue
			}
			if err := tr.Send(ports.Frame{Type: ports.MsgWelcome, Payload: ports.MarshalWelcome(ports.Welcome{SessionID: target})}); err != nil {
				return
			}
			welcomed = true
			continue
		}
		if handoff.Endpoint != "" {
			_ = tr.Send(ports.Frame{Type: ports.MsgAttachTarget, Payload: ports.MarshalAttachTarget(handoff)})
			for {
				if _, err := tr.Recv(); err != nil {
					return
				}
			}
		}
		data := []byte("remote:" + target)
		if outputs != nil {
			outputs <- string(data)
		}
		payload, err := ports.MarshalOutput(ports.Output{
			Epoch: 1, New: 1, Full: true, Size: domain.Size{Cols: 80, Rows: 24}, Data: data,
		})
		if err != nil {
			return
		}
		if err := tr.Send(ports.Frame{Type: ports.MsgOutput, Payload: payload}); err != nil {
			return
		}
		_ = tr.Send(ports.Frame{Type: ports.MsgDetached, Payload: ports.MarshalDetached(ports.Detached{Reason: ports.ReasonDetach})})
		for {
			if _, err := tr.Recv(); err != nil {
				return
			}
		}
	}
}

type acceptanceRemoteTerminal struct {
	in  io.Reader
	out bytes.Buffer
}

func (t *acceptanceRemoteTerminal) EnterRaw() (func() error, error) {
	return func() error { return nil }, nil
}
func (*acceptanceRemoteTerminal) Size() (domain.Size, error) {
	return domain.Size{Cols: 80, Rows: 24}, nil
}
func (*acceptanceRemoteTerminal) ResizeEvents() <-chan domain.Size { return nil }
func (t *acceptanceRemoteTerminal) In() io.Reader                  { return t.in }
func (t *acceptanceRemoteTerminal) Out() io.Writer                 { return &t.out }
func (*acceptanceRemoteTerminal) Flush() error                     { return nil }

func TestAcceptanceRemoteDirectAndPickerUseOnlyRemoteTransports(t *testing.T) {
	outputs := make(chan string, 4)
	factory := &acceptanceRemoteFactory{outputs: outputs}
	localCalls := 0
	terminals := make([]*acceptanceRemoteTerminal, 0, 3)
	deps := runAttachDeps{
		localDialer: func() ports.Dialer {
			localCalls++
			return acceptanceRemoteDialer{}
		},
		remoteDialerFactory:     factory,
		selectedRemoteTransport: string(ports.RemoteTransportStdio),
		runClient: func(ctx context.Context, deps client.Dependencies, request client.AttachRequest) error {
			term := &acceptanceRemoteTerminal{in: bytes.NewReader(nil)}
			terminals = append(terminals, term)
			deps.Terminal = term
			deps.Logger = slog.New(slog.DiscardHandler)
			deps.Remote = true
			result := client.NewRunner(deps).Run(ctx, request)
			if request.SessionName == "work" {
				require.NoError(t, result, "the client handoff completes before the test injects the composition-root handoff")
				return &client.AttachTargetError{Target: ports.AttachTarget{Endpoint: "picked.example", Session: "picked", Intent: ports.IntentAttach}}
			}
			return result
		},
	}

	wireTransport, err := (acceptanceRemoteDialer{
		target:  "picker-wire.example",
		handoff: ports.AttachTarget{Endpoint: "picked.example", Session: "picked", Intent: ports.IntentAttach},
	}).Dial(context.Background())
	require.NoError(t, err)
	require.NoError(t, wireTransport.Send(ports.Frame{Type: ports.MsgHello, Payload: ports.MarshalHello(ports.Hello{
		Version: ports.ProtocolVersion, Intent: ports.IntentAttach, Name: "work", Size: domain.Size{Cols: 80, Rows: 24},
	})}))
	welcome, err := wireTransport.Recv()
	require.NoError(t, err)
	require.Equal(t, ports.MsgWelcome, welcome.Type)
	require.NoError(t, wireTransport.Send(ports.Frame{Type: ports.MsgTheme, Payload: ports.MarshalTheme(ports.Theme{})}))
	attachTarget, err := wireTransport.Recv()
	require.NoError(t, err)
	target, err := ports.UnmarshalAttachTarget(attachTarget.Payload)
	require.NoError(t, err)
	require.Equal(t, ports.MsgAttachTarget, attachTarget.Type)
	require.Equal(t, "picked.example", target.Endpoint)
	require.NoError(t, wireTransport.Close())

	require.NoError(t, runAttachWithDeps(context.Background(), ports.IntentAttach, "direct", "direct.example", "", nil, deps))
	require.NoError(t, runAttachWithDeps(context.Background(), ports.IntentAttach, "work", "picker.example", "", nil, deps))
	factory.mu.Lock()
	calls := append([]string(nil), factory.calls...)
	factory.mu.Unlock()
	require.Equal(t, []string{"direct.example", "picker.example", "picked.example"}, calls)
	require.Zero(t, localCalls, "remote direct and picker connections must not create a local shadow")
	require.Len(t, terminals, 3)
	require.Equal(t, []string{"remote:direct.example", "remote:picker.example", "remote:picked.example"}, []string{<-outputs, <-outputs, <-outputs})
}
