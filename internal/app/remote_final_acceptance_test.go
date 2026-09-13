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

	remoteadapter "github.com/bnema/vev/internal/adapters/remote"
	"github.com/bnema/vev/internal/adapters/sessionwire"
	"github.com/bnema/vev/internal/adapters/sshstdio"
	"github.com/bnema/vev/internal/domain"
	portsmocks "github.com/bnema/vev/internal/ports/mocks"
	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/protocol/wire"
	"github.com/bnema/vev/internal/usecase/client"
	"github.com/stretchr/testify/require"
)

type acceptanceRemoteFactory struct {
	mu      sync.Mutex
	calls   []string
	handoff map[string]protocol.AttachTarget
	outputs chan string
}

func (f *acceptanceRemoteFactory) DialerForRemote(target string, _ string, _ remoteadapter.TransportMode, _ *slog.Logger) (wire.Dialer, error) {
	f.mu.Lock()
	f.calls = append(f.calls, target)
	targetHandoff := f.handoff[target]
	f.mu.Unlock()
	return acceptanceRemoteDialer{target: target, handoff: targetHandoff, outputs: f.outputs}, nil
}

type acceptanceRemoteDialer struct {
	target  string
	handoff protocol.AttachTarget
	outputs chan<- string
}

func (d acceptanceRemoteDialer) Dial(context.Context) (wire.Transport, error) {
	clientConn, serverConn := net.Pipe()
	clientTransport := sshstdio.NewTransport(clientConn, clientConn, clientConn.Close)
	serverTransport := sshstdio.NewTransport(serverConn, serverConn, serverConn.Close)
	go serveAcceptanceRemote(serverTransport, d.target, d.handoff, d.outputs)
	return clientTransport, nil
}

func serveAcceptanceRemote(raw wire.Transport, target string, handoff protocol.AttachTarget, outputs chan<- string) {
	tr := sessionwire.NewServerConnection(raw)
	defer func() { _ = tr.Close() }()
	welcomed := false
	for {
		message, err := tr.ReceiveClient()
		if err != nil {
			return
		}
		if !welcomed {
			if _, ok := message.(protocol.Hello); !ok {
				continue
			}
			if err := tr.SendServer(protocol.Welcome{SessionID: target}); err != nil {
				return
			}
			welcomed = true
			continue
		}
		if handoff.Endpoint != "" {
			_ = tr.SendServer(handoff)
			for {
				if _, err := tr.ReceiveClient(); err != nil {
					return
				}
			}
		}
		data := []byte("remote:" + target)
		if outputs != nil {
			outputs <- string(data)
		}
		if err := tr.SendServer(protocol.Output{
			Epoch: 1, New: 1, Full: true, Size: domain.Size{Cols: 80, Rows: 24}, Data: data,
			Context: &protocol.ViewContext{
				Publication: 1,
				Route:       protocol.CommittedRouteIdentity{Target: protocol.ExactSessionTarget{LifecycleID: domain.SessionLifecycleID{1}, SessionName: "remote-fixture"}},
				TabID:       "tab-1", FocusedPaneID: "pane-1",
			},
		}); err != nil {
			return
		}
		_ = tr.SendServer(protocol.Detached{Reason: protocol.ReasonDetach})
		for {
			if _, err := tr.ReceiveClient(); err != nil {
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
func (*acceptanceRemoteTerminal) Geometry() (domain.Geometry, error) {
	return domain.Geometry{Size: domain.Size{Cols: 80, Rows: 24}}, nil
}
func (*acceptanceRemoteTerminal) ResizeEvents() <-chan domain.Geometry { return nil }
func (t *acceptanceRemoteTerminal) In() io.Reader                      { return t.in }
func (t *acceptanceRemoteTerminal) Out() io.Writer                     { return &t.out }
func (*acceptanceRemoteTerminal) Flush() error                         { return nil }

func TestAcceptanceRemoteDirectAndPickerUseOnlyRemoteTransports(t *testing.T) {
	outputs := make(chan string, 4)
	factory := &acceptanceRemoteFactory{outputs: outputs}
	hostStore := portsmocks.NewMockRemoteHostStore(t)
	localCalls := 0
	terminals := make([]*acceptanceRemoteTerminal, 0, 3)
	deps := runAttachDeps{
		hostStore: hostStore,
		localDialer: func() wire.Dialer {
			localCalls++
			return acceptanceRemoteDialer{}
		},
		remoteDialerFactory:     factory.DialerForRemote,
		selectedRemoteTransport: string(remoteadapter.TransportStdio),
		runClient: func(ctx context.Context, deps client.Dependencies, request client.AttachRequest) error {
			term := &acceptanceRemoteTerminal{in: bytes.NewReader(nil)}
			terminals = append(terminals, term)
			deps.Terminal = term
			deps.Logger = slog.New(slog.DiscardHandler)
			deps.Remote = true
			result := client.NewRunner(deps).Run(ctx, request)
			if request.SessionName == "work" {
				require.NoError(t, result, "the client handoff completes before the test injects the composition-root handoff")
				return &client.AttachTargetError{Target: protocol.AttachTarget{Endpoint: "picked.example", Session: "picked", Intent: protocol.IntentAttach}}
			}
			return result
		},
	}

	probeRaw, err := (acceptanceRemoteDialer{
		target:  "picker-wire.example",
		handoff: protocol.AttachTarget{Endpoint: "picked.example", Session: "picked", Intent: protocol.IntentAttach},
	}).Dial(context.Background())
	require.NoError(t, err)
	wireTransport := sessionwire.NewClientConnection(probeRaw)
	require.NoError(t, wireTransport.SendClient(protocol.Hello{
		Version: protocol.Version, Intent: protocol.IntentAttach, Name: "work", Size: domain.Size{Cols: 80, Rows: 24},
	}))
	welcome, err := wireTransport.ReceiveServer()
	require.NoError(t, err)
	require.IsType(t, protocol.Welcome{}, welcome)
	require.NoError(t, wireTransport.SendClient(protocol.Theme{}))
	attachTargetMessage, err := wireTransport.ReceiveServer()
	require.NoError(t, err)
	target, ok := attachTargetMessage.(protocol.AttachTarget)
	require.True(t, ok, "expected attach target, got %T", attachTargetMessage)
	require.Equal(t, "picked.example", target.Endpoint)
	require.NoError(t, wireTransport.Close())

	require.NoError(t, runAttachWithDeps(context.Background(), protocol.IntentAttach, "direct", "direct.example", "", nil, deps))
	require.NoError(t, runAttachWithDeps(context.Background(), protocol.IntentAttach, "work", "picker.example", "", nil, deps))
	factory.mu.Lock()
	calls := append([]string(nil), factory.calls...)
	factory.mu.Unlock()
	require.Equal(t, []string{"direct.example", "picker.example", "picked.example"}, calls)
	require.Zero(t, localCalls, "remote direct and picker connections must not create a local shadow")
	require.Len(t, terminals, 3)
	require.Equal(t, []string{"remote:direct.example", "remote:picker.example", "remote:picked.example"}, []string{<-outputs, <-outputs, <-outputs})
}
