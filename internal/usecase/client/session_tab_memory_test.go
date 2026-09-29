package client

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/protocol"
)

func TestSessionTabMemoryIsolation(t *testing.T) {
	target := protocol.ExactSessionTarget{LifecycleID: domain.SessionLifecycleID{1}, SessionName: "alpha"}
	local := routeAuthority{local: true}
	var memory sessionTabMemory
	memory.remember(local, target, "t_second")
	memory.remember(local, target, "t_latest")
	var otherClient sessionTabMemory
	for _, tt := range []struct {
		name      string
		memory    *sessionTabMemory
		authority routeAuthority
		target    protocol.ExactSessionTarget
		explicit  domain.TabStableID
		want      domain.TabStableID
	}{
		{name: "latest committed tab", memory: &memory, authority: local, target: target, want: "t_latest"},
		{name: "explicit selection wins", memory: &memory, authority: local, target: target, explicit: "t_explicit", want: "t_explicit"},
		{name: "other host", memory: &memory, authority: routeAuthority{endpoint: "remote"}, target: target},
		{name: "other client", memory: &otherClient, authority: local, target: target},
		{name: "recreated session", memory: &memory, authority: local, target: protocol.ExactSessionTarget{LifecycleID: domain.SessionLifecycleID{2}, SessionName: "alpha"}},
		{name: "no memory", authority: local, target: target},
	} {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, tt.memory.preferred(tt.authority, tt.target, tt.explicit))
		})
	}
}

func TestSessionTabMemorySurvivesAttachmentSwap(t *testing.T) {
	for _, local := range []bool{true, false} {
		name := "remote"
		if local {
			name = "local"
		}
		t.Run(name, func(t *testing.T) {
			terminal := newWorkerTestTerminal()
			supervisor := &Supervisor{
				cfg: SupervisorConfig{Terminal: terminal}, clientID: [16]byte{1},
				attachments: &attachmentHost{geometry: domain.Geometry{Size: domain.Size{Cols: 80, Rows: 24}}, geometryValid: true},
			}
			request := sessionTestRequest(local)
			environment := sessionTestWorkerConfig(request).SessionEnvironment
			worker, err := supervisor.newAttachmentWorker(request, attachmentTab{}, nil, environment)
			require.NoError(t, err)
			view := *sessionTestOutput(1, "").Context
			view.TabID = "t_second"
			worker.(*sessionAttachmentWorker).noteCommitted(nil, view)
			other := view
			other.Route.Target = protocol.ExactSessionTarget{LifecycleID: domain.SessionLifecycleID{2}, SessionName: "beta"}
			other.TabID = "t_beta"
			worker.(*sessionAttachmentWorker).noteCommitted(nil, other)

			for _, explicit := range []domain.TabStableID{"", "t_explicit"} {
				stream := newSessionTestStream()
				attachments := newSessionTestAttachments()
				next, err := supervisor.newAttachmentWorker(request, attachmentTab{preferred: explicit}, nil, environment)
				require.NoError(t, err)
				host := newWorkerTestHost(terminal, nil, attachments.record)
				events := make(chan AttachmentEvent, 1)
				go func() {
					event, _ := host.Run(t.Context(), AttachmentToken{Generation: 1, Attempt: 1}, next, stream)
					events <- event
				}()
				hello := awaitHello(t, stream)
				want := explicit
				if want == "" {
					want = "t_second"
				}
				require.Equal(t, want, hello.PreferredTabID)
				require.NoError(t, protocol.ValidateHello(hello))
				require.NoError(t, stream.Close())
				<-events
			}
		})
	}
}

func TestSessionTabMemorySamePeerRoundTrip(t *testing.T) {
	stream := newSessionTestStream()
	terminal := newWorkerTestTerminal()
	attachments := newSessionTestAttachments()
	var memory sessionTabMemory
	cfg := sessionTestWorkerConfig(sessionTestRequest(true))
	cfg.Tabs = &memory
	events := sessionTestRun(t, stream, terminal, cfg, attachments)
	t.Cleanup(func() { _ = stream.Close(); <-events })
	awaitHello(t, stream)
	stream.deliver(protocol.Welcome{SessionName: "alpha"})
	initial := sessionTestOutput(1, "initial")
	initial.Context.TabID = "t_second"
	stream.deliver(initial)
	attachments.token(t)
	alpha := initial.Context.Route.Target
	beta := protocol.ExactSessionTarget{LifecycleID: domain.SessionLifecycleID{2}, SessionName: "beta"}
	stream.deliver(protocol.AttachTarget{Session: "beta", Intent: protocol.IntentAttach, ExactTarget: &beta, SamePeer: true, EnvironmentPolicy: protocol.EnvironmentPolicyDaemonOwned})
	first := awaitSent(t, stream, "SamePeerSwitchRequest", isSent[protocol.SamePeerSwitchRequest]).(protocol.SamePeerSwitchRequest)
	require.Empty(t, first.PreferredTabID)
	output := sessionTestOutput(2, "beta")
	output.Epoch = 2
	output.Context.Route.Target = beta
	output.Context.TabID = "t_beta"
	stream.deliver(output)
	awaitSent(t, stream, "Ack", func(message protocol.ClientMessage) bool {
		ack, ok := message.(protocol.Ack)
		return ok && ack.Epoch == output.Epoch
	})
	stream.deliver(protocol.AttachTarget{Session: "alpha", Intent: protocol.IntentAttach, ExactTarget: &alpha, SamePeer: true, EnvironmentPolicy: protocol.EnvironmentPolicyDaemonOwned})
	second := awaitSent(t, stream, "return switch", func(message protocol.ClientMessage) bool {
		request, ok := message.(protocol.SamePeerSwitchRequest)
		return ok && request.RequestID == 2
	}).(protocol.SamePeerSwitchRequest)
	require.Equal(t, domain.TabStableID("t_second"), second.PreferredTabID)
}
