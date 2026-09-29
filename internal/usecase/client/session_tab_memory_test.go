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
	stream.deliver(protocol.SamePeerSwitchFailure{RequestID: 2, Code: protocol.SamePeerSwitchStaleTarget})
	stream.deliver(protocol.AttachTarget{Session: "alpha", Intent: protocol.IntentAttach, ExactTarget: &alpha, SamePeer: true, EnvironmentPolicy: protocol.EnvironmentPolicyDaemonOwned, PreferredTabID: "t_explicit"})
	third := awaitSent(t, stream, "explicit switch", func(message protocol.ClientMessage) bool {
		request, ok := message.(protocol.SamePeerSwitchRequest)
		return ok && request.RequestID == 3
	}).(protocol.SamePeerSwitchRequest)
	require.Equal(t, domain.TabStableID("t_explicit"), third.PreferredTabID)
}

func TestSessionTabMemoryLifecycleCleanup(t *testing.T) {
	local := routeAuthority{local: true}
	remote := routeAuthority{endpoint: "remote"}
	alpha := protocol.ExactSessionTarget{LifecycleID: domain.SessionLifecycleID{1}, SessionName: "alpha"}
	beta := protocol.ExactSessionTarget{LifecycleID: domain.SessionLifecycleID{1}, SessionName: "beta"}
	replacement := protocol.ExactSessionTarget{LifecycleID: domain.SessionLifecycleID{2}, SessionName: "alpha"}
	var memory sessionTabMemory
	memory.remember(local, alpha, "t_old")
	memory.remember(remote, alpha, "t_remote")
	memory.remember(local, beta, "t_beta")
	memory.remember(local, replacement, "t_new")
	memory.remember(local, replacement, "t_latest")
	for _, tt := range []struct {
		name      string
		authority routeAuthority
		target    protocol.ExactSessionTarget
		want      domain.TabStableID
	}{
		{name: "old lifecycle reclaimed", authority: local, target: alpha},
		{name: "new lifecycle retained", authority: local, target: replacement, want: "t_latest"},
		{name: "other host retained", authority: remote, target: alpha, want: "t_remote"},
		{name: "other session retained", authority: local, target: beta, want: "t_beta"},
	} {
		t.Run(tt.name, func(t *testing.T) { require.Equal(t, tt.want, memory.preferred(tt.authority, tt.target, "")) })
	}
}

func TestSessionTabMemoryPreservesStoppedRemoteSelector(t *testing.T) {
	request := sessionTestRequest(false)
	var memory sessionTabMemory
	memory.remember(requestAuthority(request), request.Target, "t_remembered")
	selector := protocol.SessionAttachTarget{LifecycleID: request.Target.LifecycleID, SessionName: request.Target.SessionName, Stopped: true, TabID: "t_explicit", TabIndex: protocol.NoTabIndex}
	cfg := sessionTestWorkerConfig(request)
	cfg.Tabs = &memory
	cfg.Tab = attachmentTab{stopped: &selector}
	cfg.SessionEnvironment = SessionEnvironment{Provenance: SessionEnvironmentRemote}
	stream := newSessionTestStream()
	events := sessionTestRun(t, stream, newWorkerTestTerminal(), cfg, newSessionTestAttachments())
	t.Cleanup(func() { _ = stream.Close(); <-events })
	hello := awaitHello(t, stream)
	require.Equal(t, &selector, hello.SessionTarget)
	require.Empty(t, hello.PreferredTabID)
	require.NoError(t, protocol.ValidateHello(hello))
}
