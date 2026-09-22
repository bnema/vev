package daemonmux

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol"
)

// attachmentRequest builds one valid local attachment Open under policy for the
// supplied admission variant, name, target, and environment, so a listener test
// exercises the exact metadata a real attachment carries.
func attachmentRequest(stream int, policy ports.BrokerPolicy, admission ports.BrokerStreamAdmission, name string, target protocol.ExactSessionTarget, env []string) ports.BrokerOpenStreamRequest {
	return ports.BrokerOpenStreamRequest{
		Epoch:      1,
		Purpose:    ports.BrokerStreamAttachment,
		Admission:  admission,
		Name:       name,
		Local:      true,
		Connection: ports.BrokerConnectionID{1},
		Stream:     ports.BrokerStreamID(stream),
		Target:     target,
		Env:        append([]string(nil), env...),
		Policy:     policy,
		StartMode:  ports.BrokerDaemonStartIfNeeded,
	}
}

// admittedConnection opens one request over the harness while a concurrent
// Accept confirms it, returning the accepted typed daemon-side connection. A
// listener confirms Opened only as Accept delivers the stream, so the two must
// run together.
func admittedConnection(t *testing.T, h *listenerHarness, request ports.BrokerOpenStreamRequest) *ListenerConnection {
	t.Helper()
	accepted := h.acceptAsync()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	_, err := h.connector.Open(ctx, request)
	require.NoError(t, err)
	return h.awaitAccept(accepted)
}

// TestListenerStampsProvisionedOrigin proves the accepted connection's admission
// comes from the provisioned binding, not the peer's Open: a listener
// provisioned as local stamps Origin=Local with the accepted policy, and every
// declared field of the Open is carried verbatim.
func TestListenerStampsProvisionedOrigin(t *testing.T) {
	policy := logicalTestPolicy()
	h := newListenerHarness(t, listenerIdleBudget, MaxAcceptQueue)

	// A local control stream carries no attachment state.
	connection := admittedConnection(t, h, controlRequest(1))
	admission, ok := connection.SessionAdmission()
	require.True(t, ok)
	require.Equal(t, ports.SessionOriginLocal, admission.Origin, "the provisioned locality is stamped")
	require.Equal(t, policy, admission.Policy, "the accepted policy is the provisioned one")
	require.Equal(t, ports.BrokerStreamControl, admission.Purpose)
	require.Zero(t, admission.Admission)
	require.Empty(t, admission.Env)
	require.NoError(t, admission.Validate())

	// An attachment stream carries its closed variant, validated name, exact
	// target, and bounded environment.
	target := protocol.ExactSessionTarget{LifecycleID: domain.SessionLifecycleID{7}, SessionName: "work"}
	attach := admittedConnection(t, h, attachmentRequest(2, policy, ports.BrokerAdmissionExact, "", target, []string{"TERM=xterm-256color"}))
	attachAdmission, ok := attach.SessionAdmission()
	require.True(t, ok)
	require.Equal(t, ports.SessionOriginLocal, attachAdmission.Origin)
	require.Equal(t, ports.BrokerStreamAttachment, attachAdmission.Purpose)
	require.Equal(t, ports.BrokerAdmissionExact, attachAdmission.Admission)
	require.Equal(t, target, attachAdmission.Target)
	require.Equal(t, []string{"TERM=xterm-256color"}, attachAdmission.Env)
	require.NoError(t, attachAdmission.Validate())
}

// TestListenerRefusesLiarLocalOrigin proves the accepted origin is enforced, not
// echoed: a listener provisioned for a remote carriage refuses a local Open (and
// one provisioned local refuses a remote Open) exactly on its own stream, so a
// peer can never stamp a provenance the accepting side did not provision.
func TestListenerRefusesLiarLocalOrigin(t *testing.T) {
	policy := logicalTestPolicy()

	t.Run("remote carriage refuses a local Open", func(t *testing.T) {
		brokerPump, daemonPump, start := newPairedPumpsUnstarted(t, DefaultMuxCeilings())
		clock := newListenerClock(time.Now())
		listener, err := newListener(daemonPump, listenerIdleBudget, MaxAcceptQueue, clock, remoteAcceptance(policy))
		require.NoError(t, err)
		t.Cleanup(func() { _ = listener.Close() })
		start()
		connector := mustConnector(t, brokerPump)

		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		_, err = connector.Open(ctx, controlRequest(1))
		require.Error(t, err, "a local Open is refused by a remote carriage")

		refused := requireStreamState(t, daemonPump, 1, StreamTerminal)
		require.Equal(t, domain.RemoteFailureTransport, refused.FailureKind)
		var detail ports.BrokerError
		require.ErrorAs(t, refused.Err, &detail)
		require.Equal(t, invalidAdmissionText, detail.Text)

		// Nothing was queued for the daemon: a refused Open is never delivered.
		listener.mu.Lock()
		queued := len(listener.queue)
		listener.mu.Unlock()
		require.Zero(t, queued, "a refused Open is never queued")
	})

	t.Run("local carriage refuses a remote Open", func(t *testing.T) {
		h := newListenerHarness(t, listenerIdleBudget, MaxAcceptQueue)
		remote := controlRequest(1)
		remote.Local = false
		remote.Endpoint = "dev@host:22"
		remote.Registration = testRegistration()

		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		_, err := h.connector.Open(ctx, remote)
		require.Error(t, err, "a remote Open is refused by a local carriage")

		refused := requireStreamState(t, h.daemonPump, 1, StreamTerminal)
		var detail ports.BrokerError
		require.ErrorAs(t, refused.Err, &detail)
		require.Equal(t, invalidAdmissionText, detail.Text)
	})
}

// TestListenerAdmissionIsolationAndDeepCopy proves every accepted connection
// owns an independent admission: mutating one connection's returned copy never
// changes a sibling's, and concurrent streams each receive exactly the metadata
// their own Open declared.
func TestListenerAdmissionIsolationAndDeepCopy(t *testing.T) {
	policy := logicalTestPolicy()
	h := newListenerHarness(t, listenerIdleBudget, MaxAcceptQueue)

	first := admittedConnection(t, h, attachmentRequest(1, policy, ports.BrokerAdmissionCreateNamed, "alpha", protocol.ExactSessionTarget{}, []string{"TERM=one"}))

	// The provider hands out a defensive copy: mutating it must not alter what
	// the connection (or a later read) reports.
	mutated, ok := first.SessionAdmission()
	require.True(t, ok)
	mutated.Env[0] = "TERM=mutated"
	mutated.Name = "mutated"
	mutated.Origin = ports.SessionOriginRemote
	reread, ok := first.SessionAdmission()
	require.True(t, ok)
	require.Equal(t, "alpha", reread.Name)
	require.Equal(t, ports.SessionOriginLocal, reread.Origin)
	require.Equal(t, []string{"TERM=one"}, reread.Env)

	// Two reads never share the same backing slice.
	left, _ := first.SessionAdmission()
	right, _ := first.SessionAdmission()
	left.Env[0] = "TERM=left"
	require.Equal(t, "TERM=one", right.Env[0])

	// A sibling stream carries only its own declaration; the first's mutation
	// never leaks into it.
	second := admittedConnection(t, h, attachmentRequest(2, policy, ports.BrokerAdmissionCreateEphemeral, "", protocol.ExactSessionTarget{}, nil))
	secondAdmission, ok := second.SessionAdmission()
	require.True(t, ok)
	require.Equal(t, ports.BrokerAdmissionCreateEphemeral, secondAdmission.Admission)
	require.Empty(t, secondAdmission.Name)
	require.Empty(t, secondAdmission.Env)
}

// TestListenerConcurrentAdmissionsKeepDistinctMetadata proves the listener's
// per-stream admission stamp is per stream under concurrency: every admitted
// attachment receives exactly the name, target, and environment its own Open
// declared, and no stream sees a sibling's.
func TestListenerConcurrentAdmissionsKeepDistinctMetadata(t *testing.T) {
	policy := logicalTestPolicy()
	h := newListenerHarness(t, listenerIdleBudget, 64)

	const streams = 24
	type expectation struct {
		admission ports.SessionAdmission
	}
	want := make(map[ports.BrokerStreamID]expectation, streams)
	opened := make(chan *ListenerConnection, streams)
	openErrs := make(chan error, streams)
	for i := 0; i < streams; i++ {
		stream := ports.BrokerStreamID(i + 1)
		name := ""
		if i%2 == 0 {
			name = []string{"alpha", "beta", "gamma"}[i%3]
		}
		env := []string{[]string{"TERM=first", "TERM=second", "TERM=third"}[i%3]}
		admission := ports.BrokerAdmissionCreateNamed
		if name == "" {
			admission = ports.BrokerAdmissionCreateEphemeral
		}
		want[stream] = expectation{admission: ports.SessionAdmission{
			Origin: ports.SessionOriginLocal, Policy: policy, Purpose: ports.BrokerStreamAttachment,
			Admission: admission, Name: name, Env: env,
		}}
		request := attachmentRequest(i+1, policy, admission, name, protocol.ExactSessionTarget{}, env)
		go func(request ports.BrokerOpenStreamRequest) {
			// The Accept that confirms this stream must be in flight before the
			// Open, because a listener sends Opened only as Accept delivers.
			accepted := h.acceptAsync()
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			_, err := h.connector.Open(ctx, request)
			if err != nil {
				openErrs <- err
				return
			}
			opened <- h.awaitAccept(accepted)
		}(request)
	}

	for i := 0; i < streams; i++ {
		select {
		case err := <-openErrs:
			require.NoError(t, err)
		case connection := <-opened:
			admission, ok := connection.SessionAdmission()
			require.True(t, ok)
			expected, tracked := want[connection.Ref().Client]
			require.True(t, tracked)
			require.Equal(t, expected.admission, admission, "stream %d", connection.Ref().Client)
		case <-time.After(p3cTestDeadline):
			t.Fatal("concurrent admissions did not all complete")
		}
	}
}
