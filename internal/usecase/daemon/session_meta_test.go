package daemon

import (
	"errors"
	"io"
	"testing"

	"github.com/bnema/vev/internal/ports"
	"github.com/stretchr/testify/require"
)

type sessionMetaFailTransport struct{}

func (sessionMetaFailTransport) Send(ports.Frame) error     { return errors.New("metadata send failed") }
func (sessionMetaFailTransport) Recv() (ports.Frame, error) { return ports.Frame{}, io.EOF }
func (sessionMetaFailTransport) Close() error               { return nil }

func TestProxiedMetadataPrecedesChangedOutput(t *testing.T) {
	d, sess, ac, sends := newManualSessionWithPTYs(t, newQuietPTY())
	ac.proxied = true

	emit := func(title string) {
		state := cacheBarState(title, true)
		state.attachment = ac
		composed := composeFrame(state, ac.pipelineCache, ac.pipelineScratch)
		ac.sendMu.Lock()
		require.True(t, d.emitFrame(sess, ac, &state, composed))
	}

	emit("initial")
	firstMetaFrame := <-sends
	firstOutputFrame := <-sends
	require.Equal(t, ports.MsgSessionMeta, firstMetaFrame.Type)
	require.Equal(t, ports.MsgOutput, firstOutputFrame.Type)
	firstMeta, err := ports.UnmarshalSessionMeta(firstMetaFrame.Payload)
	require.NoError(t, err)
	require.Equal(t, "work", firstMeta.SessionName)

	sess.mu.Lock()
	sess.tabs[0].name = "renamed"
	sess.mu.Unlock()
	emit("changed")
	changedMetaFrame := <-sends
	changedOutputFrame := <-sends
	require.Equal(t, ports.MsgSessionMeta, changedMetaFrame.Type)
	require.Equal(t, ports.MsgOutput, changedOutputFrame.Type)
	changedMeta, err := ports.UnmarshalSessionMeta(changedMetaFrame.Payload)
	require.NoError(t, err)
	require.Equal(t, "renamed", changedMeta.Tabs[0].Name)
}

func TestProxiedMetadataSendsWithoutOutputBytes(t *testing.T) {
	d, sess, ac, sends := newManualSessionWithPTYs(t, newQuietPTY())
	ac.proxied = true

	emit := func(reset bool) {
		state := cacheBarState("steady", reset)
		state.attachment = ac
		composed := composeFrame(state, ac.pipelineCache, ac.pipelineScratch)
		ac.sendMu.Lock()
		require.True(t, d.emitFrame(sess, ac, &state, composed))
	}

	emit(true)
	require.Equal(t, ports.MsgSessionMeta, (<-sends).Type)
	require.Equal(t, ports.MsgOutput, (<-sends).Type)

	sess.mu.Lock()
	sess.tabs[0].name = "renamed"
	sess.mu.Unlock()
	// The recomposed frame is identical, so this transaction carries metadata
	// only: a zero-byte terminal frame must still refresh the proxied snapshot.
	emit(false)

	metaFrame := awaitTestValue(t, sends, "a zero-byte frame did not publish changed metadata")
	require.Equal(t, ports.MsgSessionMeta, metaFrame.Type)
	meta, err := ports.UnmarshalSessionMeta(metaFrame.Payload)
	require.NoError(t, err)
	require.Equal(t, "renamed", meta.Tabs[0].Name)
	requireNoOutputFrame(t, sends)
}

func TestSessionMetaSnapshotIsImmutable(t *testing.T) {
	_, sess, _, _ := newManualSessionWithPTYs(t, newQuietPTY())
	meta, ok := sess.sessionMetaSnapshot()
	require.True(t, ok)

	sess.mu.Lock()
	sess.name = "renamed-session"
	sess.tabs[0].name = "renamed-tab"
	sess.mu.Unlock()

	require.Equal(t, "work", meta.SessionName)
	require.Equal(t, "", meta.Tabs[0].Name)
}

func TestSessionMetaShadowUpdatesOnlyAfterSuccessfulSend(t *testing.T) {
	_, sess, ac, _ := newManualSessionWithPTYs(t, newQuietPTY())
	ac.proxied = true
	ac.replaceTransport(sessionMetaFailTransport{})

	ac.sendMu.Lock()
	err := ac.sendSessionMetaIfChanged(sess, ac.transportSnapshot(), nil)
	ac.sendMu.Unlock()
	require.Error(t, err)
	require.False(t, ac.sessionMetaSent)
}

func TestProxiedAttachedCommandRejectionPreservesRequestID(t *testing.T) {
	tests := []struct {
		name     string
		request  ports.CommandRequest
		wantCode uint16
		wantText string
	}{
		{
			name: "valid attached command",
			request: ports.CommandRequest{
				Version: ports.ProtocolVersion, RequestID: 41, Attached: true, Slug: "new-tab",
			},
			wantCode: ports.ErrNotScriptable,
			wantText: "attached command relay is not enabled",
		},
		{
			name: "target override is rejected",
			request: ports.CommandRequest{
				Version: ports.ProtocolVersion, RequestID: 42, Attached: true, Slug: "new-tab", TargetSession: "other",
			},
			wantCode: ports.ErrNotScriptable,
			wantText: "attached command relay is not enabled",
		},
		{
			name: "missing attached flag is rejected",
			request: ports.CommandRequest{
				Version: ports.ProtocolVersion, RequestID: 43, Slug: "new-tab",
			},
			wantCode: ports.ErrNotScriptable,
			wantText: "attached command relay is not enabled",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d, sess, ac, sends := newManualSessionWithPTYs(t, newQuietPTY())
			ac.proxied = true
			d.attachCoordinator(sess, nil, ac, true)
			payload, err := ports.MarshalCommandRequest(tt.request)
			require.NoError(t, err)

			token := sess.attachmentToken(ac, ac.transport())
			require.False(t, d.handleActiveClientFrame(token, ports.Frame{Type: ports.MsgCommand, Payload: payload}))

			reply := <-sends
			require.Equal(t, ports.MsgCommandResult, reply.Type)
			result, err := ports.UnmarshalCommandResult(reply.Payload)
			require.NoError(t, err)
			require.Equal(t, tt.request.RequestID, result.RequestID)
			require.False(t, result.OK)
			require.Equal(t, tt.wantCode, result.Code)
			require.Equal(t, tt.wantText, result.Text)
		})
	}
}
