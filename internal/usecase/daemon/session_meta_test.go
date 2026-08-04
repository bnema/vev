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
	firstScreenFrame := <-sends
	require.Equal(t, ports.MsgSessionMeta, firstMetaFrame.Type)
	require.Equal(t, ports.MsgScreenUpdate, firstScreenFrame.Type)
	firstScreen, err := ports.UnmarshalScreenUpdate(firstScreenFrame.Payload)
	require.NoError(t, err)
	require.Equal(t, ports.ScreenUpdateSnapshot, firstScreen.Kind)
	firstMeta, err := ports.UnmarshalSessionMeta(firstMetaFrame.Payload)
	require.NoError(t, err)
	require.Equal(t, "work", firstMeta.SessionName)

	sess.mu.Lock()
	sess.tabs[0].name = "renamed"
	sess.mu.Unlock()
	emit("changed")
	changedMetaFrame := <-sends
	changedScreenFrame := <-sends
	require.Equal(t, ports.MsgSessionMeta, changedMetaFrame.Type)
	require.Equal(t, ports.MsgScreenUpdate, changedScreenFrame.Type)
	changedScreen, err := ports.UnmarshalScreenUpdate(changedScreenFrame.Payload)
	require.NoError(t, err)
	require.Equal(t, ports.ScreenUpdateSnapshot, changedScreen.Kind)
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
	require.Equal(t, ports.MsgScreenUpdate, (<-sends).Type)

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
	select {
	case frame := <-sends:
		t.Fatalf("unexpected frame after metadata-only paint: %v", frame.Type)
	default:
	}
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

func TestProxiedAttachedCommandMutationFailureDoesNotReportExecution(t *testing.T) {
	d, sess, ac, _ := newManualSessionWithPTYs(t, newQuietPTY())
	ac.proxied = true
	d.attachCoordinator(sess, ac, true)
	d.ptys = failingPTYFactory{err: errors.New("tab open failed")}
	token := sess.attachmentToken(ac, ac.transport())

	result := d.executeAttachedCommand(token, ports.CommandRequest{
		Version: ports.ProtocolVersion, RequestID: 41, Attached: true, Slug: "new-tab",
	})

	require.False(t, result.OK)
	require.Equal(t, ports.ErrInternal, result.Code)
	require.Contains(t, result.Text, "tab open failed")
	sess.mu.Lock()
	require.Len(t, sess.tabs, 1, "failed attached command must not publish a new tab")
	sess.mu.Unlock()
}

func TestProxiedAttachedCommandValidationPreservesRequestID(t *testing.T) {
	tests := []struct {
		name    string
		request ports.CommandRequest
		ok      bool
		code    uint16
		text    string
	}{
		{
			name: "valid attached command",
			request: ports.CommandRequest{
				Version: ports.ProtocolVersion, RequestID: 41, Attached: true, Slug: "next-tab",
			},
			ok: true,
		},
		{
			name: "target override is rejected",
			request: ports.CommandRequest{
				Version: ports.ProtocolVersion, RequestID: 42, Attached: true, Slug: "next-tab", TargetSession: "other",
			},
			code: ports.ErrInvalidCommandArgs,
			text: "attached commands cannot override their active session target",
		},
		{
			name: "missing attached flag is rejected",
			request: ports.CommandRequest{
				Version: ports.ProtocolVersion, RequestID: 43, Slug: "next-tab",
			},
			code: ports.ErrInvalidCommandArgs,
			text: "attached command flag is required",
		},
		{
			name: "unsupported version is rejected",
			request: ports.CommandRequest{
				Version: ports.ProtocolVersion + 1, RequestID: 44, Attached: true, Slug: "next-tab",
			},
			code: ports.ErrInvalidCommandArgs,
			text: "unsupported command protocol version",
		},
		{
			name: "missing request id is rejected",
			request: ports.CommandRequest{
				Version: ports.ProtocolVersion, Attached: true, Slug: "next-tab",
			},
			code: ports.ErrInvalidCommandArgs,
			text: "command request id is required",
		},
		{
			name: "self target is rejected",
			request: ports.CommandRequest{
				Version: ports.ProtocolVersion, RequestID: 45, Attached: true, Self: true, Slug: "next-tab",
			},
			code: ports.ErrInvalidCommandArgs,
			text: "attached commands cannot target themselves",
		},
		{
			name: "unknown slug is rejected",
			request: ports.CommandRequest{
				Version: ports.ProtocolVersion, RequestID: 46, Attached: true, Slug: "missing",
			},
			code: ports.ErrUnknownCommand,
			text: "unknown command: missing",
		},
		{
			name: "local detach is not scriptable",
			request: ports.CommandRequest{
				Version: ports.ProtocolVersion, RequestID: 47, Attached: true, Slug: "detach",
			},
			code: ports.ErrNotScriptable,
			text: "detach is owned by the local proxy daemon",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d, sess, ac, sends := newManualSessionWithPTYs(t, newQuietPTY())
			ac.proxied = true
			d.attachCoordinator(sess, ac, true)
			payload, err := ports.MarshalCommandRequest(tt.request)
			require.NoError(t, err)

			token := sess.attachmentToken(ac, ac.transport())
			require.False(t, d.handleAttachmentClientFrame(token, ports.Frame{Type: ports.MsgCommand, Payload: payload}))

			reply := awaitFrame(t, sends, ports.MsgCommandResult)
			result, err := ports.UnmarshalCommandResult(reply.Payload)
			require.NoError(t, err)
			require.Equal(t, tt.request.RequestID, result.RequestID)
			require.Equal(t, tt.ok, result.OK)
			require.Equal(t, tt.code, result.Code)
			require.Equal(t, tt.text, result.Text)
		})
	}
}
