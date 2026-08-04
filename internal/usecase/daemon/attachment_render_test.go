package daemon

import (
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/stretchr/testify/require"
)

type blockedAttachmentTransport struct {
	entered chan struct{}
	release chan struct{}
	sends   chan ports.Frame
	blocked atomic.Bool
	once    sync.Once
}

func (t *blockedAttachmentTransport) Send(frame ports.Frame) error {
	if frame.Type == ports.MsgOutput && t.blocked.Load() {
		t.once.Do(func() { close(t.entered) })
		<-t.release
	}
	t.sends <- frame
	return nil
}
func (*blockedAttachmentTransport) Recv() (ports.Frame, error) { return ports.Frame{}, io.EOF }
func (*blockedAttachmentTransport) Close() error               { return nil }

func TestAttachmentResizeKeepsSessionContentAndPeersFixed(t *testing.T) {
	pty, releasePTY := newBlockingPTY(t)
	defer releasePTY()
	d := newTestDaemon(t, newFactory(t, pty), stubClock{})
	firstTransport, _ := newCapturingTransport(t)
	sess, first, err := d.route(ports.Hello{
		Version: ports.ProtocolVersion, Intent: ports.IntentNew, Name: "work",
		Size: domain.Size{Cols: 80, Rows: 24}, ClientID: [16]byte{1},
	}, firstTransport)
	require.NoError(t, err)
	secondTransport, _ := newCapturingTransport(t)
	_, second, err := d.route(ports.Hello{
		Version: ports.ProtocolVersion, Intent: ports.IntentAttach, Name: "work",
		Size: domain.Size{Cols: 100, Rows: 40}, ClientID: [16]byte{2},
	}, secondTransport)
	require.NoError(t, err)

	tb := sess.tabs[0]
	tb.mu.Lock()
	contentBefore := tb.size
	tb.mu.Unlock()
	firstSize := first.size
	secondRevision := second.viewSnapshot().revision
	secondEpoch := second.output.currentEpoch()

	rc := sess.renderCoordinator()
	lease := rc.attachmentLease(second)
	require.True(t, rc.markAttachmentReady(lease))
	token := sess.attachmentToken(second, secondTransport)
	token.lease = lease
	ticket, admitted := second.beginAttachmentEffect(token)
	require.True(t, admitted)
	token.effect = ticket
	require.True(t, d.resizeAttachmentForLease(token, domain.Size{Cols: 120, Rows: 50}))
	ticket.End()

	tb.mu.Lock()
	require.Equal(t, contentBefore, tb.size)
	tb.mu.Unlock()
	require.Equal(t, firstSize, first.size)
	require.Equal(t, domain.Size{Cols: 120, Rows: 50}, second.size)
	require.Equal(t, secondRevision+1, second.viewSnapshot().revision)
	require.Greater(t, second.output.currentEpoch(), secondEpoch)
}

func TestAttachmentPaintFanoutDoesNotWaitForBlockedPeer(t *testing.T) {
	pty, releasePTY := newBlockingPTY(t)
	defer releasePTY()
	d := newTestDaemon(t, newFactory(t, pty), stubClock{})
	firstTransport := &blockedAttachmentTransport{entered: make(chan struct{}), release: make(chan struct{}), sends: make(chan ports.Frame, 8)}
	sess, _, err := d.route(ports.Hello{Version: ports.ProtocolVersion, Intent: ports.IntentNew, Name: "work", Size: domain.Size{Cols: 80, Rows: 24}, ClientID: [16]byte{1}}, firstTransport)
	require.NoError(t, err)
	secondTransport := &blockedAttachmentTransport{entered: make(chan struct{}), release: make(chan struct{}), sends: make(chan ports.Frame, 8)}
	_, _, err = d.route(ports.Hello{Version: ports.ProtocolVersion, Intent: ports.IntentAttach, Name: "work", Size: domain.Size{Cols: 80, Rows: 24}, ClientID: [16]byte{2}}, secondTransport)
	require.NoError(t, err)
	rc := sess.renderCoordinator()
	for _, ac := range sess.snapshotAttachments() {
		require.True(t, rc.markAttachmentReady(rc.attachmentLease(ac)))
	}
	firstTransport.blocked.Store(true)

	sess.tabs[0].focusedPane().screen.Write([]byte("shared"))
	rc.invalidate(renderInvalidation{class: invalidateUrgent, reset: true, producer: "test"})
	rc.fireCurrent(false)
	select {
	case <-firstTransport.entered:
	case <-time.After(time.Second):
		t.Fatal("first attachment did not reach blocked send")
	}
	select {
	case frame := <-secondTransport.sends:
		require.Equal(t, ports.MsgOutput, frame.Type)
		out, err := ports.UnmarshalOutput(frame.Payload)
		require.NoError(t, err)
		require.True(t, out.Full, "fan-out peers must send a fresh first frame even after another attachment acknowledges shared damage")
	case <-time.After(time.Second):
		t.Fatal("second attachment waited for first attachment transport")
	}
	close(firstTransport.release)
}
