package daemon

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/mock"

	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/protocol/wire"
	"github.com/stretchr/testify/require"
)

func TestUIFenceDispatchReceivesACKAtFullOutputWindow(t *testing.T) {
	writes := make(chan []byte, 1)
	pty, release := newBlockingPTYWithWrites(t, writes)
	t.Cleanup(release)
	d, sess, ac, sends := newManualSessionWithPTYs(t, pty)
	rc := d.attachCoordinator(sess, nil, ac, true)
	lease := rc.attachmentLease(ac)
	invalidated := make(chan struct{}, 1)
	rc.opts.onInvalidate = func(inv renderInvalidation) {
		if inv.producer == "ui-fence" {
			invalidated <- struct{}{}
		}
	}
	t.Cleanup(func() { rc.beginSessionTeardown().finish(); rc.waitForTimerWorkers() })
	ac.output.setWindow(1)
	require.Equal(t, paintEmitted, d.paint(sess, ac, true, lease))
	initial, err := wire.UnmarshalOutput(awaitFrame(t, sends, wire.MsgOutput).Payload)
	require.NoError(t, err)
	require.True(t, ac.output.atCapacity())
	require.Empty(t, drainAllFrames(sends))

	// A single dispatch owner receives Input, UIFence, and then the ACK that
	// makes room. Neither registration nor its synchronous test-clock render
	// fallback may wait for that same owner to dispatch the ACK.
	messages := make(chan protocol.ClientMessage)
	dispatched := make(chan bool, 1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for message := range messages {
			capability := captureAttachmentCapability(sess, ac, ac.transport())
			dispatched <- d.handleAttachmentClientMessage(capability, message)
		}
	}()
	t.Cleanup(func() { close(messages); awaitTestCompletion(t, done, "dispatch owner leaked") })
	messages <- protocol.Input{InputSeq: 1, ActionID: 9, Data: []byte("x")}
	require.False(t, awaitTestValue(t, dispatched, "input dispatch blocked"))
	require.Equal(t, []byte("x"), awaitTestValue(t, writes, "normal PTY input was bypassed"))
	messages <- protocol.UIFence{ActionID: 9}
	require.False(t, awaitTestValue(t, dispatched, "fence dispatch waited for an output ACK"))
	awaitTestValue(t, invalidated, "fence did not reach ACK-deferred render scheduling")
	require.Empty(t, drainAllFrames(sends), "full output window cannot publish or confirm the fence")
	messages <- protocol.Ack{Epoch: initial.Epoch, State: initial.New}
	require.False(t, awaitTestValue(t, dispatched, "releasing ACK did not complete"))
	update, err := wire.UnmarshalUIViewUpdate(awaitFrame(t, sends, wire.MsgUIViewUpdate).Payload)
	require.NoError(t, err)
	receipt, err := wire.UnmarshalUIReceipt(awaitFrame(t, sends, wire.MsgUIReceipt).Payload)
	require.NoError(t, err)
	require.Equal(t, protocol.UIReceipt{ActionID: 9, Epoch: update.Epoch, State: update.State, ViewPublication: update.Context.Publication, Outcome: protocol.UIReceiptProcessed}, receipt)
	require.Equal(t, initial.New, update.State, "no-op confirmation must not advance the ACK chain")
	require.Equal(t, initial.Context.Publication+1, update.Context.Publication)
	require.Zero(t, ac.echoAck.Load(), "processing is not child echo")
	require.Empty(t, drainAllFrames(sends))
}

func TestUIFencePreRegistrationCaptureCannotConfirmAction(t *testing.T) {
	d, sess, ac, sends := newManualSessionWithPTYs(t, nil)
	// A coordinator without a clock leaves scheduling under the test's exact
	// capture boundary. The registration still uses the real dispatch owner.
	rc := newRenderCoordinator(renderCoordinatorOptions{})
	installAttachmentRenderCoordinator(sess, rc)
	lease := rc.attachWithReadiness(ac, true)
	t.Cleanup(func() { rc.beginSessionTeardown().finish(); rc.waitForTimerWorkers() })
	capture := func(reset bool) *capturedRenderState {
		t.Helper()
		ac.sendMu.Lock()
		state, ok := captureRenderState(sess, ac, renderCaptureRequest{reset: reset, lease: lease, uiFence: rc.captureUIFence(lease)})
		if !ok {
			ac.sendMu.Unlock()
			t.Fatal("capture rejected")
		}
		return state
	}
	emit := func(state *capturedRenderState) {
		t.Helper()
		composed := composeFrame(*state, ac.pipelineCache)
		require.True(t, d.emitFrame(sess, ac, state, composed))
	}
	state := capture(true)
	dispatched := make(chan bool, 1)
	go func() {
		dispatched <- d.handleAttachmentClientMessage(captureAttachmentCapability(sess, ac, ac.transport()), protocol.UIFence{ActionID: 5})
	}()
	require.False(t, awaitTestValue(t, dispatched, "fence dispatch acquired render sendMu"))
	emit(state)
	initial, err := wire.UnmarshalOutput(awaitFrame(t, sends, wire.MsgOutput).Payload)
	require.NoError(t, err)
	require.Empty(t, drainAllFrames(sends), "an earlier capture cannot retire a later fence")
	require.True(t, rc.needsUIFence(lease, rc.captureUIFence(lease)))
	emit(capture(false))
	update, err := wire.UnmarshalUIViewUpdate(awaitFrame(t, sends, wire.MsgUIViewUpdate).Payload)
	require.NoError(t, err)
	receipt, err := wire.UnmarshalUIReceipt(awaitFrame(t, sends, wire.MsgUIReceipt).Payload)
	require.NoError(t, err)
	require.Equal(t, uint64(5), receipt.ActionID)
	require.Equal(t, initial.New, receipt.State)
	require.Equal(t, update.Context.Publication, receipt.ViewPublication)
	require.False(t, rc.needsUIFence(lease, rc.captureUIFence(lease)))
}

func TestUIFenceFailedPublicationOrReceiptDoesNotConfirm(t *testing.T) {
	for _, failedType := range []wire.MsgType{wire.MsgUIViewUpdate, wire.MsgUIReceipt} {
		t.Run(map[wire.MsgType]string{wire.MsgUIViewUpdate: "metadata", wire.MsgUIReceipt: "receipt"}[failedType], func(t *testing.T) {
			d, sess, ac, _ := newManualSessionWithPTYs(t, nil)
			tr := newMockServerConnection(t)
			sends := make(chan wire.Frame, 16)
			fail := false
			tr.EXPECT().Send(mock.Anything).RunAndReturn(func(frame wire.Frame) error {
				if fail && frame.Type == failedType {
					return errors.New("injected send failure")
				}
				sends <- frame
				return nil
			}).Maybe()
			tr.EXPECT().Close().Return(nil).Maybe()
			ac.tr = tr
			rc := newRenderCoordinator(renderCoordinatorOptions{})
			installAttachmentRenderCoordinator(sess, rc)
			lease := rc.attachWithReadiness(ac, true)
			cleanupEntered, releaseCleanup := make(chan struct{}), make(chan struct{})
			d.beforeAttachmentSendErrorCleanup = func(attachmentCapability) {
				close(cleanupEntered)
				<-releaseCleanup
			}
			t.Cleanup(func() {
				close(releaseCleanup)
				d.attachmentCleanupWg.Wait()
				rc.beginSessionTeardown().finish()
				rc.waitForTimerWorkers()
			})
			require.Equal(t, paintEmitted, d.paint(sess, ac, true, lease))
			initial, err := wire.UnmarshalOutput(awaitFrame(t, sends, wire.MsgOutput).Payload)
			require.NoError(t, err)
			require.Empty(t, drainAllFrames(sends))
			require.False(t, d.handleAttachmentClientMessage(captureAttachmentCapability(sess, ac, tr), protocol.UIFence{ActionID: 13}))
			fail = true
			require.Equal(t, paintEmitted, d.paint(sess, ac, false, lease))
			awaitTestValue(t, cleanupEntered, "failed send did not reserve exact-attachment cleanup")
			if failedType == wire.MsgUIViewUpdate {
				require.Equal(t, initial.Context.Publication, ac.output.viewPublication)
				require.True(t, rc.needsUIFence(lease, rc.captureUIFence(lease)), "failed metadata cannot select a receipt")
			} else {
				update, err := wire.UnmarshalUIViewUpdate(awaitFrame(t, sends, wire.MsgUIViewUpdate).Payload)
				require.NoError(t, err)
				require.Equal(t, initial.Context.Publication+1, update.Context.Publication)
				rc.mu.Lock()
				pending := lease.pendingUIFence
				rc.mu.Unlock()
				require.Nil(t, pending, "failed receipt send must release its slot")
				require.False(t, rc.needsUIFence(lease, rc.captureUIFence(lease)))
			}
			require.Empty(t, drainAllFrames(sends), "no successful receipt after a failed publication/send")
		})
	}
}
