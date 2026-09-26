package client

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol"
)

// Supervisor halves of picker parity (Plan 003 B1/B2): the in-place tab switch
// over a live attachment and the `x` kill through the broker operations seam.
// They drive the real supervisor, host, foreground, and worker with bounded
// waits and no sleeps.

// parityTestPicker adds the optional tab, kill, and notice seams of the real
// picker to the scripted picker. The fields are guarded by attachTestPicker.mu.
type parityTestPicker struct {
	*attachTestPicker
	tab     attachmentTab
	kill    pickerKillTarget
	notices []string
}

func newParityTestPicker() *parityTestPicker {
	return &parityTestPicker{attachTestPicker: newAttachTestPicker()}
}

func (p *parityTestPicker) ResolveKeyTarget(key string, base pickerResolveBase) (ports.BrokerOpenStreamRequest, attachmentTab, error) {
	request, err := p.ResolveKey(key, base)
	p.mu.Lock()
	defer p.mu.Unlock()
	return request, p.tab, err
}

func (p *parityTestPicker) ResolveKill(string) (pickerKillTarget, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.kill, nil
}

func (p *parityTestPicker) Notify(n domain.Notification) {
	p.mu.Lock()
	p.notices = append(p.notices, n.Message)
	p.mu.Unlock()
}

func (p *parityTestPicker) invalidatePresentation() {}

func (p *parityTestPicker) noticeList() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]string(nil), p.notices...)
}

// TestPickerOverlayCommitAnotherTabSwitchesInPlace pins the tab-aware same
// target: committing another tab of the attached session switches the live
// attachment's view with SelectTab and never reconnects; the attached tab or
// the session itself just closes the overlay.
func TestPickerOverlayCommitAnotherTabSwitchesInPlace(t *testing.T) {
	tests := []struct {
		name    string
		tab     attachmentTab
		inPlace bool
	}{
		{name: "another tab", tab: attachmentTab{preferred: "t_other"}, inPlace: true},
		{name: "the attached tab", tab: attachmentTab{preferred: "t_abc123"}},
		{name: "the session row", tab: attachmentTab{}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			picker := newParityTestPicker()
			harness := startAttachHarness(t, picker)
			stream := attachLiveSession(t, harness, picker.attachTestPicker)
			foreground := harness.sup.attachments.authority.foreground()
			stream.deliver(navigationOffer(1))
			awaitPresentation(t, harness.sup, PresentAttachedPicker)

			picker.mu.Lock()
			picker.tab = tt.tab
			picker.mu.Unlock()
			picker.recordOp(pickerOp{commit: true}, "row", sessionTestRequest(true))

			awaitPresentation(t, harness.sup, PresentAttached)
			awaitSent(t, stream, "OutputResetRequest", isSent[protocol.OutputResetRequest])
			if tt.inPlace {
				selected := awaitSent(t, stream, "SelectTab", isSent[protocol.SelectTab]).(protocol.SelectTab)
				require.Equal(t, tt.tab.preferred, selected.TabID)
			} else {
				require.Zero(t, countSent[protocol.SelectTab](stream))
			}
			require.Same(t, foreground, harness.sup.attachments.authority.foreground(), "the attachment is never replaced")
			require.Len(t, harness.service.openedRequests(), 1, "a tab switch never reconnects")
			require.Zero(t, countSent[protocol.Detach](stream))
			require.False(t, stream.closedNow())
		})
	}
}

// TestSupervisorPickerKill pins B1 end to end: `x` resolves the row, sends one
// typed Kill on its own local control stream, reports the typed outcome as a
// notice, and asks the broker to re-observe the local daemon. An unknown
// outcome is reported once and never replayed.
func TestSupervisorPickerKill(t *testing.T) {
	tests := []struct {
		name       string
		stopped    bool
		overlay    bool
		reply      func(*brokerOpsTestStream)
		wantNotice string
	}{
		{name: "live session", reply: func(s *brokerOpsTestStream) {
			s.deliver(protocol.KillResult{RequestID: brokerOperationRequestID, Outcome: protocol.KillSucceeded})
		}, wantNotice: "killed session alpha"},
		{name: "stopped session history", stopped: true, reply: func(s *brokerOpsTestStream) {
			s.deliver(protocol.KillResult{RequestID: brokerOperationRequestID, Outcome: protocol.KillSucceeded})
		}, wantNotice: "deleted stopped session alpha"},
		{name: "daemon refusal", reply: func(s *brokerOpsTestStream) {
			s.deliver(protocol.KillResult{RequestID: brokerOperationRequestID, Outcome: protocol.KillFailed, Code: protocol.ErrNoSuchSession, Text: "no such session: alpha"})
		}, wantNotice: "couldn't kill alpha: no such session: alpha"},
		{name: "outcome unknown is never replayed", reply: func(s *brokerOpsTestStream) {
			s.failReceive(errors.New("carriage lost"))
			_ = s.Close()
		}, wantNotice: "kill alpha: outcome unknown"},
		{name: "from the overlay", overlay: true, reply: func(s *brokerOpsTestStream) {
			s.deliver(protocol.KillResult{RequestID: brokerOperationRequestID, Outcome: protocol.KillSucceeded})
		}, wantNotice: "killed session alpha"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			picker := newParityTestPicker()
			harness := startAttachHarness(t, picker)
			harness.service.publishSnapshot(localDaemonSnapshot(3, 2))

			var mu sync.Mutex
			controls := make([]*brokerOpsTestStream, 0, 1)
			session := newSessionTestStream()
			harness.service.setOpenStream(func(_ context.Context, request ports.BrokerOpenStreamRequest) (ports.BrokerLogicalConnection, error) {
				if request.Purpose != ports.BrokerStreamControl {
					return session, nil
				}
				control := newBrokerOpsTestStream()
				mu.Lock()
				controls = append(controls, control)
				mu.Unlock()
				return control, nil
			})
			if tt.overlay {
				picker.commit(sessionTestRequest(true))
				awaitStreamHello(t, &sync.Mutex{}, &session)
				deliverReadyStream(t, session)
				awaitAttachedState(t, harness.sup)
				session.deliver(navigationOffer(1))
				awaitPresentation(t, harness.sup, PresentAttachedPicker)
			}

			picker.mu.Lock()
			picker.kill = pickerKillTarget{route: BrokerOperationRoute{Epoch: 3, Destination: ports.BrokerEndpointFence{Local: true}}, name: "alpha", stopped: tt.stopped}
			picker.mu.Unlock()
			picker.recordOp(pickerOp{kill: true}, "row", ports.BrokerOpenStreamRequest{})

			var control *brokerOpsTestStream
			require.Eventually(t, func() bool {
				mu.Lock()
				defer mu.Unlock()
				if len(controls) == 0 {
					return false
				}
				control = controls[0]
				return true
			}, 5*time.Second, time.Millisecond, "the kill never opened a control stream")
			control.awaitDispatch(t)
			tt.reply(control)

			require.Eventually(t, func() bool {
				for _, notice := range picker.noticeList() {
					if notice == tt.wantNotice {
						return true
					}
				}
				return false
			}, 5*time.Second, time.Millisecond, "notice %q never shown (got %v)", tt.wantNotice, picker.noticeList())
			require.Eventually(t, func() bool { return len(harness.service.reconcileHints()) > 0 }, 5*time.Second, time.Millisecond)
			for _, hint := range harness.service.reconcileHints() {
				require.Empty(t, hint, "the local daemon is re-observed")
			}

			mu.Lock()
			require.Len(t, controls, 1, "a kill is never replayed")
			mu.Unlock()
			sent := control.messages()
			require.Len(t, sent, 1)
			kill, ok := sent[0].(protocol.Kill)
			require.True(t, ok)
			require.Equal(t, protocol.Kill{RequestID: brokerOperationRequestID, Name: "alpha", Scope: protocol.KillSession}, kill)
			var controlOpens int
			for _, request := range harness.service.openedRequests() {
				if request.Purpose == ports.BrokerStreamControl {
					controlOpens++
					require.True(t, request.Local)
					require.Equal(t, ports.BrokerEpoch(3), request.Epoch)
				}
			}
			require.Equal(t, 1, controlOpens)
			if tt.overlay {
				require.Equal(t, PresentAttachedPicker, harness.sup.State().Presentation, "a kill keeps the overlay open")
				require.False(t, session.closedNow())
			}
			require.False(t, strings.Contains(strings.Join(picker.noticeList(), "\n"), "already in progress"))
		})
	}
}
