package daemon

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAfterFrameQueueRunsOnlyAfterALaterCapture(t *testing.T) {
	tests := []struct {
		name       string
		earlier    int // captures started before registration
		emitted    []uint64
		wantRanAll bool
	}{
		{name: "nothing emitted", earlier: 0, wantRanAll: false},
		{name: "capture started before registration", earlier: 1, emitted: []uint64{1}, wantRanAll: false},
		{name: "capture started after registration", earlier: 1, emitted: []uint64{2}, wantRanAll: true},
		{name: "unnumbered emission", earlier: 0, emitted: []uint64{0}, wantRanAll: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var q afterFrameQueue
			for range tt.earlier {
				q.beginCapture()
			}
			ran := 0
			q.add(func() { ran++ })
			q.beginCapture()
			for _, capture := range tt.emitted {
				q.emitted(capture)
			}
			q.runReady()
			q.runReady()
			if tt.wantRanAll {
				require.Equal(t, 1, ran, "work must run exactly once")
			} else {
				require.Zero(t, ran)
			}
		})
	}
}

// Work queued for an attachment whose capability changed before the frame is
// dropped instead of sending on a stale connection.
func TestAfterNextFrameDropsWorkAfterCapabilityChange(t *testing.T) {
	d, source, ac, _, _, releases, _ := setupMovePickerSessionsCore(t, stubClock{}, 0)
	defer releaseAll(releases)
	effect, admitted := ac.beginAttachmentEffect(captureAttachmentCapability(source, ac, ac.transport()))
	require.True(t, admitted)
	stale := effect.capability()
	stale.transport.incarnation++
	stale.transport.transport = nil
	effect.End()

	ran := false
	ac.afterFrame.add(func() {
		fresh, ok := ac.beginAttachmentEffect(stale)
		if ok {
			ran = true
			fresh.End()
		}
	})
	d.paint(source, ac, true, nil)
	require.False(t, ran)
}
