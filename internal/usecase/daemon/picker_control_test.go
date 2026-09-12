package daemon

import (
	"testing"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/protocol"
	"github.com/stretchr/testify/require"
)

func TestPickerControlObserveReportsExactAttention(t *testing.T) {
	d, sess, _, _ := newManualSessionWithPTYs(t, nil)
	sess.mu.Lock()
	sess.incarnation = domain.SessionLifecycleID{9}
	sess.tabs[0].attention = true
	target := protocol.ExactSessionTarget{LifecycleID: sess.incarnation, SessionName: sess.name}
	sess.mu.Unlock()
	missing := protocol.ExactSessionTarget{LifecycleID: domain.SessionLifecycleID{8}, SessionName: sess.name}

	response := d.answerPickerControl(protocol.PickerControlRequest{
		Version: protocol.Version, RequestID: 1, Operation: protocol.PickerControlObserve,
		Targets: []protocol.ExactSessionTarget{target, missing},
	})

	require.Equal(t, protocol.PickerSourceOK, response.Status)
	require.Equal(t, []protocol.PickerRouteObservation{
		{Target: target, Presence: protocol.PickerRoutePresent, Attention: true},
		{Target: missing, Presence: protocol.PickerRouteAbsent},
	}, response.Observations)
	require.NoError(t, protocol.ValidatePickerControlResponse(response))
}
