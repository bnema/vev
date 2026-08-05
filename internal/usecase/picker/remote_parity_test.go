package picker

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
)

func remoteParityLifecycle(seed byte) domain.SessionLifecycleID {
	var id domain.SessionLifecycleID
	id[0] = seed
	return id
}

func TestRemoteRowsWithDuplicateLabelsKeepDistinctRoutingIdentity(t *testing.T) {
	firstLifecycle := remoteParityLifecycle(1)
	secondLifecycle := remoteParityLifecycle(2)
	firstKey := domain.RemoteSessionKey{Host: "arch", Name: "work", LifecycleID: firstLifecycle, DisplayOrigin: "same-origin"}
	secondKey := domain.RemoteSessionKey{Host: "mule", Name: "work", LifecycleID: secondLifecycle, DisplayOrigin: "same-origin"}
	firstTarget := domain.RemoteSessionTarget{Endpoint: "arch", DisplayOrigin: "same-origin", LifecycleID: firstLifecycle, SessionName: "work", LiveTabID: "tab-a"}
	secondTarget := domain.RemoteSessionTarget{Endpoint: "mule", DisplayOrigin: "same-origin", LifecycleID: secondLifecycle, SessionName: "work", LiveTabID: "tab-b"}
	model := New([]SessionView{
		{
			ID: firstKey.ID(), Name: "same visible label", Tabs: []TabEntry{{TabID: "tab-a", Name: "main"}},
			RemoteKey: &firstKey, RemoteTarget: &firstTarget, RemoteAvailability: RemoteFresh, RemoteAttachReady: true,
		},
		{
			ID: secondKey.ID(), Name: "same visible label", Tabs: []TabEntry{{TabID: "tab-b", Name: "main"}},
			RemoteKey: &secondKey, RemoteTarget: &secondTarget, RemoteAvailability: RemoteFresh, RemoteAttachReady: true,
		},
	}, SelectionConfig{Mode: SelectNavigationTab})

	first, ok := model.Selected()
	require.True(t, ok)
	require.Equal(t, firstKey.ID(), first.Session)
	require.NotNil(t, first.RemoteTarget)
	require.Equal(t, firstLifecycle, first.RemoteTarget.LifecycleID)
	require.Equal(t, domain.TabStableID("tab-a"), first.RemoteTarget.LiveTabID)

	model.Down()
	second, ok := model.Selected()
	require.True(t, ok)
	require.Equal(t, secondKey.ID(), second.Session)
	require.NotNil(t, second.RemoteTarget)
	require.Equal(t, secondLifecycle, second.RemoteTarget.LifecycleID)
	require.Equal(t, domain.TabStableID("tab-b"), second.RemoteTarget.LiveTabID)
}
