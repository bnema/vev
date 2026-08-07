package picker

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/pkg/renderer"
)

func remoteParityLifecycle(seed byte) domain.SessionLifecycleID {
	var id domain.SessionLifecycleID
	id[0] = seed
	return id
}

func TestRemoteSessionHeaderIncludesOrigin(t *testing.T) {
	lifecycle := remoteParityLifecycle(1)
	key := domain.RemoteSessionKey{Host: "vev@arch", Name: "work", LifecycleID: lifecycle, DisplayOrigin: "arch"}
	target := domain.RemoteSessionTarget{Endpoint: key.Host, DisplayOrigin: key.DisplayOrigin, LifecycleID: lifecycle, SessionName: key.Name, LiveTabID: "tab-1"}
	model := New([]SessionView{{
		ID: key.ID(), Name: key.Name, Tabs: []TabEntry{{TabID: "tab-1", Name: "main"}},
		RemoteKey: &key, RemoteTarget: &target, RemoteAvailability: RemoteFresh, RemoteAttachReady: true,
	}}, SelectionConfig{Mode: SelectNavigationTab})

	require.Equal(t, "work", model.rows[0].dispName)
	require.Equal(t, "@arch", model.rows[0].detail)
	frame := model.Render(domain.Size{Cols: 32, Rows: 4}, Preview{}, RenderStyles{
		Name: renderer.Style{Bold: true}, Detail: renderer.Style{Attrs: renderer.AttrDim},
	})
	require.Equal(t, '@', frame.At(4, 0).Rune)
	require.True(t, frame.At(4, 0).Style.Equal(renderer.Style{Attrs: renderer.AttrDim}))
	selected, ok := model.Selected()
	require.True(t, ok)
	require.Equal(t, key.ID(), selected.Session)
	require.Equal(t, &target, selected.RemoteTarget)
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

	expected := []struct {
		name      string
		session   domain.SessionID
		endpoint  string
		lifecycle domain.SessionLifecycleID
		tabID     domain.TabStableID
	}{
		{name: "first", session: firstKey.ID(), endpoint: "arch", lifecycle: firstLifecycle, tabID: "tab-a"},
		{name: "second", session: secondKey.ID(), endpoint: "mule", lifecycle: secondLifecycle, tabID: "tab-b"},
	}
	for i, want := range expected {
		t.Run(want.name, func(t *testing.T) {
			selected, ok := model.Selected()
			require.True(t, ok)
			require.Equal(t, want.session, selected.Session)
			require.NotNil(t, selected.RemoteTarget)
			require.Equal(t, want.endpoint, selected.RemoteTarget.Endpoint)
			require.Equal(t, want.lifecycle, selected.RemoteTarget.LifecycleID)
			require.Equal(t, want.tabID, selected.RemoteTarget.LiveTabID)
		})
		if i+1 < len(expected) {
			model.Down()
		}
	}
}
