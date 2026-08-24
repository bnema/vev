package daemon

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
)

func TestRemoteStoppedSelectorsResolveOnlyAgainstExactTabMetadata(t *testing.T) {
	lifecycle := remoteLifecycleForTest()
	stopped := inactiveSession{
		name: "work", incarnation: lifecycle,
		tabNames: []string{"alpha", "beta"},
		tabRecords: []domain.CatalogueTabRecord{
			{StableID: "tab-a", Name: "alpha"},
			{StableID: "tab-b", Name: "beta"},
		},
	}

	stable := domain.RemoteSessionTarget{
		Endpoint: "arch", DisplayOrigin: "arch", LifecycleID: lifecycle,
		SessionName: "work", Stopped: true,
		StoppedTab: domain.NewStableTabSelector("tab-b"),
	}
	index, ok := remoteTargetTabIndexInactive(stopped, stable)
	require.True(t, ok)
	require.Equal(t, 1, index)

	ordinal := stable
	ordinal.StoppedTab = domain.NewOrdinalTabSelector(1, "beta", 2)
	index, ok = remoteTargetTabIndexInactive(stopped, ordinal)
	require.True(t, ok)
	require.Equal(t, 1, index)

	for _, invalid := range []domain.RemoteSessionTarget{
		func() domain.RemoteSessionTarget {
			candidate := stable
			candidate.StoppedTab = domain.NewStableTabSelector("missing")
			return candidate
		}(),
		func() domain.RemoteSessionTarget {
			candidate := ordinal
			candidate.StoppedTab = domain.NewOrdinalTabSelector(1, "replaced", 2)
			return candidate
		}(),
		func() domain.RemoteSessionTarget {
			candidate := ordinal
			candidate.StoppedTab = domain.NewOrdinalTabSelector(1, "beta", 3)
			return candidate
		}(),
	} {
		_, ok := remoteTargetTabIndexInactive(stopped, invalid)
		require.False(t, ok)
	}

	reordered := stopped
	reordered.tabRecords = []domain.CatalogueTabRecord{
		{StableID: "tab-b", Name: "beta"},
		{StableID: "tab-a", Name: "alpha"},
	}
	index, ok = remoteTargetTabIndexInactive(reordered, stable)
	require.True(t, ok)
	require.Equal(t, 0, index, "stable selectors follow identity, not the old ordinal")
}
