package daemon

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
)

func TestInactiveSessionPredicates(t *testing.T) {
	pending := make(chan struct{})
	settled := make(chan struct{})
	close(settled)

	tests := []struct {
		name           string
		entry          inactiveSession
		visible        bool
		broken         bool
		canResume      bool
		restorePending bool
	}{
		{name: "healthy down", entry: inactiveSession{state: ports.SessionDown}, visible: true, canResume: true},
		{name: "restore pending remains resumable after wait", entry: inactiveSession{state: ports.SessionDown, restoreDone: pending}, visible: true, canResume: true, restorePending: true},
		{name: "settled restore", entry: inactiveSession{state: ports.SessionDown, restoreDone: settled}, visible: true, canResume: true},
		{name: "broken", entry: inactiveSession{state: ports.SessionBroken}, visible: true, broken: true},
		{name: "degraded", entry: inactiveSession{state: ports.SessionDown, record: domain.CatalogueRecord{DegradedReason: "checkpoint unavailable"}}, visible: true, broken: true},
		{name: "purging", entry: inactiveSession{state: ports.SessionDown, purging: true}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.visible, test.entry.visible())
			require.Equal(t, test.broken, test.entry.broken())
			require.Equal(t, test.canResume, test.entry.canResume())
			require.Equal(t, test.restorePending, test.entry.restorePending())
		})
	}
}

func TestInactiveSessionLifecycleMatch(t *testing.T) {
	entry := inactiveSession{name: "work", createdAt: 7, incarnation: domain.IncarnationID{1}}
	require.True(t, entry.sameLifecycle(entry))

	replacement := entry
	replacement.incarnation[0]++
	require.False(t, entry.sameLifecycle(replacement))
}
