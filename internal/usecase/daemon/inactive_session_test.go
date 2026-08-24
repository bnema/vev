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
	for _, test := range []struct {
		name   string
		mutate func(*inactiveSession)
		want   bool
	}{
		{name: "same lifecycle", want: true},
		{name: "different name", mutate: func(candidate *inactiveSession) { candidate.name = "other" }},
		{name: "different creation time", mutate: func(candidate *inactiveSession) { candidate.createdAt++ }},
		{name: "different incarnation", mutate: func(candidate *inactiveSession) { candidate.incarnation[0]++ }},
	} {
		t.Run(test.name, func(t *testing.T) {
			candidate := entry
			if test.mutate != nil {
				test.mutate(&candidate)
			}
			require.Equal(t, test.want, entry.sameLifecycle(candidate))
		})
	}
}
