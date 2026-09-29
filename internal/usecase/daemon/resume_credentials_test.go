package daemon

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestResumeCredentialsLifecycle(t *testing.T) {
	sessA, sessB := &session{}, &session{}
	owner, other := &attachedClient{resumeToken: 7}, &attachedClient{resumeToken: 7}
	tests := []struct {
		name        string
		run         func(c *resumeCredentials)
		wantParked  bool
		wantParking bool
		wantSession *session
	}{
		{name: "parking marker is visible", run: func(c *resumeCredentials) { c.markParking(sessA, owner) }, wantParking: true, wantSession: sessA},
		{name: "publishing park clears the owner's marker", run: func(c *resumeCredentials) {
			c.markParking(sessA, owner)
			c.publishParked(7, &parkedAttachment{sess: sessA, ac: owner})
		}, wantParked: true, wantSession: sessA},
		{name: "another attachment cannot clear the marker", run: func(c *resumeCredentials) {
			c.markParking(sessA, owner)
			c.clearParking(7, other)
		}, wantParking: true, wantSession: sessA},
		{name: "purging another session keeps the marker", run: func(c *resumeCredentials) {
			c.markParking(sessA, owner)
			c.purgeParking(sessB)
		}, wantParking: true, wantSession: sessA},
		{name: "purging all markers", run: func(c *resumeCredentials) {
			c.markParking(sessA, owner)
			c.purgeParking(nil)
		}},
		{name: "stale take is refused", run: func(c *resumeCredentials) {
			current := &parkedAttachment{sess: sessB, ac: owner}
			c.publishParked(7, current)
			require.False(t, c.takeParked(7, &parkedAttachment{sess: sessA, ac: owner}))
		}, wantParked: true, wantSession: sessB},
		{name: "current take removes", run: func(c *resumeCredentials) {
			current := &parkedAttachment{sess: sessA, ac: owner}
			c.publishParked(7, current)
			require.True(t, c.takeParked(7, current))
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			credentials := newResumeCredentials()
			tt.run(&credentials)
			require.Equal(t, tt.wantParked, credentials.parkedFor(7) != nil)
			require.Equal(t, tt.wantParking, credentials.parkingFor(7) != nil)
			require.Same(t, tt.wantSession, credentials.sessionFor(7))
			require.Equal(t, tt.wantSession == sessA, credentials.retainsSession(sessA))
			require.Equal(t, tt.wantSession == sessB, credentials.retainsSession(sessB))
		})
	}
}

func TestResumeCredentialsIssueAvoidsHeldTokens(t *testing.T) {
	credentials := newResumeCredentials()
	for range 64 {
		token := credentials.issue()
		require.NotZero(t, token)
		require.Nil(t, credentials.parkedFor(token))
		credentials.publishParked(token, &parkedAttachment{ac: &attachedClient{}})
	}
	require.Len(t, credentials.parkedForSession(nil), 64)
}
