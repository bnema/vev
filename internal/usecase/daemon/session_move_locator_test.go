package daemon

import (
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
)

func TestSessionMoveLocatorSynchronizesMutableNameRead(t *testing.T) {
	sess := &session{
		id:          domain.SessionID("session-id"),
		incarnation: domain.IncarnationID{1},
		name:        "initial",
	}
	start := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		<-start
		for i := range 10_000 {
			sess.mu.Lock()
			sess.name = fmt.Sprintf("session-%d", i)
			sess.mu.Unlock()
		}
	}()
	go func() {
		defer wg.Done()
		<-start
		for range 10_000 {
			_ = sessionMoveLocator(sess)
		}
	}()
	close(start)
	wg.Wait()

	locator := sessionMoveLocator(sess)
	require.Equal(t, sess.id, locator.ID)
	require.Equal(t, sess.incarnation, locator.Incarnation)
	require.Equal(t, "session-9999", locator.Name)
}
