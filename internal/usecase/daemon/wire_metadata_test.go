package daemon

import (
	"fmt"
	"sync"
	"testing"
)

func TestServerWelcomeSnapshotsSessionMetadataUnderLock(t *testing.T) {
	sess := &session{sessionCore: sessionCore{id: "session", name: "initial"}}
	ac := &attachedClient{resumeToken: 42}

	var wg sync.WaitGroup
	wg.Go(func() {
		for i := range 1000 {
			sess.mu.Lock()
			sess.name = fmt.Sprintf("session-%d", i)
			sess.ephemeral = i%2 == 0
			sess.mu.Unlock()
		}
	})

	for range 1000 {
		welcome, err := serverWelcome(sess, ac.resumeToken)
		if err != nil {
			t.Fatalf("serverWelcome() error = %v", err)
		}
		if welcome.SessionID != "session" || welcome.SessionName == "" {
			t.Fatalf("welcome metadata = %+v", welcome)
		}
	}
	wg.Wait()
}
