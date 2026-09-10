package webterm

import (
	"context"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestShutdownAdmissionRace(t *testing.T) {
	for range 100 {
		server, err := NewServer(t.Context(), Settings{}, strings.Repeat("d", 43), func(context.Context, *Terminal) error { return nil })
		require.NoError(t, err)
		var racers sync.WaitGroup
		start := make(chan struct{})
		admitted := make(chan bool, 1)
		release := make(chan struct{})
		drained := make(chan struct{})
		racers.Add(2)
		go func() {
			defer racers.Done()
			<-start
			ok := server.admit()
			admitted <- ok
			if ok {
				<-release
				server.workers.Done()
			}
		}()
		go func() { defer racers.Done(); <-start; server.Wait(); close(drained) }()
		close(start)
		if <-admitted {
			select {
			case <-drained:
				t.Fatal("shutdown missed an admitted worker")
			default:
			}
		}
		close(release)
		racers.Wait()
		require.False(t, server.admit(), "shutdown must permanently close admission")
	}
}

func TestCanceledServerRejectsAdmission(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	server, err := NewServer(ctx, Settings{}, strings.Repeat("e", 43), func(context.Context, *Terminal) error { return nil })
	require.NoError(t, err)
	cancel()
	require.False(t, server.admit())
	server.Wait()
}
