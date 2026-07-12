package daemon

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	portsmocks "github.com/bnema/vev/internal/ports/mocks"
)

func TestFloatingOpenCancellationBoundsSessionTeardownAndClosesLatePTY(t *testing.T) {
	factory := portsmocks.NewMockPTYFactory(t)
	opened := make(chan struct{})
	latePTY := portsmocks.NewMockPTY(t)
	closed := make(chan struct{})
	latePTY.EXPECT().Close().RunAndReturn(func() error { close(closed); return nil }).Once()
	factory.EXPECT().Open(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, _ string, _ []string, _ []string, _ string, _ domain.Size) (ports.PTY, error) {
			close(opened)
			<-ctx.Done()
			return latePTY, nil
		}).Once()
	d := newTestDaemon(t, factory, stubClock{})
	tb := newFloatingTestTab(t)
	ctx, cancel := context.WithCancel(t.Context())
	sess := &session{id: "blocked-open", name: "work", tabs: []*tab{tb}, ctx: ctx, cancel: cancel}
	d.mu.Lock()
	d.sessions[sess.id] = sess
	d.mu.Unlock()
	d.startFloating(sess, tb, true)
	select {
	case <-opened:
	case <-time.After(time.Second):
		t.Fatal("floating Open did not begin")
	}

	killed := make(chan error, 1)
	go func() { killed <- d.killSession(sess, ports.ReasonSessionKilled, false) }()
	select {
	case err := <-killed:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("session teardown waited for blocked Open")
	}
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("late Open PTY was not closed")
	}
	tb.mu.Lock()
	require.Nil(t, tb.floating.pane, "late PTY must not be installed")
	tb.mu.Unlock()
}

func TestFloatingOpensFromUnrelatedSessionsAreConcurrent(t *testing.T) {
	factory := portsmocks.NewMockPTYFactory(t)
	entered := make(chan struct{}, 2)
	release := make(chan struct{})
	var once sync.Once
	factory.EXPECT().Open(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).RunAndReturn(
		func(_ context.Context, _ string, _ []string, _ []string, _ string, _ domain.Size) (ports.PTY, error) {
			entered <- struct{}{}
			<-release
			return newQuietPTY(), nil
		}).Twice()
	d := newTestDaemon(t, factory, stubClock{})
	tabs := make([]*tab, 0, 2)
	cancels := make([]context.CancelFunc, 0, 2)
	for _, id := range []domain.SessionID{"one", "two"} {
		tb := newFloatingTestTab(t)
		ctx, cancel := context.WithCancel(t.Context())
		sess := &session{id: id, name: string(id), tabs: []*tab{tb}, ctx: ctx, cancel: cancel}
		tabs = append(tabs, tb)
		cancels = append(cancels, cancel)
		d.startFloating(sess, tb, false)
	}
	for range 2 {
		select {
		case <-entered:
		case <-time.After(time.Second):
			t.Fatal("unrelated floating launch was serialized")
		}
	}
	once.Do(func() { close(release) })
	for i, cancel := range cancels {
		cancel()
		d.teardownFloating(tabs[i], nil)
	}
	d.sessWg.Wait()
}
