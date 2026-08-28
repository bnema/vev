package app

import (
	"fmt"
	"log/slog"
	"sync"
	"testing"

	remoteadapter "github.com/bnema/vev/internal/adapters/remote"
	"github.com/bnema/vev/internal/protocol/wire"
)

type remoteDialerFactoryMock struct {
	mu    sync.Mutex
	calls []*remoteDialerFactoryCall
}

type remoteDialerFactoryExpecter struct{ mock *remoteDialerFactoryMock }

type remoteDialerFactoryCall struct {
	target, session string
	mode            remoteadapter.TransportMode
	dialer          wire.Dialer
	err             error
	remaining       int
}

func newRemoteDialerFactoryMock(t *testing.T) *remoteDialerFactoryMock {
	t.Helper()
	m := &remoteDialerFactoryMock{}
	t.Cleanup(func() {
		m.mu.Lock()
		defer m.mu.Unlock()
		for _, call := range m.calls {
			if call.remaining != 0 {
				t.Errorf("unmet DialerForRemote(%q, %q, %q) calls: %d", call.target, call.session, call.mode, call.remaining)
			}
		}
	})
	return m
}

func (m *remoteDialerFactoryMock) EXPECT() *remoteDialerFactoryExpecter {
	return &remoteDialerFactoryExpecter{mock: m}
}

func (e *remoteDialerFactoryExpecter) DialerForRemote(target, session string, mode remoteadapter.TransportMode, _ any) *remoteDialerFactoryCall {
	call := &remoteDialerFactoryCall{target: target, session: session, mode: mode, remaining: 1}
	e.mock.mu.Lock()
	e.mock.calls = append(e.mock.calls, call)
	e.mock.mu.Unlock()
	return call
}

func (c *remoteDialerFactoryCall) Return(dialer wire.Dialer, err error) *remoteDialerFactoryCall {
	c.dialer, c.err = dialer, err
	return c
}

func (c *remoteDialerFactoryCall) Once() *remoteDialerFactoryCall { return c.Times(1) }

func (c *remoteDialerFactoryCall) Times(n int) *remoteDialerFactoryCall {
	c.remaining = n
	return c
}

func (m *remoteDialerFactoryMock) DialerForRemote(target, session string, mode remoteadapter.TransportMode, _ *slog.Logger) (wire.Dialer, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, call := range m.calls {
		if call.remaining == 0 || call.target != target || call.session != session || call.mode != mode {
			continue
		}
		call.remaining--
		return call.dialer, call.err
	}
	return nil, fmt.Errorf("unexpected DialerForRemote(%q, %q, %q)", target, session, mode)
}
