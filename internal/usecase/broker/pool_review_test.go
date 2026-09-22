package broker

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/stretchr/testify/require"
)

func TestPoolAdapterFailures(t *testing.T) {
	for _, stage := range []string{"resolver", "connector", "open"} {
		for _, tc := range []struct {
			name string
			err  error
			code ports.BrokerErrorCode
		}{
			{"unknown", errors.New("adapter failure"), ports.BrokerErrorUnavailable},
			{"cancel", context.Canceled, ports.BrokerErrorCancelled},
			{"deadline", context.DeadlineExceeded, ports.BrokerErrorTimeout},
			{"semantic", ports.BrokerError{Code: ports.BrokerErrorConflictingPolicy}, ports.BrokerErrorConflictingPolicy},
		} {
			t.Run(stage+"/"+tc.name, func(t *testing.T) {
				p, _ := setupPool(t, func(_ context.Context, e ports.BrokerDialTarget) (ports.BrokerPhysicalConnection, error) {
					if stage == "connector" {
						return nil, tc.err
					}
					return &fakePhysical{endpoint: e, done: make(chan struct{}), open: func(context.Context, ports.BrokerOpenStreamRequest) (ports.BrokerLogicalConnection, error) {
						return nil, tc.err
					}}, nil
				})
				if stage == "resolver" {
					p.routes = poolResolver(func(context.Context, ports.BrokerOpenStreamRequest) (ports.BrokerDialTarget, error) {
						return ports.BrokerDialTarget{}, tc.err
					})
				}
				id, _ := p.RegisterClient()
				_, err := p.OpenStream(context.Background(), poolRequest(id, 1))
				require.ErrorIs(t, err, tc.err)
				var typed ports.BrokerError
				require.ErrorAs(t, err, &typed)
				require.Equal(t, tc.code, typed.Code)
			})
		}
	}
}

func TestPoolRejectedPaths(t *testing.T) {
	for _, name := range []string{"closed", "epoch", "invalid endpoint", "nil raw", "retiring"} {
		t.Run(name, func(t *testing.T) {
			p, _ := setupPool(t, func(_ context.Context, e ports.BrokerDialTarget) (ports.BrokerPhysicalConnection, error) {
				return &fakePhysical{endpoint: e, done: make(chan struct{}), open: func(context.Context, ports.BrokerOpenStreamRequest) (ports.BrokerLogicalConnection, error) {
					return nil, nil
				}}, nil
			})
			id, _ := p.RegisterClient()
			req := poolRequest(id, 1)
			switch name {
			case "closed":
				require.NoError(t, p.Close())
			case "epoch":
				req.Epoch++
			case "invalid endpoint":
				p.routes = poolResolver(func(context.Context, ports.BrokerOpenStreamRequest) (ports.BrokerDialTarget, error) {
					return ports.BrokerDialTarget{}, nil
				})
			case "retiring":
				key := poolKey{"canonical", poolPolicy()}
				p.entries[key] = &poolEntry{key: key, retiring: true}
			}
			_, err := p.OpenStream(context.Background(), req)
			require.Error(t, err)
			if name == "closed" {
				require.ErrorIs(t, err, ports.BrokerAdmissionClosed)
				_, err = p.RegisterClient()
				require.ErrorIs(t, err, ports.BrokerAdmissionClosed)
			} else {
				var typed ports.BrokerError
				require.ErrorAs(t, err, &typed)
				want := ports.BrokerErrorIncompatible
				if name == "epoch" {
					want = ports.BrokerErrorStaleEpoch
				}
				if name == "retiring" {
					want = ports.BrokerErrorUnavailable
				}
				require.Equal(t, want, typed.Code)
			}
			p.mu.Lock()
			require.Zero(t, p.streams)
			p.mu.Unlock()
		})
	}
}

func TestPoolRedirectRejectsUnavailableWinnerWithoutResurrectingRefs(t *testing.T) {
	for _, state := range []string{"removed", "retiring"} {
		t.Run(state, func(t *testing.T) {
			p, _ := setupPool(t, func(context.Context, ports.BrokerDialTarget) (ports.BrokerPhysicalConnection, error) { panic("unused") })
			key := poolKey{"canonical", poolPolicy()}
			winner := &poolEntry{key: key, refs: 2}
			loser := &poolEntry{redirect: winner, refs: 1}
			reservation := &reservation{entry: loser}
			if state == "retiring" {
				winner.retiring = true
				p.entries[key] = winner
			}

			got, err := p.redirectReservation(loser, reservation)
			require.Nil(t, got)
			var typed ports.BrokerError
			require.ErrorAs(t, err, &typed)
			require.Equal(t, ports.BrokerErrorUnavailable, typed.Code)
			require.Equal(t, 1, loser.refs)
			require.Equal(t, 2, winner.refs)
			require.Same(t, loser, reservation.entry)
		})
	}
}

func TestPoolPostOpenPhysicalLoss(t *testing.T) {
	raw := newFakeLogical()
	p, _ := setupPool(t, func(_ context.Context, e ports.BrokerDialTarget) (ports.BrokerPhysicalConnection, error) {
		f := &fakePhysical{endpoint: e, done: make(chan struct{})}
		f.open = func(context.Context, ports.BrokerOpenStreamRequest) (ports.BrokerLogicalConnection, error) {
			_ = f.Close()
			return raw, nil
		}
		return f, nil
	})
	id, _ := p.RegisterClient()
	_, err := p.OpenStream(context.Background(), poolRequest(id, 1))
	var lost ports.BrokerStreamLost
	require.ErrorAs(t, err, &lost)
	require.NoError(t, lost.Validate())
	require.Equal(t, domain.RemoteFailureTransport, lost.Cause)
	await(t, raw.Done())
}

func TestPoolTerminalPrecedence(t *testing.T) {
	for _, name := range []string{"physical over logical", "request over physical", "shutdown over physical"} {
		t.Run(name, func(t *testing.T) {
			p, _ := setupPool(t, func(context.Context, ports.BrokerDialTarget) (ports.BrokerPhysicalConnection, error) {
				panic("unused")
			})
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			f := &fakePhysical{done: make(chan struct{})}
			_ = f.Close()
			e := &poolEntry{ctx: context.Background(), physical: f}
			if name == "request over physical" {
				cancel()
			}
			if name == "shutdown over physical" {
				p.cancel()
			}
			err := p.streamTerminal(ctx, e, poolRequest(ports.BrokerConnectionID{1}, 1), nil)
			if name == "physical over logical" {
				var lost ports.BrokerStreamLost
				require.ErrorAs(t, err, &lost)
				require.NoError(t, lost.Validate())
			} else {
				var typed ports.BrokerError
				require.ErrorAs(t, err, &typed)
				require.Equal(t, ports.BrokerErrorCancelled, typed.Code)
			}
		})
	}
}

// Reset deliberately does not drain, matching older buffered timer adapters.
type bufferedPoolTimer struct {
	ch     chan time.Time
	active bool
}

func (t *bufferedPoolTimer) C() <-chan time.Time      { return t.ch }
func (t *bufferedPoolTimer) Stop() bool               { was := t.active; t.active = false; return was }
func (t *bufferedPoolTimer) Reset(time.Duration) bool { was := t.active; t.active = true; return was }
func TestPoolTimerStopDrain(t *testing.T) {
	for _, name := range []string{"active", "expired buffered", "expired consumed", "stopped"} {
		t.Run(name, func(t *testing.T) {
			timer := &bufferedPoolTimer{ch: make(chan time.Time, 1), active: name == "active"}
			if name == "expired buffered" {
				timer.ch <- time.Time{}
			}
			stopPoolTimer(timer)
			timer.Reset(time.Minute)
			select {
			case <-timer.C():
				t.Fatal("stale expiry survived reset")
			default:
			}
			stopPoolTimer(timer)
			require.False(t, timer.active)
		})
	}
}

type closeErrorLogical struct {
	*fakeLogical
	err error
}

func (l *closeErrorLogical) Close() error { _ = l.fakeLogical.Close(); return l.err }
func TestPooledStreamPreservesCloseError(t *testing.T) {
	cause := errors.New("close failed")
	calls := 0
	s := &pooledStream{BrokerLogicalConnection: &closeErrorLogical{newFakeLogical(), cause}, done: make(chan struct{}), release: func() { calls++ }}
	require.ErrorIs(t, s.Close(), cause)
	require.ErrorIs(t, s.Close(), cause)
	require.Equal(t, 1, calls)
}

func TestPoolCaps(t *testing.T) {
	for _, name := range []string{"clients", "streams", "physical"} {
		t.Run(name, func(t *testing.T) {
			p, _ := setupPool(t, func(_ context.Context, e ports.BrokerDialTarget) (ports.BrokerPhysicalConnection, error) {
				return &fakePhysical{endpoint: e, done: make(chan struct{})}, nil
			})
			p.limits.Clients = 1
			p.limits.Streams = 1
			p.limits.Physical = 1
			id, _ := p.RegisterClient()
			if name == "clients" {
				_, err := p.RegisterClient()
				require.ErrorIs(t, err, ports.BrokerAdmissionLimit)
				return
			}
			s, err := p.OpenStream(context.Background(), poolRequest(id, 1))
			require.NoError(t, err)
			defer s.Close()
			if name == "physical" {
				p.limits.Streams = 2
				p.routes = poolResolver(func(_ context.Context, r ports.BrokerOpenStreamRequest) (ports.BrokerDialTarget, error) {
					return ports.BrokerDialTarget{Fence: ports.BrokerEndpointFence{Local: true}, Policy: r.Policy, Address: "route", StartMode: r.StartMode, ExpectedIdentity: ports.BrokerExpectedIdentity{Identity: "other", Bound: true}}, nil
				})
			}
			_, err = p.OpenStream(context.Background(), poolRequest(id, 2))
			require.ErrorIs(t, err, ports.BrokerAdmissionLimit)
		})
	}
}

func TestPoolShutdownStreamsCancelled(t *testing.T) {
	p, _ := setupPool(t, func(_ context.Context, e ports.BrokerDialTarget) (ports.BrokerPhysicalConnection, error) {
		return &fakePhysical{endpoint: e, done: make(chan struct{})}, nil
	})
	id, _ := p.RegisterClient()
	s, err := p.OpenStream(context.Background(), poolRequest(id, 1))
	require.NoError(t, err)
	require.NoError(t, p.Close())
	await(t, s.Done())
	var typed ports.BrokerError
	require.ErrorAs(t, s.Err(), &typed)
	require.Equal(t, ports.BrokerErrorCancelled, typed.Code)
}
