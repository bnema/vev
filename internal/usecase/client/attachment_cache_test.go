package client

import (
	"context"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol"
	"github.com/stretchr/testify/require"
)

// The peer uses typed messages so the tests exercise publication correlation,
// reader ownership and terminal isolation independently of carriage encoding.
type cacheTestPeer struct {
	ports.ClientConnection
	messages   chan protocol.ServerMessage
	sent       chan protocol.ClientMessage
	closed     chan struct{}
	once       sync.Once
	identity   protocol.CommittedRouteIdentity
	reject     bool
	activation func(protocol.ActivateAttachment)
}

func newCacheTestPeer() *cacheTestPeer {
	return &cacheTestPeer{messages: make(chan protocol.ServerMessage, 16), sent: make(chan protocol.ClientMessage, 16), closed: make(chan struct{}), identity: testOutputView(1).Route}
}
func (p *cacheTestPeer) Close() error { p.once.Do(func() { close(p.closed) }); return nil }
func (p *cacheTestPeer) ReceiveServer() (protocol.ServerMessage, error) {
	select {
	case m := <-p.messages:
		return m, nil
	case <-p.closed:
		return nil, io.EOF
	}
}
func (p *cacheTestPeer) SendClient(message protocol.ClientMessage) error {
	p.sent <- message
	switch m := message.(type) {
	case protocol.SuspendAttachment:
		p.messages <- protocol.AttachmentSuspended{RequestID: m.RequestID, Target: p.identity.Target}
	case protocol.ActivateAttachment:
		if p.activation != nil {
			p.activation(m)
			return nil
		}
		if p.reject {
			p.messages <- protocol.ErrorMsg{}
			return nil
		}
		view := testOutputView(2)
		p.messages <- protocol.Output{Epoch: 2, New: 1, Full: true, Context: &view, Data: []byte("full")}
		p.messages <- protocol.RoutePosition{Target: view.Route.Target, ActiveTabID: "tab-2"}
		p.messages <- protocol.AttachmentActivated{RequestID: m.RequestID, Identity: view.Route, Epoch: 2, State: 1, ViewPublication: 2}
	}
	return nil
}
func cacheTestRequest(endpoint string) AttachRequest {
	return AttachRequest{Remote: true, OriginKey: endpoint, Intent: protocol.IntentAttach, SessionName: "work"}
}
func parkTestPeer(t *testing.T, c *attachmentCoordinator, endpoint string) (*cachedAttachment, *cacheTestPeer) {
	t.Helper()
	p := newCacheTestPeer()
	e := newCachedAttachment(p)
	e.startReader()
	e.epoch = 1
	require.True(t, c.suspend(context.Background(), e, cacheTestRequest(endpoint), attachResult{committedIdentity: &p.identity, resumeToken: 7}))
	return e, p
}
func TestAttachmentCacheMultipleEndpointsAndFullActivation(t *testing.T) {
	c := newAttachmentCoordinator(newInventoryTestClock(), time.Minute)
	defer c.close()
	a, pa := parkTestPeer(t, c, "a")
	b, pb := parkTestPeer(t, c, "b")
	for _, tt := range []struct {
		key   string
		entry *cachedAttachment
		peer  *cacheTestPeer
	}{{"a", a, pa}, {"b", b, pb}} {
		t.Run(tt.key, func(t *testing.T) {
			geometry := domain.Geometry{Size: domain.Size{Cols: 101, Rows: 37}}
			got, ack, full, position := c.activate(context.Background(), cacheTestRequest(tt.key), geometry)
			require.Same(t, tt.entry, got)
			require.NotNil(t, ack)
			require.Equal(t, protocol.RoutePosition{Target: ack.Identity.Target, ActiveTabID: "tab-2"}, *position)
			require.Equal(t, []byte("full"), full.Data)
			require.IsType(t, protocol.SuspendAttachment{}, <-tt.peer.sent)
			activation := (<-tt.peer.sent).(protocol.ActivateAttachment)
			require.Equal(t, geometry.Size, activation.Size)
			got.close()
			<-got.readerDone
		})
	}
}
func TestAttachmentCacheEviction(t *testing.T) {
	for _, scenario := range []string{"expiry", "death", "stale lifecycle", "activation failure", "cancellation"} {
		t.Run(scenario, func(t *testing.T) {
			clock := newInventoryTestClock()
			c := newAttachmentCoordinator(clock, time.Minute)
			defer c.close()
			e, p := parkTestPeer(t, c, "a")
			request := cacheTestRequest("a")
			ctx := context.Background()
			switch scenario {
			case "expiry":
				e.timer.(*inventoryTestTimer).ch <- clock.Now()
				<-e.dormantDone
			case "death":
				_ = p.Close()
				<-e.dormantDone
			case "stale lifecycle":
				target := p.identity.Target
				target.LifecycleID[0]++
				request.ExactTarget = &target
			case "activation failure":
				p.reject = true
			case "cancellation":
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			}
			got, _, _, _ := c.activate(ctx, request, domain.Geometry{Size: domain.Size{Cols: 80, Rows: 24}})
			require.Nil(t, got)
			<-p.closed
			<-e.readerDone
			require.Empty(t, c.entries)
		})
	}
}
func TestAttachmentCacheDormantDrainAndExit(t *testing.T) {
	c := newAttachmentCoordinator(newInventoryTestClock(), time.Minute)
	e, p := parkTestPeer(t, c, "a")
	for i := 0; i < 100; i++ {
		p.messages <- protocol.Output{Data: []byte("must never reach terminal")}
	}
	c.close()
	<-p.closed
	<-e.readerDone
	<-e.dormantDone
	require.Empty(t, c.entries)
	require.Len(t, p.sent, 1, "dormant entry emits no input, resize or output ACK")
}

func TestAttachmentCacheRejectsUncommittedPublication(t *testing.T) {
	for _, scenario := range []string{"ack before full", "wrong request", "wrong epoch", "wrong publication", "wrong lifecycle", "old output chain", "ordinary output after activation", "missing position", "invalid position", "wrong position target", "position before full", "duplicate position"} {
		t.Run(scenario, func(t *testing.T) {
			c := newAttachmentCoordinator(newInventoryTestClock(), time.Minute)
			defer c.close()
			e, p := parkTestPeer(t, c, "a")
			p.activation = func(request protocol.ActivateAttachment) {
				view := testOutputView(2)
				full := protocol.Output{Epoch: 2, New: 1, Full: true, Context: &view}
				ack := protocol.AttachmentActivated{RequestID: request.RequestID, Identity: view.Route, Epoch: 2, State: 1, ViewPublication: 2}
				switch scenario {
				case "ack before full":
					p.messages <- ack
					return
				case "wrong request":
					ack.RequestID++
				case "wrong epoch":
					ack.Epoch++
				case "wrong publication":
					ack.ViewPublication++
				case "wrong lifecycle":
					ack.Identity.Target.LifecycleID[0]++
				case "ordinary output after activation":
					full.Full = false
				case "old output chain":
					full.Epoch = 1
				}
				position := protocol.RoutePosition{Target: view.Route.Target, ActiveTabID: "tab-2"}
				switch scenario {
				case "invalid position":
					position.ActiveTabID = ""
				case "wrong position target":
					position.Target.LifecycleID[0]++
				case "position before full":
					p.messages <- position
				}
				p.messages <- full
				if scenario != "missing position" {
					p.messages <- position
				}
				if scenario == "duplicate position" {
					p.messages <- position
				}
				p.messages <- ack
			}
			got, _, _, _ := c.activate(context.Background(), cacheTestRequest("a"), domain.Geometry{Size: domain.Size{Cols: 80, Rows: 24}})
			require.Nil(t, got)
			<-e.readerDone
			require.False(t, c.has("a"))
		})
	}
}

func TestAttachmentCachePreferredTabEligibility(t *testing.T) {
	for _, tt := range []struct {
		name      string
		preferred domain.TabStableID
		warm      bool
	}{
		{"no preference", "", true},
		{"matching committed tab", "tab-2", true},
		{"different tab", "tab-1", false},
		{"disappeared tab", "missing", false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			c := newAttachmentCoordinator(newInventoryTestClock(), time.Minute)
			defer c.close()
			e, p := parkTestPeer(t, c, "a")
			e.request.PreferredTabID = "tab-2"
			request := cacheTestRequest("a")
			request.PreferredTabID = tt.preferred
			got, _, _, _ := c.activate(context.Background(), request, domain.Geometry{Size: domain.Size{Cols: 80, Rows: 24}})
			if tt.warm {
				require.Same(t, e, got)
				got.release()
			} else {
				require.Nil(t, got)
				require.Len(t, p.sent, 1, "mismatched preference must fall back before activation")
			}
		})
	}
}

func TestAttachmentCacheActivationDrainsQueuedDormantMessages(t *testing.T) {
	for _, tt := range []struct {
		name   string
		queued recvResult
		warm   bool
	}{
		{"ordinary output", recvResult{message: protocol.Output{Data: []byte("discard")}}, true},
		{"receive error", recvResult{err: io.EOF}, false},
		{"detached", recvResult{message: protocol.Detached{}}, false},
		{"server error", recvResult{message: protocol.ErrorMsg{}}, false},
		{"premature activation", recvResult{message: protocol.AttachmentActivated{}}, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			c := newAttachmentCoordinator(newInventoryTestClock(), time.Minute)
			defer c.close()
			e, p := parkTestPeer(t, c, "a")
			// Freeze dormant consumption so the message is deterministically
			// queued at ownership transfer, rather than racing the worker.
			close(e.stopDormant)
			e.waitDormant()
			e.stopDormant = make(chan struct{})
			e.inbox <- tt.queued
			got, _, full, _ := c.activate(context.Background(), cacheTestRequest("a"), domain.Geometry{Size: domain.Size{Cols: 80, Rows: 24}})
			if tt.warm {
				require.Same(t, e, got)
				require.Equal(t, []byte("full"), full.Data)
				got.release()
			} else {
				require.Nil(t, got)
				require.Len(t, p.sent, 1)
			}
		})
	}
}
