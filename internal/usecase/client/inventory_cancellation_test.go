package client

import (
	"context"
	"io"
	"sync"
	"testing"
	"time"

	portsmocks "github.com/bnema/vev/internal/ports/mocks"
	"github.com/bnema/vev/internal/protocol"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestInventoryReopenRetainsPhysicalQuerySlotUntilCancellationCompletes(t *testing.T) {
	clock := newInventoryTestClock()
	closed := make(chan struct{})
	entered := make(chan struct{})
	var once sync.Once
	conn := portsmocks.NewMockClientConnection(t)
	conn.EXPECT().SendClient(mock.Anything).Return(nil).Once()
	conn.EXPECT().ReceiveServer().RunAndReturn(func() (protocol.ServerMessage, error) { close(entered); <-closed; return nil, io.EOF }).Once()
	conn.EXPECT().Close().RunAndReturn(func() error { once.Do(func() { close(closed) }); return nil }).Maybe()
	dialer := portsmocks.NewMockClientDialer(t)
	dialer.EXPECT().Dial(mock.Anything).Return(conn, nil).Once()
	relay := newInventoryRelay(clock, dialer)
	relay.setOpen(true, 1)
	query, ok := relay.beginPoll()
	require.True(t, ok)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { _, err := relay.querySnapshot(ctx, 1); done <- err }()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("query never started")
	}
	for interaction := uint64(2); interaction < 6; interaction++ {
		relay.setOpen(false, interaction-1)
		cancel()
		relay.setOpen(true, interaction)
		_, ok := relay.beginPoll()
		require.False(t, ok, "close/reopen cannot release an unaccounted worker")
	}
	select {
	case err := <-done:
		require.Error(t, err)
	case <-time.After(time.Second):
		t.Fatal("cancellation did not close exact connection")
	}
	relay.endPoll(query)
	_, ok = relay.beginPoll()
	require.True(t, ok, "latest interaction starts immediately after old worker completes")
}
