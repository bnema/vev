package uidriver

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/bnema/vev/internal/ports"
	portsmocks "github.com/bnema/vev/internal/ports/mocks"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func testSnapshot() ports.UISnapshot {
	return ports.UISnapshot{Revision: 1, Columns: 1, Rows: 1, Context: ports.UIContext{AttachmentHandle: "a", Generation: 1, Status: ports.UIStatusAttached}, Cells: []ports.UICell{{Text: "x", Width: 1}}}
}

func testReady(control bool) Ready {
	return Ready{Attachment: "a", Generation: 1, Control: control, Status: ports.UIStatusAttached}
}

func readEnvelope(t *testing.T, decoder *json.Decoder) envelope {
	t.Helper()
	var response envelope
	require.NoError(t, decoder.Decode(&response))
	return response
}

func TestServeCaptureProceedsWhileWaitIsPending(t *testing.T) {
	service := portsmocks.NewMockUIService(t)
	started := make(chan struct{})
	release := make(chan struct{})
	service.EXPECT().Wait(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, _ ports.UIWaitRequest) (ports.UIWaitResult, error) {
		close(started)
		select {
		case <-release:
			return ports.UIWaitResult{Revision: 2}, nil
		case <-ctx.Done():
			return ports.UIWaitResult{}, ctx.Err()
		}
	}).Once()
	service.EXPECT().Capture("a").Return(testSnapshot(), nil).Once()
	server := New(service, nil)
	host, peer := net.Pipe()
	defer func() { _ = peer.Close() }()
	require.NoError(t, peer.SetDeadline(time.Now().Add(2*time.Second)))
	done := make(chan error, 1)
	go func() { done <- server.Serve(context.Background(), host, testReady(true)) }()
	decoder := json.NewDecoder(peer)
	ready := readEnvelope(t, decoder)
	require.Zero(t, ready.ID)
	_, err := io.WriteString(peer, "{\"version\":1,\"id\":1,\"op\":\"wait\",\"attachment\":\"a\",\"expect\":{\"status\":\"attached\"}}\n")
	require.NoError(t, err)
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("wait not started")
	}
	for range 2 {
		_, err = io.WriteString(peer, "{\"version\":1,\"id\":1,\"op\":\"capture\",\"attachment\":\"a\"}\n")
		require.NoError(t, err)
		duplicate := readEnvelope(t, decoder)
		require.Equal(t, ports.UIErrInvalidRequest, duplicate.Error.Code)
	}
	_, err = io.WriteString(peer, "{\"version\":1,\"id\":2,\"op\":\"capture\",\"attachment\":\"a\"}\n")
	require.NoError(t, err)
	capture := readEnvelope(t, decoder)
	require.Equal(t, uint64(2), capture.ID)
	require.Nil(t, capture.Error)
	close(release)
	waited := readEnvelope(t, decoder)
	require.Equal(t, uint64(1), waited.ID)
	require.Nil(t, waited.Error)
	require.NoError(t, peer.Close())
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("connection did not stop")
	}
	require.Zero(t, server.serialized)
}

func TestServeReadOnlyRejectsBeforeServiceAction(t *testing.T) {
	service := portsmocks.NewMockUIService(t)
	host, peer := net.Pipe()
	defer func() { _ = peer.Close() }()
	require.NoError(t, peer.SetDeadline(time.Now().Add(2*time.Second)))
	done := make(chan error, 1)
	go func() { done <- New(service, nil).Serve(context.Background(), host, testReady(false)) }()
	decoder := json.NewDecoder(peer)
	readEnvelope(t, decoder)
	_, err := io.WriteString(peer, "{\"version\":1,\"id\":1,\"op\":\"text\",\"attachment\":\"a\",\"generation\":1,\"text\":\"x\"}\n")
	require.NoError(t, err)
	denied := readEnvelope(t, decoder)
	require.Equal(t, ports.UIErrPermissionDenied, denied.Error.Code)
	require.False(t, denied.Error.Accepted)
	require.NoError(t, peer.Close())
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("connection did not stop")
	}
	service.AssertNotCalled(t, "Action", mock.Anything, mock.Anything)
}

type finiteStream struct {
	input *strings.Reader
	bytes.Buffer
}

func (s *finiteStream) Read(data []byte) (int, error) { return s.input.Read(data) }
func (*finiteStream) Close() error                    { return nil }

func TestServeAcceptsFinalObjectWithoutNewline(t *testing.T) {
	service := portsmocks.NewMockUIService(t)
	service.EXPECT().Capture("a").Return(testSnapshot(), nil).Once()
	stream := &finiteStream{input: strings.NewReader(`{"version":1,"id":1,"op":"capture","attachment":"a"}`)}
	server := New(service, nil)
	require.NoError(t, server.Serve(context.Background(), stream, testReady(true)))
	decoder := json.NewDecoder(bytes.NewReader(stream.Bytes()))
	require.Zero(t, readEnvelope(t, decoder).ID)
	require.Equal(t, uint64(1), readEnvelope(t, decoder).ID)
	require.Zero(t, server.serialized)
}

func TestServeWriteDeadlineClosesNonreadingStream(t *testing.T) {
	clock := portsmocks.NewMockClock(t)
	timer := portsmocks.NewMockTimer(t)
	expired := make(chan time.Time, 1)
	created := make(chan struct{})
	clock.EXPECT().NewTimer(writeTimeout).RunAndReturn(func(time.Duration) ports.Timer { close(created); return timer }).Once()
	timer.EXPECT().C().Return(expired).Once()
	timer.EXPECT().Stop().Return(true).Once()
	server := New(nil, clock)
	host, peer := net.Pipe()
	defer func() { _ = peer.Close() }()
	done := make(chan error, 1)
	go func() { done <- server.Serve(context.Background(), host, testReady(false)) }()
	select {
	case <-created:
	case <-time.After(time.Second):
		t.Fatal("writer did not arm deadline")
	}
	expired <- time.Time{}
	select {
	case err := <-done:
		require.Error(t, err)
	case <-time.After(time.Second):
		t.Fatal("nonreading observer retained workers")
	}
	require.Zero(t, server.serialized)
	require.Zero(t, server.connections)
}

func TestSerializedBudgetKeepsSpaceForBusyErrors(t *testing.T) {
	server := New(nil, nil)
	for range 3 {
		require.True(t, server.reserve(maxResponseBytes))
	}
	require.False(t, server.reserve(maxResponseBytes))
	require.True(t, server.reserve(errorResponseReserve))
	require.LessOrEqual(t, server.serialized, maxSerializedBytes)
}
