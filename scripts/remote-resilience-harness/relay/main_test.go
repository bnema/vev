package main

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestTCPRelayPreservesBytesAndStops(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	r := &relay{ctx: ctx, cancel: cancel}
	source, writer := net.Pipe()
	reader, destination := net.Pipe()
	defer source.Close()
	defer writer.Close()
	defer reader.Close()
	defer destination.Close()
	done := make(chan struct{})
	go func() { defer close(done); r.copy(destination, source) }()
	payload := []byte("fixture input in order")
	go func() { writer.Write(payload); writer.Close() }()
	reader.SetReadDeadline(time.Now().Add(time.Second))
	got := make([]byte, len(payload))
	_, err := io.ReadFull(reader, got)
	require.NoError(t, err)
	require.Equal(t, payload, got)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("relay did not stop after EOF")
	}
	require.Equal(t, uint64(len(payload)), r.bytes.Load())
}

// controlSocketPath returns a socket path in a short private directory: the
// macOS t.TempDir path exceeds the 104-byte AF_UNIX limit.
func controlSocketPath(t *testing.T) string {
	t.Helper()
	directory, err := os.MkdirTemp("/tmp", "relay")
	require.NoError(t, err)
	t.Cleanup(func() { _ = os.RemoveAll(directory) })
	require.NoError(t, os.Chmod(directory, 0700))
	return filepath.Join(directory, "control.sock")
}

func TestRelayShutdownClosesListeners(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	r := &relay{ctx: ctx, cancel: cancel, connections: make(map[net.Conn]struct{})}
	path := controlSocketPath(t)
	done := make(chan error, 1)
	go func() { done <- r.serve("127.0.0.1:0", path) }()
	require.Eventually(t, func() bool { _, err := os.Stat(path); return err == nil }, time.Second, time.Millisecond)
	cancel()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("relay shutdown hung")
	}
	_, err := os.Stat(path)
	require.True(t, os.IsNotExist(err))
}

func TestTCPRelayBlackoutHoldsBytes(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	r := &relay{ctx: ctx, cancel: cancel}
	_, err := r.apply(command{Op: "blackout", TCP: true, Active: true})
	require.NoError(t, err)
	source, writer := net.Pipe()
	reader, destination := net.Pipe()
	defer source.Close()
	defer writer.Close()
	defer reader.Close()
	defer destination.Close()
	done := make(chan struct{})
	go func() { defer close(done); r.copy(destination, source) }()
	payload := []byte("held")
	wrote := make(chan error, 1)
	go func() { _, err := writer.Write(payload); wrote <- err }()
	select {
	case err := <-wrote:
		t.Fatalf("blackout read the source: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	require.Zero(t, r.bytes.Load())
	_, err = r.apply(command{Op: "blackout", TCP: true, Active: false})
	require.NoError(t, err)
	reader.SetReadDeadline(time.Now().Add(time.Second))
	got := make([]byte, len(payload))
	_, err = io.ReadFull(reader, got)
	require.NoError(t, err)
	require.Equal(t, payload, got)
	require.NoError(t, <-wrote)
	cancel()
	writer.Close()
	<-done
}

func TestRelayDisconnectClosesConnections(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	r := &relay{ctx: ctx, cancel: cancel, connections: make(map[net.Conn]struct{})}
	local, peer := net.Pipe()
	defer peer.Close()
	r.connections[local] = struct{}{}
	_, err := r.apply(command{Op: "disconnect"})
	require.NoError(t, err)
	peer.SetReadDeadline(time.Now().Add(time.Second))
	_, err = peer.Read(make([]byte, 1))
	require.ErrorIs(t, err, io.EOF)
}

func TestRelayControlRoundTrip(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	r := &relay{ctx: ctx, cancel: cancel, connections: make(map[net.Conn]struct{})}
	path := controlSocketPath(t)
	done := make(chan error, 1)
	go func() { done <- r.serve("127.0.0.1:0", path) }()
	require.Eventually(t, func() bool { _, err := os.Stat(path); return err == nil }, time.Second, time.Millisecond)
	send := func(c command) map[string]any {
		conn, err := net.Dial("unix", path)
		require.NoError(t, err)
		defer conn.Close()
		require.NoError(t, json.NewEncoder(conn).Encode(c))
		line, err := bufio.NewReader(conn).ReadBytes('\n')
		require.NoError(t, err)
		var reply map[string]any
		require.NoError(t, json.Unmarshal(line, &reply))
		return reply
	}
	reply := send(command{ID: 7, Op: "blackout", TCP: true, Active: true})
	require.EqualValues(t, 7, reply["id"])
	require.NotContains(t, reply, "error")
	require.True(t, r.blackout.Load())
	reply = send(command{ID: 8, Op: "unknown"})
	require.Equal(t, "unknown command", reply["error"])
	reply = send(command{ID: 9, Op: "stop"})
	require.NotContains(t, reply, "error")
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("stop command did not shut the relay down")
	}
}

func TestRelayRejectsInvalidControl(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	r := &relay{ctx: ctx, cancel: cancel}
	for _, c := range []command{{Op: "unknown"}, {Op: "udp", Port: 0}, {Op: "udp", Port: 65536}} {
		_, err := r.apply(c)
		require.Error(t, err)
	}
	_, err := r.apply(command{Op: "stop"})
	require.NoError(t, err)
	require.ErrorIs(t, ctx.Err(), context.Canceled)
}
