package uidriver

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/bnema/vev/internal/ports"
	portsmocks "github.com/bnema/vev/internal/ports/mocks"
	"github.com/bnema/vev/pkg/safedir"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestValidateSocketPathUsesPlatformLimit(t *testing.T) {
	const (
		expectedDarwinSocketMax = 103
		expectedLinuxSocketMax  = 107
	)
	expectedMax := expectedLinuxSocketMax
	switch runtime.GOOS {
	case "darwin", "freebsd", "openbsd", "netbsd", "dragonfly":
		expectedMax = expectedDarwinSocketMax
	}
	tests := []struct {
		name string
		path string
		want bool
	}{
		{name: "at limit", path: "/" + strings.Repeat("a", expectedMax-1), want: true},
		{name: "above limit", path: "/" + strings.Repeat("a", expectedMax), want: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := validateSocketPath(test.path)
			if test.want {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
		})
	}
}

func TestListenUnixServesPrivateAttachmentAndCleansOwnedSocket(t *testing.T) {
	service := portsmocks.NewMockUIService(t)
	service.EXPECT().Capture("attachment").Return(testSnapshot(), nil).Once()
	directory := shortDir(t)
	path := filepath.Join(directory, "ui.sock")
	server := New(service, nil)
	endpoint, err := ListenUnix(path, server, func() Ready {
		return testReady(false)
	})
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o600), fileMode(t, path))
	defer func() { _ = endpoint.Close() }()

	dialer := &net.Dialer{Timeout: time.Second}
	conn, err := dialer.DialContext(context.Background(), "unix", path)
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()
	decoder := json.NewDecoder(bufio.NewReader(conn))
	var ready envelope
	require.NoError(t, decoder.Decode(&ready))
	require.Zero(t, ready.ID)
	_, err = conn.Write([]byte(`{"version":1,"id":1,"op":"capture","attachment":"attachment"}` + "\n"))
	require.NoError(t, err)
	var response envelope
	require.NoError(t, decoder.Decode(&response))
	require.Equal(t, uint64(1), response.ID)
	require.Nil(t, response.Error)

	require.NoError(t, endpoint.Close())
	_, err = os.Lstat(path)
	require.ErrorIs(t, err, os.ErrNotExist)
}

func TestListenUnixRejectsUnsafeParentSymlink(t *testing.T) {
	root := shortDir(t)
	target := filepath.Join(root, "target")
	require.NoError(t, os.Mkdir(target, 0o700))
	link := filepath.Join(root, "link")
	require.NoError(t, os.Symlink(target, link))
	_, err := ListenUnix(filepath.Join(link, "ui.sock"), New(nil, nil), nil)
	require.Error(t, err)
}

func TestListenUnixRejectsExistingPathWithoutRemovingIt(t *testing.T) {
	path := filepath.Join(shortDir(t), "ui.sock")
	require.NoError(t, os.WriteFile(path, []byte("keep"), 0o600))
	_, err := ListenUnix(path, New(nil, nil), nil)
	require.Error(t, err)
	data, readErr := os.ReadFile(path)
	require.NoError(t, readErr)
	require.Equal(t, []byte("keep"), data)
}

func TestUnixEndpointClosePreservesReplacementPath(t *testing.T) {
	directory := shortDir(t)
	path := filepath.Join(directory, "ui.sock")
	endpoint, err := ListenUnix(path, New(nil, nil), nil)
	require.NoError(t, err)
	defer func() { _ = endpoint.Close() }()

	replacement := filepath.Join(directory, "replacement")
	require.NoError(t, os.Rename(path, replacement))
	require.NoError(t, os.WriteFile(path, []byte("keep"), 0o600))
	require.NoError(t, endpoint.Close())

	data, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, []byte("keep"), data)
	require.NoError(t, os.Remove(replacement))
}

func TestServeUsesAttachmentWideActionLimitAcrossConnections(t *testing.T) {
	service := portsmocks.NewMockUIService(t)
	started := make(chan struct{})
	release := make(chan struct{})
	service.EXPECT().Action(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, _ ports.UIActionRequest) (ports.UIActionResult, error) {
		close(started)
		select {
		case <-release:
			return ports.UIActionResult{ActionID: 1, Accepted: true, Status: ports.UIActionProcessed}, nil
		case <-ctx.Done():
			return ports.UIActionResult{}, ctx.Err()
		}
	}).Once()
	server := New(service, nil)
	firstHost, firstPeer := net.Pipe()
	secondHost, secondPeer := net.Pipe()
	defer func() { _ = firstPeer.Close() }()
	defer func() { _ = secondPeer.Close() }()
	firstDone := make(chan error, 1)
	secondDone := make(chan error, 1)
	go func() { firstDone <- server.Serve(context.Background(), firstHost, testReady(true)) }()
	go func() { secondDone <- server.Serve(context.Background(), secondHost, testReady(true)) }()
	firstDecoder := json.NewDecoder(firstPeer)
	secondDecoder := json.NewDecoder(secondPeer)
	require.Zero(t, readEnvelope(t, firstDecoder).ID)
	require.Zero(t, readEnvelope(t, secondDecoder).ID)
	request := `{"version":1,"id":1,"op":"text","attachment":"a","generation":1,"text":"x"}` + "\n"
	_, err := firstPeer.Write([]byte(request))
	require.NoError(t, err)
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("first action did not start")
	}
	_, err = secondPeer.Write([]byte(request))
	require.NoError(t, err)
	busy := readEnvelope(t, secondDecoder)
	require.Equal(t, ports.UIErrBusy, busy.Error.Code)
	close(release)
	processed := readEnvelope(t, firstDecoder)
	require.Nil(t, processed.Error)
	require.NoError(t, firstPeer.Close())
	require.NoError(t, secondPeer.Close())
	select {
	case <-firstDone:
	case <-time.After(time.Second):
		t.Fatal("first socket server did not stop")
	}
	select {
	case <-secondDone:
	case <-time.After(time.Second):
		t.Fatal("second socket server did not stop")
	}
}

func TestBridgeUsesExistingSocketWithoutCreatingAttachment(t *testing.T) {
	directory := shortDir(t)
	path := filepath.Join(directory, "ui.sock")
	endpoint, err := ListenUnix(path, New(nil, nil), func() Ready { return testReady(false) })
	require.NoError(t, err)
	defer func() { _ = endpoint.Close() }()

	var output bytes.Buffer
	require.NoError(t, Bridge(context.Background(), path, strings.NewReader(""), &output))
	decoder := json.NewDecoder(&output)
	var ready envelope
	require.NoError(t, decoder.Decode(&ready))
	require.Zero(t, ready.ID)
}

func shortDir(t *testing.T) string {
	t.Helper()
	directory, err := os.MkdirTemp("/tmp", "vev")
	require.NoError(t, err)
	require.NoError(t, safedir.EnsurePrivate(directory))
	t.Cleanup(func() { require.NoError(t, os.RemoveAll(directory)) })
	return directory
}

func fileMode(t *testing.T, path string) os.FileMode {
	t.Helper()
	info, err := os.Stat(path)
	require.NoError(t, err)
	return info.Mode().Perm()
}
