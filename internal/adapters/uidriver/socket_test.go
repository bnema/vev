package uidriver

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/bnema/vev/internal/ports"
	portsmocks "github.com/bnema/vev/internal/ports/mocks"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestListenUnixServesPrivateAttachmentAndCleansOwnedSocket(t *testing.T) {
	service := portsmocks.NewMockUIService(t)
	service.EXPECT().Capture("attachment").Return(testSnapshot(), nil).Once()
	directory := t.TempDir()
	require.NoError(t, os.Chmod(directory, 0o700))
	path := filepath.Join(directory, "ui.sock")
	server := New(service, nil)
	endpoint, err := ListenUnix(path, server, func() Ready {
		return testReady(false)
	})
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o600), fileMode(t, path))
	defer endpoint.Close()

	conn, err := net.DialTimeout("unix", path, time.Second)
	require.NoError(t, err)
	defer conn.Close()
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
	root := t.TempDir()
	target := filepath.Join(root, "target")
	require.NoError(t, os.Mkdir(target, 0o700))
	link := filepath.Join(root, "link")
	require.NoError(t, os.Symlink(target, link))
	_, err := ListenUnix(filepath.Join(link, "ui.sock"), New(nil, nil), nil)
	require.Error(t, err)
}

func TestListenUnixRejectsExistingPathWithoutRemovingIt(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ui.sock")
	require.NoError(t, os.WriteFile(path, []byte("keep"), 0o600))
	_, err := ListenUnix(path, New(nil, nil), nil)
	require.Error(t, err)
	data, readErr := os.ReadFile(path)
	require.NoError(t, readErr)
	require.Equal(t, []byte("keep"), data)
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
	defer firstPeer.Close()
	defer secondPeer.Close()
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
	firstPeer.Close()
	secondPeer.Close()
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
	directory := t.TempDir()
	require.NoError(t, os.Chmod(directory, 0o700))
	path := filepath.Join(directory, "ui.sock")
	endpoint, err := ListenUnix(path, New(nil, nil), func() Ready { return testReady(false) })
	require.NoError(t, err)
	defer endpoint.Close()

	var output bytes.Buffer
	require.NoError(t, Bridge(context.Background(), path, strings.NewReader(""), &output))
	decoder := json.NewDecoder(&output)
	var ready envelope
	require.NoError(t, decoder.Decode(&ready))
	require.Zero(t, ready.ID)
}

func fileMode(t *testing.T, path string) os.FileMode {
	t.Helper()
	info, err := os.Stat(path)
	require.NoError(t, err)
	return info.Mode().Perm()
}
