package uidriver

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"sync"

	"github.com/bnema/vev/pkg/safedir"
)

const (
	unixSocketMode = 0o600
	unixSocketMax  = 107
)

// UnixEndpoint exposes one already-bound attachment. It never creates or
// owns a Runner; closing it only closes observer connections and its socket.
type UnixEndpoint struct {
	path   string
	ln     *net.UnixListener
	server *Server
	ready  func() Ready

	ctx    context.Context
	cancel context.CancelFunc
	done   chan struct{}
	wg     sync.WaitGroup
	once   sync.Once
	file   os.FileInfo
}

// ListenUnix binds a private, per-client UI socket. Existing paths are always
// rejected, including stale sockets, so cleanup can never remove a caller's
// file.
func ListenUnix(path string, server *Server, ready func() Ready) (*UnixEndpoint, error) {
	if server == nil {
		return nil, errors.New("uidriver: nil server")
	}
	if err := validateSocketPath(path); err != nil {
		return nil, err
	}
	parent := filepath.Dir(path)
	if err := safedir.EnsurePrivate(parent); err != nil {
		return nil, fmt.Errorf("uidriver: secure socket directory: %w", err)
	}
	if _, err := os.Lstat(path); err == nil {
		return nil, errors.New("uidriver: socket path already exists")
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("uidriver: inspect socket path: %w", err)
	}
	address := &net.UnixAddr{Name: path, Net: "unix"}
	listener, err := net.ListenUnix("unix", address)
	if err != nil {
		return nil, fmt.Errorf("uidriver: listen on socket: %w", err)
	}
	cleanup := func(cleanErr error) (*UnixEndpoint, error) {
		_ = listener.Close()
		_ = os.Remove(path)
		return nil, cleanErr
	}
	if err := os.Chmod(path, unixSocketMode); err != nil {
		return cleanup(fmt.Errorf("uidriver: secure socket: %w", err))
	}
	info, err := os.Lstat(path)
	if err != nil {
		return cleanup(fmt.Errorf("uidriver: stat socket: %w", err))
	}
	ctx, cancel := context.WithCancel(context.Background())
	endpoint := &UnixEndpoint{path: path, ln: listener, server: server, ready: ready, ctx: ctx, cancel: cancel, done: make(chan struct{}), file: info}
	endpoint.wg.Add(1)
	go endpoint.accept()
	return endpoint, nil
}

func validateSocketPath(path string) error {
	if path == "" || len(path) > unixSocketMax || !filepath.IsAbs(path) {
		return errors.New("uidriver: socket path must be an absolute path within Unix socket limits")
	}
	if filepath.Clean(path) != path || path == string(filepath.Separator) || filepath.Base(path) == "." || filepath.Base(path) == ".." {
		return errors.New("uidriver: invalid socket path")
	}
	for i := 0; i < len(path); i++ {
		if path[i] == 0 {
			return errors.New("uidriver: socket path contains a NUL")
		}
	}
	return nil
}

// DefaultSocketPath chooses a short private path under the current vev runtime
// directory. The handle is already opaque and contains only hex characters.
func DefaultSocketPath(runtimeDir, handle string) string {
	return filepath.Join(runtimeDir, "ui", handle+".sock")
}

func (e *UnixEndpoint) accept() {
	defer close(e.done)
	defer e.wg.Done()
	for {
		conn, err := e.ln.AcceptUnix()
		if err != nil {
			if e.ctx.Err() != nil || errors.Is(err, net.ErrClosed) {
				return
			}
			continue
		}
		if !peerIsCurrentUser(conn) {
			_ = conn.Close()
			continue
		}
		e.wg.Add(1)
		go func() {
			defer e.wg.Done()
			ready := Ready{}
			if e.ready != nil {
				ready = e.ready()
			}
			_ = e.server.Serve(e.ctx, conn, ready)
		}()
	}
}

// Path is the bound filesystem path.
func (e *UnixEndpoint) Path() string { return e.path }

// Close stops accepting connections and removes only the socket inode created
// by this endpoint.
func (e *UnixEndpoint) Close() error {
	var closeErr error
	e.once.Do(func() {
		e.cancel()
		if err := e.ln.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			closeErr = err
		}
		e.wg.Wait()
		if info, err := os.Lstat(e.path); err == nil {
			if os.SameFile(info, e.file) {
				if err := os.Remove(e.path); err != nil && !errors.Is(err, os.ErrNotExist) && closeErr == nil {
					closeErr = err
				}
			}
		} else if !errors.Is(err, os.ErrNotExist) && closeErr == nil {
			closeErr = err
		}
	})
	return closeErr
}

// Bridge connects stdio to an opted-in interactive UI socket without creating
// another attachment.
func Bridge(ctx context.Context, path string, input io.Reader, output io.Writer) error {
	if err := validateSocketPath(path); err != nil {
		return err
	}
	conn, err := (&net.Dialer{}).DialContext(ctx, "unix", path)
	if err != nil {
		return fmt.Errorf("uidriver: connect socket: %w", err)
	}
	defer conn.Close()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	inputDone := make(chan error, 1)
	outputDone := make(chan error, 1)
	go func() {
		_, copyErr := io.Copy(conn, input)
		inputDone <- copyErr
	}()
	go func() {
		_, copyErr := io.Copy(output, conn)
		outputDone <- copyErr
	}()
	select {
	case inputErr := <-inputDone:
		if inputErr != nil && !errors.Is(inputErr, io.EOF) {
			_ = conn.Close()
			return inputErr
		}
		if unixConn, ok := conn.(*net.UnixConn); ok {
			_ = unixConn.CloseWrite()
		} else {
			_ = conn.Close()
		}
		select {
		case outputErr := <-outputDone:
			return bridgeCopyError(outputErr)
		case <-ctx.Done():
			return ctx.Err()
		}
	case err := <-outputDone:
		_ = conn.Close()
		return bridgeCopyError(err)
	case <-ctx.Done():
		_ = conn.Close()
		return ctx.Err()
	}
}

func bridgeCopyError(err error) error {
	if err == nil || errors.Is(err, io.EOF) || errors.Is(err, net.ErrClosed) {
		return nil
	}
	return err
}
