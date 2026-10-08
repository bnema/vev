// Command relay is disposable acceptance infrastructure, never a production proxy.
package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/bnema/vev/internal/adapters/quicnettest"
)

type command struct {
	ID     uint64 `json:"id"`
	Op     string `json:"op"`
	Port   int    `json:"port,omitempty"`
	UDP    bool   `json:"udp,omitempty"`
	TCP    bool   `json:"tcp,omitempty"`
	Active bool   `json:"active,omitempty"`
}

type relay struct {
	ctx         context.Context
	cancel      context.CancelFunc
	remote      string
	listen      net.IP
	degraded    bool
	blackout    atomic.Bool
	bytes       atomic.Uint64
	mu          sync.Mutex
	connections map[net.Conn]struct{}
	proxies     []*quicnettest.Proxy
	udpBlackout bool
	wg          sync.WaitGroup
}

func main() {
	remote := flag.String("remote", "remote", "fixture upstream host")
	control := flag.String("control", "", "required private Unix control path")
	// The relay forwards without authentication, so it listens on loopback
	// unless the disposable fixture asks for every interface.
	tcp := flag.String("tcp", "127.0.0.1:2222", "SSH listener")
	listen := flag.String("listen", "127.0.0.1", "UDP proxy listen IP")
	degraded := flag.Bool("degraded", false, "enable seeded degraded profile")
	flag.Parse()
	if *control == "" {
		fmt.Fprintln(os.Stderr, "-control is required")
		os.Exit(2)
	}
	listenIP := net.ParseIP(*listen)
	if listenIP == nil {
		fmt.Fprintln(os.Stderr, "-listen must be an IP address")
		os.Exit(2)
	}
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	r := &relay{ctx: ctx, cancel: cancel, remote: *remote, listen: listenIP, degraded: *degraded, connections: make(map[net.Conn]struct{})}
	if err := r.serve(*tcp, *control); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

// listenIP is the UDP proxy bind address, loopback when unset.
func (r *relay) listenIP() net.IP {
	if r.listen == nil {
		return net.IPv4(127, 0, 0, 1)
	}
	return r.listen
}

func (r *relay) serve(addr, path string) error {
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	defer listener.Close()
	parent, err := os.Stat(filepath.Dir(path))
	if err != nil {
		return err
	}
	if !parent.IsDir() || parent.Mode().Perm() != 0700 {
		return errors.New("control socket requires a private 0700 directory")
	}
	owner, ok := parent.Sys().(*syscall.Stat_t)
	if !ok || owner.Uid != uint32(os.Getuid()) {
		return errors.New("control directory must belong to the current user")
	}
	control, err := net.Listen("unix", path)
	if err != nil {
		return err
	}
	defer control.Close()
	watchDone := make(chan struct{})
	defer close(watchDone)
	go func() {
		select {
		case <-r.ctx.Done():
			listener.Close()
			control.Close()
		case <-watchDone:
		}
	}()
	defer os.Remove(path)
	if err := os.Chmod(path, 0600); err != nil {
		return err
	}
	defer func() { listener.Close(); r.close() }()
	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			r.mu.Lock()
			if len(r.connections) >= 64 {
				r.mu.Unlock()
				conn.Close()
				continue
			}
			r.connections[conn] = struct{}{}
			r.mu.Unlock()
			r.wg.Add(1)
			go func() { defer r.wg.Done(); r.forward(conn) }()
		}
	}()
	for {
		conn, err := control.Accept()
		if err != nil {
			if r.ctx.Err() != nil {
				return nil
			}
			return err
		}
		// Commands are bounded and handled serially; a stalled caller gets 2s.
		conn.SetDeadline(time.Now().Add(2 * time.Second))
		scanner := bufio.NewScanner(conn)
		scanner.Buffer(make([]byte, 4096), 4096)
		if scanner.Scan() {
			var c command
			err := json.Unmarshal(scanner.Bytes(), &c)
			var result any
			if err == nil {
				result, err = r.apply(c)
			}
			reply := map[string]any{"id": c.ID, "result": result}
			if err != nil {
				reply["error"] = err.Error()
			}
			json.NewEncoder(conn).Encode(reply)
		}
		conn.Close()
		if r.ctx.Err() != nil {
			listener.Close()
			return nil
		}
	}
}

func (r *relay) apply(c command) (any, error) {
	switch c.Op {
	case "udp":
		if c.Port <= 0 || c.Port > 65535 {
			return nil, errors.New("invalid port")
		}
		upstream, err := net.ResolveUDPAddr("udp", net.JoinHostPort(r.remote, fmt.Sprint(c.Port)))
		if err != nil {
			return nil, err
		}
		link := quicnettest.LinkConfig{}
		if r.degraded {
			link = quicnettest.LinkConfig{BaseLatency: 150 * time.Millisecond, Jitter: 50 * time.Millisecond, LossPercent: 5}
		}
		r.mu.Lock()
		defer r.mu.Unlock()
		if len(r.proxies) >= 32 {
			return nil, errors.New("UDP listener limit")
		}
		p, err := quicnettest.New(quicnettest.Config{ServerAddr: upstream, ListenAddr: &net.UDPAddr{IP: r.listenIP()}, Seed: 317, ToServer: link, ToClient: link})
		if err != nil {
			return nil, err
		}
		p.SetBlackout(r.udpBlackout)
		r.proxies = append(r.proxies, p)
		return p.Addr().Port, nil
	case "blackout":
		if c.TCP {
			r.blackout.Store(c.Active)
		}
		if c.UDP {
			r.mu.Lock()
			r.udpBlackout = c.Active
			for _, p := range r.proxies {
				p.SetBlackout(c.Active)
			}
			r.mu.Unlock()
		}
		return nil, nil
	case "disconnect":
		r.mu.Lock()
		for conn := range r.connections {
			conn.Close()
		}
		r.mu.Unlock()
		return nil, nil
	case "stats":
		r.mu.Lock()
		defer r.mu.Unlock()
		stats := make([]quicnettest.Stats, 0, len(r.proxies))
		for _, p := range r.proxies {
			stats = append(stats, p.Stats())
		}
		return map[string]any{"udp": stats, "tcp_bytes": r.bytes.Load()}, nil
	case "stop":
		r.cancel()
		return nil, nil
	default:
		return nil, errors.New("unknown command")
	}
}

func (r *relay) forward(client net.Conn) {
	defer func() { client.Close(); r.mu.Lock(); delete(r.connections, client); r.mu.Unlock() }()
	server, err := (&net.Dialer{Timeout: 5 * time.Second}).DialContext(r.ctx, "tcp", net.JoinHostPort(r.remote, "22"))
	if err != nil {
		return
	}
	defer server.Close()
	r.mu.Lock()
	r.connections[server] = struct{}{}
	r.mu.Unlock()
	defer func() { r.mu.Lock(); delete(r.connections, server); r.mu.Unlock() }()
	done := make(chan struct{})
	go func() { r.copy(server, client); server.Close(); client.Close(); close(done) }()
	r.copy(client, server)
	server.Close()
	client.Close()
	<-done
}

func (r *relay) wait(delay time.Duration) bool {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-timer.C:
		return true
	case <-r.ctx.Done():
		return false
	}
}

func (r *relay) copy(dst, src net.Conn) {
	buf := make([]byte, 32<<10)
	for {
		for r.blackout.Load() {
			if !r.wait(10 * time.Millisecond) {
				return
			}
		}
		n, err := src.Read(buf)
		if n > 0 {
			for r.blackout.Load() {
				if !r.wait(10 * time.Millisecond) {
					return
				}
			}
			if r.degraded && !r.wait(150*time.Millisecond+time.Duration(n)*time.Second/(64<<10)) {
				return
			}
			written, writeErr := io.Copy(dst, bytes.NewReader(buf[:n]))
			r.bytes.Add(uint64(written))
			if writeErr != nil {
				return
			}
		}
		if err != nil {
			return
		}
	}
}

func (r *relay) close() {
	r.cancel()
	r.mu.Lock()
	for conn := range r.connections {
		conn.Close()
	}
	for _, p := range r.proxies {
		p.Close()
	}
	r.mu.Unlock()
	r.wg.Wait()
}
