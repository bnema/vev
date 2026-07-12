package main

import (
	"errors"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

// udpNetem is a harness-owned, user-space UDP relay. It sits between the
// public dgram client and vev's public _udp-proxy readiness address, so fixture
// impairment is real network carriage rather than an ignored vev option.
type udpNetem interface {
	Port() int
	Close() error
}

type udpNetemFactory func(udpNetemConfig) (udpNetem, error)

type udpNetemConfig struct {
	// RTT is split equally across both relay directions.
	RTT         time.Duration
	LossPercent int
	TargetPath  string
}

type udpNetemRelay struct {
	config udpNetemConfig
	conn   net.PacketConn
	done   chan struct{}
	wg     sync.WaitGroup
	once   sync.Once

	mu          sync.Mutex
	client, dst net.Addr
	packet      uint64
}

func newUDPNetem(config udpNetemConfig) (udpNetem, error) {
	if config.RTT < 0 || config.LossPercent < 0 || config.LossPercent > 100 || config.TargetPath == "" {
		return nil, fmt.Errorf("invalid UDP netem fixture: rtt=%s loss=%d target=%q", config.RTT, config.LossPercent, config.TargetPath)
	}
	conn, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	r := &udpNetemRelay{config: config, conn: conn, done: make(chan struct{})}
	r.wg.Add(1)
	go r.run()
	return r, nil
}

func (r *udpNetemRelay) Port() int {
	return r.conn.LocalAddr().(*net.UDPAddr).Port
}

func (r *udpNetemRelay) Close() error {
	var closeErr error
	r.once.Do(func() {
		close(r.done)
		closeErr = r.conn.Close()
		r.wg.Wait()
	})
	return closeErr
}

func (r *udpNetemRelay) run() {
	defer r.wg.Done()
	buf := make([]byte, 64*1024)
	for {
		n, from, err := r.conn.ReadFrom(buf)
		if err != nil {
			return
		}
		to, ok := r.destination(from)
		if !ok || r.drop() {
			continue
		}
		packet := append([]byte(nil), buf[:n]...)
		r.schedule(packet, to)
	}
}

func (r *udpNetemRelay) destination(from net.Addr) (net.Addr, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.dst == nil {
		dst, err := udpNetemTarget(r.config.TargetPath)
		if err != nil {
			return nil, false
		}
		r.dst = dst
	}
	if sameUDPAddress(from, r.dst) {
		return r.client, r.client != nil
	}
	r.client = from
	return r.dst, true
}

func (r *udpNetemRelay) drop() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.packet++
	// The deterministic cycle makes the configured percentage auditable: a 1%
	// fixture drops exactly one carriage packet in every 100 attempts.
	return r.config.LossPercent > 0 && int((r.packet-1)%100) < r.config.LossPercent
}

func (r *udpNetemRelay) schedule(packet []byte, to net.Addr) {
	delay := r.config.RTT / 2
	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		if delay > 0 {
			timer := time.NewTimer(delay)
			defer timer.Stop()
			select {
			case <-timer.C:
			case <-r.done:
				return
			}
		}
		select {
		case <-r.done:
			return
		default:
			_, _ = r.conn.WriteTo(packet, to)
		}
	}()
}

func udpNetemTarget(path string) (net.Addr, error) {
	line, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	fields := strings.Fields(string(line))
	if len(fields) != 3 || fields[0] != "VEV-UDP" {
		return nil, errors.New("invalid UDP proxy readiness")
	}
	port, err := strconv.Atoi(fields[1])
	if err != nil || port < 1 || port > 65535 || fields[2] == "" {
		return nil, errors.New("invalid UDP proxy readiness")
	}
	return &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: port}, nil
}

func sameUDPAddress(a, b net.Addr) bool {
	ua, aOK := a.(*net.UDPAddr)
	ub, bOK := b.(*net.UDPAddr)
	return aOK && bOK && ua.Port == ub.Port && ua.IP.Equal(ub.IP)
}
