package main

import (
	"container/heap"
	"errors"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
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

// udpNetemQueueCapacity bounds both packets waiting to be scheduled and
// packets waiting for their delivery deadline. When it is full, the relay
// drops the arriving packet and records it in queueOverflowDrops.
const udpNetemQueueCapacity = 256

type scheduledPacket struct {
	packet []byte
	to     net.Addr
	due    time.Time
}

type scheduledPackets []scheduledPacket

func (p scheduledPackets) Len() int           { return len(p) }
func (p scheduledPackets) Less(i, j int) bool { return p[i].due.Before(p[j].due) }
func (p scheduledPackets) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }
func (p *scheduledPackets) Push(x any) {
	packet, ok := x.(scheduledPacket)
	if !ok {
		panic(fmt.Sprintf("scheduled packet type %T", x))
	}
	*p = append(*p, packet)
}
func (p *scheduledPackets) Pop() any {
	old := *p
	n := len(old)
	item := old[n-1]
	*p = old[:n-1]
	return item
}

type udpNetemRelay struct {
	config   udpNetemConfig
	conn     net.PacketConn
	done     chan struct{}
	wg       sync.WaitGroup
	once     sync.Once
	closeErr error

	// slots bounds queued and deadline-pending packets together. The reader and
	// the sole scheduler are the relay's only goroutines.
	slots              chan struct{}
	queue              chan scheduledPacket
	queueOverflowDrops atomic.Uint64
	deliveryErrors     atomic.Uint64

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
	r := &udpNetemRelay{
		config: config,
		conn:   conn,
		done:   make(chan struct{}),
		slots:  make(chan struct{}, udpNetemQueueCapacity),
		queue:  make(chan scheduledPacket, udpNetemQueueCapacity),
	}
	r.wg.Add(2)
	go r.run()
	go r.schedule()
	return r, nil
}

func (r *udpNetemRelay) Port() int {
	addr, ok := r.conn.LocalAddr().(*net.UDPAddr)
	if !ok {
		return 0
	}
	return addr.Port
}

func (r *udpNetemRelay) Close() error {
	r.once.Do(func() {
		close(r.done)
		r.closeErr = r.conn.Close()
		r.wg.Wait()
		if drops := r.queueOverflowDrops.Load(); drops != 0 {
			r.closeErr = errors.Join(r.closeErr, fmt.Errorf("udp netem queue overflow drops: %d", drops))
		}
		if writes := r.deliveryErrors.Load(); writes != 0 {
			r.closeErr = errors.Join(r.closeErr, fmt.Errorf("udp netem delivery errors: %d", writes))
		}
	})
	return r.closeErr
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
		// Copy because PacketConn reuses buf on its next read.
		r.enqueue(scheduledPacket{packet: append([]byte(nil), buf[:n]...), to: to, due: time.Now().Add(r.config.RTT / 2)})
	}
}

// enqueue implements the explicit overflow policy: never block the reader;
// drop a packet if the bounded scheduler capacity has been reached.
func (r *udpNetemRelay) enqueue(packet scheduledPacket) bool {
	select {
	case r.slots <- struct{}{}:
	case <-r.done:
		return false
	default:
		r.queueOverflowDrops.Add(1)
		return false
	}
	select {
	case r.queue <- packet:
		return true
	case <-r.done:
		<-r.slots
		return false
	}
}

func (r *udpNetemRelay) schedule() {
	defer r.wg.Done()
	pending := scheduledPackets{}
	heap.Init(&pending)
	timer := time.NewTimer(time.Hour)
	if !timer.Stop() {
		<-timer.C
	}
	defer timer.Stop()
	var timerC <-chan time.Time
	resetTimer := func() {
		if timerC != nil && !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		if pending.Len() == 0 {
			timerC = nil
			return
		}
		delay := time.Until(pending[0].due)
		if delay < 0 {
			delay = 0
		}
		timer.Reset(delay)
		timerC = timer.C
	}
	for {
		resetTimer()
		select {
		case <-r.done:
			return
		case packet := <-r.queue:
			heap.Push(&pending, packet)
		case <-timerC:
			now := time.Now()
			for pending.Len() > 0 && !pending[0].due.After(now) {
				packet, ok := heap.Pop(&pending).(scheduledPacket)
				if !ok {
					panic("scheduled packet heap contained unexpected type")
				}
				select {
				case <-r.done:
					return
				default:
					if _, err := r.conn.WriteTo(packet.packet, packet.to); err != nil {
						select {
						case <-r.done:
						default:
							r.deliveryErrors.Add(1)
						}
					}
					<-r.slots
				}
			}
		}
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
