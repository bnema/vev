package quicnettest

import (
	"container/heap"
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

// ErrClosed is returned by operations on a closed proxy.
var ErrClosed = errors.New("quicnettest: proxy is closed")

// Proxy is a bounded, deterministic UDP relay with per-direction impairment. It
// listens on an ephemeral loopback port for clients and forwards to
// Config.ServerAddr from a separate upstream socket that can be rebound to a
// new source address at any time.
type Proxy struct {
	cfg    Config
	clock  Clock
	server *net.UDPAddr

	clientConn *net.UDPConn
	upstream   atomic.Pointer[net.UDPConn]

	done     chan struct{}
	wg       sync.WaitGroup
	once     sync.Once
	closeErr error

	// slots bounds queued plus deadline-pending packets; queue only hands
	// packets to the scheduler and is drained eagerly.
	slots chan struct{}
	queue chan packet

	start time.Time
	seq   atomic.Uint64

	toServer impairer
	toClient impairer

	toServerCounters linkCounters
	toClientCounters linkCounters

	strayPackets atomic.Uint64
	rebinds      atomic.Uint64
	rebindErrors atomic.Uint64
	sentToServer atomic.Uint64

	rebindMu       sync.Mutex
	clientAddr     atomic.Pointer[net.UDPAddr]
	manualBlackout atomic.Bool

	onPacket func(Event)
}

type linkCounters struct {
	received       atomic.Uint64
	sent           atomic.Uint64
	lossDrops      atomic.Uint64
	blackoutDrops  atomic.Uint64
	oversizeDrops  atomic.Uint64
	overflowDrops  atomic.Uint64
	duplicates     atomic.Uint64
	reordered      atomic.Uint64
	deliveryErrors atomic.Uint64
}

func (c *linkCounters) snapshot() LinkStats {
	return LinkStats{
		Received:       c.received.Load(),
		Sent:           c.sent.Load(),
		LossDrops:      c.lossDrops.Load(),
		BlackoutDrops:  c.blackoutDrops.Load(),
		OversizeDrops:  c.oversizeDrops.Load(),
		OverflowDrops:  c.overflowDrops.Load(),
		Duplicates:     c.duplicates.Load(),
		Reordered:      c.reordered.Load(),
		DeliveryErrors: c.deliveryErrors.Load(),
	}
}

type packet struct {
	data     []byte
	dst      *net.UDPAddr
	toServer bool
	due      time.Time
	seq      uint64
}

type packetHeap []packet

func (h packetHeap) Len() int { return len(h) }

func (h packetHeap) Less(i, j int) bool {
	if h[i].due.Equal(h[j].due) {
		return h[i].seq < h[j].seq
	}
	return h[i].due.Before(h[j].due)
}

func (h packetHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

func (h *packetHeap) Push(value any) {
	pkt, ok := value.(packet)
	if !ok {
		panic(fmt.Sprintf("quicnettest: packet heap received %T", value))
	}
	*h = append(*h, pkt)
}

func (h *packetHeap) Pop() any {
	old := *h
	n := len(old)
	pkt := old[n-1]
	old[n-1] = packet{}
	*h = old[:n-1]
	return pkt
}

// New starts a proxy in front of cfg.ServerAddr and returns it. Callers must
// Close the proxy to release its three goroutines and two sockets.
func New(cfg Config) (*Proxy, error) {
	cfg = cfg.withDefaults()
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	clientConn, err := listenLoopback()
	if err != nil {
		return nil, fmt.Errorf("quicnettest: listen client socket: %w", err)
	}
	upstream, err := listenLoopback()
	if err != nil {
		_ = clientConn.Close()
		return nil, fmt.Errorf("quicnettest: listen upstream socket: %w", err)
	}
	start := cfg.Clock.Now()
	var shared RNG
	if cfg.RNG != nil {
		shared = &lockedRNG{rng: cfg.RNG}
	}
	rngFor := func(direction Direction) RNG {
		if shared != nil {
			return shared
		}
		return newRNG(cfg.Seed, direction)
	}
	p := &Proxy{
		cfg:        cfg,
		clock:      cfg.Clock,
		server:     cfg.ServerAddr,
		clientConn: clientConn,
		done:       make(chan struct{}),
		slots:      make(chan struct{}, cfg.QueueCapacity),
		queue:      make(chan packet, cfg.QueueCapacity),
		start:      start,
		toServer:   impairer{link: cfg.ToServer, rng: rngFor(ToServer), start: start},
		toClient:   impairer{link: cfg.ToClient, rng: rngFor(ToClient), start: start},
		onPacket:   cfg.OnPacket,
	}
	p.upstream.Store(upstream)
	p.wg.Add(3)
	go p.runClientReader()
	go p.runServerReader()
	go p.runScheduler()
	return p, nil
}

// Addr returns the client-facing address. Sending here is equivalent to sending
// to the impaired server.
func (p *Proxy) Addr() *net.UDPAddr {
	addr, ok := p.clientConn.LocalAddr().(*net.UDPAddr)
	if !ok {
		return nil
	}
	return &net.UDPAddr{IP: append(net.IP(nil), addr.IP...), Port: addr.Port}
}

// UpstreamAddr returns the current server-facing local address, or nil once the
// proxy is closed. It changes on every successful Rebind.
func (p *Proxy) UpstreamAddr() *net.UDPAddr {
	conn := p.upstream.Load()
	if conn == nil {
		return nil
	}
	addr, ok := conn.LocalAddr().(*net.UDPAddr)
	if !ok {
		return nil
	}
	return &net.UDPAddr{IP: append(net.IP(nil), addr.IP...), Port: addr.Port}
}

// SetBlackout toggles a manual blackout applied on top of the scheduled
// windows. It is safe to call from any goroutine.
func (p *Proxy) SetBlackout(active bool) { p.manualBlackout.Store(active) }

// Rebind replaces the upstream socket, changing the source address the server
// observes. In-flight packets that the server sends to the previous address are
// lost, which mirrors a NAT rebinding.
func (p *Proxy) Rebind() error {
	p.rebindMu.Lock()
	defer p.rebindMu.Unlock()
	if p.stopping() {
		return ErrClosed
	}
	conn, err := listenLoopback()
	if err != nil {
		return fmt.Errorf("quicnettest: rebind: %w", err)
	}
	if p.stopping() {
		_ = conn.Close()
		return ErrClosed
	}
	old := p.upstream.Swap(conn)
	if old != nil {
		_ = old.Close()
	}
	p.rebinds.Add(1)
	return nil
}

// Stats returns a snapshot of the proxy counters.
func (p *Proxy) Stats() Stats {
	return Stats{
		ToServer:     p.toServerCounters.snapshot(),
		ToClient:     p.toClientCounters.snapshot(),
		StrayPackets: p.strayPackets.Load(),
		Rebinds:      p.rebinds.Load(),
		RebindErrors: p.rebindErrors.Load(),
	}
}

// Close stops the proxy and waits for its goroutines to exit. It is idempotent
// and returns the same error on every call.
func (p *Proxy) Close() error {
	p.once.Do(func() {
		close(p.done)
		var errs []error
		if err := p.clientConn.Close(); err != nil {
			errs = append(errs, err)
		}
		p.rebindMu.Lock()
		if upstream := p.upstream.Swap(nil); upstream != nil {
			if err := upstream.Close(); err != nil {
				errs = append(errs, err)
			}
		}
		p.rebindMu.Unlock()
		p.wg.Wait()
		p.closeErr = errors.Join(errs...)
	})
	return p.closeErr
}

func (p *Proxy) stopping() bool {
	select {
	case <-p.done:
		return true
	default:
		return false
	}
}

func (p *Proxy) counters(toServer bool) *linkCounters {
	if toServer {
		return &p.toServerCounters
	}
	return &p.toClientCounters
}

func (p *Proxy) linkFor(toServer bool) LinkConfig {
	if toServer {
		return p.cfg.ToServer
	}
	return p.cfg.ToClient
}

func (p *Proxy) impairerFor(toServer bool) *impairer {
	if toServer {
		return &p.toServer
	}
	return &p.toClient
}

func (p *Proxy) runClientReader() {
	defer p.wg.Done()
	buf := make([]byte, p.cfg.MaxPacketSize+1)
	for {
		n, from, err := p.clientConn.ReadFromUDP(buf)
		if err != nil {
			return
		}
		p.clientAddr.Store(from)
		p.ingest(true, buf[:n], p.server)
	}
}

func (p *Proxy) runServerReader() {
	defer p.wg.Done()
	buf := make([]byte, p.cfg.MaxPacketSize+1)
	for {
		conn := p.upstream.Load()
		if conn == nil {
			return
		}
		n, from, err := conn.ReadFromUDP(buf)
		if err != nil {
			if p.stopping() {
				return
			}
			if conn != p.upstream.Load() {
				// The socket was rebound; continue on the new one.
				continue
			}
			return
		}
		if !sameUDPAddr(from, p.server) {
			p.strayPackets.Add(1)
			continue
		}
		client := p.clientAddr.Load()
		if client == nil {
			p.strayPackets.Add(1)
			continue
		}
		p.ingest(false, buf[:n], client)
	}
}

func (p *Proxy) runScheduler() {
	defer p.wg.Done()
	var pending packetHeap
	heap.Init(&pending)
	timer := p.clock.NewTimer(time.Hour)
	if !timer.Stop() {
		<-timer.C()
	}
	defer timer.Stop()
	for {
		// Drain everything already queued before evaluating deadlines. A test
		// clock jump must observe all queued packets, otherwise a later packet
		// with an earlier deadline could be delivered behind a later one.
		p.drainQueue(&pending)
		if len(pending) == 0 {
			select {
			case <-p.done:
				return
			case pkt := <-p.queue:
				heap.Push(&pending, pkt)
			}
			continue
		}
		now := p.clock.Now()
		if !pending[0].due.After(now) {
			p.deliverDue(&pending, now)
			continue
		}
		if !timer.Stop() {
			select {
			case <-timer.C():
			default:
			}
		}
		timer.ResetAt(pending[0].due)
		select {
		case <-p.done:
			return
		case pkt := <-p.queue:
			heap.Push(&pending, pkt)
		case <-timer.C():
		}
	}
}

func (p *Proxy) drainQueue(pending *packetHeap) {
	for {
		select {
		case pkt := <-p.queue:
			heap.Push(pending, pkt)
		default:
			return
		}
	}
}

func (p *Proxy) deliverDue(pending *packetHeap, now time.Time) {
	for pending.Len() > 0 && !(*pending)[0].due.After(now) {
		pkt, ok := heap.Pop(pending).(packet)
		if !ok {
			return
		}
		p.deliver(pkt)
		<-p.slots
	}
}

// ingest applies impairments to one freshly read datagram and schedules its
// copies. It never blocks: a full budget drops the packet instead.
func (p *Proxy) ingest(toServer bool, raw []byte, dst *net.UDPAddr) {
	counters := p.counters(toServer)
	seq := p.seq.Add(1)
	now := p.clock.Now()
	if len(raw) > p.cfg.MaxPacketSize {
		counters.oversizeDrops.Add(1)
		p.emit(Event{
			Direction: directionOf(toServer),
			Sequence:  seq,
			Bytes:     len(raw),
			At:        now,
			Decision:  Decision{Drop: true, Oversize: true},
		})
		return
	}
	counters.received.Add(1)

	var decision Decision
	if p.manualBlackout.Load() {
		decision = Decision{Drop: true, Blackout: true}
	} else {
		decision = p.impairerFor(toServer).decide(now)
	}
	if decision.Drop {
		if decision.Blackout {
			counters.blackoutDrops.Add(1)
		} else {
			counters.lossDrops.Add(1)
		}
		p.emit(Event{
			Direction: directionOf(toServer),
			Sequence:  seq,
			Bytes:     len(raw),
			At:        now,
			Decision:  decision,
		})
		return
	}

	link := p.linkFor(toServer)
	if decision.Duplicates > 0 {
		counters.duplicates.Add(uint64(decision.Duplicates))
	}
	if decision.Reordered {
		counters.reordered.Add(1)
	}
	due := now.Add(link.BaseLatency + decision.ExtraDelay)
	for copyIndex := 0; copyIndex <= decision.Duplicates; copyIndex++ {
		copyDue := due.Add(time.Duration(copyIndex) * link.DuplicateDelay)
		if !p.enqueue(packet{
			data:     append([]byte(nil), raw...),
			dst:      dst,
			toServer: toServer,
			due:      copyDue,
			seq:      seq,
		}) {
			counters.overflowDrops.Add(1)
		}
	}
	p.emit(Event{
		Direction: directionOf(toServer),
		Sequence:  seq,
		Bytes:     len(raw),
		At:        now,
		Due:       due,
		Decision:  decision,
	})
}

func (p *Proxy) enqueue(pkt packet) bool {
	select {
	case p.slots <- struct{}{}:
	case <-p.done:
		return false
	default:
		return false
	}
	select {
	case p.queue <- pkt:
		return true
	case <-p.done:
		<-p.slots
		return false
	}
}

func (p *Proxy) deliver(pkt packet) {
	counters := p.counters(pkt.toServer)
	conn := p.clientConn
	if pkt.toServer {
		conn = p.upstream.Load()
	}
	if conn == nil {
		if !p.stopping() {
			counters.deliveryErrors.Add(1)
		}
		return
	}
	// Count before writing so that a peer observing the packet implies the
	// counter has been updated.
	counters.sent.Add(1)
	if _, err := conn.WriteToUDP(pkt.data, pkt.dst); err != nil {
		if !p.stopping() {
			counters.deliveryErrors.Add(1)
		}
		return
	}
	if pkt.toServer && p.cfg.NATRebindAfterPackets > 0 {
		every := uint64(p.cfg.NATRebindAfterPackets)
		if sent := p.sentToServer.Add(1); sent%every == 0 {
			if err := p.Rebind(); err != nil {
				p.rebindErrors.Add(1)
			}
		}
	}
}

func (p *Proxy) emit(event Event) {
	if p.onPacket == nil {
		return
	}
	p.onPacket(event)
}

func directionOf(toServer bool) Direction {
	if toServer {
		return ToServer
	}
	return ToClient
}

func listenLoopback() (*net.UDPConn, error) {
	var lc net.ListenConfig
	conn, err := lc.ListenPacket(context.Background(), "udp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	udp, ok := conn.(*net.UDPConn)
	if !ok {
		_ = conn.Close()
		return nil, fmt.Errorf("quicnettest: udp listener returned %T", conn)
	}
	return udp, nil
}

func sameUDPAddr(a, b *net.UDPAddr) bool {
	return a != nil && b != nil && a.Port == b.Port && a.IP.Equal(b.IP)
}
