package observability

import (
	"sync"

	"github.com/bnema/vev/internal/ports"
)

// NewSerialized isolates producers from a potentially slow observer with one
// ordered worker and a bounded queue.
func NewSerialized(observer ports.RuntimeObserver, capacity int) ports.SerializedRuntimeObserver {
	if observer == nil {
		return nil
	}
	if capacity < 1 {
		capacity = 1
	}
	o := &serializedRuntimeObserver{observer: observer, capacity: capacity, done: make(chan struct{})}
	o.cond = sync.NewCond(&o.mu)
	go o.run()
	return o
}

// EnsureSerialized reuses an existing serialized owner or creates one.
func EnsureSerialized(observer ports.RuntimeObserver, capacity int) (serialized ports.SerializedRuntimeObserver, owned bool) {
	if observer == nil {
		return nil, false
	}
	if serialized, ok := observer.(ports.SerializedRuntimeObserver); ok {
		return serialized, false
	}
	return NewSerialized(observer, capacity), true
}

type serializedRuntimeObserver struct {
	observer ports.RuntimeObserver
	capacity int

	mu      sync.Mutex
	cond    *sync.Cond
	marks   []ports.RuntimeMark
	dropped uint64
	closed  bool
	active  bool
	done    chan struct{}
}

func (o *serializedRuntimeObserver) ObserveRuntime(mark ports.RuntimeMark) {
	if o == nil {
		return
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.closed {
		return
	}
	if o.dropped != 0 {
		if len(o.marks)+2 > o.capacity {
			o.dropped++
			return
		}
		o.marks = append(o.marks, gapMark(o.dropped), mark)
		o.dropped = 0
		o.cond.Signal()
		return
	}
	if len(o.marks) == o.capacity {
		o.dropped = 1
		return
	}
	o.marks = append(o.marks, mark)
	o.cond.Signal()
}

func gapMark(dropped uint64) ports.RuntimeMark {
	return ports.NewRuntimeMark("observer", ports.RuntimeTransportDiagnostic, dropped, false)
}

func (o *serializedRuntimeObserver) Flush() {
	if o == nil {
		return
	}
	o.mu.Lock()
	for len(o.marks) != 0 || o.active || o.dropped != 0 {
		o.cond.Signal()
		o.cond.Wait()
	}
	closed := o.closed
	o.mu.Unlock()
	if closed {
		<-o.done
	}
}

func (o *serializedRuntimeObserver) Close() {
	if o == nil {
		return
	}
	o.mu.Lock()
	if !o.closed {
		o.closed = true
		o.cond.Broadcast()
	}
	o.mu.Unlock()
	<-o.done
}

func (o *serializedRuntimeObserver) run() {
	defer close(o.done)
	for {
		o.mu.Lock()
		for len(o.marks) == 0 && o.dropped == 0 && !o.closed {
			o.cond.Wait()
		}
		if len(o.marks) == 0 && o.dropped != 0 {
			o.marks = append(o.marks, gapMark(o.dropped))
			o.dropped = 0
		}
		if len(o.marks) == 0 && o.closed {
			o.cond.Broadcast()
			o.mu.Unlock()
			return
		}
		mark := o.marks[0]
		o.marks = o.marks[1:]
		o.active = true
		o.mu.Unlock()

		o.observer.ObserveRuntime(mark)

		o.mu.Lock()
		o.active = false
		o.cond.Broadcast()
		o.mu.Unlock()
	}
}
