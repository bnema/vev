package daemon

import "sync"

// Guarded protects one value behind a mutex.
type Guarded[T any] struct {
	mu sync.Mutex
	v  T
}

func (g *Guarded[T]) Get() T {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.v
}

func (g *Guarded[T]) Set(v T) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.v = v
}

func (g *Guarded[T]) With(fn func(*T)) {
	g.mu.Lock()
	defer g.mu.Unlock()
	fn(&g.v)
}
