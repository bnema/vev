package client

import (
	"sync"

	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol"
)

type routeObserverKey struct {
	origin    protocol.RouteOrigin
	originKey string
}

type routeObserverAuthority struct {
	key    routeObserverKey
	epoch  uint64
	dialer ports.ClientDialer
}

type routeObserverRegistry struct {
	mu          sync.RWMutex
	nextEpoch   uint64
	authorities map[routeObserverKey]routeObserverAuthority
}

func newRouteObserverRegistry() *routeObserverRegistry {
	return &routeObserverRegistry{authorities: make(map[routeObserverKey]routeObserverAuthority)}
}

func (r *routeObserverRegistry) register(request AttachRequest, dialer ports.ClientDialer) {
	if r == nil || dialer == nil {
		return
	}
	origin := normalizeRouteOrigin(request.Origin, request.Remote)
	key := routeObserverKey{origin: origin, originKey: normalizeRouteOriginKey(request.OriginKey, origin)}
	r.mu.Lock()
	defer r.mu.Unlock()
	if current, ok := r.authorities[key]; ok && current.dialer == dialer {
		return
	}
	r.nextEpoch++
	r.authorities[key] = routeObserverAuthority{key: key, epoch: r.nextEpoch, dialer: dialer}
}

func (r *routeObserverRegistry) snapshot() []routeObserverAuthority {
	if r == nil {
		return nil
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	authorities := make([]routeObserverAuthority, 0, len(r.authorities))
	for _, authority := range r.authorities {
		authorities = append(authorities, authority)
	}
	return authorities
}

func (r *routeObserverRegistry) current(authority routeObserverAuthority) bool {
	if r == nil {
		return false
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	current, ok := r.authorities[authority.key]
	return ok && current.epoch == authority.epoch && current.dialer == authority.dialer
}
