package client

import (
	"sync"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/protocol"
)

// sessionTabMemory belongs to one client process and survives attachment swaps.
// Authority and lifecycle identity prevent unrelated hosts or recreated sessions
// from inheriting a selection. Only committed views update the memory.
type sessionTabMemory struct {
	mu   sync.Mutex
	tabs map[sessionTabKey]domain.TabStableID
}

type sessionTabKey struct {
	authority routeAuthority
	target    protocol.ExactSessionTarget
}

func (m *sessionTabMemory) remember(authority routeAuthority, target protocol.ExactSessionTarget, tab domain.TabStableID) {
	if m == nil || target.Validate() != nil || domain.ValidateTabStableID(tab) != nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.tabs == nil {
		m.tabs = make(map[sessionTabKey]domain.TabStableID)
	}
	// A recreated session cannot use its predecessor's cursor.
	for key := range m.tabs {
		if key.authority == authority && key.target.SessionName == target.SessionName && key.target != target {
			delete(m.tabs, key)
		}
	}
	m.tabs[sessionTabKey{authority: authority, target: target}] = tab
}

func (m *sessionTabMemory) preferred(authority routeAuthority, target protocol.ExactSessionTarget, explicit domain.TabStableID) domain.TabStableID {
	if explicit != "" || m == nil {
		return explicit
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.tabs[sessionTabKey{authority: authority, target: target}]
}
