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
	key := sessionTabKey{authority: authority, target: target}
	if previous, known := m.tabs[key]; known {
		if previous != tab {
			m.tabs[key] = tab
		}
		return
	}
	// Reclaim obsolete lifecycles only on the first view of a session.
	// The exact key, not this cleanup, isolates recreated sessions.
	for old := range m.tabs {
		if old.authority == authority && old.target.SessionName == target.SessionName {
			delete(m.tabs, old)
		}
	}
	m.tabs[key] = tab
}

func (m *sessionTabMemory) preferred(authority routeAuthority, target protocol.ExactSessionTarget, explicit domain.TabStableID) domain.TabStableID {
	if explicit != "" || m == nil {
		return explicit
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.tabs[sessionTabKey{authority: authority, target: target}]
}
