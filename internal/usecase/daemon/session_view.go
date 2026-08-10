package daemon

import (
	"time"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/usecase/picker"
)

// viewOptions controls optional (costly) fields captured by snapshotView.
type viewOptions struct {
	tabDetails    bool
	focusedTitles bool
	terminalTitle bool
}

// tabView is an immutable, value-only description of one tab.
type tabView struct {
	id           domain.TabStableID
	name         string
	focusedTitle string
	attention    bool
	attentionAt  time.Time
}

// sessionView is an immutable, value-only description of a session. It
// deliberately retains no live pointers, mirroring route snapshot entries.
// Listing paths (picker, palette, and list-sessions) read local session state
// through snapshotView.
type sessionView struct {
	id           domain.SessionID
	incarnation  domain.IncarnationID
	name         string
	ephemeral    bool
	createdAt    int64
	defaultTab   int
	mruAt        uint64
	attached     bool
	tabCount     int
	hasAttention bool
	tabs         []tabView
}

// snapshotView reads session fields under s.mu and samples the independently
// atomic mruAt while that lock is held. tabCount and hasAttention are always
// filled; tabs is allocated only when opts.tabDetails is true (non-nil empty
// slice when there are no tabs). Never call while holding a pane lock.
func (s *session) snapshotView(opts viewOptions) sessionView {
	if opts.focusedTitles && !opts.tabDetails {
		opts.tabDetails = true
	}
	s.mu.Lock()
	// Session-level listings use the first ordered tab as their deterministic
	// default. Interactive callers pass an attachment and resolve its stable
	// view separately.
	view := sessionView{
		id:          s.id,
		incarnation: s.incarnation,
		name:        s.name,
		ephemeral:   s.ephemeral,
		createdAt:   s.createdAt,
		defaultTab:  0,
		mruAt:       s.mruAt.Load(),
		attached:    len(s.attachments) != 0,
		tabCount:    len(s.tabs),
	}
	if opts.tabDetails {
		view.tabs = make([]tabView, 0, len(s.tabs))
	}
	for i, tb := range s.tabs {
		if tb.attention {
			view.hasAttention = true
			if !opts.tabDetails {
				break
			}
		}
		if !opts.tabDetails {
			continue
		}
		entry := tabView{
			id:          domain.TabStableID(tb.stableID),
			name:        tabDisplayName(tb, i),
			attention:   tb.attention,
			attentionAt: tb.attentionAt,
		}
		if opts.focusedTitles {
			entry.focusedTitle = tb.focusedPaneTitle(opts.terminalTitle)
		}
		view.tabs = append(view.tabs, entry)
	}
	s.mu.Unlock()
	return view
}

// pickerView renders this snapshot as the picker's value type. Field-for-field
// equivalent to the inline construction previously in pickerViews.
func (view sessionView) pickerView() picker.SessionView {
	out := picker.SessionView{
		ID:          view.id,
		Incarnation: view.incarnation,
		Name:        view.name,
		TargetName:  view.name,
		Active:      view.defaultTab,
		Tabs:        make([]picker.TabEntry, 0, len(view.tabs)),
	}
	if !view.ephemeral {
		createdAt := view.createdAt
		out.ExpectedCreatedAt = &createdAt
	}
	attention := false
	for _, tb := range view.tabs {
		out.Tabs = append(out.Tabs, picker.TabEntry{
			TabID:     tb.id,
			Name:      tb.name,
			Detail:    tabTitleDetail(tb.name, tb.focusedTitle),
			Attention: tb.attention,
		})
		if tb.attention {
			attention = true
		}
	}
	if attention {
		out.Name = attentionSuffix(out.Name)
	}
	return out
}
