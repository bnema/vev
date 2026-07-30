package daemon

import (
	"reflect"
	"testing"

	"github.com/bnema/vev/internal/domain"
)

func TestSnapshotViewCapturesSessionFields(t *testing.T) {
	tests := []struct {
		name  string
		build func() *session
		opts  viewOptions
		want  sessionView
	}{
		{
			name: "named session with attention tab",
			build: func() *session {
				s := &session{sessionCore: sessionCore{id: domain.SessionID("s1"),
					incarnation: domain.IncarnationID{1},
					name:        "alpha",
					createdAt:   42,

					client: &attachedClient{}}, active: 1,

					tabs: []*tab{
						{stableID: "t1", name: "build"},
						{stableID: "t2", name: "logs", attention: true},
					},
				}
				s.mruAt.Store(7)
				return s
			},
			opts: viewOptions{tabDetails: true},
			want: sessionView{
				id:           domain.SessionID("s1"),
				incarnation:  domain.IncarnationID{1},
				name:         "alpha",
				createdAt:    42,
				active:       1,
				mruAt:        7,
				attached:     true,
				tabCount:     2,
				hasAttention: true,
				tabs: []tabView{
					{id: domain.TabStableID("t1"), name: "build"},
					{id: domain.TabStableID("t2"), name: "logs", attention: true},
				},
			},
		},
		{
			name: "ephemeral detached session without tabs",
			build: func() *session {
				return &session{sessionCore: sessionCore{id: domain.SessionID("s2"), name: "9", ephemeral: true}}
			},
			opts: viewOptions{},
			want: sessionView{
				id:        domain.SessionID("s2"),
				name:      "9",
				ephemeral: true,
				tabCount:  0,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.build().snapshotView(tt.opts)
			if !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("snapshotView() = %+v, want %+v", got, tt.want)
			}
		})
	}
}

func TestSnapshotViewFocusedTitlesOption(t *testing.T) {
	s := &session{sessionCore: sessionCore{id: domain.SessionID("s1"),
		name: "alpha"}, tabs: []*tab{{stableID: "t1", name: "build"}},
	}
	off := s.snapshotView(viewOptions{tabDetails: true})
	if off.tabs[0].focusedTitle != "" {
		t.Fatalf("focusedTitle without focusedTitles option = %q, want empty", off.tabs[0].focusedTitle)
	}
	// A pane-less tab has no focused pane: the option must be exercised
	// without panicking and still yield an empty title.
	on := s.snapshotView(viewOptions{tabDetails: true, focusedTitles: true, terminalTitle: true})
	if on.tabs[0].focusedTitle != "" {
		t.Fatalf("focusedTitle for pane-less tab = %q, want empty", on.tabs[0].focusedTitle)
	}
}

func TestSnapshotViewAggregatesWithoutTabDetails(t *testing.T) {
	s := &session{tabs: []*tab{
		{stableID: "t1", name: "build"},
		{stableID: "t2", name: "logs", attention: true},
	}}
	got := s.snapshotView(viewOptions{})
	if got.tabCount != 2 || !got.hasAttention {
		t.Fatalf("aggregates = (tabCount %d, hasAttention %t), want (2, true)", got.tabCount, got.hasAttention)
	}
	if got.tabs != nil {
		t.Fatalf("tabs without tabDetails = %#v, want nil", got.tabs)
	}

	empty := (&session{}).snapshotView(viewOptions{tabDetails: true})
	if empty.tabCount != 0 || empty.tabs == nil || len(empty.tabs) != 0 {
		t.Fatalf("empty detailed snapshot = (tabCount %d, tabs %#v), want (0, non-nil empty)", empty.tabCount, empty.tabs)
	}
}
