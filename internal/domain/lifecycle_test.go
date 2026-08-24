package domain

import "testing"

func TestOrdinalTabSelectorPreservesUnnamedTabs(t *testing.T) {
	tests := []struct {
		name     string
		selector TabSelector
		tabs     []TabSelectorTab
		want     int
	}{
		{name: "unnamed middle tab", selector: NewOrdinalTabSelector(1, "", 3), tabs: []TabSelectorTab{{Name: "first"}, {Name: ""}, {Name: "trailing"}}, want: 1},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			index, ok := test.selector.Resolve(test.tabs)
			if !ok || index != test.want {
				t.Fatalf("Resolve() = %d, %t, want %d, true", index, ok, test.want)
			}
		})
	}
}

func TestRemoteSessionTargetResolveTab(t *testing.T) {
	base := RemoteSessionTarget{Endpoint: "host", DisplayOrigin: "host", LifecycleID: SessionLifecycleID{1}, SessionName: "work"}
	tests := []struct {
		name   string
		target RemoteSessionTarget
		tabs   []TabSelectorTab
		want   int
		ok     bool
	}{
		{name: "down without metadata", target: RemoteSessionTarget{Endpoint: base.Endpoint, DisplayOrigin: base.DisplayOrigin, LifecycleID: base.LifecycleID, SessionName: base.SessionName, Stopped: true}, ok: true},
		{name: "down fresh default tab", target: RemoteSessionTarget{Endpoint: base.Endpoint, DisplayOrigin: base.DisplayOrigin, LifecycleID: base.LifecycleID, SessionName: base.SessionName, Stopped: true}, tabs: []TabSelectorTab{{ID: "fresh"}}, ok: true},
		{name: "down without selector rejects ambiguity", target: RemoteSessionTarget{Endpoint: base.Endpoint, DisplayOrigin: base.DisplayOrigin, LifecycleID: base.LifecycleID, SessionName: base.SessionName, Stopped: true}, tabs: []TabSelectorTab{{ID: "one"}, {ID: "two"}}},
		{name: "down stable selector", target: RemoteSessionTarget{Endpoint: base.Endpoint, DisplayOrigin: base.DisplayOrigin, LifecycleID: base.LifecycleID, SessionName: base.SessionName, Stopped: true, StoppedTab: NewStableTabSelector("two")}, tabs: []TabSelectorTab{{ID: "one"}, {ID: "two"}}, want: 1, ok: true},
		{name: "down invalid selector", target: RemoteSessionTarget{Endpoint: base.Endpoint, DisplayOrigin: base.DisplayOrigin, LifecycleID: base.LifecycleID, SessionName: base.SessionName, Stopped: true, StoppedTab: TabSelector{Kind: TabSelectorByStableID, Ordinal: 1}}, tabs: []TabSelectorTab{{ID: "one"}}},
		{name: "live stable tab", target: RemoteSessionTarget{Endpoint: base.Endpoint, DisplayOrigin: base.DisplayOrigin, LifecycleID: base.LifecycleID, SessionName: base.SessionName, LiveTabID: "two"}, tabs: []TabSelectorTab{{ID: "one"}, {ID: "two"}}, want: 1, ok: true},
		{name: "live missing tab", target: RemoteSessionTarget{Endpoint: base.Endpoint, DisplayOrigin: base.DisplayOrigin, LifecycleID: base.LifecycleID, SessionName: base.SessionName, LiveTabID: "missing"}, tabs: []TabSelectorTab{{ID: "one"}, {ID: "two"}}, want: -1},
		{name: "live duplicate tab rejected", target: RemoteSessionTarget{Endpoint: base.Endpoint, DisplayOrigin: base.DisplayOrigin, LifecycleID: base.LifecycleID, SessionName: base.SessionName, LiveTabID: "two"}, tabs: []TabSelectorTab{{ID: "two"}, {ID: "two"}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, ok := test.target.ResolveTab(test.tabs)
			if got != test.want || ok != test.ok {
				t.Fatalf("ResolveTab() = %d, %t, want %d, %t", got, ok, test.want, test.ok)
			}
		})
	}
}

func TestValidateTabStableIDRejectsUnsafeIdentity(t *testing.T) {
	tests := []struct {
		name string
		id   TabStableID
	}{
		{name: "empty", id: ""},
		{name: "space", id: "tab id"},
		{name: "newline", id: "tab\n1"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if err := ValidateTabStableID(test.id); err == nil {
				t.Fatalf("ValidateTabStableID(%q) = nil, want error", test.id)
			}
		})
	}
}
