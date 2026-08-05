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
