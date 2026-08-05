package domain

import "testing"

func TestOrdinalTabSelectorPreservesUnnamedTabs(t *testing.T) {
	selector := NewOrdinalTabSelector(1, "", 3)
	tabs := []TabSelectorTab{{Name: "first"}, {Name: ""}, {Name: "trailing"}}
	index, ok := selector.Resolve(tabs)
	if !ok || index != 1 {
		t.Fatalf("Resolve() = %d, %t, want 1, true", index, ok)
	}
}

func TestValidateTabStableIDRejectsUnsafeIdentity(t *testing.T) {
	for _, id := range []TabStableID{"", "tab id", "tab\n1"} {
		if err := ValidateTabStableID(id); err == nil {
			t.Fatalf("ValidateTabStableID(%q) = nil, want error", id)
		}
	}
}
