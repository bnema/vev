package domain

import "testing"

func TestAnchor(t *testing.T) {
	tests := []struct {
		name   string
		anchor Anchor
		valid  bool
		text   string
	}{
		{"center", AnchorCenter, true, "center"},
		{"top left", AnchorTopLeft, true, "top-left"},
		{"top", AnchorTop, true, "top"},
		{"top right", AnchorTopRight, true, "top-right"},
		{"left", AnchorLeft, true, "left"},
		{"right", AnchorRight, true, "right"},
		{"bottom left", AnchorBottomLeft, true, "bottom-left"},
		{"bottom", AnchorBottom, true, "bottom"},
		{"bottom right", AnchorBottomRight, true, "bottom-right"},
		{"invalid", Anchor(255), false, "unknown"},
	}

	if AnchorCenter != 0 {
		t.Errorf("AnchorCenter = %d, want 0", AnchorCenter)
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.anchor.Valid(); got != tt.valid {
				t.Errorf("Anchor(%d).Valid() = %v, want %v", tt.anchor, got, tt.valid)
			}
			if got := tt.anchor.String(); got != tt.text {
				t.Errorf("Anchor(%d).String() = %q, want %q", tt.anchor, got, tt.text)
			}
		})
	}
}

func TestParseAnchor(t *testing.T) {
	tests := []struct {
		input string
		want  Anchor
		ok    bool
	}{
		{"center", AnchorCenter, true},
		{" TOP-LEFT ", AnchorTopLeft, true},
		{"Top", AnchorTop, true},
		{"top-right", AnchorTopRight, true},
		{"LEFT", AnchorLeft, true},
		{"right", AnchorRight, true},
		{"bottom-left", AnchorBottomLeft, true},
		{"Bottom", AnchorBottom, true},
		{"bottom-right", AnchorBottomRight, true},
		{"auto", AnchorCenter, false},
		{"", AnchorCenter, false},
		{"diagonal", AnchorCenter, false},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got, ok := ParseAnchor(tt.input)
			if got != tt.want || ok != tt.ok {
				t.Errorf("ParseAnchor(%q) = (%q, %v), want (%q, %v)", tt.input, got, ok, tt.want, tt.ok)
			}
		})
	}
}

func TestSizeValid(t *testing.T) {
	tests := []struct {
		name string
		sz   Size
		want bool
	}{
		{"positive", Size{Cols: 80, Rows: 24}, true},
		{"zero cols", Size{Cols: 0, Rows: 24}, false},
		{"zero rows", Size{Cols: 80, Rows: 0}, false},
		{"zero both", Size{Cols: 0, Rows: 0}, false},
		{"negative cols", Size{Cols: -1, Rows: 24}, false},
		{"negative rows", Size{Cols: 80, Rows: -1}, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.sz.Valid(); got != tt.want {
				t.Errorf("Size%+v.Valid() = %v, want %v", tt.sz, got, tt.want)
			}
		})
	}
}
