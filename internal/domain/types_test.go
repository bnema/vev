package domain

import "testing"

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
