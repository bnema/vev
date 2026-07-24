package daemon

import (
	"testing"

	"github.com/bnema/vev/internal/domain"
)

func TestFullViewportSize(t *testing.T) {
	tests := []struct {
		name string
		sess *session
		want domain.Size
	}{
		{
			name: "derives from active tab content size",
			sess: &session{tabs: []*tab{{size: domain.Size{Cols: 120, Rows: 38}}}, active: 0},
			want: domain.Size{Cols: 120, Rows: 40},
		},
		{
			name: "round-trips through tabSize",
			sess: &session{tabs: []*tab{{size: tabSize(domain.Size{Cols: 80, Rows: 24})}}, active: 0},
			want: domain.Size{Cols: 80, Rows: 24},
		},
		{
			name: "falls back to defaultSize with no tabs",
			sess: &session{},
			want: defaultSize,
		},
		{
			name: "falls back to defaultSize with invalid tab size",
			sess: &session{tabs: []*tab{{}}, active: 0},
			want: defaultSize,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.sess.fullViewportSize(); got != tt.want {
				t.Fatalf("fullViewportSize() = %+v, want %+v", got, tt.want)
			}
		})
	}
}
