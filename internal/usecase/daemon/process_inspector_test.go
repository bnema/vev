package daemon

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestProcessInspectorOptionsComposition(t *testing.T) {
	cwd := func(int) (string, error) { return "/override", nil }
	base := &processInspectorFake{
		cwd:  func(int) (string, error) { return "/base", nil },
		comm: func(int) (string, error) { return "vim", nil },
	}

	tests := []struct {
		name        string
		opts        []Option
		wantDetails bool
		wantCwd     string
		wantComm    string
		wantCommErr error
	}{
		{name: "none", wantDetails: false},
		{
			name:        "cwd reader only",
			opts:        []Option{WithCwdReader(cwd)},
			wantDetails: false,
			wantCwd:     "/override",
			wantCommErr: errProcessInspectionUnavailable,
		},
		{
			name:        "inspector only",
			opts:        []Option{WithProcessInspector(base)},
			wantDetails: true,
			wantCwd:     "/base",
			wantComm:    "vim",
		},
		{
			name:        "inspector then cwd reader keeps base details",
			opts:        []Option{WithProcessInspector(base), WithCwdReader(cwd)},
			wantDetails: true,
			wantCwd:     "/override",
			wantComm:    "vim",
		},
		{
			name:        "cwd reader then inspector replaces override",
			opts:        []Option{WithCwdReader(cwd), WithProcessInspector(base)},
			wantDetails: true,
			wantCwd:     "/base",
			wantComm:    "vim",
		},
		{
			name:        "repeated cwd readers do not stack",
			opts:        []Option{WithProcessInspector(base), WithCwdReader(cwd), WithCwdReader(cwd)},
			wantDetails: true,
			wantCwd:     "/override",
			wantComm:    "vim",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := &Daemon{}
			for _, opt := range tt.opts {
				opt(d)
			}
			require.Equal(t, tt.wantDetails, d.hasProcessDetails())
			if d.proc == nil {
				return
			}
			got, err := d.proc.Cwd(1)
			require.NoError(t, err)
			require.Equal(t, tt.wantCwd, got)
			comm, err := d.proc.Comm(1)
			require.ErrorIs(t, err, tt.wantCommErr)
			require.Equal(t, tt.wantComm, comm)
			if o, ok := d.proc.(cwdOverrideInspector); ok {
				_, nested := o.base.(cwdOverrideInspector)
				require.False(t, nested)
			}
		})
	}
}
