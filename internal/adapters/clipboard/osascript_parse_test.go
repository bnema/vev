package clipboard

import (
	"bytes"
	"testing"
)

func TestParseOSAData(t *testing.T) {
	tests := []struct {
		name    string
		output  string
		class   string
		want    []byte
		wantErr bool
	}{
		{
			name:   "png data",
			output: "«data PNGf89504E470D0A1A0A»\n",
			class:  "PNGf",
			want:   []byte{0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a},
		},
		{
			name:   "jpeg data",
			output: "  «data JPEGffd8ffe000104a464946»  ",
			class:  "JPEG",
			want:   []byte{0xff, 0xd8, 0xff, 0xe0, 0x00, 0x10, 0x4a, 0x46, 0x49, 0x46},
		},
		{
			name:    "missing data wrapper",
			output:  "PNGf89504E47",
			class:   "PNGf",
			wantErr: true,
		},
		{
			name:    "trailing output",
			output:  "«data PNGf89504E47» unexpected",
			class:   "PNGf",
			wantErr: true,
		},
		{
			name:    "wrong data class",
			output:  "«data JPEGffd8»",
			class:   "PNGf",
			wantErr: true,
		},
		{
			name:    "missing hex data",
			output:  "«data PNGf»",
			class:   "PNGf",
			wantErr: true,
		},
		{
			name:    "odd length hex data",
			output:  "«data PNGf895»",
			class:   "PNGf",
			wantErr: true,
		},
		{
			name:    "invalid hex data",
			output:  "«data PNGf89504ZZ»",
			class:   "PNGf",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseOSAData(tt.output, tt.class)
			if tt.wantErr {
				if err == nil {
					t.Fatal("parseOSAData() error = nil, want error")
				}
				return
			}
			if err != nil {
				t.Fatalf("parseOSAData() error = %v", err)
			}
			if !bytes.Equal(got, tt.want) {
				t.Fatalf("parseOSAData() = %x, want %x", got, tt.want)
			}
		})
	}
}
