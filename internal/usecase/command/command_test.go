package command

import (
	"errors"
	"testing"
)

func TestParsePositiveDecimal(t *testing.T) {
	for _, tt := range []struct {
		name    string
		args    []string
		want    int
		invalid bool
	}{
		{"one", []string{"1"}, 1, false},
		{"zero", []string{"0"}, 0, true},
		{"negative", []string{"-1"}, 0, true},
		{"extra", []string{"1", "2"}, 0, true},
		{"space", []string{" 1"}, 0, true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParsePositiveDecimal(tt.args)
			if tt.invalid {
				if !errors.Is(err, ErrInvalidArguments) {
					t.Fatalf("error = %v", err)
				}
				return
			}
			if err != nil || got != tt.want {
				t.Fatalf("ParsePositiveDecimal() = %d, %v", got, err)
			}
		})
	}
}
