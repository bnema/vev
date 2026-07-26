package command

import (
	"errors"
	"testing"
)

func TestParsePositiveUint64(t *testing.T) {
	const maxUint64 = "18446744073709551615"
	got, err := ParsePositiveUint64([]string{maxUint64})
	if err != nil || got != ^uint64(0) {
		t.Fatalf("ParsePositiveUint64(max) = %d, %v", got, err)
	}
	if _, err := ParsePositiveUint64([]string{"18446744073709551616"}); !errors.Is(err, ErrInvalidArguments) {
		t.Fatalf("ParsePositiveUint64(overflow) error = %v", err)
	}
}

func TestParsePositiveDecimal(t *testing.T) {
	for _, tt := range []struct {
		name    string
		args    []string
		want    int
		invalid bool
	}{
		{"one", []string{"1"}, 1, false},
		{"multi-digit", []string{"42"}, 42, false},
		{"overflow", []string{"999999999999999999999999999999999999"}, 0, true},
		{"leading zero", []string{"01"}, 0, true},
		{"multiple leading zeros", []string{"0001"}, 0, true},
		{"all zeros", []string{"00"}, 0, true},
		{"leading zero on multi-digit value", []string{"010"}, 0, true},
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
