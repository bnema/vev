package snapshot

import (
	"errors"
	"testing"
)

func TestHeadCodecIsDeterministicAndStrict(t *testing.T) {
	head := Head{Generation: 7, ManifestDigest: SnapshotDigest{1}}
	encoded, err := MarshalHead(head)
	if err != nil {
		t.Fatal(err)
	}
	encodedAgain, err := MarshalHead(head)
	if err != nil {
		t.Fatal(err)
	}
	if string(encoded) != string(encodedAgain) {
		t.Fatal("head encoding is not deterministic")
	}
	got, err := UnmarshalHead(encoded)
	if err != nil || got != head {
		t.Fatalf("head round trip = %#v, %v", got, err)
	}
	for n := range len(encoded) {
		if _, err := UnmarshalHead(encoded[:n]); err == nil {
			t.Fatalf("prefix %d accepted", n)
		}
	}
	if _, err := UnmarshalHead(append(encoded, 0)); !errors.Is(err, ErrTrailingBytes) {
		t.Fatalf("trailing data error = %v", err)
	}
}

func TestHeadCodecRejectsInvalidDeclarations(t *testing.T) {
	for _, head := range []Head{
		{ManifestDigest: SnapshotDigest{1}},
		{Generation: 1},
	} {
		if _, err := MarshalHead(head); !errors.Is(err, ErrInvalidData) {
			t.Fatalf("MarshalHead(%#v) error = %v, want invalid data", head, err)
		}
	}
}
