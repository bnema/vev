package snapshot

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"math"
	"testing"

	"github.com/bnema/vev/internal/usecase/layout"
)

func TestManifestWeightRoundTripAndOldCompatibility(t *testing.T) {
	weighted := testManifest(&layout.Tree{Root: &layout.Node{
		Kind: layout.Split,
		Dir:  layout.Horizontal,
		Children: []*layout.Node{
			{Kind: layout.Leaf, Leaf: "one", Weight: 2},
			{Kind: layout.Leaf, Leaf: "two", Weight: 1},
		},
	}}, nil)

	encoded, err := MarshalManifest(weighted)
	if err != nil {
		t.Fatal(err)
	}
	weightedBody, err := unmarshalManifestEnvelope(encoded, manifestMagic)
	if err != nil {
		t.Fatal(err)
	}
	extensionSize := len(manifestWeightTag) + 4 + 3*manifestWeightSize
	if len(weightedBody) < extensionSize {
		t.Fatalf("weighted body length = %d, want at least %d", len(weightedBody), extensionSize)
	}
	unweightedBody := weightedBody[:len(weightedBody)-extensionSize]
	extension := weightedBody[len(unweightedBody):]
	if string(extension[:len(manifestWeightTag)]) != manifestWeightTag || binary.BigEndian.Uint32(extension[len(manifestWeightTag):]) != 3 {
		t.Fatalf("weight extension header = %x", extension[:len(manifestWeightTag)+4])
	}
	for i, want := range []float64{0, 2, 1} {
		offset := len(manifestWeightTag) + 4 + i*manifestWeightSize
		if got := math.Float64frombits(binary.BigEndian.Uint64(extension[offset:])); got != want {
			t.Fatalf("preorder weight %d = %v, want %v", i, got, want)
		}
	}

	got, err := UnmarshalManifest(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if got.Tabs[0].Tree.Root.Weight != 0 || got.Tabs[0].Tree.Root.Children[0].Weight != 2 || got.Tabs[0].Tree.Root.Children[1].Weight != 1 {
		t.Fatalf("decoded weights = root %v, children %v/%v", got.Tabs[0].Tree.Root.Weight, got.Tabs[0].Tree.Root.Children[0].Weight, got.Tabs[0].Tree.Root.Children[1].Weight)
	}

	unweighted := testManifest(&layout.Tree{Root: &layout.Node{
		Kind: layout.Split,
		Dir:  layout.Horizontal,
		Children: []*layout.Node{
			layout.NewLeaf("one"),
			layout.NewLeaf("two"),
		},
	}}, nil)
	oldEncoded, err := MarshalManifest(unweighted)
	if err != nil {
		t.Fatal(err)
	}
	oldBody, err := unmarshalManifestEnvelope(oldEncoded, manifestMagic)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(oldBody, unweightedBody) {
		t.Fatalf("weighted manifest changed the legacy body before its extension")
	}
	oldDecoded, err := UnmarshalManifest(oldEncoded)
	if err != nil {
		t.Fatal(err)
	}
	for _, node := range manifestNodes(oldDecoded) {
		if node.Weight != 0 {
			t.Fatalf("old manifest decoded weight = %v, want zero", node.Weight)
		}
	}
}

func TestSnapshotEnvelopeCompatibility(t *testing.T) {
	const (
		headGolden     = "56455648000100000000002898ebcac200000000000000070100000000000000000000000000000000000000000000000000000000000000"
		objectGolden   = "5645564f00010000000000111cb8f09f030000000c63616e6f6e6963616c2d7674"
		manifestGolden = "5645564d0004000000000133d118cc030000000000000001010000000000000000000000000000000000056e616d6564000000000000000000000001000374616200500018000000000000000000036f6e65010000020000036f6e6500000374776f000200036f6e6500036f6e6500042f6f6e65000101010000000000000000000000000000000000000000000000000000000000000000000016020200000000000000000000000000000000000000000000000000000000000000000000160303000000000000000000000000000000000000000000000000000000000000000000001600000374776f000374776f00042f74776f0000020400000000000000000000000000000000000000000000000000000000000000000000160305000000000000000000000000000000000000000000000000000000000000000000001600"
	)
	if ManifestVersion != 4 {
		t.Fatalf("ManifestVersion = %d, want 4", ManifestVersion)
	}

	head, err := MarshalHead(Head{Generation: 7, ManifestDigest: SnapshotDigest{1}})
	if err != nil {
		t.Fatal(err)
	}
	object, err := MarshalObject(RecoveryTranscript, []byte("canonical-vt"))
	if err != nil {
		t.Fatal(err)
	}
	manifest, err := MarshalManifest(unweightedTestManifest())
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name   string
		got    []byte
		golden string
	}{
		{name: "HEAD", got: head, golden: headGolden},
		{name: "object", got: object.Data, golden: objectGolden},
		{name: "unweighted manifest", got: manifest, golden: manifestGolden},
	} {
		t.Run(tc.name, func(t *testing.T) {
			want, err := hex.DecodeString(tc.golden)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(tc.got, want) {
				t.Fatalf("bytes changed:\n got %x\nwant %x", tc.got, want)
			}
		})
	}
}

func TestManifestWeightExtensionMalformedCorpus(t *testing.T) {
	oldEncoded, err := MarshalManifest(unweightedTestManifest())
	if err != nil {
		t.Fatal(err)
	}
	oldBody, err := unmarshalManifestEnvelope(oldEncoded, manifestMagic)
	if err != nil {
		t.Fatal(err)
	}

	weighted := unweightedTestManifest()
	weighted.Tabs[0].Tree.Root.Children[0].Weight = 2
	weightedEncoded, err := MarshalManifest(weighted)
	if err != nil {
		t.Fatal(err)
	}
	weightedBody, err := unmarshalManifestEnvelope(weightedEncoded, manifestMagic)
	if err != nil {
		t.Fatal(err)
	}
	extensionOffset := len(oldBody)
	if len(weightedBody) <= extensionOffset || string(weightedBody[extensionOffset:extensionOffset+len(manifestWeightTag)]) != manifestWeightTag {
		t.Fatalf("weighted body has no %q extension", manifestWeightTag)
	}

	mutate := func(fn func([]byte)) []byte {
		body := append([]byte(nil), weightedBody...)
		fn(body)
		return body
	}
	firstWeight := extensionOffset + len(manifestWeightTag) + 4
	corpus := []struct {
		name  string
		body  []byte
		valid bool
	}{
		{name: "legacy without extension", body: oldBody, valid: true},
		{name: "valid extension", body: weightedBody, valid: true},
		{name: "malformed tag", body: mutate(func(body []byte) { body[extensionOffset] ^= 0xff })},
		{name: "node count mismatch", body: mutate(func(body []byte) { binary.BigEndian.PutUint32(body[extensionOffset+len(manifestWeightTag):], 2) })},
		{name: "truncated weight", body: weightedBody[:len(weightedBody)-1]},
		{name: "trailing garbage", body: append(append([]byte(nil), weightedBody...), 0)},
		{name: "negative weight", body: mutate(func(body []byte) { binary.BigEndian.PutUint64(body[firstWeight:], math.Float64bits(-1)) })},
		{name: "NaN weight", body: mutate(func(body []byte) { binary.BigEndian.PutUint64(body[firstWeight:], math.Float64bits(math.NaN())) })},
		{name: "infinite weight", body: mutate(func(body []byte) { binary.BigEndian.PutUint64(body[firstWeight:], math.Float64bits(math.Inf(1))) })},
	}

	for _, tc := range corpus {
		t.Run(tc.name, func(t *testing.T) {
			preflightErr := preflightManifest(tc.body)
			_, decodeErr := decodeManifest(tc.body)
			if (preflightErr == nil) != tc.valid {
				t.Fatalf("preflight error = %v, valid=%v", preflightErr, tc.valid)
			}
			if (decodeErr == nil) != tc.valid {
				t.Fatalf("decode error = %v, valid=%v", decodeErr, tc.valid)
			}
			if (preflightErr == nil) != (decodeErr == nil) {
				t.Fatalf("preflight/decode disagreement: %v / %v", preflightErr, decodeErr)
			}
		})
	}
}

func TestMarshalManifestRejectsInvalidWeights(t *testing.T) {
	for _, weight := range []float64{-1, math.NaN(), math.Inf(1), math.Inf(-1)} {
		manifest := unweightedTestManifest()
		manifest.Tabs[0].Tree.Root.Weight = weight
		if _, err := MarshalManifest(manifest); err == nil {
			t.Fatalf("MarshalManifest() accepted weight %v", weight)
		}
	}
}

func unweightedTestManifest() Manifest {
	return testManifest(&layout.Tree{Root: &layout.Node{
		Kind: layout.Split,
		Dir:  layout.Horizontal,
		Children: []*layout.Node{
			layout.NewLeaf("one"),
			layout.NewLeaf("two"),
		},
	}}, nil)
}
