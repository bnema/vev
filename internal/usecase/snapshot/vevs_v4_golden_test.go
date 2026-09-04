package snapshot

import (
	"bytes"
	"encoding/hex"
	"testing"

	vt "github.com/bnema/vev-vt"
	"github.com/bnema/vev/internal/usecase/layout"
)

const compactVEVSGolden = "564556530005000000000104dbb0765f0007726573746f7265000000000000007b0000000100037461620050001800000000000000020001700000017000010001700006737461626c6500042f746d70000000010000003f5654433100000001000000000000002a0000000200000001000000010000000000000000000000290000000100000000410000000000000000200000000000000000105654433100000000000000000000002a000000645654433100000001000000000000002b000000020000000100000002000000000900000000000000000000ffffffffffffffff010203000000000000000000000000000000000000000000002a00000002000000754c000000010000000000000000010100"

func TestVEVSV4CompactGoldenRestore(t *testing.T) {
	const golden = compactVEVSGolden
	data, err := hex.DecodeString(golden)
	if err != nil {
		t.Fatal(err)
	}

	got, err := Unmarshal(data)
	if err != nil {
		t.Fatalf("Unmarshal golden VEVS: %v", err)
	}
	if got.Name != "restore" || got.CreatedAt != 123 || len(got.Tabs) != 1 {
		t.Fatalf("session header = %#v", got)
	}
	tab := got.Tabs[0]
	if tab.StableID != "tab" || tab.Cols != 80 || tab.Rows != 24 || tab.NextPaneID != 2 || tab.Focus != layout.PaneID("p") || len(tab.Panes) != 1 {
		t.Fatalf("tab header = %#v", tab)
	}
	pane := tab.Panes[0]
	if pane.ID != layout.PaneID("p") || pane.StableID != "stable" || pane.Cwd != "/tmp" || pane.Process != nil || len(pane.SealedChunks) != 1 {
		t.Fatalf("pane header = %#v", pane)
	}

	sealed, err := vt.UnmarshalHistory(pane.SealedChunks[0])
	if err != nil {
		t.Fatalf("sealed history: %v", err)
	}
	if sealed.RowID(0) != 41 || sealed.Row(0)[0].Rune != 'A' {
		t.Fatalf("sealed history = row %d %#v", sealed.RowID(0), sealed.Row(0))
	}
	tail, err := vt.UnmarshalHistory(pane.Tail)
	if err != nil {
		t.Fatalf("history tail: %v", err)
	}
	if tail.Len() != 0 || tail.NextRowID() != 42 {
		t.Fatalf("history tail = len %d next %d", tail.Len(), tail.NextRowID())
	}
	transcript, err := vt.UnmarshalHistory(pane.Transcript)
	if err != nil {
		t.Fatalf("recovery transcript: %v", err)
	}
	if transcript.RowID(0) != 42 || transcript.Row(0)[0].Rune != '界' {
		t.Fatalf("recovery transcript = row %d %#v", transcript.RowID(0), transcript.Row(0))
	}

	screen, err := vt.NewScreenWithRecoveryTranscript(8, 2, vt.HistoryConfig{MaxRows: 8, ChunkRows: 2}, pane.SealedChunks, pane.Tail, pane.Transcript)
	if err != nil {
		t.Fatalf("restore VT history: %v", err)
	}
	view := screen.History().View()
	if view.Len() != 2 || view.RowID(0) != 41 || view.RowID(1) != 42 || view.NextRowID() != 43 {
		t.Fatalf("restored history = len %d IDs %d/%d next %d", view.Len(), view.RowID(0), view.RowID(1), view.NextRowID())
	}
}

func TestVEVSV4GoldenRejectsMalformedInput(t *testing.T) {
	const golden = compactVEVSGolden
	valid, err := hex.DecodeString(golden)
	if err != nil {
		t.Fatal(err)
	}
	mutate := func(fn func([]byte)) []byte {
		out := append([]byte(nil), valid...)
		fn(out)
		return out
	}
	cases := []struct {
		name string
		data []byte
	}{
		{name: "truncated", data: valid[:len(valid)-1]},
		{name: "trailing bytes", data: append(append([]byte(nil), valid...), 0)},
		{name: "bad magic", data: mutate(func(data []byte) { data[0] ^= 0xff })},
		{name: "bad version", data: mutate(func(data []byte) { data[4] = 3 })},
		{name: "bad flags", data: mutate(func(data []byte) { data[7] = 1 })},
		{name: "bad crc", data: mutate(func(data []byte) { data[15] ^= 0xff })},
	}

	body := append([]byte(nil), valid[16:]...)
	if offset := bytes.Index(body, []byte("VTC1")); offset < 0 {
		t.Fatal("golden has no VT blob")
	} else {
		body[offset+3] = '2'
		cases = append(cases, struct {
			name string
			data []byte
		}{name: "bad nested VT version", data: v4Envelope(version, 0, body)})
	}

	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			reject(t, test.data, nil)
		})
	}
	for length := 0; length < len(valid); length++ {
		reject(t, valid[:length], nil)
	}
}
