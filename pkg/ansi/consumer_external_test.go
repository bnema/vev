package ansi_test

import (
	"bytes"
	"testing"

	"github.com/bnema/vev/pkg/ansi"
	"github.com/bnema/vev/pkg/vtcore"
)

func TestExternalANSIConsumesCoreFrameIndependently(t *testing.T) {
	frame := vtcore.NewFrame(2, 1)
	frame.Set(0, 0, vtcore.Cell{Rune: 'O', Style: vtcore.DefaultStyle()})
	frame.Set(1, 0, vtcore.Cell{Rune: 'K', Style: vtcore.DefaultStyle()})

	output, err := ansi.New(ansi.Capabilities{}).Draw(frame, []vtcore.Damage{vtcore.FullRedraw()})
	if err != nil {
		t.Fatalf("Draw: %v", err)
	}
	if !bytes.Contains(output, []byte("OK")) {
		t.Fatalf("ANSI output = %q, want visible core frame content", output)
	}
}
