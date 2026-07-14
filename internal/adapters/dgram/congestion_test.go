package dgram

import (
	"testing"
	"time"
)

func TestCongestionControllerBoundsAndIntegerGrowth(t *testing.T) {
	const mtu = 1200
	c := newCongestionController(mtu)
	if got, want := c.cwndBytes, initialCongestionPackets*mtu; got != want {
		t.Fatalf("initial cwnd=%d, want %d", got, want)
	}
	if got, want := c.burstBytes(), dataBurstPackets*mtu; got != want {
		t.Fatalf("initial burst=%d, want %d", got, want)
	}
	if got, want := c.bytesPerSecond(), initialCongestionPackets*mtu*int(time.Second/initialPacingRTT); got != want {
		t.Fatalf("initial pacing rate=%d, want %d", got, want)
	}

	c.onLoss()
	if got, want := c.cwndBytes, initialCongestionPackets*mtu/2; got != want {
		t.Fatalf("cwnd after first loss=%d, want %d", got, want)
	}
	c.cwndBytes = 3 * mtu
	c.onLoss()
	if got, want := c.cwndBytes, minimumCongestionPackets*mtu; got != want {
		t.Fatalf("minimum cwnd=%d, want %d", got, want)
	}

	c.cwndBytes = 10 * mtu
	c.onACK(1)
	if got, want := c.cwndBytes, 10*mtu+1; got != want {
		t.Fatalf("minimum additive increase cwnd=%d, want %d", got, want)
	}
	c.onACK(10 * mtu)
	if got, want := c.cwndBytes, 10*mtu+1+(mtu*10*mtu)/(10*mtu+1); got != want {
		t.Fatalf("integer additive increase cwnd=%d, want %d", got, want)
	}
	c.cwndBytes = maximumCongestionBytes
	c.onACK(maximumCongestionBytes)
	if got := c.cwndBytes; got != maximumCongestionBytes {
		t.Fatalf("maximum cwnd=%d, want %d", got, maximumCongestionBytes)
	}

	c.onRTT(500 * time.Microsecond)
	if got, want := c.bytesPerSecond(), maximumCongestionBytes*1000; got != want {
		t.Fatalf("sub-millisecond pacing rate=%d, want %d", got, want)
	}
}

func BenchmarkCongestionControllerACK(b *testing.B) {
	c := newCongestionController(defaultMTU)
	b.ReportAllocs()
	for b.Loop() {
		c.onACK(defaultMTU)
		if c.cwndBytes == c.maxBytes {
			c = newCongestionController(defaultMTU)
		}
	}
}
