package dgram

import "time"

const (
	initialCongestionPackets = 10
	minimumCongestionPackets = 2
	maximumCongestionBytes   = 4 << 20
	initialPacingRTT         = 250 * time.Millisecond
	dataBurstPackets         = 2
)

type congestionController struct {
	mtu       int
	cwndBytes int
	minBytes  int
	maxBytes  int
	srtt      time.Duration
}

func newCongestionController(mtu int) congestionController {
	minimum := minimumCongestionPackets * mtu
	maximum := maximumCongestionBytes
	if maximum < minimum {
		maximum = minimum
	}
	initial := initialCongestionPackets * mtu
	if initial > maximum {
		initial = maximum
	}
	return congestionController{
		mtu:       mtu,
		cwndBytes: initial,
		minBytes:  minimum,
		maxBytes:  maximum,
		srtt:      initialPacingRTT,
	}
}

func (c *congestionController) onRTT(srtt time.Duration) {
	if srtt > 0 {
		c.srtt = srtt
	}
}

func (c *congestionController) onACK(ackedBytes int) {
	if ackedBytes <= 0 || c.cwndBytes >= c.maxBytes {
		return
	}
	increase := int(int64(c.mtu) * int64(ackedBytes) / int64(c.cwndBytes))
	if increase < 1 {
		increase = 1
	}
	c.cwndBytes = min(c.cwndBytes+increase, c.maxBytes)
}

func (c *congestionController) onLoss() {
	c.cwndBytes = max(c.cwndBytes/2, c.minBytes)
}

func (c congestionController) bytesPerSecond() int {
	rtt := max(c.srtt, time.Millisecond)
	return int(int64(c.cwndBytes) * int64(time.Second) / int64(rtt))
}

func (c congestionController) burstBytes() int {
	return min(c.cwndBytes, dataBurstPackets*c.mtu)
}
