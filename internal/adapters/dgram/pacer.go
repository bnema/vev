package dgram

import (
	"errors"
	"time"

	"github.com/bnema/vev/internal/ports"
)

var errPacerClosed = errors.New("dgram: pacer closed")

type paceLimits func() (bytesPerSecond int, burstBytes int)

type bytePacer struct {
	clk    ports.Clock
	tokens int64
	last   time.Time
}

func (p *bytePacer) wait(done <-chan struct{}, n int, limits paceLimits) error {
	if n <= 0 {
		return nil
	}
	for {
		now := p.clk.Now()
		rate, burst := limits()
		if rate < 1 {
			rate = 1
		}
		if burst < n {
			burst = n
		}
		if p.last.IsZero() {
			p.last = now
			p.tokens = int64(burst)
		} else {
			elapsed := now.Sub(p.last)
			if elapsed > 0 {
				p.tokens += int64(elapsed) * int64(rate) / int64(time.Second)
				p.last = now
			}
			p.tokens = min(p.tokens, int64(burst))
		}
		if p.tokens >= int64(n) {
			p.tokens -= int64(n)
			return nil
		}

		deficit := int64(n) - p.tokens
		wait := time.Duration((deficit*int64(time.Second) + int64(rate) - 1) / int64(rate))
		timer := p.clk.NewTimer(wait)
		select {
		case <-timer.C():
			timer.Stop()
		case <-done:
			timer.Stop()
			return errPacerClosed
		}
	}
}
