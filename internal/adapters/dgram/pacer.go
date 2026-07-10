package dgram

import (
	"errors"
	"math"
	"math/bits"
	"time"

	"github.com/bnema/vev/internal/ports"
)

var errPacerClosed = errors.New("dgram: pacer closed")

type paceLimits func() (bytesPerSecond int, burstBytes int)

type bytePacer struct {
	clk        ports.Clock
	tokens     int64
	last       time.Time
	afterTimer func(time.Time)
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
				capacity := int64(burst) - p.tokens
				p.tokens += mulDivFloorSaturating(int64(elapsed), int64(rate), int64(time.Second), capacity)
				p.last = now
			}
			p.tokens = min(p.tokens, int64(burst))
		}
		if p.tokens >= int64(n) {
			p.tokens -= int64(n)
			return nil
		}

		deficit := int64(n) - p.tokens
		wait := time.Duration(mulDivCeilSaturating(deficit, int64(time.Second), int64(rate)))
		timer := p.clk.NewTimer(wait)
		if p.afterTimer != nil {
			p.afterTimer(now.Add(wait))
		}
		select {
		case <-timer.C():
			timer.Stop()
		case <-done:
			timer.Stop()
			return errPacerClosed
		}
	}
}

func mulDivFloorSaturating(a, b, divisor, limit int64) int64 {
	if a <= 0 || b <= 0 || limit <= 0 {
		return 0
	}
	hi, lo := bits.Mul64(uint64(a), uint64(b))
	if hi >= uint64(divisor) {
		return limit
	}
	quotient, _ := bits.Div64(hi, lo, uint64(divisor))
	return min(int64(min(quotient, uint64(math.MaxInt64))), limit)
}

func mulDivCeilSaturating(a, b, divisor int64) int64 {
	hi, lo := bits.Mul64(uint64(a), uint64(b))
	if hi >= uint64(divisor) {
		return math.MaxInt64
	}
	quotient, remainder := bits.Div64(hi, lo, uint64(divisor))
	if remainder != 0 {
		quotient++
	}
	return int64(min(quotient, uint64(math.MaxInt64)))
}
