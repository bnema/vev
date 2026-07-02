package renderer

import (
	"context"
	"time"
)

const DefaultFrameInterval = 10 * time.Millisecond

type Scheduler struct {
	interval time.Duration
	requests chan struct{}
}

func NewScheduler(interval time.Duration) *Scheduler {
	if interval <= 0 {
		interval = DefaultFrameInterval
	}
	return &Scheduler{interval: interval, requests: make(chan struct{}, 1)}
}

func (s *Scheduler) Request() {
	select {
	case s.requests <- struct{}{}:
	default:
	}
}

func (s *Scheduler) Run(ctx context.Context) <-chan struct{} {
	frames := make(chan struct{}, 1)
	go func() {
		defer close(frames)
		var timer *time.Timer
		var timerC <-chan time.Time
		pending := false
		for {
			select {
			case <-ctx.Done():
				if timer != nil {
					timer.Stop()
				}
				return
			case <-s.requests:
				if pending {
					continue
				}
				pending = true
				timer = time.NewTimer(s.interval)
				timerC = timer.C
			case <-timerC:
				pending = false
				timerC = nil
				select {
				case frames <- struct{}{}:
				case <-ctx.Done():
					return
				}
			}
		}
	}()
	return frames
}
