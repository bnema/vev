package daemon

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"time"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/protocol"
)

const (
	remotePreviewCacheTTL = 5 * time.Second
	remotePreviewCooldown = 1 * time.Second
	remotePreviewCacheMax = 64
	remotePreviewSlots    = 4
)

var errRemotePreviewCooldown = errors.New("remote preview request is cooling down")

type remotePreviewCacheKey struct {
	Endpoint      string
	DisplayOrigin string
	LifecycleID   domain.SessionLifecycleID
	SessionName   string
	LiveTabID     domain.TabStableID
	Stopped       bool
	StoppedTab    domain.TabSelector
	Width         uint16
	Height        uint16
}

type remotePreviewCacheEntry struct {
	Preview protocol.RemotePreview
	Fetched time.Time
	Used    time.Time
}

type remotePreviewFlight struct {
	done    chan struct{}
	preview protocol.RemotePreview
	err     error
}

type remotePreviewState struct {
	mu        sync.Mutex
	cache     map[remotePreviewCacheKey]remotePreviewCacheEntry
	flights   map[remotePreviewCacheKey]*remotePreviewFlight
	cooldowns map[remotePreviewCacheKey]time.Time
	slots     chan struct{}
}

func cloneRemotePreview(preview protocol.RemotePreview) protocol.RemotePreview {
	preview.Cells = slices.Clone(preview.Cells)
	return preview
}

func (s *remotePreviewState) initializeLocked() {
	if s.cache == nil {
		s.cache = make(map[remotePreviewCacheKey]remotePreviewCacheEntry)
	}
	if s.flights == nil {
		s.flights = make(map[remotePreviewCacheKey]*remotePreviewFlight)
	}
	if s.cooldowns == nil {
		s.cooldowns = make(map[remotePreviewCacheKey]time.Time)
	}
	if s.slots == nil {
		s.slots = make(chan struct{}, remotePreviewSlots)
	}
}

func remotePreviewKeyFor(target domain.RemoteSessionTarget, width, height uint16) remotePreviewCacheKey {
	return remotePreviewCacheKey{
		Endpoint: target.Endpoint, DisplayOrigin: target.DisplayOrigin,
		LifecycleID: target.LifecycleID, SessionName: target.SessionName,
		LiveTabID: target.LiveTabID, Stopped: target.Stopped,
		StoppedTab: target.StoppedTab, Width: width, Height: height,
	}
}

func (d *Daemon) fetchRemotePreview(ctx context.Context, target domain.RemoteSessionTarget, width, height uint16) (protocol.RemotePreview, error) {
	if d == nil || d.remotePreviewClient == nil {
		return protocol.RemotePreview{}, errors.New("remote preview client unavailable")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return protocol.RemotePreview{}, err
	}
	request := protocol.RemotePreviewRequest{
		Version: protocol.RemotePreviewSchemaVersion,
		Target:  target,
		Width:   width,
		Height:  height,
	}
	if err := protocol.ValidateRemotePreviewRequest(request); err != nil {
		return protocol.RemotePreview{}, err
	}

	key := remotePreviewKeyFor(target, width, height)
	now := d.clock.Now()
	d.remotePreview.mu.Lock()
	d.remotePreview.initializeLocked()

	if cached, ok := d.remotePreview.cache[key]; ok {
		cached.Used = now
		d.remotePreview.cache[key] = cached
		if now.Sub(cached.Fetched) <= remotePreviewCacheTTL {
			preview := cloneRemotePreview(cached.Preview)
			d.remotePreview.mu.Unlock()
			return preview, nil
		}

		// Expired data remains useful to the picker. Revalidation is detached
		// from the navigation request so cursor movement never waits on remote
		// I/O, and cooldowns prevent a failed host from being hammered.
		if until, cooling := d.remotePreview.cooldowns[key]; cooling && now.Before(until) {
			preview := cloneRemotePreview(cached.Preview)
			d.remotePreview.mu.Unlock()
			return preview, nil
		}
		if flight := d.remotePreview.flights[key]; flight != nil {
			preview := cloneRemotePreview(cached.Preview)
			d.remotePreview.mu.Unlock()
			return preview, nil
		}
		flight := &remotePreviewFlight{done: make(chan struct{})}
		d.remotePreview.flights[key] = flight
		preview := cloneRemotePreview(cached.Preview)
		d.remotePreview.mu.Unlock()
		go d.runRemotePreviewFlight(d.remotePreviewContext(), key, target, width, height, flight)
		return preview, nil
	}

	if until, cooling := d.remotePreview.cooldowns[key]; cooling && now.Before(until) {
		d.remotePreview.mu.Unlock()
		return protocol.RemotePreview{}, errRemotePreviewCooldown
	}
	if flight := d.remotePreview.flights[key]; flight != nil {
		d.remotePreview.mu.Unlock()
		return waitRemotePreviewFlight(ctx, flight)
	}
	flight := &remotePreviewFlight{done: make(chan struct{})}
	d.remotePreview.flights[key] = flight
	d.remotePreview.mu.Unlock()

	d.runRemotePreviewFlight(ctx, key, target, width, height, flight)
	return cloneRemotePreview(flight.preview), flight.err
}

func (d *Daemon) remotePreviewContext() context.Context {
	if d != nil && d.serveCtx != nil {
		return d.serveCtx
	}
	return context.Background()
}

func waitRemotePreviewFlight(ctx context.Context, flight *remotePreviewFlight) (protocol.RemotePreview, error) {
	select {
	case <-flight.done:
		return cloneRemotePreview(flight.preview), flight.err
	case <-ctx.Done():
		return protocol.RemotePreview{}, ctx.Err()
	}
}

// awaitRemotePreviewRefresh lets a picker display expired content immediately
// while still publishing the refreshed result once the same-key flight finishes.
// A completed flight is read from the cache without starting a second remote
// request.
func (d *Daemon) awaitRemotePreviewRefresh(ctx context.Context, target domain.RemoteSessionTarget, width, height uint16) (protocol.RemotePreview, error) {
	key := remotePreviewKeyFor(target, width, height)
	d.remotePreview.mu.Lock()
	flight := d.remotePreview.flights[key]
	d.remotePreview.mu.Unlock()
	if flight != nil {
		return waitRemotePreviewFlight(ctx, flight)
	}
	return d.fetchRemotePreview(ctx, target, width, height)
}

func (d *Daemon) runRemotePreviewFlight(ctx context.Context, key remotePreviewCacheKey, target domain.RemoteSessionTarget, width, height uint16, flight *remotePreviewFlight) {
	select {
	case d.remotePreview.slots <- struct{}{}:
	case <-ctx.Done():
		d.finishRemotePreviewFlight(key, flight, protocol.RemotePreview{}, ctx.Err())
		return
	}
	var preview protocol.RemotePreview
	var err error
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("remote preview flight panicked: %v", recovered)
			preview = protocol.RemotePreview{}
			d.finishRemotePreviewFlight(key, flight, preview, err)
			panic(recovered)
		}
		if err != nil {
			preview = protocol.RemotePreview{}
		}
		d.finishRemotePreviewFlight(key, flight, preview, err)
	}()
	defer func() { <-d.remotePreview.slots }()

	preview, err = d.remotePreviewClient.Preview(ctx, target, width, height)
	if err == nil {
		if validationErr := protocol.ValidateRemotePreview(preview); validationErr != nil {
			err = validationErr
		} else if preview.Status != protocol.RemotePreviewOK {
			err = fmt.Errorf("remote preview returned status %d", preview.Status)
		} else if preview.LifecycleID != target.LifecycleID || preview.TabID != target.LiveTabID {
			err = errors.New("remote preview identity changed")
		}
	}
}

func (d *Daemon) finishRemotePreviewFlight(key remotePreviewCacheKey, flight *remotePreviewFlight, preview protocol.RemotePreview, err error) {
	now := d.clock.Now()
	d.remotePreview.mu.Lock()
	for cooldownKey, until := range d.remotePreview.cooldowns {
		if !now.Before(until) {
			delete(d.remotePreview.cooldowns, cooldownKey)
		}
	}
	flight.preview = cloneRemotePreview(preview)
	flight.err = err
	delete(d.remotePreview.flights, key)
	if err == nil {
		d.remotePreview.cache[key] = remotePreviewCacheEntry{
			Preview: cloneRemotePreview(preview), Fetched: now, Used: now,
		}
		delete(d.remotePreview.cooldowns, key)
		for len(d.remotePreview.cache) > remotePreviewCacheMax {
			var oldestKey remotePreviewCacheKey
			var oldest time.Time
			for candidate, entry := range d.remotePreview.cache {
				used := entry.Used
				if used.IsZero() {
					used = entry.Fetched
				}
				if oldest.IsZero() || used.Before(oldest) {
					oldestKey, oldest = candidate, used
				}
			}
			delete(d.remotePreview.cache, oldestKey)
		}
	} else if !errors.Is(err, context.Canceled) {
		d.remotePreview.cooldowns[key] = now.Add(remotePreviewCooldown)
	}
	close(flight.done)
	d.remotePreview.mu.Unlock()
}
