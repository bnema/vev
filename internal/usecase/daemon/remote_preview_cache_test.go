package daemon

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	renderer "github.com/bnema/vev-vt"
	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol"
)

type remotePreviewTestClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *remotePreviewTestClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *remotePreviewTestClock) NewTimer(time.Duration) ports.Timer { return stubTimer{} }

func (c *remotePreviewTestClock) Advance(delta time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(delta)
	c.mu.Unlock()
}

type remotePreviewTestClient struct {
	mu             sync.Mutex
	calls          int
	result         protocol.RemotePreview
	err            error
	started        chan struct{}
	finished       chan struct{}
	release        chan struct{}
	lastWidth      uint16
	lastHeight     uint16
	lastTarget     domain.RemoteSessionTarget
	ignoreCancel   bool
	startedSignal  bool
	finishedSignal bool
}

func (c *remotePreviewTestClient) Preview(ctx context.Context, target domain.RemoteSessionTarget, width, height uint16) (protocol.RemotePreview, error) {
	c.mu.Lock()
	c.calls++
	c.lastTarget = target
	c.lastWidth, c.lastHeight = width, height
	result, err := c.result, c.err
	started, release, finished := c.started, c.release, c.finished
	if started != nil && !c.startedSignal {
		c.startedSignal = true
		close(started)
	}
	c.mu.Unlock()
	if release != nil {
		if c.ignoreCancel {
			<-release
		} else {
			select {
			case <-release:
			case <-ctx.Done():
				return protocol.RemotePreview{}, ctx.Err()
			}
		}
	}
	if finished != nil {
		c.mu.Lock()
		if !c.finishedSignal {
			c.finishedSignal = true
			close(finished)
		}
		c.mu.Unlock()
	}
	return cloneRemotePreview(result), err
}

func (c *remotePreviewTestClient) Calls() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.calls
}

func (c *remotePreviewTestClient) LastSize() (uint16, uint16) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.lastWidth, c.lastHeight
}

func (c *remotePreviewTestClient) LastTarget() domain.RemoteSessionTarget {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.lastTarget
}

func remotePreviewCacheTarget() domain.RemoteSessionTarget {
	var lifecycle domain.SessionLifecycleID
	lifecycle[0] = 0x42
	return domain.RemoteSessionTarget{
		Endpoint: "arch", DisplayOrigin: "arch", LifecycleID: lifecycle,
		SessionName: "work", LiveTabID: "tab-1",
	}
}

func remotePreviewCacheResult(target domain.RemoteSessionTarget, revision uint64) protocol.RemotePreview {
	return remotePreviewCacheResultRune(target, revision, 'x')
}

func remotePreviewCacheResultRune(target domain.RemoteSessionTarget, revision uint64, value rune) protocol.RemotePreview {
	return protocol.RemotePreview{
		Version: protocol.RemotePreviewSchemaVersion, Status: protocol.RemotePreviewOK,
		LifecycleID: target.LifecycleID, TabID: target.LiveTabID,
		Revision: revision, Width: 1, Height: 1,
		Cells: []renderer.Cell{{Rune: value, Style: renderer.DefaultStyle()}},
	}
}

func TestFetchRemotePreviewSingleFlightAndCopiesCache(t *testing.T) {
	clock := &remotePreviewTestClock{now: time.Unix(100, 0)}
	target := remotePreviewCacheTarget()
	client := &remotePreviewTestClient{
		result:  remotePreviewCacheResult(target, 1),
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
	d := newTestDaemon(t, nil, clock)
	d.remotePreviewClient = client

	firstDone := make(chan struct{})
	var first protocol.RemotePreview
	var firstErr error
	go func() {
		first, firstErr = d.fetchRemotePreview(context.Background(), target, 1, 1)
		close(firstDone)
	}()
	<-client.started

	secondDone := make(chan struct{})
	var second protocol.RemotePreview
	var secondErr error
	go func() {
		second, secondErr = d.fetchRemotePreview(context.Background(), target, 1, 1)
		close(secondDone)
	}()
	close(client.release)
	<-firstDone
	<-secondDone
	require.NoError(t, firstErr)
	require.NoError(t, secondErr)
	require.Equal(t, first, second)
	require.Equal(t, 1, client.Calls(), "same key must use one remote request")

	first.Cells[0].Rune = 'm'
	cached, err := d.fetchRemotePreview(context.Background(), target, 1, 1)
	require.NoError(t, err)
	require.Equal(t, rune('x'), cached.Cells[0].Rune, "callers must not mutate the memory-only cache")
	require.Equal(t, 1, client.Calls())
}

func TestFetchRemotePreviewAdapterTimeoutAppliesCooldown(t *testing.T) {
	clock := &remotePreviewTestClock{now: time.Unix(250, 0)}
	target := remotePreviewCacheTarget()
	client := &remotePreviewTestClient{err: protocol.ErrRemotePreviewTimeout}
	d := newTestDaemon(t, nil, clock)
	d.remotePreviewClient = client

	_, firstErr := d.fetchRemotePreview(context.Background(), target, 1, 1)
	require.ErrorIs(t, firstErr, protocol.ErrRemotePreviewTimeout)
	_, secondErr := d.fetchRemotePreview(context.Background(), target, 1, 1)
	require.ErrorIs(t, secondErr, errRemotePreviewCooldown)
	require.Equal(t, 1, client.Calls())
}

func TestFetchRemotePreviewRejectsMalformedResponseAndAppliesCooldown(t *testing.T) {
	clock := &remotePreviewTestClock{now: time.Unix(200, 0)}
	target := remotePreviewCacheTarget()
	client := &remotePreviewTestClient{err: errors.New("remote unavailable")}
	d := newTestDaemon(t, nil, clock)
	d.remotePreviewClient = client

	_, firstErr := d.fetchRemotePreview(context.Background(), target, 1, 1)
	require.Error(t, firstErr)
	_, secondErr := d.fetchRemotePreview(context.Background(), target, 1, 1)
	require.Error(t, secondErr)
	require.Equal(t, 1, client.Calls(), "a failed target must be cooled down")

	clock.Advance(remotePreviewCooldown)
	_, thirdErr := d.fetchRemotePreview(context.Background(), target, 1, 1)
	require.Error(t, thirdErr)
	require.Equal(t, 2, client.Calls(), "cooldown expiry must permit a retry")
}

func TestFetchRemotePreviewServesStaleWhileRefreshing(t *testing.T) {
	clock := &remotePreviewTestClock{now: time.Unix(300, 0)}
	target := remotePreviewCacheTarget()
	client := &remotePreviewTestClient{result: remotePreviewCacheResult(target, 1)}
	d := newTestDaemon(t, nil, clock)
	d.remotePreviewClient = client

	first, err := d.fetchRemotePreview(context.Background(), target, 1, 1)
	require.NoError(t, err)
	require.Equal(t, uint64(1), first.Revision)

	clock.Advance(remotePreviewCacheTTL + time.Second)
	client.mu.Lock()
	client.result = remotePreviewCacheResult(target, 2)
	client.started = make(chan struct{})
	client.release = make(chan struct{})
	client.startedSignal = false
	client.finishedSignal = false
	client.mu.Unlock()

	stale, err := d.fetchRemotePreview(context.Background(), target, 1, 1)
	require.NoError(t, err)
	require.Equal(t, uint64(1), stale.Revision, "expired content remains available during revalidation")
	<-client.started
	key := remotePreviewKeyFor(target, 1, 1)
	d.remotePreview.mu.Lock()
	flight := d.remotePreview.flights[key]
	d.remotePreview.mu.Unlock()
	require.NotNil(t, flight)
	close(client.release)
	<-flight.done

	fresh, err := d.fetchRemotePreview(context.Background(), target, 1, 1)
	require.NoError(t, err)
	require.Equal(t, uint64(2), fresh.Revision)
	require.Equal(t, 2, client.Calls())
}

type immediateRemotePreviewTimer struct{ ch <-chan time.Time }

func (t immediateRemotePreviewTimer) C() <-chan time.Time      { return t.ch }
func (t immediateRemotePreviewTimer) Reset(time.Duration) bool { return false }
func (t immediateRemotePreviewTimer) Stop() bool               { return true }

type immediateRemotePreviewClock struct{}

func (immediateRemotePreviewClock) Now() time.Time { return time.Unix(500, 0) }
func (immediateRemotePreviewClock) NewTimer(time.Duration) ports.Timer {
	ch := make(chan time.Time, 1)
	ch <- time.Unix(500, 0)
	return immediateRemotePreviewTimer{ch: ch}
}

func TestFetchRemotePreviewRejectsInvalidDimensionsBeforeRemoteIO(t *testing.T) {
	clock := &remotePreviewTestClock{now: time.Unix(400, 0)}
	client := &remotePreviewTestClient{result: remotePreviewCacheResult(remotePreviewCacheTarget(), 1)}
	d := newTestDaemon(t, nil, clock)
	d.remotePreviewClient = client

	_, err := d.fetchRemotePreview(context.Background(), remotePreviewCacheTarget(), protocol.RemotePreviewMaxWidth+1, 1)
	require.ErrorIs(t, err, protocol.ErrInvalidRemotePreviewRequest)
	require.Zero(t, client.Calls())
}
