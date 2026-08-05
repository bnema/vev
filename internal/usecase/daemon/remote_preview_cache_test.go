package daemon

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/usecase/picker"
	"github.com/bnema/vev/pkg/renderer"
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
	result         ports.RemotePreview
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

func (c *remotePreviewTestClient) Preview(ctx context.Context, target domain.RemoteSessionTarget, width, height uint16) (ports.RemotePreview, error) {
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
				return ports.RemotePreview{}, ctx.Err()
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

func remotePreviewCacheResult(target domain.RemoteSessionTarget, revision uint64) ports.RemotePreview {
	return remotePreviewCacheResultRune(target, revision, 'x')
}

func remotePreviewCacheResultRune(target domain.RemoteSessionTarget, revision uint64, value rune) ports.RemotePreview {
	return ports.RemotePreview{
		Version: ports.RemotePreviewSchemaVersion, Status: ports.RemotePreviewOK,
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
	var first ports.RemotePreview
	var firstErr error
	go func() {
		first, firstErr = d.fetchRemotePreview(context.Background(), target, 1, 1)
		close(firstDone)
	}()
	<-client.started

	secondDone := make(chan struct{})
	var second ports.RemotePreview
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
	client := &remotePreviewTestClient{err: ports.ErrRemotePreviewTimeout}
	d := newTestDaemon(t, nil, clock)
	d.remotePreviewClient = client

	_, firstErr := d.fetchRemotePreview(context.Background(), target, 1, 1)
	require.ErrorIs(t, firstErr, ports.ErrRemotePreviewTimeout)
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

func TestRemotePickerPreviewLateResponseCannotPublishAfterCloseAndReopen(t *testing.T) {
	d := newTestDaemon(t, nil, immediateRemotePreviewClock{})
	firstTarget := remotePreviewCacheTarget()
	firstStarted := make(chan struct{})
	firstRelease := make(chan struct{})
	client := &remotePreviewTestClient{
		result:       remotePreviewCacheResult(firstTarget, 1),
		started:      firstStarted,
		release:      firstRelease,
		ignoreCancel: true,
	}
	d.remotePreviewClient = client
	_, ac, _ := addRemoteRefreshPickerOwner(t, d, "owner")

	firstKey := domain.RemoteSessionKey{Host: firstTarget.Endpoint, Name: firstTarget.SessionName, LifecycleID: firstTarget.LifecycleID, DisplayOrigin: firstTarget.DisplayOrigin}
	firstView := picker.SessionView{
		ID: firstKey.ID(), Name: firstTarget.SessionName,
		Tabs:      []picker.TabEntry{{TabID: firstTarget.LiveTabID, Name: "main"}},
		RemoteKey: &firstKey, RemoteTarget: &firstTarget,
		RemoteAvailability: picker.RemoteFresh, RemoteAttachReady: true,
	}
	firstModel := picker.New([]picker.SessionView{firstView}, picker.SelectionConfig{Mode: picker.SelectNavigationTab})
	firstSelected, ok := firstModel.Selected()
	require.True(t, ok)
	ac.overlays.pickerMu.Lock()
	ac.overlays.picker = firstModel
	ac.overlays.pickerIntent = pickerNavigate
	ac.overlays.pickerPreviewGeneration = 1
	ac.overlays.pickerMu.Unlock()
	d.startRemotePickerPreview(ac, firstSelected, 1)
	<-firstStarted

	d.closePicker(ac)

	secondTarget := firstTarget
	secondTarget.LifecycleID[0]++
	secondTarget.LiveTabID = "tab-2"
	client.mu.Lock()
	client.result = remotePreviewCacheResultRune(secondTarget, 2, 'y')
	client.started = make(chan struct{})
	client.release = make(chan struct{})
	client.startedSignal = false
	client.finishedSignal = false
	client.mu.Unlock()
	secondKey := domain.RemoteSessionKey{Host: secondTarget.Endpoint, Name: secondTarget.SessionName, LifecycleID: secondTarget.LifecycleID, DisplayOrigin: secondTarget.DisplayOrigin}
	secondView := picker.SessionView{
		ID: secondKey.ID(), Name: secondTarget.SessionName,
		Tabs:      []picker.TabEntry{{TabID: secondTarget.LiveTabID, Name: "replacement"}},
		RemoteKey: &secondKey, RemoteTarget: &secondTarget,
		RemoteAvailability: picker.RemoteFresh, RemoteAttachReady: true,
	}
	secondModel := picker.New([]picker.SessionView{secondView}, picker.SelectionConfig{Mode: picker.SelectNavigationTab})
	secondSelected, ok := secondModel.Selected()
	require.True(t, ok)
	ac.overlays.pickerMu.Lock()
	ac.overlays.picker = secondModel
	ac.overlays.pickerIntent = pickerNavigate
	ac.overlays.pickerPreviewGeneration++
	secondGeneration := ac.overlays.pickerPreviewGeneration
	ac.overlays.pickerMu.Unlock()
	d.startRemotePickerPreview(ac, secondSelected, secondGeneration)
	<-client.started

	close(firstRelease)
	close(client.release)
	width, height := remotePickerPreviewSize(ac.sizeSnapshot())
	for _, key := range []remotePreviewCacheKey{
		remotePreviewKeyFor(firstTarget, width, height),
		remotePreviewKeyFor(secondTarget, width, height),
	} {
		d.remotePreview.mu.Lock()
		flight := d.remotePreview.flights[key]
		d.remotePreview.mu.Unlock()
		if flight != nil {
			<-flight.done
		}
	}
	var preview picker.Preview
	require.Eventually(t, func() bool {
		ac.overlays.pickerMu.Lock()
		preview = ac.overlays.pickerRemotePreview
		currentGeneration := ac.overlays.pickerPreviewGeneration
		ac.overlays.pickerMu.Unlock()
		return currentGeneration == secondGeneration && len(preview.Rows) != 0 && len(preview.Rows[0]) != 0 && preview.Rows[0][0].Rune == 'y'
	}, time.Second, time.Millisecond)
	require.Equal(t, rune('y'), preview.Rows[0][0].Rune)
	require.Equal(t, 2, client.Calls())
}

func TestRemotePickerPreviewSelectionChangeFencesOldTarget(t *testing.T) {
	d := newTestDaemon(t, nil, immediateRemotePreviewClock{})
	firstTarget := remotePreviewCacheTarget()
	secondTarget := firstTarget
	secondTarget.LifecycleID[0]++
	secondTarget.LiveTabID = "tab-2"
	client := &remotePreviewTestClient{result: remotePreviewCacheResult(firstTarget, 1), started: make(chan struct{})}
	d.remotePreviewClient = client
	_, ac, _ := addRemoteRefreshPickerOwner(t, d, "owner")
	firstKey := domain.RemoteSessionKey{Host: firstTarget.Endpoint, Name: firstTarget.SessionName, LifecycleID: firstTarget.LifecycleID, DisplayOrigin: firstTarget.DisplayOrigin}
	secondKey := domain.RemoteSessionKey{Host: secondTarget.Endpoint, Name: secondTarget.SessionName, LifecycleID: secondTarget.LifecycleID, DisplayOrigin: secondTarget.DisplayOrigin}
	views := []picker.SessionView{
		{ID: firstKey.ID(), Name: firstTarget.SessionName, Tabs: []picker.TabEntry{{TabID: firstTarget.LiveTabID, Name: "first"}}, RemoteKey: &firstKey, RemoteTarget: &firstTarget, RemoteAvailability: picker.RemoteFresh, RemoteAttachReady: true},
		{ID: secondKey.ID(), Name: secondTarget.SessionName, Tabs: []picker.TabEntry{{TabID: secondTarget.LiveTabID, Name: "second"}}, RemoteKey: &secondKey, RemoteTarget: &secondTarget, RemoteAvailability: picker.RemoteFresh, RemoteAttachReady: true},
	}
	model := picker.New(views, picker.SelectionConfig{Mode: picker.SelectNavigationTab})
	ac.overlays.pickerMu.Lock()
	ac.overlays.picker = model
	ac.overlays.pickerIntent = pickerNavigate
	ac.overlays.pickerPreviewGeneration = 1
	ac.overlays.pickerMu.Unlock()
	d.registerPreviewForSelection(ac)
	<-client.started
	require.Equal(t, firstTarget, client.LastTarget())

	client.mu.Lock()
	client.result = remotePreviewCacheResult(secondTarget, 2)
	client.started = make(chan struct{})
	client.startedSignal = false
	client.mu.Unlock()
	ac.overlays.pickerMu.Lock()
	model.Down()
	ac.overlays.pickerMu.Unlock()
	d.registerPreviewForSelection(ac)
	<-client.started
	require.Equal(t, secondTarget, client.LastTarget())
	require.Equal(t, 2, client.Calls())
}

func TestRemotePickerPreviewErrorCannotPublishAfterSelectionChange(t *testing.T) {
	d := newTestDaemon(t, nil, immediateRemotePreviewClock{})
	firstTarget := remotePreviewCacheTarget()
	secondTarget := firstTarget
	secondTarget.LifecycleID[0]++
	secondTarget.LiveTabID = "tab-2"
	client := &remotePreviewTestClient{
		err:     errors.New("remote unavailable"),
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
	d.remotePreviewClient = client
	_, ac, _ := addRemoteRefreshPickerOwner(t, d, "owner")
	firstKey := domain.RemoteSessionKey{Host: firstTarget.Endpoint, Name: firstTarget.SessionName, LifecycleID: firstTarget.LifecycleID, DisplayOrigin: firstTarget.DisplayOrigin}
	secondKey := domain.RemoteSessionKey{Host: secondTarget.Endpoint, Name: secondTarget.SessionName, LifecycleID: secondTarget.LifecycleID, DisplayOrigin: secondTarget.DisplayOrigin}
	model := picker.New([]picker.SessionView{
		{ID: firstKey.ID(), Name: firstTarget.SessionName, Tabs: []picker.TabEntry{{TabID: firstTarget.LiveTabID, Name: "first"}}, RemoteKey: &firstKey, RemoteTarget: &firstTarget, RemoteAvailability: picker.RemoteFresh, RemoteAttachReady: true},
		{ID: secondKey.ID(), Name: secondTarget.SessionName, Tabs: []picker.TabEntry{{TabID: secondTarget.LiveTabID, Name: "second"}}, RemoteKey: &secondKey, RemoteTarget: &secondTarget, RemoteAvailability: picker.RemoteFresh, RemoteAttachReady: true},
	}, picker.SelectionConfig{Mode: picker.SelectNavigationTab})
	selected, ok := model.Selected()
	require.True(t, ok)
	ac.overlays.pickerMu.Lock()
	ac.overlays.picker = model
	ac.overlays.pickerIntent = pickerNavigate
	ac.overlays.pickerPreviewGeneration = 1
	ac.overlays.pickerMu.Unlock()
	d.startRemotePickerPreview(ac, selected, 1)
	<-client.started

	ac.overlays.pickerMu.Lock()
	ac.overlays.pickerRemotePreview = picker.Preview{Width: 1, Height: 1, Rows: [][]renderer.Cell{{{Rune: 'k', Style: renderer.DefaultStyle()}}}}
	model.Down()
	ac.overlays.pickerMu.Unlock()
	close(client.release)

	require.Eventually(t, func() bool {
		ac.overlays.pickerMu.Lock()
		defer ac.overlays.pickerMu.Unlock()
		return len(ac.overlays.pickerRemotePreview.Rows) == 1 && ac.overlays.pickerRemotePreview.Rows[0][0].Rune == 'k'
	}, time.Second, time.Millisecond)
}

func TestRemotePickerPreviewRefreshesAfterAttachmentResize(t *testing.T) {
	d := newTestDaemon(t, nil, immediateRemotePreviewClock{})
	target := remotePreviewCacheTarget()
	firstStarted := make(chan struct{})
	client := &remotePreviewTestClient{
		result:  remotePreviewCacheResult(target, 1),
		started: firstStarted,
		release: make(chan struct{}),
	}
	d.remotePreviewClient = client
	sess, ac, _ := addRemoteRefreshPickerOwner(t, d, "owner")
	key := domain.RemoteSessionKey{Host: target.Endpoint, Name: target.SessionName, LifecycleID: target.LifecycleID, DisplayOrigin: target.DisplayOrigin}
	view := picker.SessionView{
		ID: key.ID(), Name: target.SessionName,
		Tabs:      []picker.TabEntry{{TabID: target.LiveTabID, Name: "main"}},
		RemoteKey: &key, RemoteTarget: &target,
		RemoteAvailability: picker.RemoteFresh, RemoteAttachReady: true,
	}
	model := picker.New([]picker.SessionView{view}, picker.SelectionConfig{Mode: picker.SelectNavigationTab})
	selected, ok := model.Selected()
	require.True(t, ok)
	ac.overlays.pickerMu.Lock()
	ac.overlays.picker = model
	ac.overlays.pickerIntent = pickerNavigate
	ac.overlays.pickerPreviewGeneration = 1
	ac.overlays.pickerMu.Unlock()
	d.startRemotePickerPreview(ac, selected, 1)
	<-firstStarted

	secondStarted := make(chan struct{})
	client.mu.Lock()
	client.started = secondStarted
	client.release = nil
	client.startedSignal = false
	client.mu.Unlock()
	token := sess.attachmentToken(ac, ac.transport())
	effect, admitted := ac.beginAttachmentEffect(token)
	require.True(t, admitted)
	token.effect = effect
	defer effect.End()
	require.True(t, d.resizeAttachmentForLease(token, domain.Size{Cols: 100, Rows: 30}))
	<-secondStarted
	wantWidth, wantHeight := remotePickerPreviewSize(domain.Size{Cols: 100, Rows: 30})
	gotWidth, gotHeight := client.LastSize()
	require.Equal(t, wantWidth, gotWidth)
	require.Equal(t, wantHeight, gotHeight)

	require.Eventually(t, func() bool {
		ac.overlays.pickerMu.Lock()
		defer ac.overlays.pickerMu.Unlock()
		return ac.overlays.pickerRemotePreview.Width == 1 && ac.overlays.pickerRemotePreview.Height == 1
	}, time.Second, time.Millisecond)
	require.Equal(t, 2, client.Calls())
}

func TestFetchRemotePreviewRejectsInvalidDimensionsBeforeRemoteIO(t *testing.T) {
	clock := &remotePreviewTestClock{now: time.Unix(400, 0)}
	client := &remotePreviewTestClient{result: remotePreviewCacheResult(remotePreviewCacheTarget(), 1)}
	d := newTestDaemon(t, nil, clock)
	d.remotePreviewClient = client

	_, err := d.fetchRemotePreview(context.Background(), remotePreviewCacheTarget(), ports.RemotePreviewMaxWidth+1, 1)
	require.ErrorIs(t, err, ports.ErrInvalidRemotePreviewRequest)
	require.Zero(t, client.Calls())
}
