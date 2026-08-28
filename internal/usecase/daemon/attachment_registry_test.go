package daemon

import (
	"sync"
	"testing"

	"github.com/bnema/vev/internal/domain"
	"github.com/stretchr/testify/require"
)

func registryTestSession() *session {
	sz := domain.Size{Cols: 80, Rows: 23}
	first := newTabWithStableID("tab-a", "pane-a", nil, sz)
	second := newTabWithStableID("tab-b", "pane-b", nil, sz)
	return &session{
		sessionCore: sessionCore{id: "registry-test"},
		tabs:        []*tab{first, second},
	}
}

func registryTestAttachment(id byte) *attachedClient {
	ac := &attachedClient{}
	ac.clientID[0] = id
	return ac
}

func TestAttachmentRegistryPreservesOtherAttachmentsOnDetach(t *testing.T) {
	sess := registryTestSession()
	first, second, third := registryTestAttachment(1), registryTestAttachment(2), registryTestAttachment(3)

	for _, ac := range []*attachedClient{first, second, third} {
		if !sess.registerAttachment(ac) {
			t.Fatalf("register %v failed", ac.clientID[0])
		}
	}
	if got := sess.snapshotAttachments(); len(got) != 3 {
		t.Fatalf("attachment count = %d, want 3", len(got))
	}
	if !sess.unregisterAttachment(second) {
		t.Fatal("unregister second failed")
	}
	got := sess.snapshotAttachments()
	if len(got) != 2 || got[0] != first || got[1] != third {
		t.Fatalf("remaining attachments = %v, want first and third", got)
	}
}

func TestAttachmentRegistryRepairsRemovedStableTarget(t *testing.T) {
	sess := registryTestSession()
	ac := registryTestAttachment(1)
	if !sess.registerAttachment(ac) || !sess.selectAttachmentTab(ac, "tab-b") {
		t.Fatal("failed to select second tab")
	}
	before := ac.viewSnapshot()
	if before.tabID != "tab-b" || before.paneID != "pane-b" {
		t.Fatalf("selected view = %#v", before)
	}

	if err := sess.runMutation(func() error {
		sess.mu.Lock()
		defer sess.mu.Unlock()
		sess.tabs = sess.tabs[:1]
		sess.repairAttachmentViewsLocked()
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	after := ac.viewSnapshot()
	if after.tabID != "tab-a" || after.paneID != "pane-a" {
		t.Fatalf("repaired view = %#v, want tab-a/pane-a", after)
	}
	if after.revision <= before.revision || !after.liveBottom {
		t.Fatalf("repaired view revision/liveBottom = %d/%v, want revision increase and live bottom", after.revision, after.liveBottom)
	}
}

func TestTabForAttachmentRepairRebasesOutputBeforePublishingRevision(t *testing.T) {
	sess := registryTestSession()
	ac := registryTestAttachment(1)
	ac.size = domain.Size{Cols: 80, Rows: 23}
	ac.output = newOutputStateStream()
	ac.output.attachment = ac
	require.True(t, sess.registerAttachment(ac))
	require.True(t, sess.selectAttachmentTab(ac, "tab-b"))

	before := ac.viewSnapshot()
	beforeEpoch := ac.output.currentEpoch()
	require.NoError(t, sess.runMutation(func() error {
		sess.mu.Lock()
		sess.tabs = sess.tabs[:1]
		sess.mu.Unlock()
		return nil
	}))

	require.Same(t, sess.tabs[0], sess.tabForAttachment(ac))
	after := ac.viewSnapshot()
	require.Greater(t, after.revision, before.revision)
	require.Greater(t, ac.output.currentEpoch(), beforeEpoch)
	output, err := ac.output.sideEffect([]byte("x"), 0)
	require.NoError(t, err)
	require.Equal(t, after.revision, output.ViewRevision)
	require.Equal(t, ac.output.currentEpoch(), output.Epoch)
}

func TestPrepareRemovedTabViewRebasesOutputBeforeSideEffect(t *testing.T) {
	sess := registryTestSession()
	ac := registryTestAttachment(1)
	ac.size = domain.Size{Cols: 80, Rows: 23}
	ac.output = newOutputStateStream()
	ac.output.attachment = ac
	require.True(t, sess.registerAttachment(ac))
	require.True(t, sess.selectAttachmentTab(ac, "tab-b"))

	before := ac.viewSnapshot()
	beforeEpoch := ac.output.currentEpoch()
	sess.mu.Lock()
	sess.prepareAttachmentViewsForRemovedTabLocked(sess.tabs[1], 1)
	sess.tabs = sess.tabs[:1]
	sess.mu.Unlock()

	after := ac.viewSnapshot()
	require.Equal(t, domain.TabStableID("tab-a"), after.tabID)
	require.Equal(t, domain.PaneStableID(""), after.paneID)
	require.Equal(t, before.revision+1, after.revision)
	require.Greater(t, ac.output.currentEpoch(), beforeEpoch)

	output, err := ac.output.sideEffect([]byte("replacement"), 0)
	require.NoError(t, err)
	require.Equal(t, after.revision, output.ViewRevision)
	require.NotEqual(t, beforeEpoch, output.Epoch)
	require.Equal(t, ac.output.currentEpoch(), output.Epoch)
}

func TestAttachmentRegistryViewMutationIsSerialized(t *testing.T) {
	sess := registryTestSession()
	ac := registryTestAttachment(1)
	if !sess.registerAttachment(ac) {
		t.Fatal("register failed")
	}
	before := ac.viewSnapshot()
	if !sess.updateAttachmentView(ac, func(view *attachmentView) {
		view.windowTop = 4
		view.windowRows = 12
		view.bookmark = 9
		view.liveBottom = false
	}) {
		t.Fatal("view update was not published")
	}
	after := ac.viewSnapshot()
	if after.windowTop != 4 || after.windowRows != 12 || after.bookmark != 9 || after.liveBottom || after.revision != before.revision+1 {
		t.Fatalf("view after mutation = %#v, want updated fields and one revision", after)
	}
}

func TestAttachmentRegistryConcurrentRegistrationSnapshotIsStable(t *testing.T) {
	sess := registryTestSession()
	attachments := []*attachedClient{
		registryTestAttachment(3),
		registryTestAttachment(1),
		registryTestAttachment(2),
	}
	start := make(chan struct{})
	var wg sync.WaitGroup
	for _, ac := range attachments {
		wg.Add(1)
		go func(ac *attachedClient) {
			defer wg.Done()
			<-start
			sess.registerAttachment(ac)
		}(ac)
	}
	close(start)
	wg.Wait()

	got := sess.snapshotAttachments()
	if len(got) != len(attachments) {
		t.Fatalf("attachment count = %d, want %d", len(got), len(attachments))
	}
	for i, want := range []byte{1, 2, 3} {
		if got[i].clientID[0] != want {
			t.Fatalf("snapshot[%d] id = %d, want %d", i, got[i].clientID[0], want)
		}
	}
	ordered := sess.repairAttachmentViewsForTest()
	for i, want := range []byte{1, 2, 3} {
		if ordered[i].clientID[0] != want {
			t.Fatalf("ordered attachment[%d] id = %d, want %d", i, ordered[i].clientID[0], want)
		}
	}
}

func (s *session) repairAttachmentViewsForTest() []*attachedClient {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.repairAttachmentViewsLocked()
	return s.snapshotAttachmentsLocked()
}
