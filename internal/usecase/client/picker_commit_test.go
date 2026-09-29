package client

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol/catalogue"
)

// TestPickerCommitResolvesTheCommittedRow drives the whole picker commit path
// through the real controller and catalogue: a keypress records the commit
// decision with its row key, TakeOp hands the key over, and ResolveCommit
// revalidates it against the latest publication into the exact broker stream
// request and tab. A publication between commit and resolution must never
// retarget the commit.
func TestPickerCommitResolvesTheCommittedRow(t *testing.T) {
	now := time.Unix(1000, 0)
	// tabbed publishes session "work" at revision with tabs t1, t2, ... named
	// in order. A session with tabs is a header; only its tab rows commit.
	tabbed := func(revision ports.BrokerRevision, names ...string) ports.BrokerSnapshot {
		tabs := make([]catalogue.RemoteCatalogTab, 0, len(names))
		for i, name := range names {
			tabs = append(tabs, pickerTestTab(fmt.Sprintf("t%d", i+1), name, "", false))
		}
		return ports.BrokerSnapshot{Epoch: 3, Revision: revision, Daemons: []ports.BrokerDaemonObservation{
			pickerTestLocalObservation(now, pickerTestTabbed("work", 3, catalogue_Up, 1, "t1", tabs...)),
		}}
	}
	tests := []struct {
		name string
		// row is the label of the tab row the cursor commits.
		row         string
		republished *ports.BrokerSnapshot
		wantTab     domain.TabStableID
		wantErr     pickerCatalogueErrorCode
	}{
		{name: "first tab commits its tab", row: "shell", wantTab: "t1"},
		{name: "second tab commits its tab", row: "build", wantTab: "t2"},
		{name: "unrelated republication keeps the commit", row: "build", republished: ptr(tabbed(2, "shell", "build", "logs")), wantTab: "t2"},
		{name: "tab closed after commit is refused", row: "build", republished: ptr(tabbed(2, "shell")), wantErr: pickerCatalogueGone},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			controller, _ := pickerTestController(t)
			controller.ApplySnapshot(tabbed(1, "shell", "build"))
			key := pickerTabKey(t, controller.Catalogue().RecentLines(), "work", tt.row)
			for i := 0; i < 16; i++ {
				if current, ok := controller.CursorKey(); ok && current == key {
					break
				}
				require.True(t, controller.ConsumeTerminalRead([]byte("j")))
			}
			require.True(t, controller.ConsumeTerminalRead([]byte("\r")))
			op, committed := controller.TakeOp()
			require.True(t, op.commit)
			require.Equal(t, key, committed, "the commit captures the row under the cursor")

			if tt.republished != nil {
				controller.ApplySnapshot(*tt.republished)
			}
			request, tab, err := controller.ResolveCommit(committed, pickerTestBase())

			if tt.wantErr != 0 {
				require.True(t, pickerCatalogueErrorIs(err, tt.wantErr), "want %v, got %v", tt.wantErr, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, ports.BrokerAdmissionExact, request.Admission)
			require.Equal(t, "work", request.Target.SessionName)
			require.Equal(t, pickerTestBase().Stream, request.Stream)
			require.Equal(t, tt.wantTab, tab.preferred)
		})
	}
}

// TestPickerCatalogueResolveTargetKinds covers every selection kind a commit
// key can carry: destinations resolve to their admission, while a CLI-only
// attach-by-name identity is refused as a picker commit.
func TestPickerCatalogueResolveTargetKinds(t *testing.T) {
	observation := pickerTestLocalObservation(time.Unix(1000, 0), pickerTestSession("alpha", 1, catalogue_Up))
	tests := []struct {
		name          string
		ref           pickerSelectionRef
		wantAdmission ports.BrokerStreamAdmission
		wantErr       pickerCatalogueErrorCode
	}{
		{name: "exact session", ref: pickerSelectionRef{kind: pickerSelectionExact, lifecycle: pickerTestLifecycle(1), name: "alpha"}, wantAdmission: ports.BrokerAdmissionExact},
		{name: "create named", ref: pickerSelectionRef{kind: pickerSelectionCreateNamed, createName: "fresh"}, wantAdmission: ports.BrokerAdmissionCreateNamed},
		{name: "create named with invalid name", ref: pickerSelectionRef{kind: pickerSelectionCreateNamed, createName: "bad name/"}, wantErr: pickerCatalogueInvalidName},
		{name: "create ephemeral", ref: pickerSelectionRef{kind: pickerSelectionCreateEphemeral}, wantAdmission: ports.BrokerAdmissionCreateEphemeral},
		{name: "attach named is not a picker destination", ref: pickerSelectionRef{kind: pickerSelectionAttachNamed, name: "alpha"}, wantErr: pickerCatalogueUnavailable},
		{name: "unknown key", wantErr: pickerCatalogueUnknown},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			catalogue, _ := pickerTestCatalogue(t)
			require.True(t, catalogue.Apply(ports.BrokerSnapshot{Epoch: 3, Revision: 1, Daemons: []ports.BrokerDaemonObservation{observation}}))
			key := "missing"
			if tt.ref.kind != 0 {
				key = "injected"
				tt.ref.epoch = 3
				tt.ref.local = true
				catalogue.mu.Lock()
				catalogue.refs[key] = tt.ref
				catalogue.mu.Unlock()
			}

			request, _, err := catalogue.ResolveTarget(key, pickerTestBase())

			if tt.wantErr != 0 {
				require.True(t, pickerCatalogueErrorIs(err, tt.wantErr), "want %v, got %v", tt.wantErr, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.wantAdmission, request.Admission)
		})
	}
}

func ptr[T any](v T) *T { return &v }
