package client

import (
	"testing"

	"github.com/bnema/vev/internal/protocol"
	"github.com/stretchr/testify/require"
)

func testAttachRoute(name string) attachRoute {
	return attachRoute{request: AttachRequest{SessionName: name}}
}

func TestNavigationTransitionRetiresRecoveryOnce(t *testing.T) {
	for _, tc := range []struct {
		name  string
		begin func(*navigationTransition)
	}{
		{"creation", func(n *navigationTransition) { n.beginCreation(testAttachRoute("return"), 7) }},
		{"recent", func(n *navigationTransition) { n.beginRecent(testAttachRoute("return")) }},
		{"inventory", func(n *navigationTransition) {
			n.beginInventory(testAttachRoute("return"), protocol.NavigationInventorySelection{
				CauseActionID:         1,
				InteractionGeneration: 2,
				SourceKey:             protocol.NavigationInventoryLocalSourceKey,
				EntryKey:              "entry",
			})
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, success := range []bool{false, true} {
				var n navigationTransition
				tc.begin(&n)
				require.True(t, n.active())
				route, ok := n.restore()
				require.True(t, ok)
				require.Equal(t, "return", route.request.SessionName)
				if success {
					n.settleSuccess()
				} else {
					n.settleFailure()
				}
				require.False(t, n.active())
				_, ok = n.restore()
				require.False(t, ok)
				n.clear()
				require.False(t, n.active())
			}
		})
	}
}

func TestNavigationTransitionInventoryFailure(t *testing.T) {
	for _, tc := range []struct {
		name      string
		begin     func(*navigationTransition)
		wantCode  protocol.NavigationInventoryFailureCode
		wantValid bool
	}{
		{
			name: "inventory retains selection for failed handoff",
			begin: func(n *navigationTransition) {
				n.beginInventory(testAttachRoute("return"), protocol.NavigationInventorySelection{
					CauseActionID:         9,
					InteractionGeneration: 3,
					SourceKey:             protocol.NavigationInventoryLocalSourceKey,
					EntryKey:              "entry",
				})
			},
			wantCode:  protocol.NavigationInventoryNavigationFailed,
			wantValid: true,
		},
		{
			name:      "creation carries no inventory failure",
			begin:     func(n *navigationTransition) { n.beginCreation(testAttachRoute("return"), 7) },
			wantValid: false,
		},
		{
			name:      "recent carries no inventory failure",
			begin:     func(n *navigationTransition) { n.beginRecent(testAttachRoute("return")) },
			wantValid: false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var n navigationTransition
			tc.begin(&n)
			failure, ok := n.inventoryNavigationFailure()
			require.Equal(t, tc.wantValid, ok)
			if !tc.wantValid {
				return
			}
			require.Equal(t, tc.wantCode, failure.Code)
			require.Equal(t, uint64(9), failure.CauseActionID)
			require.Equal(t, uint64(3), failure.InteractionGeneration)
			require.Equal(t, protocol.NavigationInventoryLocalSourceKey, failure.SourceKey)
			require.Equal(t, "entry", failure.EntryKey)
			require.NoError(t, protocol.ValidateNavigationInventoryFailure(failure))
		})
	}
}
