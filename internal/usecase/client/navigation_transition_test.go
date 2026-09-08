package client

import (
	"testing"

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
		{"inventory", func(n *navigationTransition) { n.beginInventory(testAttachRoute("return")) }},
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
