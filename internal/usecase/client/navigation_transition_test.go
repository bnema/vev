package client

import (
	"testing"
)

func testAttachRoute(name string) attachRoute {
	return attachRoute{request: AttachRequest{SessionName: name}}
}

func TestNavigationTransitionSettlesOnce(t *testing.T) {
	tests := []struct {
		name  string
		begin func(t *navigationTransition)
	}{
		{name: "creation", begin: func(t *navigationTransition) {
			t.beginCreation(testAttachRoute("authority"), testAttachRoute("return"), 7)
		}},
		{name: "recent", begin: func(t *navigationTransition) {
			t.beginRecent(testAttachRoute("authority"), testAttachRoute("return"), 3, 1)
		}},
		{name: "inventory", begin: func(t *navigationTransition) {
			t.beginInventory(testAttachRoute("local"), testAttachRoute("remote"), 9)
			t.admitDestination()
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var transition navigationTransition
			tt.begin(&transition)
			if !transition.active() {
				t.Fatal("transition should be active after begin")
			}
			route, ok := transition.restore()
			if !ok || route.request.SessionName != "return" && route.request.SessionName != "remote" {
				t.Fatalf("restore() = %#v, %v; want the return route", route, ok)
			}
			if !transition.settleSuccess() {
				t.Fatal("first settleSuccess should allow history")
			}
			if transition.settleSuccess() {
				t.Fatal("second settleSuccess must not re-commit history")
			}
			if transition.settleFailure() {
				t.Fatal("settle after settle must report false")
			}
			if _, ok := transition.restore(); ok {
				t.Fatal("restore after settle must fail")
			}
		})
	}
}

func TestNavigationTransitionPendingPredicates(t *testing.T) {
	var transition navigationTransition
	if transition.pendingInventory() {
		t.Fatal("zero transition must not report pending inventory")
	}
	transition.beginCreation(testAttachRoute("a"), testAttachRoute("b"), 1)
	if transition.pendingInventory() {
		t.Fatal("creation must not report pending inventory")
	}
	transition.beginRecent(testAttachRoute("a"), testAttachRoute("b"), 1, 1)
	if transition.pendingInventory() {
		t.Fatal("recent must not report pending inventory")
	}
	transition.beginInventory(testAttachRoute("local"), testAttachRoute("remote"), 9)
	if !transition.pendingInventory() {
		t.Fatal("inventory must report pending until settled")
	}
	transition.settleSuccess()
	if transition.pendingInventory() {
		t.Fatal("settled inventory must not report pending")
	}
}

func TestNavigationTransitionAuthorityNeverOverwritesReturn(t *testing.T) {
	var transition navigationTransition
	transition.beginInventory(testAttachRoute("local-source"), testAttachRoute("active-remote"), 11)
	if transition.targetAuthority.request.SessionName != "local-source" {
		t.Fatalf("authority = %q, want local-source", transition.targetAuthority.request.SessionName)
	}
	route, ok := transition.restore()
	if !ok {
		t.Fatal("restore should succeed while pending")
	}
	if route.request.SessionName != "active-remote" {
		t.Fatalf("restore = %q, want active-remote (never the target authority)", route.request.SessionName)
	}
	// A second restore repeats the same route without state change.
	again, ok := transition.restore()
	if !ok || again.request.SessionName != "active-remote" {
		t.Fatalf("second restore = %#v, %v; want the same return route", again, ok)
	}
	if !transition.settleFailure() {
		t.Fatal("settleFailure should succeed once")
	}
	if transition.settleFailure() {
		t.Fatal("second settleFailure must report false: no indefinite retry")
	}
}

func TestNavigationTransitionAdmitOnlyInventory(t *testing.T) {
	var creation navigationTransition
	creation.beginCreation(testAttachRoute("a"), testAttachRoute("b"), 1)
	creation.admitDestination()
	if creation.stage != navigationStageDestinationPending {
		t.Fatalf("creation stage = %d, want destination pending", creation.stage)
	}
	var inventory navigationTransition
	inventory.beginInventory(testAttachRoute("a"), testAttachRoute("b"), 2)
	if inventory.stage != navigationStagePrepare {
		t.Fatalf("inventory stage = %d, want prepare", inventory.stage)
	}
	inventory.admitDestination()
	if inventory.stage != navigationStageDestinationPending {
		t.Fatalf("inventory stage after admit = %d, want destination pending", inventory.stage)
	}
	var empty navigationTransition
	empty.clear()
	if empty.active() {
		t.Fatal("cleared transition must be inactive")
	}
	if _, ok := empty.restore(); ok {
		t.Fatal("restore on inactive transition must fail")
	}
}
