package daemon

import (
	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/protocol"
)

func testRouteTarget(name string, marker byte) protocol.ExactSessionTarget {
	return protocol.ExactSessionTarget{LifecycleID: domain.SessionLifecycleID{marker}, SessionName: name}
}

func testRouteEntry(key, generation uint64, name string, marker byte, kind protocol.RouteKind) protocol.RecentRouteEntry {
	return protocol.RecentRouteEntry{
		Key: key, Generation: generation, Target: testRouteTarget(name, marker),
		Name: name, Kind: kind,
	}
}
