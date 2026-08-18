package daemon

import (
	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
)

func testRouteTarget(name string, marker byte) ports.ExactSessionTarget {
	return ports.ExactSessionTarget{LifecycleID: domain.SessionLifecycleID{marker}, SessionName: name}
}

func testRouteEntry(key, generation uint64, name string, marker byte, kind ports.RouteKind) ports.RecentRouteEntry {
	return ports.RecentRouteEntry{
		Key: key, Generation: generation, Target: testRouteTarget(name, marker),
		Name: name, Kind: kind,
	}
}
