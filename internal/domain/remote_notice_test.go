package domain

import "testing"

func TestRemoteHealthTransition(t *testing.T) {
	healthy := RemoteHealth{Availability: RemoteAvailabilityReachable}
	unknown := RemoteHealth{Availability: RemoteAvailabilityUnknown}
	noDaemon := RemoteHealth{Availability: RemoteAvailabilityNoDaemon, Episode: 1}
	down := func(episode uint64) RemoteHealth {
		return RemoteHealth{Availability: RemoteAvailabilityUnreachable, Episode: episode}
	}
	tests := []struct {
		name     string
		prev     RemoteHealth
		seen     bool
		cur      RemoteHealth
		want     RemoteHealthEvent
		wantNext RemoteHealth
	}{
		{name: "first healthy sighting is silent", cur: healthy, want: RemoteHealthSilent, wantNext: healthy},
		{name: "first unknown sighting is silent", cur: unknown, want: RemoteHealthSilent, wantNext: unknown},
		{name: "healthy to healthy is silent", prev: healthy, seen: true, cur: healthy, want: RemoteHealthSilent, wantNext: healthy},
		{name: "first failing sighting fails", cur: down(1), want: RemoteHealthFailed, wantNext: down(1)},
		{name: "healthy to failing fails", prev: healthy, seen: true, cur: down(1), want: RemoteHealthFailed, wantNext: down(1)},
		{name: "same episode is silent", prev: down(1), seen: true, cur: down(1), want: RemoteHealthSilent, wantNext: down(1)},
		{name: "new episode fails", prev: down(1), seen: true, cur: down(2), want: RemoteHealthFailed, wantNext: down(2)},
		{name: "new failure class in same episode fails", prev: down(1), seen: true, cur: noDaemon, want: RemoteHealthFailed, wantNext: noDaemon},
		{name: "no daemon to unreachable fails", prev: noDaemon, seen: true, cur: down(2), want: RemoteHealthFailed, wantNext: down(2)},
		{name: "recovery", prev: down(1), seen: true, cur: healthy, want: RemoteHealthRecovered, wantNext: healthy},
		{name: "unknown keeps the failing state", prev: down(1), seen: true, cur: unknown, want: RemoteHealthSilent, wantNext: down(1)},
		{name: "daemon started after no daemon is silent", prev: noDaemon, seen: true, cur: healthy, want: RemoteHealthSilent, wantNext: healthy},
		{name: "version fixed recovers", prev: RemoteHealth{Availability: RemoteAvailabilityReachable, VersionMismatch: true}, seen: true, cur: healthy, want: RemoteHealthRecovered, wantNext: healthy},
		{name: "version mismatch fails", cur: RemoteHealth{Availability: RemoteAvailabilityReachable, VersionMismatch: true}, want: RemoteHealthFailed, wantNext: RemoteHealth{Availability: RemoteAvailabilityReachable, VersionMismatch: true}},
		{name: "incompatible fails", cur: RemoteHealth{Availability: RemoteAvailabilityIncompatible}, want: RemoteHealthFailed, wantNext: RemoteHealth{Availability: RemoteAvailabilityIncompatible}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			next, event := RemoteHealthTransition(tt.prev, tt.seen, tt.cur)
			if event != tt.want || next != tt.wantNext {
				t.Fatalf("got (%+v, %d), want (%+v, %d)", next, event, tt.wantNext, tt.want)
			}
		})
	}
}

func TestRemoteHealthRecoveryAcrossUnknown(t *testing.T) {
	steps := []RemoteHealth{
		{Availability: RemoteAvailabilityUnreachable, Episode: 1},
		{Availability: RemoteAvailabilityUnknown, Episode: 1},
		{Availability: RemoteAvailabilityReachable, Episode: 1},
	}
	want := []RemoteHealthEvent{RemoteHealthFailed, RemoteHealthSilent, RemoteHealthRecovered}
	var prev RemoteHealth
	seen := false
	for i, cur := range steps {
		var event RemoteHealthEvent
		prev, event = RemoteHealthTransition(prev, seen, cur)
		seen = true
		if event != want[i] {
			t.Fatalf("step %d: event %d, want %d", i, event, want[i])
		}
	}
}

func TestNotificationSubject(t *testing.T) {
	base := Notification{Code: NoticeRemoteObservation, SessionID: "s", Scope: "h", Message: "down"}
	tests := []struct {
		name  string
		other Notification
		same  bool
	}{
		{name: "same subject, other message", other: Notification{Code: NoticeRemoteObservation, SessionID: "s", Scope: "h", Message: "reconnected"}, same: true},
		{name: "other scope", other: Notification{Code: NoticeRemoteObservation, SessionID: "s", Scope: "other"}},
		{name: "other session", other: Notification{Code: NoticeRemoteObservation, SessionID: "t", Scope: "h"}},
		{name: "other code", other: Notification{Code: NoticeSessionKill, SessionID: "s", Scope: "h"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := base.Subject() == tt.other.Subject(); got != tt.same {
				t.Fatalf("same subject = %v, want %v", got, tt.same)
			}
		})
	}
}
