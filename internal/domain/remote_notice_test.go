package domain

import "testing"

func TestRemoteHealthNotice(t *testing.T) {
	healthy := RemoteHealth{Key: "h", Origin: "host-a", Availability: RemoteAvailabilityReachable}
	down := func(episode uint64, kind RemoteFailureKind) RemoteHealth {
		return RemoteHealth{Key: "h", Origin: "host-a", Availability: RemoteAvailabilityUnreachable, Failure: kind, Episode: episode}
	}
	tests := []struct {
		name     string
		prev     RemoteHealth
		seen     bool
		cur      RemoteHealth
		want     bool
		severity NoticeSeverity
		message  string
	}{
		{name: "first healthy sighting is silent", cur: healthy},
		{name: "healthy to healthy is silent", prev: healthy, seen: true, cur: healthy},
		{name: "first failing sighting notifies", cur: down(1, RemoteFailureTimeout), want: true, severity: NoticeError, message: "Remote check failed: host-a — SSH timed out"},
		{name: "new failure after healthy notifies", prev: healthy, seen: true, cur: down(1, RemoteFailureAuthentication), want: true, severity: NoticeError, message: "Remote check failed: host-a — SSH authentication failed; verify non-interactive SSH access"},
		{name: "same episode is silent", prev: down(1, RemoteFailureTimeout), seen: true, cur: down(1, RemoteFailureTimeout)},
		{name: "new episode notifies", prev: down(1, RemoteFailureTimeout), seen: true, cur: down(2, RemoteFailureTrust), want: true, severity: NoticeError, message: "Remote check failed: host-a — SSH host verification failed; verify the host key policy"},
		{name: "recovery notifies", prev: down(1, RemoteFailureTimeout), seen: true, cur: healthy, want: true, severity: NoticeInfo, message: "Remote host reconnected: host-a"},
		{name: "unknown after failure is silent", prev: down(1, RemoteFailureTimeout), seen: true, cur: RemoteHealth{Key: "h", Origin: "host-a", Availability: RemoteAvailabilityUnknown}},
		{name: "version mismatch notifies", cur: RemoteHealth{Key: "h", Origin: "host-a", Availability: RemoteAvailabilityReachable, VersionMismatch: true}, want: true, severity: NoticeError, message: "Remote check failed: host-a — remote vev version is incompatible"},
		{name: "no daemon warns", cur: RemoteHealth{Key: "h", Origin: "host-a", Availability: RemoteAvailabilityNoDaemon}, want: true, severity: NoticeWarn, message: "Remote check failed: host-a — no vev daemon is running"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			n, ok := RemoteHealthNotice(tt.prev, tt.seen, tt.cur)
			if ok != tt.want {
				t.Fatalf("notify = %v, want %v", ok, tt.want)
			}
			if !ok {
				return
			}
			if n.Code != NoticeRemoteObservation || n.Scope != "h" || n.Severity != tt.severity || n.Message != tt.message {
				t.Fatalf("notice = %+v", n)
			}
		})
	}
}

func TestNotificationSubject(t *testing.T) {
	a := Notification{Code: NoticeRemoteObservation, Scope: "h", Message: "down"}
	b := Notification{Code: NoticeRemoteObservation, Scope: "h", Message: "reconnected"}
	c := Notification{Code: NoticeRemoteObservation, Scope: "other"}
	if a.Subject() != b.Subject() {
		t.Fatal("same code and scope must share a subject")
	}
	if a.Subject() == c.Subject() {
		t.Fatal("different scopes must not share a subject")
	}
}
