package domain

import "testing"

func TestRemoteSessionKeyID(t *testing.T) {
	tests := []struct {
		name string
		key  RemoteSessionKey
		want SessionID
	}{
		{
			name: "uses raw URL base64 components",
			key:  RemoteSessionKey{Host: "arch", Name: "hello"},
			want: "remote:YXJjaA.aGVsbG8",
		},
		{
			name: "preserves host metacharacters without delimiters",
			key:  RemoteSessionKey{Host: "build@mule:2222/path.[x]", Name: "work.1"},
			want: "remote:YnVpbGRAbXVsZToyMjIyL3BhdGguW3hd.d29yay4x",
		},
		{
			name: "supports bracketed IPv6 hosts",
			key:  RemoteSessionKey{Host: "user@[2001:db8::1]:2222", Name: "work"},
			want: "remote:dXNlckBbMjAwMTpkYjg6OjFdOjIyMjI.d29yaw",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.key.ID(); got != tt.want {
				t.Fatalf("RemoteSessionKey.ID() = %q, want %q", got, tt.want)
			}
			if got := tt.key.ID(); got != tt.key.ID() {
				t.Fatalf("RemoteSessionKey.ID() is unstable: %q != %q", got, tt.key.ID())
			}
		})
	}
}

func TestRemoteSessionKeyIDAvoidsRawConcatenationCollisions(t *testing.T) {
	tests := []struct {
		name  string
		first RemoteSessionKey
		last  RemoteSessionKey
	}{
		{
			name:  "host component delimiter",
			first: RemoteSessionKey{Host: "arch.work", Name: "shell"},
			last:  RemoteSessionKey{Host: "arch", Name: "work.shell"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.first.Host+"."+tt.first.Name != tt.last.Host+"."+tt.last.Name {
				t.Fatal("test setup does not collide under raw concatenation")
			}
			if tt.first.ID() == tt.last.ID() {
				t.Fatalf("distinct keys have the same ID: %q", tt.first.ID())
			}
		})
	}
}

func TestRemoteSessionKeyValidate(t *testing.T) {
	tests := []struct {
		name    string
		key     RemoteSessionKey
		wantErr bool
	}{
		{name: "valid", key: RemoteSessionKey{Host: "build@mule:2222/path", Name: "work.1"}},
		{name: "empty host", key: RemoteSessionKey{Name: "work"}, wantErr: true},
		{name: "host whitespace", key: RemoteSessionKey{Host: "build mule", Name: "work"}, wantErr: true},
		{name: "empty session", key: RemoteSessionKey{Host: "arch"}, wantErr: true},
		{name: "unsafe session", key: RemoteSessionKey{Host: "arch", Name: "work/session"}, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.key.Validate()
			if tt.wantErr && err == nil {
				t.Fatal("RemoteSessionKey.Validate() = nil, want error")
			}
			if !tt.wantErr && err != nil {
				t.Fatalf("RemoteSessionKey.Validate() = %v, want nil", err)
			}
		})
	}
}

func TestRemoteSessionKeyDisplayIsPresentationOnly(t *testing.T) {
	tests := []struct {
		host          string
		displayOrigin string
		want          string
	}{
		{host: "arch", want: "hello@arch"},
		{host: "route", displayOrigin: "user@arch", want: "hello@arch"},
		{host: "test@arch", want: "hello@arch"},
		{host: "test@arch:2222", want: "hello@arch:2222"},
		{host: "test@[2001:db8::1]:2222", want: "hello@[2001:db8::1]:2222"},
		{host: "test@", want: "hello@test@"},
		{host: "@arch", want: "hello@arch"},
	}
	for _, test := range tests {
		t.Run(test.host, func(t *testing.T) {
			key := RemoteSessionKey{Host: test.host, Name: "hello", DisplayOrigin: test.displayOrigin}
			if got := key.Display(); got != test.want {
				t.Fatalf("RemoteSessionKey.Display() = %q, want %q", got, test.want)
			}
			if key.Host != test.host || key.Name != "hello" {
				t.Fatalf("Display mutated routing key: %#v", key)
			}
		})
	}
}
