package brokerstore

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCacheStrictFailures(t *testing.T) {
	raw, err := os.ReadFile("testdata/cache-v4.json")
	require.NoError(t, err)
	for name, data := range map[string]string{
		"unknown":             strings.Replace(string(raw), `"target":`, `"surprise":1,"target":`, 1),
		"duplicate":           strings.Replace(string(raw), `"version": 4`, `"version":4,"version":4`, 1),
		"trailing":            string(raw) + `{}`,
		"missing incarnation": strings.Replace(string(raw), `, "incarnation": "01000000000000000000000000000000"`, "", 1),
		"bad lifecycle":       strings.Replace(string(raw), "02000000000000000000000000000000", "00000000000000000000000000000000", 1),
		"bad time":            strings.Replace(string(raw), "1780000000000000000", "0", 1),
		"ambiguous v2":        `{"version":2,"hosts":[{"target":"user@arch","fetched_at_unix_nano":1,"sessions":[{"lifecycle_id":"02000000000000000000000000000000","name":"work","state":"up","ephemeral":false,"tabs":1,"attached":false}]}]}`,
		"too many hosts":      `{"version":4,"hosts":[` + strings.Repeat(`{"target":"user@arch","fetched_at_unix_nano":1,"incarnation":"01000000000000000000000000000000","sessions":[]},`, 64) + `{"target":"user@arch","fetched_at_unix_nano":1,"incarnation":"01000000000000000000000000000000","sessions":[]}]}`,
	} {
		t.Run(name, func(t *testing.T) {
			o := options(t)
			o.LegacyCache = filepath.Join(t.TempDir(), "cache")
			require.NoError(t, os.WriteFile(o.LegacyCache, []byte(data), 0600))
			_, err := OpenOffline(o)
			require.Error(t, err)
		})
	}
}

func TestRecoveryCorruption(t *testing.T) {
	o := options(t)
	s, err := OpenOffline(o)
	require.NoError(t, err)
	require.NoError(t, s.Close())
	require.NoError(t, os.Remove(filepath.Join(o.Dir, "state.json")))
	raw, err := readBounded(filepath.Join(o.Dir, "recovery.json"))
	require.NoError(t, err)
	var r recovery
	require.NoError(t, strict(raw, &r))
	r.Hosts = []byte("changed")
	// Use the production atomic writer without changing its digest metadata.
	writer := Store{dir: o.Dir}
	require.NoError(t, writer.write("recovery.json", r))
	_, err = OpenOffline(o)
	require.ErrorContains(t, err, "digest mismatch")
}

func TestDefensiveCopiesAndConcurrentAccess(t *testing.T) {
	o := options(t)
	s, err := OpenOffline(o)
	require.NoError(t, err)
	defer s.Close()
	h, err := s.LoadHosts()
	require.NoError(t, err)
	h.Hosts[0].Policy.Trust = "changed"
	snap, err := s.Load()
	require.NoError(t, err)
	snap.Hosts[0].Sessions[0].Tabs[0].Name = "changed"
	again, err := s.Load()
	require.NoError(t, err)
	require.Equal(t, "shell", again.Hosts[0].Sessions[0].Tabs[0].Name)
	hosts, err := s.LoadHosts()
	require.NoError(t, err)
	require.Equal(t, "known-hosts", hosts.Hosts[0].Policy.Trust)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 30; i++ {
			_, _ = s.Load()
			_, _ = s.LoadHosts()
		}
	}()
	for i := 0; i < 30; i++ {
		hosts, err = s.LoadHosts()
		require.NoError(t, err)
		require.NoError(t, s.ReplaceHosts(hosts.Revision, hosts.Hosts))
	}
	<-done
}
