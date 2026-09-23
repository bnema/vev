package brokerstore

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRecoveryCorruption(t *testing.T) {
	o := options(t)
	s, err := Open(o)
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
	_, err = Open(o)
	require.ErrorContains(t, err, "digest mismatch")
}

func TestDefensiveCopiesAndConcurrentAccess(t *testing.T) {
	o := options(t)
	s, err := Open(o)
	require.NoError(t, err)
	defer s.Close()
	h, err := s.LoadHosts()
	require.NoError(t, err)
	h.Hosts[0].Policy.Trust = "changed"
	snap, err := s.Load()
	require.NoError(t, err)
	snap.Revision = 999
	again, err := s.Load()
	require.NoError(t, err)
	require.NotEqual(t, snap.Revision, again.Revision)
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
