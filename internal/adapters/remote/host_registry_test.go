package remote

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/bnema/vev/internal/domain"
)

func TestHostStorePath(t *testing.T) {
	stateDir := filepath.Join(t.TempDir(), "state")
	want := filepath.Join(stateDir, "hosts.json")
	if got := HostStorePath(stateDir); got != want {
		t.Fatalf("HostStorePath(%q) = %q, want %q", stateDir, got, want)
	}
}

func TestNewRemoteRegistrationSamplesUniqueIncarnations(t *testing.T) {
	first, err := newRemoteRegistration("user@arch")
	if err != nil {
		t.Fatalf("newRemoteRegistration: %v", err)
	}
	second, err := newRemoteRegistration("user@arch")
	if err != nil {
		t.Fatalf("newRemoteRegistration: %v", err)
	}
	if first.Incarnation == second.Incarnation {
		t.Fatal("incarnations must be unique per registration")
	}
	if _, err := newRemoteRegistration("not a host"); err == nil {
		t.Fatal("invalid endpoint must be rejected")
	}
}

func TestHostStoreCrossProcessHelper(t *testing.T) {
	if os.Getenv("VEV_HOST_STORE_HELPER") != "1" {
		t.Skip("helper process")
	}

	path := os.Getenv("VEV_HOST_STORE_PATH")
	hosts := strings.Split(os.Getenv("VEV_HOST_STORE_HOSTS"), ",")
	store := NewFileHostStore(path)
	for _, host := range hosts {
		if host == "" {
			continue
		}
		if err := store.Remember(host); err != nil {
			t.Fatalf("Remember(%q) error = %v", host, err)
		}
	}
}

func TestHostStoreCrossProcessConcurrentRemember(t *testing.T) {
	path := filepath.Join(t.TempDir(), "vev", "hosts.json")
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatalf("mkdir: %v", err)
	}

	left := make([]string, 0, 40)
	right := make([]string, 0, 40)
	for i := 0; i < 40; i++ {
		left = append(left, "left-"+strconv.Itoa(i))
		right = append(right, "right-"+strconv.Itoa(i))
	}

	startHelper := func(hosts []string) *exec.Cmd {
		cmd := exec.Command(os.Args[0], "-test.run=^TestHostStoreCrossProcessHelper$")
		cmd.Env = append(os.Environ(),
			"VEV_HOST_STORE_HELPER=1",
			"VEV_HOST_STORE_PATH="+path,
			"VEV_HOST_STORE_HOSTS="+strings.Join(hosts, ","),
		)
		cmd.Stderr = os.Stderr
		return cmd
	}

	leftCmd := startHelper(left)
	rightCmd := startHelper(right)
	if err := leftCmd.Start(); err != nil {
		t.Fatalf("start left helper: %v", err)
	}
	if err := rightCmd.Start(); err != nil {
		t.Fatalf("start right helper: %v", err)
	}
	if err := leftCmd.Wait(); err != nil {
		t.Fatalf("left helper: %v", err)
	}
	if err := rightCmd.Wait(); err != nil {
		t.Fatalf("right helper: %v", err)
	}

	_, learned, err := NewFileHostStore(path).Hosts()
	if err != nil {
		t.Fatalf("Hosts() error = %v", err)
	}
	want := append(append([]string{}, left...), right...)
	sort.Strings(want)
	if !slices.Equal(registrationEndpoints(learned), want) {
		t.Fatalf("learned len=%d want %d; missing merges under cross-process writers", len(learned), len(want))
	}
}

func registrationEndpoints(registrations []domain.RemoteRegistration) []string {
	endpoints := make([]string, 0, len(registrations))
	for _, record := range registrations {
		endpoints = append(endpoints, record.Endpoint)
	}
	return endpoints
}

func requireValidRegistrations(t *testing.T, registrations []domain.RemoteRegistration) {
	t.Helper()
	for _, record := range registrations {
		if err := record.Validate(); err != nil {
			t.Fatalf("registration %+v invalid: %v", record, err)
		}
	}
}

func TestHostStore(t *testing.T) {
	t.Parallel()

	storePath := func(t *testing.T) string {
		t.Helper()
		dir := filepath.Join(t.TempDir(), "vev")
		return filepath.Join(dir, "hosts.json")
	}

	writeHosts := func(t *testing.T, path string, raw []byte) {
		t.Helper()
		if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
			t.Fatalf("mkdir: %v", err)
		}
		if err := os.WriteFile(path, raw, 0o600); err != nil {
			t.Fatalf("write hosts: %v", err)
		}
	}

	t.Run("missing file returns empty lists", func(t *testing.T) {
		t.Parallel()
		path := storePath(t)
		store := NewFileHostStore(path)

		pinned, learned, err := store.Hosts()
		if err != nil {
			t.Fatalf("Hosts() error = %v", err)
		}
		if len(pinned) != 0 || len(learned) != 0 {
			t.Fatalf("Hosts() = pinned %v learned %v, want empty", pinned, learned)
		}
	})

	t.Run("pinned order preserved and learned sorted with private perms", func(t *testing.T) {
		t.Parallel()
		path := storePath(t)
		store := NewFileHostStore(path)

		if err := store.AddPinned("zebra"); err != nil {
			t.Fatalf("AddPinned(zebra) error = %v", err)
		}
		if err := store.AddPinned("arch"); err != nil {
			t.Fatalf("AddPinned(arch) error = %v", err)
		}
		if err := store.Remember("mule"); err != nil {
			t.Fatalf("Remember(mule) error = %v", err)
		}
		if err := store.Remember("beta"); err != nil {
			t.Fatalf("Remember(beta) error = %v", err)
		}

		pinned, learned, err := store.Hosts()
		if err != nil {
			t.Fatalf("Hosts() error = %v", err)
		}
		wantPinned := []string{"zebra", "arch"}
		wantLearned := []string{"beta", "mule"}
		if !slices.Equal(registrationEndpoints(pinned), wantPinned) {
			t.Fatalf("pinned = %v, want %v", pinned, wantPinned)
		}
		if !slices.Equal(registrationEndpoints(learned), wantLearned) {
			t.Fatalf("learned = %v, want %v", learned, wantLearned)
		}
		requireValidRegistrations(t, pinned)
		requireValidRegistrations(t, learned)

		info, err := os.Stat(path)
		if err != nil {
			t.Fatalf("stat store: %v", err)
		}
		if perm := info.Mode().Perm(); perm != 0o600 {
			t.Fatalf("store permissions = %04o, want 0600", perm)
		}
		dirInfo, err := os.Lstat(filepath.Dir(path))
		if err != nil {
			t.Fatalf("stat parent: %v", err)
		}
		if perm := dirInfo.Mode().Perm(); perm != 0o700 {
			t.Fatalf("parent permissions = %04o, want 0700", perm)
		}

		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read store: %v", err)
		}
		var decoded hostsFile
		if err := json.Unmarshal(raw, &decoded); err != nil {
			t.Fatalf("decode store: %v", err)
		}
		if decoded.Version != 3 {
			t.Fatalf("version = %d, want 3", decoded.Version)
		}
		stored := make([]domain.RemoteRegistration, 0, len(decoded.Pinned)+len(decoded.Learned))
		for _, record := range decoded.Pinned {
			registration, err := decodeRecord(record)
			if err != nil {
				t.Fatalf("decode stored pinned: %v", err)
			}
			stored = append(stored, registration)
		}
		for _, record := range decoded.Learned {
			registration, err := decodeRecord(record)
			if err != nil {
				t.Fatalf("decode stored learned: %v", err)
			}
			stored = append(stored, registration)
		}
		if !slices.Equal(registrationEndpoints(stored[:len(decoded.Pinned)]), wantPinned) {
			t.Fatalf("stored pinned = %v, want %v", decoded.Pinned, wantPinned)
		}
		if !slices.Equal(registrationEndpoints(stored[len(decoded.Pinned):]), wantLearned) {
			t.Fatalf("stored learned = %v, want %v", decoded.Learned, wantLearned)
		}
		requireValidRegistrations(t, stored)
	})

	t.Run("overlap allowed across pinned and learned", func(t *testing.T) {
		t.Parallel()
		path := storePath(t)
		store := NewFileHostStore(path)

		if err := store.AddPinned("arch"); err != nil {
			t.Fatalf("AddPinned(arch) error = %v", err)
		}
		if err := store.Remember("arch"); err != nil {
			t.Fatalf("Remember(arch) error = %v", err)
		}
		pinned, learned, err := store.Hosts()
		if err != nil {
			t.Fatalf("Hosts() error = %v", err)
		}
		if !slices.Equal(registrationEndpoints(pinned), []string{"arch"}) {
			t.Fatalf("pinned = %v, want [arch]", pinned)
		}
		if !slices.Equal(registrationEndpoints(learned), []string{"arch"}) {
			t.Fatalf("learned = %v, want [arch]", learned)
		}
		// Overlap shares one registration: pinning a learned host and
		// remembering a pinned host retain the incarnation.
		if !pinned[0].Equal(learned[0]) {
			t.Fatalf("overlapping registrations differ: pinned %+v learned %+v", pinned[0], learned[0])
		}
	})

	t.Run("idempotent mutations and remove from both sources", func(t *testing.T) {
		t.Parallel()
		path := storePath(t)
		store := NewFileHostStore(path)

		if err := store.AddPinned("arch"); err != nil {
			t.Fatalf("AddPinned(arch) error = %v", err)
		}
		if err := store.AddPinned("arch"); err != nil {
			t.Fatalf("AddPinned(arch) again error = %v", err)
		}
		if err := store.Remember("mule"); err != nil {
			t.Fatalf("Remember(mule) error = %v", err)
		}
		if err := store.Remember("mule"); err != nil {
			t.Fatalf("Remember(mule) again error = %v", err)
		}
		if err := store.Remember("arch"); err != nil {
			t.Fatalf("Remember(arch) error = %v", err)
		}

		if err := store.RemovePinned("missing"); err != nil {
			t.Fatalf("RemovePinned(missing) error = %v", err)
		}
		if err := store.Forget("missing"); err != nil {
			t.Fatalf("Forget(missing) error = %v", err)
		}
		if deleted, err := store.Remove("missing"); err != nil {
			t.Fatalf("Remove(missing) error = %v", err)
		} else if deleted {
			t.Fatal("Remove(missing) deleted = true, want false")
		}

		if deleted, err := store.Remove("arch"); err != nil {
			t.Fatalf("Remove(arch) error = %v", err)
		} else if !deleted {
			t.Fatal("Remove(arch) deleted = false, want true")
		}
		pinned, learned, err := store.Hosts()
		if err != nil {
			t.Fatalf("Hosts() error = %v", err)
		}
		if len(pinned) != 0 {
			t.Fatalf("pinned after Remove(arch) = %v, want empty", pinned)
		}
		if !slices.Equal(registrationEndpoints(learned), []string{"mule"}) {
			t.Fatalf("learned after Remove(arch) = %v, want [mule]", learned)
		}

		if err := store.Forget("mule"); err != nil {
			t.Fatalf("Forget(mule) error = %v", err)
		}
		if err := store.Forget("mule"); err != nil {
			t.Fatalf("Forget(mule) again error = %v", err)
		}
		_, learned, err = store.Hosts()
		if err != nil {
			t.Fatalf("Hosts() after forget error = %v", err)
		}
		if len(learned) != 0 {
			t.Fatalf("learned = %v, want empty", learned)
		}
	})

	t.Run("malformed file is preserved and fails closed", func(t *testing.T) {
		t.Parallel()
		path := storePath(t)
		malformed := []byte(`{"version":2,"pinned":`)
		writeHosts(t, path, malformed)

		store := NewFileHostStore(path)
		if _, _, err := store.Hosts(); err == nil {
			t.Fatal("Hosts() error = nil, want malformed error")
		}
		if err := store.AddPinned("arch"); err == nil {
			t.Fatal("AddPinned() error = nil, want malformed error")
		}
		if err := store.Remember("arch"); err == nil {
			t.Fatal("Remember() error = nil, want malformed error")
		}
		if _, err := store.Remove("arch"); err == nil {
			t.Fatal("Remove() error = nil, want malformed error")
		}

		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read preserved file: %v", err)
		}
		if string(raw) != string(malformed) {
			t.Fatalf("malformed file changed: got %q, want %q", raw, malformed)
		}
	})

	t.Run("legacy version 1 migrates on read with backup", func(t *testing.T) {
		t.Parallel()
		cases := []struct {
			name        string
			raw         string
			wantLearned []string
		}{
			{name: "empty", raw: `{"version":1,"hosts":[]}`, wantLearned: []string{}},
			{name: "hosts", raw: `{"version":1,"hosts":["mule","arch"]}`, wantLearned: []string{"arch", "mule"}},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				t.Parallel()
				path := storePath(t)
				writeHosts(t, path, []byte(tc.raw))
				store := NewFileHostStore(path)

				pinned, learned, err := store.Hosts()
				if err != nil {
					t.Fatalf("Hosts() error = %v", err)
				}
				if len(pinned) != 0 || !slices.Equal(registrationEndpoints(learned), tc.wantLearned) {
					t.Fatalf("Hosts() = pinned %v learned %v, want pinned empty learned %v", pinned, learned, tc.wantLearned)
				}
				requireValidRegistrations(t, learned)
				// The read migrated: the file is now version 3 and the
				// pre-migration bytes are preserved privately.
				raw, err := os.ReadFile(path)
				if err != nil {
					t.Fatalf("read migrated store: %v", err)
				}
				var decoded hostsFile
				if err := json.Unmarshal(raw, &decoded); err != nil {
					t.Fatalf("decode migrated store: %v", err)
				}
				if decoded.Version != hostsFileVersion {
					t.Fatalf("migrated version = %d, want %d", decoded.Version, hostsFileVersion)
				}
				backup, err := os.ReadFile(path + hostsBackupSuffix)
				if err != nil {
					t.Fatalf("read migration backup: %v", err)
				}
				if string(backup) != tc.raw {
					t.Fatalf("backup = %q, want pre-migration %q", backup, tc.raw)
				}
				info, err := os.Stat(path + hostsBackupSuffix)
				if err != nil {
					t.Fatalf("stat backup: %v", err)
				}
				if perm := info.Mode().Perm(); perm != 0o600 {
					t.Fatalf("backup permissions = %04o, want 0600", perm)
				}
				// A repeated load retains the migrated identity.
				againPinned, againLearned, err := store.Hosts()
				if err != nil {
					t.Fatalf("Hosts() again error = %v", err)
				}
				if len(againPinned) != 0 || len(againLearned) != len(learned) {
					t.Fatalf("Hosts() again = pinned %v learned %v", againPinned, againLearned)
				}
				for i := range learned {
					if !learned[i].Equal(againLearned[i]) {
						t.Fatalf("identity not retained: %+v vs %+v", learned[i], againLearned[i])
					}
				}
			})
		}
	})

	t.Run("legacy version 2 migrates on read preserving order", func(t *testing.T) {
		t.Parallel()
		path := storePath(t)
		legacy := `{"version":2,"pinned":["zebra","arch"],"learned":["mule","beta"]}`
		writeHosts(t, path, []byte(legacy))
		store := NewFileHostStore(path)

		pinned, learned, err := store.Hosts()
		if err != nil {
			t.Fatalf("Hosts() error = %v", err)
		}
		if !slices.Equal(registrationEndpoints(pinned), []string{"zebra", "arch"}) {
			t.Fatalf("pinned = %v, want [zebra arch]", pinned)
		}
		if !slices.Equal(registrationEndpoints(learned), []string{"beta", "mule"}) {
			t.Fatalf("learned = %v, want [beta mule]", learned)
		}
		requireValidRegistrations(t, pinned)
		requireValidRegistrations(t, learned)

		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read migrated store: %v", err)
		}
		var decoded hostsFile
		if err := json.Unmarshal(raw, &decoded); err != nil {
			t.Fatalf("decode migrated store: %v", err)
		}
		if decoded.Version != hostsFileVersion {
			t.Fatalf("migrated version = %d, want %d", decoded.Version, hostsFileVersion)
		}
		backup, err := os.ReadFile(path + hostsBackupSuffix)
		if err != nil {
			t.Fatalf("read migration backup: %v", err)
		}
		if string(backup) != legacy {
			t.Fatalf("backup = %q, want pre-migration %q", backup, legacy)
		}
	})

	t.Run("legacy version 2 overlapping membership shares one registration", func(t *testing.T) {
		t.Parallel()
		path := storePath(t)
		legacy := `{"version":2,"pinned":["arch"],"learned":["arch","mule"]}`
		writeHosts(t, path, []byte(legacy))
		store := NewFileHostStore(path)

		pinned, learned, err := store.Hosts()
		if err != nil {
			t.Fatalf("Hosts() error = %v", err)
		}
		requireValidRegistrations(t, pinned)
		requireValidRegistrations(t, learned)
		if len(pinned) != 1 || len(learned) != 2 {
			t.Fatalf("pinned = %v, learned = %v, want 1 pinned and 2 learned", pinned, learned)
		}
		if pinned[0] != learned[0] {
			t.Fatalf("overlapping endpoint has two incarnations: pinned %+v vs learned %+v", pinned[0], learned[0])
		}
		// Removing only the pin must not surface a second identity
		// for an endpoint that was never fully removed.
		if err := store.RemovePinned("arch"); err != nil {
			t.Fatalf("RemovePinned(arch) error = %v", err)
		}
		_, againLearned, err := store.Hosts()
		if err != nil {
			t.Fatalf("Hosts() error = %v", err)
		}
		if len(againLearned) != 2 || againLearned[0] != pinned[0] {
			t.Fatalf("identity not retained after unpin: %+v vs %+v", againLearned, pinned)
		}
	})

	t.Run("conflicting version 3 overlap fails closed", func(t *testing.T) {
		t.Parallel()
		path := storePath(t)
		conflict := `{"version":3,"pinned":[{"endpoint":"arch","incarnation":"01000000000000000000000000000000","generation":1}],"learned":[{"endpoint":"arch","incarnation":"02000000000000000000000000000000","generation":1}]}`
		writeHosts(t, path, []byte(conflict))
		store := NewFileHostStore(path)

		if _, _, err := store.Hosts(); err == nil {
			t.Fatal("Hosts() error = nil, want conflicting identity rejection")
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read store: %v", err)
		}
		if string(raw) != conflict {
			t.Fatalf("rejected store changed: got %q, want %q", raw, conflict)
		}
	})

	t.Run("remove and re-add creates a new incarnation", func(t *testing.T) {
		t.Parallel()
		path := storePath(t)
		store := NewFileHostStore(path)
		if err := store.Remember("arch"); err != nil {
			t.Fatalf("Remember(arch) error = %v", err)
		}
		_, learned, err := store.Hosts()
		if err != nil {
			t.Fatalf("Hosts() error = %v", err)
		}
		first := learned[0]

		if deleted, err := store.Remove("arch"); err != nil || !deleted {
			t.Fatalf("Remove(arch) = %t, %v", deleted, err)
		}
		if err := store.Remember("arch"); err != nil {
			t.Fatalf("Remember(arch) again error = %v", err)
		}
		_, relearned, err := store.Hosts()
		if err != nil {
			t.Fatalf("Hosts() again error = %v", err)
		}
		if len(relearned) != 1 {
			t.Fatalf("Hosts() again = %v, want one registration", relearned)
		}
		if relearned[0].Equal(first) {
			t.Fatalf("re-added registration %+v must not equal removed %+v", relearned[0], first)
		}
		if relearned[0].Incarnation == first.Incarnation {
			t.Fatalf("re-added incarnation reused after full removal")
		}
	})

	t.Run("schema failures fail closed and preserve file", func(t *testing.T) {
		t.Parallel()
		cases := []struct {
			name      string
			raw       string
			wantError string
		}{
			{name: "unsupported version", raw: `{"version":99,"pinned":[],"learned":[]}`, wantError: "unsupported hosts file version 99"},
			{name: "null pinned", raw: `{"version":2,"pinned":null,"learned":[]}`, wantError: "malformed hosts file"},
			{name: "null learned", raw: `{"version":2,"pinned":[],"learned":null}`, wantError: "malformed hosts file"},
			{name: "duplicate pinned", raw: `{"version":2,"pinned":["arch","arch"],"learned":[]}`, wantError: "malformed hosts file"},
			{name: "duplicate learned", raw: `{"version":2,"pinned":[],"learned":["mule","mule"]}`, wantError: "malformed hosts file"},
			{name: "invalid target", raw: `{"version":2,"pinned":[" arch"],"learned":[]}`, wantError: "malformed hosts file"},
			{name: "invalid utf8", raw: "{\"version\":2,\"pinned\":[\"\xff\"],\"learned\":[]}", wantError: "malformed hosts file"},
			{name: "legacy missing hosts", raw: `{"version":1}`, wantError: "malformed hosts file: missing hosts"},
			{name: "legacy null hosts", raw: `{"version":1,"hosts":null}`, wantError: "malformed hosts file: missing hosts"},
			{name: "legacy duplicate hosts", raw: `{"version":1,"hosts":["arch","arch"]}`, wantError: "malformed hosts file: duplicate learned host"},
			{name: "legacy invalid target", raw: `{"version":1,"hosts":[" arch"]}`, wantError: "malformed hosts file"},
			{name: "legacy invalid utf8", raw: "{\"version\":1,\"hosts\":[\"\xff\"]}", wantError: "malformed hosts file"},
			{name: "v3 string pinned is malformed", raw: `{"version":3,"pinned":["arch"],"learned":[]}`, wantError: "malformed hosts file"},
			{name: "v3 missing incarnation", raw: `{"version":3,"pinned":[{"endpoint":"arch","generation":1}],"learned":[]}`, wantError: "malformed hosts file"},
			{name: "v3 malformed incarnation", raw: `{"version":3,"pinned":[{"endpoint":"arch","incarnation":"zzzz","generation":1}],"learned":[]}`, wantError: "malformed hosts file"},
			{name: "v3 short incarnation", raw: `{"version":3,"pinned":[{"endpoint":"arch","incarnation":"0123","generation":1}],"learned":[]}`, wantError: "malformed hosts file"},
			{name: "v3 zero incarnation", raw: `{"version":3,"pinned":[{"endpoint":"arch","incarnation":"00000000000000000000000000000000","generation":1}],"learned":[]}`, wantError: "malformed hosts file"},
			{name: "v3 zero generation", raw: `{"version":3,"pinned":[{"endpoint":"arch","incarnation":"01000000000000000000000000000000","generation":0}],"learned":[]}`, wantError: "malformed hosts file"},
			{name: "v3 duplicate pinned", raw: `{"version":3,"pinned":[{"endpoint":"arch","incarnation":"01000000000000000000000000000000","generation":1},{"endpoint":"arch","incarnation":"02000000000000000000000000000000","generation":1}],"learned":[]}`, wantError: "duplicate pinned host"},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				t.Parallel()
				path := storePath(t)
				writeHosts(t, path, []byte(tc.raw))

				store := NewFileHostStore(path)
				if _, _, err := store.Hosts(); err == nil {
					t.Fatal("Hosts() error = nil, want schema error")
				} else if !strings.Contains(err.Error(), tc.wantError) {
					t.Fatalf("Hosts() error = %q, want substring %q", err, tc.wantError)
				}
				if err := store.Remember("arch"); err == nil {
					t.Fatal("Remember() error = nil, want schema error")
				}
				raw, err := os.ReadFile(path)
				if err != nil {
					t.Fatalf("read preserved file: %v", err)
				}
				if string(raw) != tc.raw {
					t.Fatalf("file changed: got %q, want %q", raw, tc.raw)
				}
			})
		}
	})

	t.Run("reads v3 without rewriting until mutation", func(t *testing.T) {
		t.Parallel()
		path := storePath(t)
		seeded := `{"version":3,"pinned":[{"endpoint":"zebra","incarnation":"01000000000000000000000000000000","generation":1},{"endpoint":"arch","incarnation":"02000000000000000000000000000000","generation":1}],"learned":[{"endpoint":"mule","incarnation":"03000000000000000000000000000000","generation":1},{"endpoint":"beta","incarnation":"04000000000000000000000000000000","generation":1}]}` + "\n"
		writeHosts(t, path, []byte(seeded))

		store := NewFileHostStore(path)
		pinned, learned, err := store.Hosts()
		if err != nil {
			t.Fatalf("Hosts() error = %v", err)
		}
		if !slices.Equal(registrationEndpoints(pinned), []string{"zebra", "arch"}) {
			t.Fatalf("pinned = %v, want [zebra arch]", pinned)
		}
		if !slices.Equal(registrationEndpoints(learned), []string{"beta", "mule"}) {
			t.Fatalf("learned = %v, want [beta mule]", learned)
		}
		// Identity survives the read; incarnations are stable.
		beforePinned, beforeLearned := pinned, learned
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read store: %v", err)
		}
		if string(raw) != seeded {
			t.Fatalf("read-only Hosts rewrote file: got %q", raw)
		}

		if err := store.Remember("gamma"); err != nil {
			t.Fatalf("Remember(gamma) error = %v", err)
		}
		raw, err = os.ReadFile(path)
		if err != nil {
			t.Fatalf("read after mutation: %v", err)
		}
		var decoded hostsFile
		if err := json.Unmarshal(raw, &decoded); err != nil {
			t.Fatalf("decode: %v", err)
		}
		afterPinned, err := validatePinnedRecords(decoded.Pinned)
		if err != nil {
			t.Fatalf("decode pinned: %v", err)
		}
		afterLearned, err := validateLearnedRecords(decoded.Learned)
		if err != nil {
			t.Fatalf("decode learned: %v", err)
		}
		if !slices.Equal(registrationEndpoints(afterPinned), []string{"zebra", "arch"}) {
			t.Fatalf("pinned after mutation = %v", decoded.Pinned)
		}
		if !slices.Equal(registrationEndpoints(sortedRegistrations(afterLearned)), []string{"beta", "gamma", "mule"}) {
			t.Fatalf("learned after mutation = %v", decoded.Learned)
		}
		// Unrelated registrations keep their incarnations across mutation.
		for i := range beforePinned {
			if !beforePinned[i].Equal(afterPinned[i]) {
				t.Fatalf("pinned identity changed: %+v vs %+v", beforePinned[i], afterPinned[i])
			}
		}
		for endpoint, before := range map[string]domain.RemoteRegistration{
			"mule": beforeLearned[1], "beta": beforeLearned[0],
		} {
			after, ok := afterLearned[endpoint]
			if !ok || !before.Equal(after) {
				t.Fatalf("learned identity changed for %q: %+v vs %+v", endpoint, before, after)
			}
		}
	})

	t.Run("rejects invalid targets", func(t *testing.T) {
		t.Parallel()
		path := storePath(t)
		store := NewFileHostStore(path)

		for _, target := range []string{"", " arch", "arch ", "ar ch", "arch\n", string([]byte{0xff})} {
			if err := store.AddPinned(target); err == nil {
				t.Fatalf("AddPinned(%q) error = nil, want validation error", target)
			}
			if err := store.Remember(target); err == nil {
				t.Fatalf("Remember(%q) error = nil, want validation error", target)
			}
			if _, err := store.Remove(target); err == nil {
				t.Fatalf("Remove(%q) error = nil, want validation error", target)
			}
		}
	})

	t.Run("rejects symlink parent", func(t *testing.T) {
		t.Parallel()
		root := t.TempDir()
		realDir := filepath.Join(root, "real")
		if err := os.MkdirAll(realDir, 0o700); err != nil {
			t.Fatalf("mkdir real: %v", err)
		}
		linkDir := filepath.Join(root, "link")
		if err := os.Symlink(realDir, linkDir); err != nil {
			t.Fatalf("symlink parent: %v", err)
		}
		path := filepath.Join(linkDir, "hosts.json")
		store := NewFileHostStore(path)

		if err := store.Remember("arch"); err == nil {
			t.Fatal("Remember() error = nil, want symlink parent rejection")
		}
	})

	t.Run("rejects non-private parent permissions", func(t *testing.T) {
		t.Parallel()
		path := storePath(t)
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatalf("mkdir: %v", err)
		}
		store := NewFileHostStore(path)
		if err := store.Remember("arch"); err == nil {
			t.Fatal("Remember() error = nil, want permission rejection")
		}
	})

	t.Run("atomic replacement leaves no temp files", func(t *testing.T) {
		t.Parallel()
		path := storePath(t)
		store := NewFileHostStore(path)
		if err := store.AddPinned("arch"); err != nil {
			t.Fatalf("AddPinned() error = %v", err)
		}
		if err := store.Remember("mule"); err != nil {
			t.Fatalf("Remember() error = %v", err)
		}

		entries, err := os.ReadDir(filepath.Dir(path))
		if err != nil {
			t.Fatalf("readdir: %v", err)
		}
		base := filepath.Base(path)
		for _, entry := range entries {
			name := entry.Name()
			if name == base || strings.HasSuffix(name, ".lock") {
				continue
			}
			if strings.Contains(name, ".tmp") {
				t.Fatalf("leftover temp file %q", name)
			}
			t.Fatalf("unexpected leftover entry %q", name)
		}
	})

	t.Run("hardens existing file and lock permissions to 0600", func(t *testing.T) {
		t.Parallel()
		path := storePath(t)
		dir := filepath.Dir(path)
		if err := os.MkdirAll(dir, 0o700); err != nil {
			t.Fatalf("mkdir: %v", err)
		}
		if err := os.WriteFile(path, []byte(`{"version":2,"pinned":[],"learned":[]}`), 0o644); err != nil {
			t.Fatalf("seed hosts: %v", err)
		}
		lockPath := path + ".lock"
		if err := os.WriteFile(lockPath, nil, 0o644); err != nil {
			t.Fatalf("seed lock: %v", err)
		}
		info, err := os.Stat(lockPath)
		if err != nil {
			t.Fatalf("stat seeded lock: %v", err)
		}
		if perm := info.Mode().Perm(); perm != 0o644 {
			t.Fatalf("seeded lock permissions = %04o, want 0644", perm)
		}

		store := NewFileHostStore(path)
		if err := store.Remember("arch"); err != nil {
			t.Fatalf("Remember() error = %v", err)
		}

		info, err = os.Stat(path)
		if err != nil {
			t.Fatalf("stat hosts: %v", err)
		}
		if perm := info.Mode().Perm(); perm != 0o600 {
			t.Fatalf("hosts permissions = %04o, want 0600", perm)
		}
		info, err = os.Stat(lockPath)
		if err != nil {
			t.Fatalf("stat lock: %v", err)
		}
		if perm := info.Mode().Perm(); perm != 0o600 {
			t.Fatalf("lock permissions = %04o, want 0600", perm)
		}
	})

	t.Run("failed mutation rolls back without partial write", func(t *testing.T) {
		t.Parallel()
		path := storePath(t)
		store := NewFileHostStore(path)
		if err := store.AddPinned("arch"); err != nil {
			t.Fatalf("AddPinned() error = %v", err)
		}
		if err := store.Remember("mule"); err != nil {
			t.Fatalf("Remember() error = %v", err)
		}
		before, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read before: %v", err)
		}

		// Replace with malformed content while holding no store lock; next mutation must fail closed.
		malformed := []byte(`{"version":2,"pinned":`)
		if err := os.WriteFile(path, malformed, 0o600); err != nil {
			t.Fatalf("inject malformed: %v", err)
		}
		if err := store.AddPinned("zebra"); err == nil {
			t.Fatal("AddPinned() error = nil, want malformed error")
		}
		after, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read after failed mutation: %v", err)
		}
		if string(after) != string(malformed) {
			t.Fatalf("failed mutation changed file: got %q want %q (previous good was %q)", after, malformed, before)
		}
	})

	t.Run("concurrent independent stores lose no hosts", func(t *testing.T) {
		t.Parallel()
		path := storePath(t)

		left := make([]string, 0, 40)
		right := make([]string, 0, 40)
		for i := 0; i < 40; i++ {
			left = append(left, "left-"+strconv.Itoa(i))
			right = append(right, "right-"+strconv.Itoa(i))
		}

		start := make(chan struct{})
		var wg sync.WaitGroup
		errs := make(chan error, 2)

		wg.Add(2)
		go func() {
			defer wg.Done()
			store := NewFileHostStore(path)
			<-start
			for _, host := range left {
				if err := store.Remember(host); err != nil {
					errs <- err
					return
				}
			}
		}()
		go func() {
			defer wg.Done()
			store := NewFileHostStore(path)
			<-start
			for _, host := range right {
				if err := store.AddPinned(host); err != nil {
					errs <- err
					return
				}
			}
		}()

		close(start)
		wg.Wait()
		close(errs)
		for err := range errs {
			t.Fatalf("concurrent mutation error = %v", err)
		}

		pinned, learned, err := NewFileHostStore(path).Hosts()
		if err != nil {
			t.Fatalf("Hosts() error = %v", err)
		}
		wantLearned := append([]string{}, left...)
		sort.Strings(wantLearned)
		if !slices.Equal(registrationEndpoints(learned), wantLearned) {
			t.Fatalf("learned len=%d want %d; missing merges under dual writers", len(learned), len(wantLearned))
		}
		if len(pinned) != len(right) {
			t.Fatalf("pinned len=%d want %d", len(pinned), len(right))
		}
		pinnedSet := make(map[string]struct{}, len(pinned))
		for _, record := range pinned {
			pinnedSet[record.Endpoint] = struct{}{}
		}
		for _, host := range right {
			if _, ok := pinnedSet[host]; !ok {
				t.Fatalf("pinned missing %q", host)
			}
		}
	})
}
