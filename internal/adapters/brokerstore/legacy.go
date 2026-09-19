package brokerstore

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"syscall"
	"unicode/utf8"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
)

func readBounded(path string) ([]byte, error) {
	fd, err := syscall.Open(path, syscall.O_RDONLY|syscall.O_CLOEXEC|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
	if err != nil {
		return nil, err
	}
	f := os.NewFile(uintptr(fd), path)
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if !st.Mode().IsRegular() || st.Size() > MaxFileBytes {
		return nil, errors.New("brokerstore: invalid file type or size")
	}
	b, err := io.ReadAll(io.LimitReader(f, MaxFileBytes+1))
	if len(b) > MaxFileBytes {
		return nil, errors.New("brokerstore: oversized input")
	}
	return b, err
}

// strict rejects duplicate keys as well as unknown fields, trailing JSON,
// invalid UTF-8 and excessive nesting. Byte bounds apply before decoding.
func strict(raw []byte, dst any) error {
	if len(raw) > MaxFileBytes || !utf8.Valid(raw) {
		return errors.New("brokerstore: invalid JSON size or encoding")
	}
	scan := json.NewDecoder(bytes.NewReader(raw))
	if err := scanValue(scan, 0); err != nil {
		return err
	}
	if _, err := scan.Token(); err != io.EOF {
		return errors.New("brokerstore: trailing JSON")
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	return dec.Decode(dst)
}
func scanValue(d *json.Decoder, depth int) error {
	if depth > 64 {
		return errors.New("brokerstore: JSON nesting limit")
	}
	t, err := d.Token()
	if err != nil {
		return err
	}
	delim, ok := t.(json.Delim)
	if !ok {
		return nil
	}
	switch delim {
	case '{':
		seen := map[string]bool{}
		for d.More() {
			key, err := d.Token()
			if err != nil {
				return err
			}
			k, ok := key.(string)
			if !ok || seen[k] {
				return errors.New("brokerstore: duplicate JSON key")
			}
			seen[k] = true
			if err := scanValue(d, depth+1); err != nil {
				return err
			}
		}
	case '[':
		for d.More() {
			if err := scanValue(d, depth+1); err != nil {
				return err
			}
		}
	default:
		return errors.New("brokerstore: unexpected delimiter")
	}
	_, err = d.Token()
	return err
}
func source(path string) ([]byte, error) {
	if path == "" {
		return nil, nil
	}
	return readBounded(path)
}
func migrate(o Options) (recovery, error) {
	var r recovery
	var err error
	if r.Hosts, err = source(o.LegacyHosts); err != nil {
		return r, err
	}
	if r.Cache, err = source(o.LegacyCache); err != nil {
		return r, err
	}
	r.State.Manifest = manifest{Version: 1, Sources: []manifestSource{
		{Label: manifestMembership, Present: o.LegacyHosts != "", SHA256: digest(r.Hosts)},
		{Label: manifestObservations, Present: o.LegacyCache != "", SHA256: digest(r.Cache)},
	}}
	r.State.Hosts = ports.BrokerHosts{Revision: 1}
	if o.LegacyHosts != "" {
		r.State.Hosts.Hosts, err = decodeHosts(r.Hosts, o.Policies)
		if err != nil {
			return r, err
		}
	}
	if o.LegacyCache != "" {
		entries, err := decodeCache(r.Cache)
		if err != nil {
			return r, err
		}
		for _, e := range entries {
			for _, h := range r.State.Hosts.Hosts {
				// Unbound v2/v3 cache data is validated and backed up, never promoted
				// to authority. v4 can seed only its exact surviving incarnation.
				if e.Host == h.Registration.Endpoint && e.Incarnation == h.Registration.Incarnation {
					// A legacy cache entry carries only observed sessions: daemon
					// identity, incarnation, protocol version, and capabilities stay
					// zero (never invented), and policy stays absent because the
					// loader re-stamps it from membership.
					r.State.Snapshot.Daemons = append(r.State.Snapshot.Daemons, ports.BrokerDaemonObservation{
						Endpoint:       e.Host,
						DisplayOrigin:  domain.RemoteDisplayOrigin(e.Host),
						Registration:   h.Registration,
						Availability:   domain.RemoteAvailabilityUnknown,
						InventoryKnown: true,
						LastSuccess:    e.FetchedAt,
						Sessions:       e.Sessions,
					})
				}
			}
		}
	}
	if len(r.State.Snapshot.Daemons) > 0 {
		var epoch [8]byte
		if _, err = rand.Read(epoch[:]); err != nil {
			return r, err
		}
		var n uint64
		for _, b := range epoch {
			n = n<<8 | uint64(b)
		}
		if n == 0 {
			n = 1
		}
		r.State.Snapshot.Epoch = ports.BrokerEpoch(n)
		r.State.Snapshot.Revision = 1
	}
	return r, validate(r.State)
}

type legacyRecord struct {
	Endpoint    string `json:"endpoint"`
	Incarnation string `json:"incarnation"`
	Generation  uint64 `json:"generation"`
}

func decodeHosts(raw []byte, policies map[string]ports.BrokerPolicy) ([]ports.BrokerHostRecord, error) {
	var header struct {
		Version int `json:"version"`
	}
	if err := json.Unmarshal(raw, &header); err != nil {
		return nil, err
	}
	var pinned, learned []legacyRecord
	switch header.Version {
	case 1:
		var f struct {
			Version int      `json:"version"`
			Hosts   []string `json:"hosts"`
		}
		if err := strict(raw, &f); err != nil {
			return nil, err
		}
		if f.Hosts == nil {
			return nil, errors.New("brokerstore: missing hosts")
		}
		for _, e := range f.Hosts {
			learned = append(learned, legacyRecord{Endpoint: e})
		}
	case 2:
		var f struct {
			Version int      `json:"version"`
			Pinned  []string `json:"pinned"`
			Learned []string `json:"learned"`
		}
		if err := strict(raw, &f); err != nil {
			return nil, err
		}
		if f.Pinned == nil || f.Learned == nil {
			return nil, errors.New("brokerstore: missing memberships")
		}
		for _, e := range f.Pinned {
			pinned = append(pinned, legacyRecord{Endpoint: e})
		}
		for _, e := range f.Learned {
			learned = append(learned, legacyRecord{Endpoint: e})
		}
	case 3:
		var f struct {
			Version int            `json:"version"`
			Pinned  []legacyRecord `json:"pinned"`
			Learned []legacyRecord `json:"learned"`
		}
		if err := strict(raw, &f); err != nil {
			return nil, err
		}
		if f.Pinned == nil || f.Learned == nil {
			return nil, errors.New("brokerstore: missing memberships")
		}
		pinned, learned = f.Pinned, f.Learned
	default:
		return nil, fmt.Errorf("brokerstore: unsupported hosts version %d", header.Version)
	}
	if len(pinned) > ports.BrokerMaxHosts || len(learned) > ports.BrokerMaxHosts {
		return nil, errors.New("brokerstore: too many hosts")
	}
	var out []ports.BrokerHostRecord
	indices := map[string]int{}
	for group, records := range [][]legacyRecord{pinned, learned} {
		seen := map[string]bool{}
		for _, record := range records {
			if seen[record.Endpoint] {
				return nil, errors.New("brokerstore: duplicate membership")
			}
			seen[record.Endpoint] = true
			reg := domain.RemoteRegistration{Endpoint: record.Endpoint, Generation: domain.RemoteGeneration(record.Generation)}
			if header.Version == 3 {
				b, err := hex.DecodeString(record.Incarnation)
				if err != nil || len(b) != 16 {
					return nil, errors.New("brokerstore: invalid incarnation")
				}
				copy(reg.Incarnation[:], b)
			}
			if index, ok := indices[record.Endpoint]; ok {
				if header.Version == 3 && !reg.Equal(out[index].Registration) {
					return nil, errors.New("brokerstore: conflicting registration")
				}
				out[index].Learned = true
				continue
			}
			if header.Version != 3 {
				if _, err := rand.Read(reg.Incarnation[:]); err != nil {
					return nil, err
				}
				reg.Generation = 1
			}
			if err := reg.Validate(); err != nil {
				return nil, err
			}
			policy, ok := policies[record.Endpoint]
			if !ok {
				return nil, fmt.Errorf("brokerstore: explicit policy required for %q", record.Endpoint)
			}
			if err := policy.Validate(); err != nil {
				return nil, err
			}
			indices[record.Endpoint] = len(out)
			out = append(out, ports.BrokerHostRecord{Registration: reg, Pinned: group == 0, Learned: group == 1, Policy: policy})
		}
	}
	return out, nil
}
